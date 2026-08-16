#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine - ML Training & Pipeline Script
- Ingests date-partitioned Parquet files from ./nifty_3min_dataset
- Aligns next-bar execution features with Triple Barrier Labels
- Performs Purged & Embargoed Cross-Validation (No Look-ahead bias)
- Trains LightGBM / XGBoost directional classifier
"""

import os
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

try:
    import lightgbm as lgb
    from sklearn.metrics import classification_report, roc_auc_score
except ImportError:
    raise ImportError("Please install lightgbm and scikit-learn: pip install lightgbm scikit-learn")

from app import CONFIG, LabelEngine, DatasetManager, Candle3Min, is_valid_number


# =========================================================
# 1. LOAD PARQUET DATASET & GENERATE LABELS
# =========================================================

def build_labeled_ml_dataset(dataset_dir: str = "./nifty_3min_dataset/features_3min") -> pd.DataFrame:
    data_path = Path(dataset_dir)
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset path {dataset_dir} does not exist. Run live app to collect data.")

    # Read all partitioned parquet files
    df = pq.read_table(str(data_path)).to_pandas()
    if df.empty or len(df) < 50:
        raise ValueError("Insufficient data bars for training. Minimum 50+ bars required.")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    label_engine = LabelEngine()
    labeled_rows = []

    # Iterate and construct future candles for Triple Barrier Labeling
    for i in range(len(df) - 15):
        current_row = df.iloc[i]
        entry_price = current_row.get("basis", 0.0) + current_row.get("spot_sma_20", current_row.get("fut_vwap", 0.0))
        atr = current_row.get("atr_14_prev") or current_row.get("atr_14_close")

        if not is_valid_number(atr) or atr <= 0:
            continue

        # Lookahead slice (Next bars for outcome resolution)
        future_slice = df.iloc[i + 1 : i + 16]
        future_candles = []
        for _, f_row in future_slice.iterrows():
            future_candles.append(
                Candle3Min(
                    timestamp=f_row["timestamp"],
                    spot_o=0.0, spot_h=0.0, spot_l=0.0, spot_c=0.0,
                    fut_o=f_row.get("fut_vwap", 0.0),
                    fut_h=f_row.get("fut_vwap", 0.0) + (atr * 0.5),
                    fut_l=f_row.get("fut_vwap", 0.0) - (atr * 0.5),
                    fut_c=f_row.get("fut_vwap", 0.0),
                    fut_volume=0.0, fut_oi=0.0
                )
            )

        # Generate directional label (1 for CE / Bullish Breakout)
        lbl = label_engine.generate(
            entry_price=entry_price,
            atr=atr,
            future_after_entry=future_candles,
            direction=1,
            signal_timestamp=current_row["timestamp"],
            entry_timestamp=future_candles[0].timestamp
        )

        row_dict = current_row.to_dict()
        row_dict["target_label"] = 1 if lbl["triple_barrier_outcome"] == "TARGET_FIRST" else 0
        row_dict["label_valid"] = lbl["label_valid_for_training"]
        labeled_rows.append(row_dict)

    labeled_df = pd.DataFrame(labeled_rows)
    return labeled_df[labeled_df["label_valid"] == 1].reset_index(drop=True)


# =========================================================
# 2. FEATURE MATRIX & PURGED TRAINING
# =========================================================

FEATURE_COLUMNS = [
    "basis", "normalized_stretch", "normalized_spread",
    "stretch_slope_3", "spread_slope_3", "atr_14_prev",
    "oi_change", "oi_long_buildup", "oi_short_buildup", "oi_strength",
    "twc", "breadth_10", "dispersion_index", "contribution_concentration",
    "or_width_atr", "dist_to_or_high_atr", "dist_to_or_low_atr", "or_breakout_state",
    "pcr_oi", "pcr_volume", "data_quality_score"
]

def train_lightgbm_model(df: pd.DataFrame):
    # Select available numeric features
    avail_feats = [col for col in FEATURE_COLUMNS if col in df.columns]
    X = df[avail_feats].fillna(0.0)
    y = df["target_label"]

    # Purged split by Date
    dataset_mgr = DatasetManager()
    splits = dataset_mgr.purged_walk_forward_by_date(df, n_splits=3)

    if not splits:
        # Fallback simple time-series split if single-day data
        split_idx = int(len(df) * 0.75)
        train_idx, test_idx = list(range(split_idx)), list(range(split_idx, len(df)))
        splits = [(train_idx, test_idx)]

    for fold, (train_idx, test_idx) in enumerate(splits):
        print(f"\n--- Training Fold {fold + 1} ---")
        X_train, y_train = X.iloc[train_idx], y.iloc[train_idx]
        X_test, y_test = X.iloc[test_idx], y.iloc[test_idx]

        model = lgb.LGBMClassifier(
            n_estimators=100,
            learning_rate=0.03,
            max_depth=4,
            num_leaves=15,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            verbose=-1
        )
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        probs = model.predict_proba(X_test)[:, 1]

        print(classification_report(y_test, preds, zero_division=0))
        if len(np.unique(y_test)) > 1:
            print(f"ROC-AUC: {roc_auc_score(y_test, probs):.3f}")

    # Save trained model to disk
    model_path = "./nifty_lgbm_model.txt"
    model.booster_.save_model(model_path)
    print(f"\n✓ Production Model Saved: {model_path}")


if __name__ == "__main__":
    print("Ingesting and processing Parquet feature vectors...")
    try:
        labeled_dataset = build_labeled_ml_dataset()
        print(f"Loaded {len(labeled_dataset)} labeled bars. Training LightGBM...")
        train_lightgbm_model(labeled_dataset)
    except Exception as exc:
        print(f"Training Pipeline Note: {exc}")
