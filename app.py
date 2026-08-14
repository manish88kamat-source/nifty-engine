#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine
Kotak Neo Integrated Research-Lock v2.0
Official Kotak Neo API v2 connectivity + dynamic discovery
Research concepts intentionally preserved.
"""

from __future__ import annotations

import os
import json
import time
import hmac
import hashlib
import struct
import base64
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import streamlit as st
except ImportError:
    st = None

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None


# =========================================================
# PURE PYTHON TOTP
# =========================================================

def generate_live_totp(secret_or_otp: str) -> str:
    raw = str(secret_or_otp or "").strip().replace(" ", "").upper()
    if raw.isdigit() and len(raw) == 6:
        return raw
    try:
        if len(raw) % 8:
            raw += "=" * (8 - len(raw) % 8)
        key = base64.b32decode(raw, casefold=True)
        counter = int(time.time() // 30)
        msg = struct.pack(">Q", counter)
        digest = hmac.new(key, msg, hashlib.sha1).digest()
        offset = digest[19] & 15
        token = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000
        return f"{token:06d}"
    except Exception:
        return raw


# =========================================================
# CONFIGURATION - RESEARCH LOCK FROZEN
# =========================================================

CONFIG = {
    "app_version": "v2.0_kotak_neo",
    "feature_version": "v2.0_research_lock",
    "label_version": "TB_v1.6_lock",
    "schema_version": "2.0",
    "weight_version": "NIFTY_STATIC_2025Q1",
    "atr_period": 14,
    "sma_period": 20,
    "triple_upper_atr": 1.0,
    "triple_lower_atr": 0.75,
    "time_barrier_min": 30,
    "mfe_horizons_min": [15, 30, 45],
    "max_label_horizon_min": 45,
    "purge_bars": 18,
    "embargo_bars": 5,
    "opening_range_minutes": 15,
    "atr_mode": "session_local",
    "execution_model": "next_bar_open",
    "session_start": "09:15",
    "session_end": "15:30",
    "bar_minutes": 3,
    "dataset_path": "./nifty_3min_dataset",
    "neo_environment": "prod",
    "nifty_index_name": "Nifty 50",
    "nifty_future_token": os.getenv("NIFTY_FUT_TOKEN", "").strip(),
    "pcr_strike_count": int(os.getenv("PCR_STRIKE_COUNT", "5")),
    "pcr_strike_step": float(os.getenv("PCR_STRIKE_STEP", "50")),
}

HEAVYWEIGHTS = {
    "HDFCBANK": 0.115,
    "RELIANCE": 0.098,
    "ICICIBANK": 0.080,
    "INFY": 0.058,
    "ITC": 0.042,
    "TCS": 0.040,
    "LT": 0.038,
    "AXISBANK": 0.033,
    "KOTAKBANK": 0.029,
    "SBIN": 0.028,
}


# =========================================================
# HELPERS
# =========================================================

def safe_float(value, default=np.nan):
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if not value:
                return default
        return float(value)
    except Exception:
        return default


def is_valid_number(value):
    try:
        return value is not None and np.isfinite(float(value))
    except Exception:
        return False


def env_or_secret(name):
    value = os.getenv(name, "")
    if value:
        return value
    if st is not None:
        try:
            value = st.secrets.get(name, "")
            if value:
                return str(value)
        except Exception:
            pass
    return ""


def floor_bar_timestamp(ts: datetime, minutes=3):
    anchor = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    if ts < anchor:
        return None
    elapsed = int((ts - anchor).total_seconds() // 60)
    return anchor + timedelta(minutes=(elapsed // minutes) * minutes)


def wilder_atr(trs: List[float], period=14):
    if len(trs) < period:
        return np.nan
    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return float(atr)


def parse_expiry(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        x = float(value)
        if x > 10_000_000_000:
            return datetime.fromtimestamp(x / 1000)
        if x > 1_000_000_000:
            return datetime.fromtimestamp(x)
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    for fmt in [
        "%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",
        "%d%b%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d"
    ]:
        try:
            return datetime.strptime(text.upper(), fmt)
        except Exception:
            pass
    return None


def expiry_from_record(record):
    for key in [
        "pExpiryDate", "lExpiryDate", "pMaturityDate", "pLastTradingDate",
        "expiryDate", "expiry", "expiry_date"
    ]:
        dt = parse_expiry(record.get(key))
        if dt is not None:
            return dt
    return None


def option_type_from_record(record):
    val = str(
        record.get("pOptionType")
        or record.get("optType")
        or record.get("option_type")
        or ""
    ).upper().strip()
    if "CE" in val or "CALL" in val:
        return "CE"
    if "PE" in val or "PUT" in val:
        return "PE"
    symbol = str(record.get("pTrdSymbol", record.get("ts", ""))).upper()
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    return ""


def strike_from_record(record):
    for key in [
        "dStrikePrice", "dStrikePrice;", "strike_price", "strikePrice",
        "dStrike", "strike", "pStrikePrice"
    ]:
        value = safe_float(record.get(key))
        if is_valid_number(value) and value > 0:
            if value > 1_000_000:
                value /= 100.0
            return value
    return np.nan


def token_from_record(record):
    for key in [
        "pSymbol", "pSymbolToken", "instrument_token", "instrumentToken",
        "tok", "token", "pToken"
    ]:
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def record_list(response):
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    for key in ["data", "result", "records", "data_list", "scrips", "list"]:
        value = response.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for k in ["data", "records", "result"]:
                if isinstance(value.get(k), list):
                    return value[k]
    return []


# =========================================================
# CANDLE STRUCTURE
# =========================================================

@dataclass
class Candle3Min:
    timestamp: datetime
    spot_o: float
    spot_h: float
    spot_l: float
    spot_c: float
    fut_o: float
    fut_h: float
    fut_l: float
    fut_c: float
    fut_volume: float
    fut_oi: float
    heavy: Dict[str, Dict[str, float]] = field(default_factory=dict)
    option_chain: Dict[str, Any] = field(default_factory=dict)


# =========================================================
# OPENING RANGE
# =========================================================

class OpeningRangeEngine:
    def __init__(self, minutes=15):
        self.minutes = minutes
        self.or_high = None
        self.or_low = None
        self.or_set = False

    def reset(self):
        self.or_high = self.or_low = None
        self.or_set = False

    def update(self, candle):
        mins = (candle.timestamp.hour * 60 + candle.timestamp.minute) - 555
        if mins < self.minutes:
            self.or_high = candle.fut_h if self.or_high is None else max(self.or_high, candle.fut_h)
            self.or_low = candle.fut_l if self.or_low is None else min(self.or_low, candle.fut_l)
        else:
            self.or_set = True

    def features(self, candle, atr):
        names = [
            "or_high", "or_low", "or_width_atr",
            "dist_to_or_high_atr", "dist_to_or_low_atr", "or_breakout_state"
        ]
        if not self.or_set or self.or_high is None or not is_valid_number(atr) or atr <= 0:
            return {k: np.nan for k in names}
        return {
            "or_high": self.or_high,
            "or_low": self.or_low,
            "or_width_atr": (self.or_high - self.or_low) / atr,
            "dist_to_or_high_atr": (candle.fut_c - self.or_high) / atr,
            "dist_to_or_low_atr": (candle.fut_c - self.or_low) / atr,
            "or_breakout_state": 1 if candle.fut_c > self.or_high else (-1 if candle.fut_c < self.or_low else 0),
        }


# =========================================================
# SESSION CONTEXT
# =========================================================

class SessionContextEngine:
    def __init__(self):
        self.prev_close = None
        self.prev_high = None
        self.prev_low = None
        self.today_open = None

    def set_previous_day(self, close, high, low):
        self.prev_close, self.prev_high, self.prev_low = close, high, low

    def set_today_open(self, open_price):
        self.today_open = open_price

    def reset(self):
        self.today_open = None

    def features(self, candle, atr):
        names = ["gap_points", "gap_atr", "gap_direction", "dist_to_pdh_atr", "dist_to_pdl_atr"]
        if self.prev_close is None or not is_valid_number(atr) or atr <= 0:
            return {k: np.nan for k in names}
        gap = (self.today_open if self.today_open is not None else candle.fut_o) - self.prev_close
        return {
            "gap_points": gap,
            "gap_atr": gap / atr,
            "gap_direction": 1 if gap > 0 else (-1 if gap < 0 else 0),
            "dist_to_pdh_atr": (candle.fut_c - self.prev_high) / atr if self.prev_high is not None else np.nan,
            "dist_to_pdl_atr": (candle.fut_c - self.prev_low) / atr if self.prev_low is not None else np.nan,
        }


# =========================================================
# OPTION CHAIN
# =========================================================

class OptionChainEngine:
    def compute(self, chain):
        keys = [
            "pcr_oi", "pcr_volume", "ce_oi_change", "pe_oi_change",
            "atm_iv", "iv_change", "ce_oi_atm", "pe_oi_atm", "atm_strike"
        ]
        if not chain:
            out = {}
            for key in keys:
                out[key] = np.nan
                out[f"{key}_missing"] = 1
            out["ce_contracts_seen"] = out["pe_contracts_seen"] = 0
            return out
        out = {}
        for key in keys:
            value = chain.get(key, np.nan)
            out[key] = value
            out[f"{key}_missing"] = int(not is_valid_number(value))
        out["ce_contracts_seen"] = int(chain.get("ce_contracts_seen", 0))
        out["pe_contracts_seen"] = int(chain.get("pe_contracts_seen", 0))
        return out


# =========================================================
# HEAVYWEIGHTS
# =========================================================

class HeavyweightEngine:
    def __init__(self, weights):
        self.base_weights = weights
        self.day_open = {}

    def set_day_open(self, symbol, price):
        if is_valid_number(price) and price > 0:
            self.day_open[symbol] = price

    def reset_day(self):
        self.day_open.clear()

    def compute(self, candle):
        contributions, returns = [], []
        bullish = 0
        for symbol, weight in self.base_weights.items():
            data = candle.heavy.get(symbol)
            if not data:
                continue
            open_price = self.day_open.get(symbol)
            if open_price is None:
                open_price = safe_float(data.get("o", data.get("c")))
                if is_valid_number(open_price) and open_price > 0:
                    self.day_open[symbol] = open_price
            close_price = safe_float(data.get("c"))
            if not is_valid_number(open_price) or open_price <= 0 or not is_valid_number(close_price):
                continue
            ret = (close_price - open_price) / open_price
            contributions.append(weight * ret)
            returns.append(ret)
            vwap = safe_float(data.get("vwap"), close_price)
            if close_price >= vwap:
                bullish += 1
        total = sum(contributions) if contributions else 0.0
        n = max(len(contributions), 1)
        return {
            "twc": total,
            "breadth_10": bullish / n,
            "dispersion_index": float(np.std(returns)) if returns else 0.0,
            "contribution_concentration": max(contributions, key=abs) / (abs(total) + 1e-9) if contributions else 0.0,
            "hw_bullish_count": bullish,
            "hw_symbols_seen": len(contributions),
        }


# =========================================================
# FEATURE ENGINE - RESEARCH LOCK
# =========================================================

class FeatureEngine:
    def __init__(self):
        self.vwap_pv = 0.0
        self.vwap_vol = 0.0
        self.tr_history = []
        self.history = []
        self.hw = HeavyweightEngine(HEAVYWEIGHTS)
        self.or_eng = OpeningRangeEngine(CONFIG["opening_range_minutes"])
        self.sess = SessionContextEngine()
        self.opt = OptionChainEngine()

    def reset_session(self):
        self.vwap_pv = self.vwap_vol = 0.0
        self.tr_history.clear()
        self.history.clear()
        self.hw.reset_day()
        self.or_eng.reset()
        self.sess.reset()

    def set_previous_day(self, close, high, low):
        self.sess.set_previous_day(close, high, low)

    def set_today_open(self, open_price):
        self.sess.set_today_open(open_price)

    def compute(self, candle, prev):
        typical = (candle.fut_h + candle.fut_l + candle.fut_c) / 3.0
        volume = safe_float(candle.fut_volume, 0.0)
        self.vwap_pv += typical * max(volume, 0.0)
        self.vwap_vol += max(volume, 0.0)
        fut_vwap = self.vwap_pv / self.vwap_vol if self.vwap_vol > 0 else typical

        if prev:
            pc = prev[-1].fut_c
            tr = max(candle.fut_h - candle.fut_l, abs(candle.fut_h - pc), abs(candle.fut_l - pc))
        else:
            tr = candle.fut_h - candle.fut_l

        atr_prev = wilder_atr(self.tr_history, CONFIG["atr_period"])
        self.tr_history.append(tr)
        atr_close = wilder_atr(self.tr_history, CONFIG["atr_period"])
        atr = atr_prev

        closes = [c.spot_c for c in prev[-(CONFIG["sma_period"] - 1):]]
        closes.append(candle.spot_c)
        sma_ready = len(closes) >= CONFIG["sma_period"]
        spot_sma = float(np.mean(closes)) if sma_ready else np.nan

        if is_valid_number(atr) and atr > 0:
            normalized_stretch = (candle.fut_c - fut_vwap) / atr
            normalized_spread = (spot_sma - fut_vwap) / atr if is_valid_number(spot_sma) else np.nan
        else:
            normalized_stretch = normalized_spread = np.nan

        last = self.history[-1] if self.history else {}
        stretch_slope = (
            normalized_stretch - last["normalized_stretch"]
            if is_valid_number(normalized_stretch) and is_valid_number(last.get("normalized_stretch"))
            else 0.0
        )
        spread_slope = (
            normalized_spread - last["normalized_spread"]
            if is_valid_number(normalized_spread) and is_valid_number(last.get("normalized_spread"))
            else 0.0
        )

        if prev:
            oi_change = candle.fut_oi - prev[-1].fut_oi
            price_up = candle.fut_c > prev[-1].fut_c
            price_down = candle.fut_c < prev[-1].fut_c
        else:
            oi_change = 0.0
            price_up = price_down = False

        oi_long_buildup = int(price_up and oi_change > 0)
        oi_short_buildup = int(price_down and oi_change > 0)
        oi_short_covering = int(price_up and oi_change < 0)
        oi_long_unwinding = int(price_down and oi_change < 0)
        oi_neutral = int(oi_change == 0 or (not price_up and not price_down))
        oi_strength = ((1 if price_up else -1) * np.sign(oi_change) * np.log1p(abs(oi_change))) if oi_change != 0 else 0.0

        self.or_eng.update(candle)

        missing_spot = int(not is_valid_number(candle.spot_c))
        missing_future = int(not is_valid_number(candle.fut_c))
        missing_oi = int(not is_valid_number(candle.fut_oi))
        missing_volume = int(not is_valid_number(candle.fut_volume) or candle.fut_volume <= 0)
        missing_heavyweight = int(len(candle.heavy) == 0)
        missing_option = int(len(candle.option_chain) == 0)
        bad_ohlc = int(candle.fut_h < candle.fut_l or candle.spot_h < candle.spot_l)
        zero_volume = int(candle.fut_volume == 0)
        zero_oi = int(candle.fut_oi == 0)
        penalties = sum([
            missing_spot, missing_future, missing_oi, missing_volume,
            missing_heavyweight, missing_option, bad_ohlc, zero_volume, zero_oi
        ])

        pcr_features = self.opt.compute(candle.option_chain)
        features = {
            "timestamp": candle.timestamp,
            "feature_version": CONFIG["feature_version"],
            "schema_version": CONFIG["schema_version"],
            "weight_version": CONFIG["weight_version"],
            "atr_mode": CONFIG["atr_mode"],
            "execution_model": CONFIG["execution_model"],
            "basis": candle.fut_c - candle.spot_c,
            "fut_vwap": fut_vwap,
            "normalized_stretch": normalized_stretch,
            "normalized_spread": normalized_spread,
            "stretch_slope_3": stretch_slope,
            "spread_slope_3": spread_slope,
            "atr_14_prev": atr_prev,
            "atr_14_close": atr_close,
            "atr_warmup_flag": int(not is_valid_number(atr)),
            "spot_sma_20": spot_sma,
            "sma20_warmup_flag": 1 - int(sma_ready),
            "oi_change": oi_change,
            "oi_long_buildup": oi_long_buildup,
            "oi_short_buildup": oi_short_buildup,
            "oi_short_covering": oi_short_covering,
            "oi_long_unwinding": oi_long_unwinding,
            "oi_neutral": oi_neutral,
            "oi_strength": oi_strength,
            "minutes_from_open": (candle.timestamp.hour * 60 + candle.timestamp.minute) - 555,
            "day_of_week": candle.timestamp.weekday(),
            **self.hw.compute(candle),
            **self.or_eng.features(candle, atr if is_valid_number(atr) else 0.0),
            **self.sess.features(candle, atr if is_valid_number(atr) else 0.0),
            **pcr_features,
            "missing_spot": missing_spot,
            "missing_future": missing_future,
            "missing_oi": missing_oi,
            "missing_volume": missing_volume,
            "missing_heavyweight": missing_heavyweight,
            "missing_option_chain": missing_option,
            "bad_ohlc": bad_ohlc,
            "zero_volume": zero_volume,
            "zero_oi": zero_oi,
            "data_quality_score": max(0.0, 1.0 - 0.1 * penalties),
            "bar_complete": 1,
        }
        self.history.append(features)
        return features


# =========================================================
# LABEL ENGINE - TRIPLE BARRIER LOCKED
# =========================================================

class LabelEngine:
    def __init__(self):
        self.upper = CONFIG["triple_upper_atr"]
        self.lower = CONFIG["triple_lower_atr"]
        self.tb_horizon = CONFIG["time_barrier_min"]
        self.mfe_horizons = CONFIG["mfe_horizons_min"]
        self.execution_model = CONFIG["execution_model"]
        if self.execution_model != "next_bar_open":
            raise ValueError("Research-Lock requires next_bar_open")

    def _excursion(self, entry, future, direction, max_bars):
        mfe = mae = 0.0
        available = min(len(future), max_bars)
        for candle in future[:available]:
            if direction == 1:
                mfe = max(mfe, candle.fut_h - entry)
                mae = max(mae, entry - candle.fut_l)
            else:
                mfe = max(mfe, entry - candle.fut_l)
                mae = max(mae, candle.fut_h - entry)
        return mfe, mae, int(available >= max_bars)

    def generate(self, entry_price, atr, future_after_entry, direction=1,
                 signal_timestamp=None, entry_timestamp=None):
        if entry_timestamp and future_after_entry and future_after_entry[0].timestamp <= entry_timestamp:
            raise ValueError("FUTURE ALIGNMENT VIOLATION")

        atr = atr if is_valid_number(atr) and atr > 0 else np.nan
        upper = entry_price + direction * self.upper * atr if not np.isnan(atr) else np.nan
        lower = entry_price - direction * self.lower * atr if not np.isnan(atr) else np.nan

        outcome, bars, mfe_tb, mae_tb, time_to_mfe = "TIMEOUT", 0, 0.0, 0.0, 0
        max_tb_bars = self.tb_horizon // CONFIG["bar_minutes"]

        for i, candle in enumerate(future_after_entry[:max_tb_bars]):
            bars = i + 1
            if direction == 1:
                mfe_tb = max(mfe_tb, candle.fut_h - entry_price)
                mae_tb = max(mae_tb, entry_price - candle.fut_l)
                hit_target = not np.isnan(upper) and candle.fut_h >= upper
                hit_stop = not np.isnan(lower) and candle.fut_l <= lower
            else:
                mfe_tb = max(mfe_tb, entry_price - candle.fut_l)
                mae_tb = max(mae_tb, candle.fut_h - entry_price)
                hit_target = not np.isnan(upper) and candle.fut_l <= upper
                hit_stop = not np.isnan(lower) and candle.fut_h >= lower

            if mfe_tb > 0 and time_to_mfe == 0:
                time_to_mfe = bars
            if hit_target and hit_stop:
                outcome = "AMBIGUOUS"
                break
            if hit_target:
                outcome = "TARGET_FIRST"
                break
            if hit_stop:
                outcome = "STOP_FIRST"
                break

        r_multiple = self.upper / self.lower if outcome == "TARGET_FIRST" else (-1.0 if outcome == "STOP_FIRST" else np.nan)
        valid = int(outcome != "AMBIGUOUS" and not np.isnan(atr))
        mfe_atr = mfe_tb / atr if not np.isnan(atr) else np.nan
        mae_atr = mae_tb / atr if not np.isnan(atr) else np.nan
        velocity = mfe_atr / max(bars, 1) if is_valid_number(mfe_atr) else np.nan

        if is_valid_number(mfe_atr):
            if mfe_atr >= 1.2 and mae_atr <= 0.45 and velocity > 0.25:
                trajectory = "IMPULSE"
            elif mfe_atr >= 0.8 and mae_atr <= 0.70 and 0.08 < velocity <= 0.25:
                trajectory = "STAIRCASE"
            elif mfe_atr >= 0.5 and velocity <= 0.08:
                trajectory = "GRIND"
            else:
                trajectory = "FAILURE"
        else:
            trajectory = "UNKNOWN"

        labels = {
            "label_version": CONFIG["label_version"],
            "execution_model": self.execution_model,
            "signal_timestamp": signal_timestamp,
            "entry_timestamp": entry_timestamp,
            "entry_price": entry_price,
            "triple_barrier_outcome": outcome,
            "label_valid_for_training": valid,
            "r_multiple": r_multiple,
            "trajectory": trajectory,
            "real_breakout": int(outcome == "TARGET_FIRST" and is_valid_number(mfe_atr) and mfe_atr >= 1.0 and mae_atr <= 0.55),
            "mfe_atr_tb": mfe_atr,
            "mae_atr_tb": mae_atr,
            "time_to_mfe": time_to_mfe,
            "bars_to_outcome": bars,
            "velocity": velocity,
        }
        for horizon in self.mfe_horizons:
            mfe_h, mae_h, complete = self._excursion(entry_price, future_after_entry, direction, horizon // CONFIG["bar_minutes"])
            labels[f"mfe_atr_{horizon}m"] = mfe_h / atr if not np.isnan(atr) else np.nan
            labels[f"mae_atr_{horizon}m"] = mae_h / atr if not np.isnan(atr) else np.nan
            labels[f"horizon_{horizon}m_complete"] = complete
        return labels


# =========================================================
# DATASET
# =========================================================

class DatasetManager:
    def __init__(self, path=None):
        self.base = Path(path or CONFIG["dataset_path"])
        self.base.mkdir(parents=True, exist_ok=True)

    def write_parquet(self, df, name="features"):
        if df.empty:
            return
        data = df.copy()
        if "timestamp" in data.columns:
            data["date"] = pd.to_datetime(data["timestamp"]).dt.date.astype(str)
        table = pa.Table.from_pandas(data, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(self.base / name),
            partition_cols=["date"] if "date" in data.columns else None,
            existing_data_behavior="overwrite_or_ignore",
        )

    def purged_walk_forward_by_date(self, df, n_splits=5):
        if "timestamp" not in df.columns:
            raise ValueError("timestamp required")
        data = df.copy()
        data["date"] = pd.to_datetime(data["timestamp"]).dt.date
        dates = sorted(data["date"].unique())
        fold = max(1, len(dates) // (n_splits + 1))
        splits = []
        for i in range(n_splits):
            train_end = (i + 1) * fold
            test_start = train_end + 1
            test_end = min(test_start + fold, len(dates))
            if test_start >= len(dates):
                break
            train_idx = data[data["date"].isin(dates[:train_end])].index.tolist()
            test_idx = data[data["date"].isin(dates[test_start:test_end])].index.tolist()
            if len(train_idx) > CONFIG["embargo_bars"]:
                train_idx = train_idx[:-CONFIG["embargo_bars"]]
            if train_idx and test_idx:
                splits.append((train_idx, test_idx))
        return splits


# =========================================================
# KOTAK NEO ADAPTER - OFFICIAL SDK V2
# =========================================================

class KotakNeoAdapter:
    def __init__(self):
        self.consumer_key = env_or_secret("KOTAK_CONSUMER_KEY")
        self.mobile = env_or_secret("KOTAK_MOBILE")
        self.ucc = env_or_secret("KOTAK_UCC")
        self.totp = env_or_secret("KOTAK_TOTP")
        self.mpin = env_or_secret("KOTAK_MPIN")

        self.client = None
        self.connected = False
        self.lock = threading.Lock()
        self.latest = {}
        self.tick_buffer = []
        self.future_token = ""
        self.future_symbol = ""
        self.future_expiry = None
        self.pcr_tokens = []
        self.pcr_records = {}
        self.heavy_tokens = {}
        self.discovery_log = []
        self.last_error = ""

    def login(self, live_totp_override=""):
        if NeoAPI is None:
            raise RuntimeError(
                "neo_api_client missing. Install the official Kotak Neo API v2 dependency from requirements.txt."
            )

        totp = (live_totp_override or "").strip() or self.totp
        required = {
            "KOTAK_CONSUMER_KEY": self.consumer_key,
            "KOTAK_MOBILE": self.mobile,
            "KOTAK_UCC": self.ucc,
            "TOTP": totp,
            "KOTAK_MPIN": self.mpin,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError("Missing credentials: " + ", ".join(missing))

        self.client = NeoAPI(
            environment=CONFIG["neo_environment"],
            access_token=None,
            neo_fin_key=None,
            consumer_key=self.consumer_key,
        )
        self.client.on_message = self.on_message
        self.client.on_error = self.on_error
        self.client.on_close = self.on_close
        self.client.on_open = self.on_open

        step1 = self.client.totp_login(
            mobile_number=self.mobile,
            ucc=self.ucc,
            totp=generate_live_totp(totp),
        )
        if isinstance(step1, dict) and step1.get("error"):
            raise RuntimeError(str(step1))

        step2 = self.client.totp_validate(mpin=self.mpin)
        if isinstance(step2, dict) and step2.get("error"):
            raise RuntimeError(str(step2))

        self.connected = True
        self.discovery_log.append("Kotak Neo API v2 authentication successful.")
        return True

    def on_open(self, message):
        print("[Kotak Neo] WebSocket opened:", message)

    def on_error(self, message):
        self.last_error = str(message)
        print("[Kotak Neo] WebSocket ERROR:", message)

    def on_close(self, message):
        self.connected = False
        print("[Kotak Neo] WebSocket closed:", message)

    def decode_message(self, message):
        if isinstance(message, (dict, list)):
            return message
        if isinstance(message, str):
            try:
                return json.loads(message)
            except Exception:
                return None
        return None

    def tick_time(self, data):
        raw = data.get("ltt") or data.get("ftdm") or data.get("tvalue") or data.get("ftm0") or data.get("timestamp")
        if raw is None:
            return datetime.now()
        try:
            if isinstance(raw, (int, float)):
                x = float(raw)
                if x > 10_000_000_000:
                    return datetime.fromtimestamp(x / 1000)
                if x > 1_000_000_000:
                    return datetime.fromtimestamp(x)
            text = str(raw).strip()
            for fmt in [
                "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S",
                "%d/%m/%Y %H:%M", "%H:%M:%S"
            ]:
                try:
                    dt = datetime.strptime(text, fmt)
                    if fmt == "%H:%M:%S":
                        now = datetime.now()
                        dt = dt.replace(year=now.year, month=now.month, day=now.day)
                    return dt
                except ValueError:
                    pass
        except Exception:
            pass
        return datetime.now()

    def process_tick(self, data):
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self.process_tick(item)
            return
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            data = data["data"]
        if not isinstance(data, dict):
            return

        token = str(data.get("tk", data.get("tok", data.get("instrument_token", data.get("token", "")))))
        exchange = str(data.get("e", data.get("exchange_segment", "")))
        symbol = str(data.get("ts", data.get("pTrdSymbol", data.get("tradingSymbol", data.get("symbol", "")))))
        name = str(data.get("name", "")).lower()

        is_index = name == "index" or symbol.lower() == CONFIG["nifty_index_name"].lower()
        ltp = safe_float(data.get("iv" if is_index else "ltp", data.get("ltp", data.get("lastPrice"))))
        if not is_valid_number(ltp):
            return

        item = {
            "timestamp": self.tick_time(data),
            "token": token,
            "exchange": exchange,
            "symbol": symbol,
            "name": name,
            "ltp": ltp,
            "volume": safe_float(data.get("v", data.get("volume")), 0.0),
            "oi": safe_float(data.get("oi"), 0.0),
            "open": safe_float(data.get("op", data.get("openingPrice"))),
            "high": safe_float(data.get("h", data.get("highPrice"))),
            "low": safe_float(data.get("lo", data.get("lowPrice"))),
            "vwap": safe_float(data.get("ap", data.get("vwap"))),
            "strike": strike_from_record(data),
            "option_type": option_type_from_record(data),
            "iv": safe_float(data.get("iv")),
            "raw": data,
        }

        with self.lock:
            self.latest[token] = item
            if is_index:
                self.latest["NIFTY_INDEX"] = item
            self.tick_buffer.append(item)

    def on_message(self, message):
        try:
            data = self.decode_message(message)
            if data is not None:
                self.process_tick(data)
        except Exception as exc:
            self.last_error = repr(exc)
            print("[Kotak Neo] Tick parse error:", repr(exc))

    def search_scrip(self, exchange_segment, symbol="", expiry=None, option_type=None, strike_price=None):
        if not self.connected or self.client is None:
            raise RuntimeError("Login first.")
        response = self.client.search_scrip(
            exchange_segment=exchange_segment,
            symbol=symbol,
            expiry=expiry,
            option_type=option_type,
            strike_price=strike_price,
        )
        return record_list(response)

    def subscribe(self, instrument_tokens, is_index=False, is_depth=False):
        if not self.connected or self.client is None:
            raise RuntimeError("Login first.")
        if not instrument_tokens:
            return False
        self.client.subscribe(
            instrument_tokens=instrument_tokens,
            isIndex=is_index,
            isDepth=is_depth,
        )
        return True

    def discover_nifty_future(self):
        records = self.search_scrip("nse_fo", "NIFTY")
        now = datetime.now()
        candidates = []
        for record in records:
            symbol = str(record.get("pTrdSymbol", record.get("ts", "")))
            inst = str(record.get("pInstType", record.get("instType", "")) or "").upper()
            expiry = expiry_from_record(record)
            token = token_from_record(record)
            if not token:
                continue
            is_future = ("FUT" in inst) or symbol.upper().endswith("FUT") or ("FUTIDX" in inst)
            if not is_future:
                continue
            if expiry is not None and expiry < now - timedelta(days=1):
                continue
            candidates.append((expiry or datetime.max, token, symbol, record))

        if not candidates:
            manual = str(CONFIG["nifty_future_token"] or "").strip()
            if not manual:
                raise RuntimeError("No valid NIFTY Future discovered. No manual NIFTY_FUT_TOKEN supplied.")
            self.future_token = manual
            self.future_symbol = "NIFTY-MANUAL-FUTURE"
            self.future_expiry = None
            self.discovery_log.append("NIFTY Future mapped using NIFTY_FUT_TOKEN.")
            return {"token": manual, "symbol": self.future_symbol, "expiry": None}

        candidates.sort(key=lambda x: x[0])
        expiry, token, symbol, record = candidates[0]
        self.future_token, self.future_symbol, self.future_expiry = str(token), symbol, expiry
        self.discovery_log.append(f"Dynamic NIFTY Future: {symbol} (Token: {token})")
        return {"token": self.future_token, "symbol": symbol, "expiry": expiry, "record": record}

    def discover_heavyweights(self):
        result = {}
        for symbol in HEAVYWEIGHTS:
            token = ""
            try:
                records = self.search_scrip("nse_cm", symbol)
                for record in records:
                    name = str(record.get("pSymbolName", "")).upper()
                    trading = str(record.get("pTrdSymbol", "")).upper()
                    rt = token_from_record(record)
                    if rt and (name == symbol or trading == f"{symbol}-EQ" or trading == symbol):
                        token = rt
                        break
            except Exception as exc:
                print(f"[Dynamic Discovery] {symbol}: {exc}")
            if token:
                result[symbol] = token
        self.heavy_tokens = result
        self.discovery_log.append(f"Dynamic Heavyweights mapped: {len(result)}/{len(HEAVYWEIGHTS)}")
        return result

    def discover_pcr_options(self, spot_price):
        if not is_valid_number(spot_price) or spot_price <= 0:
            return []
        if not self.future_expiry:
            self.discover_nifty_future()

        expiry = self.future_expiry
        expiry_text = expiry.strftime("%Y%m") if expiry else None
        step = float(CONFIG["pcr_strike_step"])
        count = int(CONFIG["pcr_strike_count"])
        atm = round(spot_price / step) * step
        low, high = atm - count * step, atm + count * step
        discovered = []

        for opt_type in ["CE", "PE"]:
            try:
                records = self.search_scrip(
                    exchange_segment="nse_fo",
                    symbol="NIFTY",
                    expiry=expiry_text,
                    option_type=opt_type,
                )
            except Exception:
                records = []

            for record in records:
                token = token_from_record(record)
                strike = strike_from_record(record)
                rtype = option_type_from_record(record)
                if not token or not is_valid_number(strike):
                    continue
                if rtype and rtype != opt_type:
                    continue
                if strike < low or strike > high:
                    continue
                discovered.append({
                    "token": token,
                    "option_type": opt_type,
                    "strike": strike,
                    "symbol": record.get("pTrdSymbol", record.get("ts", "")),
                    "expiry": expiry_text,
                })

        self.pcr_tokens = list({x["token"]: x for x in discovered}.values())
        self.discovery_log.append(f"Dynamic PCR Contracts discovered: {len(self.pcr_tokens)}")
        return self.pcr_tokens

    def auto_discover(self):
        future = self.discover_nifty_future()
        heavy = self.discover_heavyweights()
        self.discovery_log.append("Index Reference: Nifty 50")
        return {"future": future, "heavyweights": heavy}

    def subscribe_core(self):
        if not self.future_token:
            self.discover_nifty_future()
        # Index must be subscribed with isIndex=True.
        self.subscribe(
            [{"instrument_token": CONFIG["nifty_index_name"], "exchange_segment": "nse_cm"}],
            is_index=True,
            is_depth=False,
        )
        tokens = [{"instrument_token": self.future_token, "exchange_segment": "nse_fo"}]
        tokens += [
            {"instrument_token": token, "exchange_segment": "nse_cm"}
            for token in self.heavy_tokens.values()
        ]
        self.subscribe(tokens, is_index=False, is_depth=False)
        return True

    def subscribe_pcr(self, spot_price):
        contracts = self.discover_pcr_options(spot_price)
        if not contracts:
            return False
        tokens = [{"instrument_token": x["token"], "exchange_segment": "nse_fo"} for x in contracts]
        self.subscribe(tokens, is_index=False, is_depth=False)
        return True

    def calculate_pcr(self):
        ce, pe = [], []
        with self.lock:
            latest = dict(self.latest)

        for item in self.pcr_tokens:
            data = latest.get(item["token"])
            if not data:
                continue
            oi = max(safe_float(data.get("oi"), 0.0), 0.0)
            volume = max(safe_float(data.get("volume"), 0.0), 0.0)
            row = {"token": item["token"], "strike": item["strike"], "oi": oi, "volume": volume, "iv": safe_float(data.get("iv"))}
            (ce if item["option_type"] == "CE" else pe).append(row)

        ce_oi, pe_oi = sum(x["oi"] for x in ce), sum(x["oi"] for x in pe)
        ce_volume, pe_volume = sum(x["volume"] for x in ce), sum(x["volume"] for x in pe)
        pcr_oi = pe_oi / ce_oi if ce_oi > 0 else np.nan
        pcr_volume = pe_volume / ce_volume if ce_volume > 0 else np.nan

        prev_ce, prev_pe = self.pcr_records.get("ce_oi"), self.pcr_records.get("pe_oi")
        ce_change = ce_oi - prev_ce if is_valid_number(prev_ce) else np.nan
        pe_change = pe_oi - prev_pe if is_valid_number(prev_pe) else np.nan
        self.pcr_records["ce_oi"], self.pcr_records["pe_oi"] = ce_oi, pe_oi

        with self.lock:
            spot_item = self.latest.get("NIFTY_INDEX")
        spot = safe_float(spot_item.get("ltp")) if spot_item else np.nan

        all_strikes = [x["strike"] for x in ce + pe if is_valid_number(x["strike"])]
        atm_strike = min(all_strikes, key=lambda x: abs(x - spot)) if all_strikes and is_valid_number(spot) else np.nan
        ce_atm = next((x for x in ce if x["strike"] == atm_strike), None)
        pe_atm = next((x for x in pe if x["strike"] == atm_strike), None)

        ivs = []
        for x in (ce_atm, pe_atm):
            if x and is_valid_number(x.get("iv")):
                ivs.append(x["iv"])
        atm_iv = float(np.mean(ivs)) if ivs else np.nan
        previous_iv = self.pcr_records.get("atm_iv")
        iv_change = atm_iv - previous_iv if is_valid_number(atm_iv) and is_valid_number(previous_iv) else np.nan
        if is_valid_number(atm_iv):
            self.pcr_records["atm_iv"] = atm_iv

        return {
            "pcr_oi": pcr_oi,
            "pcr_volume": pcr_volume,
            "ce_oi_change": ce_change,
            "pe_oi_change": pe_change,
            "atm_iv": atm_iv,
            "iv_change": iv_change,
            "ce_oi_atm": ce_atm["oi"] if ce_atm else np.nan,
            "pe_oi_atm": pe_atm["oi"] if pe_atm else np.nan,
            "atm_strike": atm_strike,
            "ce_contracts_seen": len(ce),
            "pe_contracts_seen": len(pe),
        }

    def build_3min_candles(self):
        with self.lock:
            ticks = list(self.tick_buffer)
            self.tick_buffer.clear()
        if not ticks:
            return []

        future_token = str(self.future_token)
        buckets = {}
        for tick in ticks:
            token = str(tick.get("token", ""))
            symbol = str(tick.get("symbol", ""))
            is_index = token == "NIFTY_INDEX" or symbol.lower() == CONFIG["nifty_index_name"].lower()
            is_future = token == future_token and future_token
            if not (is_index or is_future):
                continue
            ts = tick.get("timestamp")
            if not isinstance(ts, datetime):
                continue
            bar_time = floor_bar_timestamp(ts, CONFIG["bar_minutes"])
            if bar_time is None:
                continue
            key = (token, bar_time)
            buckets.setdefault(key, []).append(tick)

        bars = []
        for (token, bar_time), rows in sorted(buckets.items()):
            rows.sort(key=lambda x: x["timestamp"])
            prices = [safe_float(x.get("ltp")) for x in rows]
            prices = [x for x in prices if is_valid_number(x)]
            if not prices:
                continue
            volumes = [safe_float(x.get("volume"), 0.0) for x in rows]
            volumes = [x for x in volumes if is_valid_number(x)]
            ois = [safe_float(x.get("oi"), 0.0) for x in rows]
            ois = [x for x in ois if is_valid_number(x)]
            bars.append({
                "token": token,
                "timestamp": bar_time,  # IMPORTANT: use bucket timestamp, not last tick timestamp
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "volume": max(0.0, max(volumes) - min(volumes)) if volumes else 0.0,
                "oi": ois[-1] if ois else 0.0,
                "symbol": rows[-1].get("symbol", ""),
                "is_index": token == "NIFTY_INDEX",
            })

        merged = {}
        for bar in bars:
            merged.setdefault(bar["timestamp"], {})
            merged[bar["timestamp"]]["spot" if bar["is_index"] else "future"] = bar

        return [
            {"timestamp": ts, "spot": item["spot"], "future": item["future"]}
            for ts, item in sorted(merged.items())
            if "spot" in item and "future" in item
        ]


# =========================================================
# ENGINE CONTROLLER
# =========================================================

class NiftyMicroEngine:
    def __init__(self):
        self.features = FeatureEngine()
        self.labels = LabelEngine()
        self.dataset = DatasetManager()
        self.prev_candles = []
        self.feature_rows = []
        self.current_date = None
        self.last_bar_timestamp = None

    def reset_if_new_day(self, timestamp):
        if self.current_date != timestamp.date():
            self.features.reset_session()
            self.prev_candles.clear()
            self.current_date = timestamp.date()
            self.last_bar_timestamp = None

    def process_candle(self, candle):
        self.reset_if_new_day(candle.timestamp)
        if self.last_bar_timestamp is not None and candle.timestamp <= self.last_bar_timestamp:
            return None
        feature = self.features.compute(candle, self.prev_candles)
        self.prev_candles.append(candle)
        self.prev_candles = self.prev_candles[-500:]
        self.last_bar_timestamp = candle.timestamp
        self.feature_rows.append(feature)
        return feature

    def dataframe(self):
        return pd.DataFrame(self.feature_rows) if self.feature_rows else pd.DataFrame()

    def save(self):
        df = self.dataframe()
        if not df.empty:
            self.dataset.write_parquet(df, name="features")


# =========================================================
# UNIT TESTS
# =========================================================

def run_unit_tests():
    print("\n================================")
    print("KOTAK NEO RESEARCH LOCK TESTS")
    print("================================")

    assert np.isnan(wilder_atr([10, 12], 14))
    print("PASS: ATR warm-up")

    candle = Candle3Min(
        datetime(2025, 1, 2, 9, 18),
        24000, 24050, 23980, 24020,
        24010, 24060, 23990, 24030,
        100000, 5_000_000,
    )
    engine = NiftyMicroEngine()
    features = engine.process_candle(candle)
    assert features is not None
    assert features["execution_model"] == "next_bar_open"
    assert "pcr_oi" in features
    assert "data_quality_score" in features
    print("PASS: Feature engine")

    label = LabelEngine()
    future = [
        Candle3Min(
            datetime(2025, 1, 2, 9, 24),
            0, 0, 0, 0,
            24100, 24200, 23900, 24050, 0, 0
        )
    ]
    result = label.generate(
        entry_price=24040,
        atr=20.0,
        future_after_entry=future,
        direction=1,
        signal_timestamp=datetime(2025, 1, 2, 9, 18),
        entry_timestamp=datetime(2025, 1, 2, 9, 21),
    )
    assert result["execution_model"] == "next_bar_open"
    assert "horizon_45m_complete" in result
    print("PASS: next_bar_open alignment")

    try:
        label.generate(
            entry_price=24040,
            atr=20.0,
            future_after_entry=[
                Candle3Min(
                    datetime(2025, 1, 2, 9, 21),
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                )
            ],
            direction=1,
            entry_timestamp=datetime(2025, 1, 2, 9, 21),
        )
    except ValueError:
        print("PASS: Future alignment lock")
    else:
        raise AssertionError("Alignment lock failed")

    print("================================")
    print("ALL TESTS PASSED")
    print("================================\n")


# =========================================================
# STREAMLIT
# =========================================================

def run_streamlit_app():
    if st is None:
        raise RuntimeError("Streamlit is not installed.")

    st.set_page_config(page_title="NIFTY 3-Min Micro Engine", layout="wide")
    st.title("NIFTY 3-Min Micro Engine")
    st.caption("Kotak Neo API v2 • Research-Lock v2.0 • Dynamic Discovery")

    if "neo" not in st.session_state:
        st.session_state.neo = None
    if "engine" not in st.session_state:
        st.session_state.engine = NiftyMicroEngine()
    if "core_subscribed" not in st.session_state:
        st.session_state.core_subscribed = False
    if "pcr_subscribed" not in st.session_state:
        st.session_state.pcr_subscribed = False

    st.sidebar.header("Kotak Neo Secrets")
    credentials = {
        "Consumer Key": bool(env_or_secret("KOTAK_CONSUMER_KEY")),
        "Mobile": bool(env_or_secret("KOTAK_MOBILE")),
        "UCC": bool(env_or_secret("KOTAK_UCC")),
        "MPIN": bool(env_or_secret("KOTAK_MPIN")),
        "TOTP Secret": bool(env_or_secret("KOTAK_TOTP")),
    }
    for name, present in credentials.items():
        st.sidebar.write(f"{name}:", "✓" if present else "✗")

    st.sidebar.divider()
    st.sidebar.subheader("Research Lock")
    st.sidebar.write("ATR:", "session_local")
    st.sidebar.write("Bar:", "3 minutes")
    st.sidebar.write("Execution:", "next_bar_open")
    st.sidebar.write("TB horizon:", "30 minutes")
    st.sidebar.write("MFE:", "15 / 30 / 45 minutes")
    st.sidebar.divider()
    st.sidebar.subheader("PCR")
    st.sidebar.write("Strike count:", CONFIG["pcr_strike_count"])
    st.sidebar.write("Strike step:", CONFIG["pcr_strike_step"])

    st.subheader("Kotak Neo Authentication")
    user_live_totp = st.text_input(
        "Live 6-Digit TOTP (optional if KOTAK_TOTP is a Base32 secret)",
        placeholder="123456",
        type="password",
    )

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Connect Kotak Neo", use_container_width=True):
            try:
                adapter = KotakNeoAdapter()
                adapter.login(live_totp_override=user_live_totp)
                st.session_state.neo = adapter
                st.success("Kotak Neo API v2 authentication successful.")
                st.rerun()
            except Exception as exc:
                st.error(f"Login failed: {exc}")

    with c2:
        if st.button("Run Unit Tests", use_container_width=True):
            try:
                run_unit_tests()
                st.success("All tests passed.")
            except Exception as exc:
                st.error(f"Tests failed: {exc}")

    neo = st.session_state.neo
    if neo is None or not neo.connected:
        st.warning("Connect Kotak Neo first to enable dynamic discovery and live streaming.")
        return

    st.subheader("Instrument Discovery")
    if st.button("Discover NIFTY Instruments", use_container_width=True):
        try:
            d = neo.auto_discover()
            st.success("Dynamic discovery completed.")
            st.write("NIFTY Future:", d["future"].get("symbol", "-"))
            st.write("Future Token:", d["future"].get("token", "-"))
            st.write("Heavyweights:", len(d["heavyweights"]))
        except Exception as exc:
            st.error(f"Discovery failed: {exc}")

    if neo.future_token:
        d1, d2, d3 = st.columns(3)
        d1.metric("NIFTY Future", neo.future_symbol or "-")
        d2.metric("Future Token", neo.future_token or "-")
        d3.metric("Heavyweights", len(neo.heavy_tokens))

    st.subheader("Live Market Feed")
    if st.button("Subscribe NIFTY + Heavyweights", use_container_width=True):
        try:
            if not neo.future_token:
                neo.auto_discover()
            neo.subscribe_core()
            st.session_state.core_subscribed = True
            st.success("NIFTY Spot + Future + Heavyweights subscribed.")
        except Exception as exc:
            st.error(f"Core subscription failed: {exc}")

    with neo.lock:
        index_item = neo.latest.get("NIFTY_INDEX")
    latest_spot = safe_float(index_item.get("ltp")) if index_item else np.nan

    if st.button("Auto Discover + Subscribe PCR", use_container_width=True):
        try:
            if not neo.future_token:
                neo.auto_discover()
            if not is_valid_number(latest_spot):
                st.warning("NIFTY spot tick pending. Subscribe core first and wait for a live tick.")
            else:
                ok = neo.subscribe_pcr(latest_spot)
                if ok:
                    st.session_state.pcr_subscribed = True
                    st.success(f"PCR contracts subscribed: {len(neo.pcr_tokens)}")
                else:
                    st.warning("No option contracts discovered.")
        except Exception as exc:
            st.error(f"PCR discovery failed: {exc}")

    pcr = neo.calculate_pcr()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("NIFTY Spot", round(latest_spot, 2) if is_valid_number(latest_spot) else "-")
    m2.metric("PCR OI", round(pcr["pcr_oi"], 3) if is_valid_number(pcr["pcr_oi"]) else "-")
    m3.metric("PCR Volume", round(pcr["pcr_volume"], 3) if is_valid_number(pcr["pcr_volume"]) else "-")
    m4.metric("PCR Contracts", pcr["ce_contracts_seen"] + pcr["pe_contracts_seen"])

    st.subheader("3-Minute Candle Engine")
    if st.button("Build Latest 3-Min Bars", use_container_width=True):
        try:
            bars = neo.build_3min_candles()
            if not bars:
                st.info("No complete aligned 3-minute bar ready yet.")
            else:
                processed = 0
                with neo.lock:
                    latest = dict(neo.latest)

                for item in bars:
                    spot, future = item["spot"], item["future"]
                    heavy_snapshot = {}
                    for symbol, token in neo.heavy_tokens.items():
                        data = latest.get(str(token))
                        if data:
                            heavy_snapshot[symbol] = {
                                "o": data.get("open"),
                                "c": data.get("ltp"),
                                "vwap": data.get("vwap"),
                                "rel_volume": 1.0,
                            }

                    candle = Candle3Min(
                        timestamp=item["timestamp"],
                        spot_o=spot["open"], spot_h=spot["high"], spot_l=spot["low"], spot_c=spot["close"],
                        fut_o=future["open"], fut_h=future["high"], fut_l=future["low"], fut_c=future["close"],
                        fut_volume=future["volume"], fut_oi=future["oi"],
                        heavy=heavy_snapshot,
                        option_chain=neo.calculate_pcr(),
                    )
                    if st.session_state.engine.process_candle(candle) is not None:
                        processed += 1
                st.success(f"Processed {processed} aligned 3-minute bar(s).")
        except Exception as exc:
            st.error(f"Bar processing failed: {exc}")

    df = st.session_state.engine.dataframe()
    if not df.empty:
        st.subheader("Latest Calculated Features")
        cols = [
            "timestamp", "basis", "fut_vwap", "normalized_stretch", "normalized_spread",
            "stretch_slope_3", "spread_slope_3", "atr_14_prev", "atr_14_close", "spot_sma_20",
            "oi_change", "oi_long_buildup", "oi_short_buildup", "oi_short_covering",
            "oi_long_unwinding", "oi_strength", "twc", "breadth_10", "dispersion_index",
            "or_breakout_state", "pcr_oi", "pcr_volume", "ce_oi_change", "pe_oi_change",
            "ce_oi_atm", "pe_oi_atm", "atm_iv", "iv_change", "atm_strike", "data_quality_score",
        ]
        cols = [x for x in cols if x in df.columns]
        st.dataframe(df[cols].tail(30), use_container_width=True)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("3-Min Bars", len(df))
        latest_future = round(float(st.session_state.engine.prev_candles[-1].fut_c), 2) if st.session_state.engine.prev_candles else "-"
        c2.metric("Latest Future", latest_future)
        latest_pcr = df.iloc[-1].get("pcr_oi", np.nan)
        c3.metric("PCR OI", round(float(latest_pcr), 3) if is_valid_number(latest_pcr) else "-")
        quality = df.iloc[-1].get("data_quality_score", np.nan)
        c4.metric("Data Quality", round(float(quality), 3) if is_valid_number(quality) else "-")

        if st.button("Save Dataset", use_container_width=True):
            try:
                st.session_state.engine.save()
                st.success("Dataset saved to ./nifty_3min_dataset")
            except Exception as exc:
                st.error(f"Dataset save failed: {exc}")
    else:
        st.info("No 3-minute bars generated yet.")

    if neo.last_error:
        st.warning(f"Latest feed error: {neo.last_error}")

    with st.expander("Instrument Discovery Log"):
        if neo.discovery_log:
            for msg in neo.discovery_log:
                st.write("•", msg)
        else:
            st.write("No dynamic discovery performed yet.")

    st.divider()
    st.caption(
        "Execution: next_bar_open | ATR: session_local | Bar: 3m | "
        "Triple Barrier: 30m | MFE: 15/30/45m | PCR: live CE/PE OI + volume | "
        "Dynamic tokens | No fabricated market data"
    )


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    if os.getenv("RUN_TESTS", "0") == "1":
        run_unit_tests()
    elif st is not None:
        run_streamlit_app()
    else:
        run_unit_tests()
