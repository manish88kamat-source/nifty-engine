"""
GSR-1.1.0 â€” Historical / Replay + Mathematical Validation Pipeline
==================================================================

Research-only historical laboratory for the isolated Global Strategy Research
Engine.

ARCHITECTURAL CONTRACT
----------------------
1. Historical replay consumes RAW / NORMALIZED market observations only.
2. It never consumes alpha, confidence, prediction, regime labels, weights,
   signals, positions, or opinions produced by app.py / next_day_alpha_engine.py.
3. The regime used in reports is computed by GSR itself during replay.
4. Trader claims remain metadata. Claims are never evidence.
5. Missing strategy rules remain UNKNOWN. This module never invents rules.
6. Registry metadata is not an executable backtest. A strategy is only
   performance-tested after an explicit, versioned ReplayRule is registered.
7. Entries are evaluated using information available at the decision bar and
   execute no earlier than the next bar open by default.
8. Chronological train/validation/OOS and walk-forward evaluation are mandatory
   validation layers. Random shuffling is forbidden.
9. Purge and embargo are supported to reduce temporal leakage.
10. Costs, slippage and latency are explicit configuration.
11. MFE, MAE and ambiguous intrabar paths are recorded whenever OHLC permits.
12. Negative evidence is retained and is part of promotion decisions.
13. Multiple-testing correction is applied before global strategy ranking.
14. This module places no orders and has no broker dependency.
15. Results are research evidence, not trading instructions.

EXPECTED REPOSITORY LAYOUT
--------------------------
nifty-engine/
    app.py
    next_day_alpha_engine.py
    strategy_registry.py
    GSR_1.1.0_MASTER_STRATEGY_REGISTRY.txt
    gsr_engine.py
    gsr_data_adapter.py
    gsr_data_store.py
    gsr_live_bridge.py
    gsr_historical_replay.py       <-- this file

INPUTS
------
CSV or JSONL containing at least:
    timestamp, symbol, open, high, low, close

Optional fields are passed through:
    volume, oi, bid, ask, futures_close, spot_close,
    iv, atm_iv, iv_change, pcr_oi, pcr_volume, etc.

The preferred one-year workflow is:
    gsr_data_store.py
          |
          v
    export raw observations
          |
          v
    gsr_historical_replay.py

This file can also replay a CSV/JSONL directly.

IMPORTANT
---------
The current Strategy-DNA registry contains many mechanism descriptions but
not complete executable rules for every strategy. Therefore this module can
replay the market and produce GSR regime observations for every registered DNA,
but it will NOT fabricate entries/exits for strategies whose rules are missing.

To test a strategy, register a versioned ReplayRule explicitly. That rule must
state exactly how it produces an entry, direction, stop/target (if used), and
holding horizon. The rule version becomes part of the validation provenance.

STANDARD LIBRARY ONLY
---------------------
No pandas/numpy/scipy/sklearn are required.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import sqlite3
import statistics
import sys
import traceback
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

try:
    from gsr_engine import GSRConfig, GSREngine, MarketSnapshot
except ImportError as exc:
    raise ImportError(
        "GSR historical replay requires gsr_engine.py in the same directory."
    ) from exc

try:
    from strategy_registry import (
        ATOMIC_STRATEGY_REGISTRY,
        REGISTRY_VERSION,
        VALIDATION_GATES,
    )
except ImportError as exc:
    raise ImportError(
        "GSR historical replay requires strategy_registry.py in the same directory."
    ) from exc


# ============================================================================
# 0. VERSION / FROZEN RESEARCH CONTRACT
# ============================================================================

REPLAY_VERSION = "GSR-1.1.0-HISTORICAL-REPLAY"
REPLAY_SCHEMA_VERSION = "GSR_REPLAY_1.1"
VALIDATION_SCHEMA_VERSION = "GSR_VALIDATION_1.1"
DEFAULT_REPLAY_DIR = Path(os.getenv("GSR_REPLAY_DIR", "./gsr_data/replay"))

FORBIDDEN_EXTERNAL_OPINION_FIELDS = frozenset(
    {
        "alpha",
        "alpha_score",
        "alpha_probability",
        "confidence",
        "confidence_score",
        "prediction",
        "predicted_direction",
        "predicted_return",
        "signal",
        "signal_score",
        "signal_type",
        "external_regime",
        "regime",
        "regime_label",
        "regime_score",
        "position",
        "position_size",
        "decision",
        "trade_decision",
        "entry_signal",
        "exit_signal",
        "model_score",
        "model_prediction",
        "engine_opinion",
        "recommendation",
        "weight",
        "weights",
    }
)

DEFAULTS: Dict[str, Any] = {
    "entry_execution": "NEXT_BAR_OPEN",
    "transaction_cost_points": float(os.getenv("GSR_REPLAY_COST_POINTS", "0")),
    "slippage_points": float(os.getenv("GSR_REPLAY_SLIPPAGE_POINTS", "0")),
    "latency_bars": int(os.getenv("GSR_REPLAY_LATENCY_BARS", "0")),
    "purge_bars": int(os.getenv("GSR_REPLAY_PURGE_BARS", "0")),
    "embargo_bars": int(os.getenv("GSR_REPLAY_EMBARGO_BARS", "0")),
    "train_fraction": 0.60,
    "validation_fraction": 0.20,
    "oos_fraction": 0.20,
    "min_total_observations": 100,
    "min_oos_observations": 50,
    "min_regime_observations": 30,
    "min_walk_forward_folds": 3,
    "min_profit_factor": 1.05,
    "min_expectancy_points": 0.0,
    "min_expectancy_r": 0.0,
    "max_drawdown_r": 10.0,
    "min_win_rate": 0.0,
    "min_positive_regimes": 1,
    "max_negative_regimes": 999999,
    "bootstrap_iterations": 2000,
    "bootstrap_alpha": 0.05,
    "fdr_alpha": 0.05,
    "min_effective_trades_for_bootstrap": 20,
    "walk_forward_train_bars": 500,
    "walk_forward_test_bars": 100,
    "walk_forward_step_bars": 100,
    "walk_forward_max_folds": 12,
    "max_concurrent_positions": 1,
    "default_holding_bars": 5,
}


# ============================================================================
# 1. HELPERS
# ============================================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if not finite(a) or not finite(b) or abs(b) < 1e-12:
        return default
    return a / b


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parse_timestamp(value: Any) -> datetime:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(
            f"Timestamp must be timezone-aware; received {value!r}"
        )
    return dt


def timestamp_key(value: Any) -> float:
    return parse_timestamp(value).timestamp()


def json_safe(value: Any) -> Any:
    try:
        json.dumps(value, default=str)
        return value
    except Exception:
        return str(value)


def mean(values: Sequence[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def stdev(values: Sequence[float]) -> float:
    return statistics.stdev(values) if len(values) >= 2 else 0.0


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = clamp(q) * (len(ordered) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    weight = pos - lo
    return ordered[lo] * (1.0 - weight) + ordered[hi] * weight


def max_drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return abs(worst)


def profit_factor(values: Sequence[float]) -> float:
    gross_profit = sum(v for v in values if v > 0)
    gross_loss = abs(sum(v for v in values if v < 0))
    if gross_loss <= 1e-12:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def normal_pvalue_from_z(z: float) -> float:
    # Two-sided normal approximation using erfc; no scipy required.
    return math.erfc(abs(z) / math.sqrt(2.0))


def sign_test_pvalue(values: Sequence[float]) -> float:
    """Exact two-sided sign-test p-value against median/positive direction."""
    signs = [1 if x > 0 else -1 if x < 0 else 0 for x in values]
    positives = sum(1 for x in signs if x > 0)
    negatives = sum(1 for x in signs if x < 0)
    n = positives + negatives
    if n == 0:
        return 1.0
    k = min(positives, negatives)
    # Binomial tail: 2 * sum_{i=0}^k C(n,i)/2^n.
    tail = sum(
        math.comb(n, i) for i in range(k + 1)
    ) / (2.0 ** n)
    return min(1.0, 2.0 * tail)


def bootstrap_mean_ci(
    values: Sequence[float],
    iterations: int,
    alpha: float,
    seed: int,
) -> Dict[str, Any]:
    values = [float(v) for v in values if finite(v)]
    n = len(values)
    if n == 0:
        return {
            "n": 0,
            "mean": 0.0,
            "lower": None,
            "upper": None,
            "alpha": alpha,
            "iterations": 0,
            "status": "NO_DATA",
        }
    if n < 2:
        return {
            "n": n,
            "mean": mean(values),
            "lower": values[0],
            "upper": values[0],
            "alpha": alpha,
            "iterations": 0,
            "status": "INSUFFICIENT_FOR_BOOTSTRAP",
        }

    rng = random.Random(seed)
    boot = []
    for _ in range(max(1, int(iterations))):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        boot.append(mean(sample))

    lo = percentile(boot, alpha / 2.0)
    hi = percentile(boot, 1.0 - alpha / 2.0)
    return {
        "n": n,
        "mean": mean(values),
        "lower": lo,
        "upper": hi,
        "alpha": alpha,
        "iterations": iterations,
        "status": "OK",
    }


def benjamini_hochberg(
    pvalues: Mapping[str, float],
    alpha: float,
) -> Dict[str, Dict[str, Any]]:
    """False-discovery-rate control without external statistics packages."""
    clean = [
        (key, clamp(float(value), 0.0, 1.0))
        for key, value in pvalues.items()
        if finite(value)
    ]
    clean.sort(key=lambda x: x[1])
    m = len(clean)
    if m == 0:
        return {}

    adjusted: Dict[str, float] = {}
    running = 1.0
    for rank in range(m, 0, -1):
        key, p = clean[rank - 1]
        q = min(running, p * m / rank)
        running = q
        adjusted[key] = q

    result = {}
    for rank, (key, p) in enumerate(clean, start=1):
        q = adjusted[key]
        result[key] = {
            "rank": rank,
            "p_value": p,
            "q_value": q,
            "fdr_alpha": alpha,
            "fdr_reject": q <= alpha,
        }
    return result


# ============================================================================
# 2. CONFIGURATION
# ============================================================================

@dataclass
class ReplayConfig:
    replay_dir: Path = DEFAULT_REPLAY_DIR
    entry_execution: str = DEFAULTS["entry_execution"]
    transaction_cost_points: float = DEFAULTS["transaction_cost_points"]
    slippage_points: float = DEFAULTS["slippage_points"]
    latency_bars: int = DEFAULTS["latency_bars"]
    purge_bars: int = DEFAULTS["purge_bars"]
    embargo_bars: int = DEFAULTS["embargo_bars"]
    train_fraction: float = DEFAULTS["train_fraction"]
    validation_fraction: float = DEFAULTS["validation_fraction"]
    oos_fraction: float = DEFAULTS["oos_fraction"]
    min_total_observations: int = DEFAULTS["min_total_observations"]
    min_oos_observations: int = DEFAULTS["min_oos_observations"]
    min_regime_observations: int = DEFAULTS["min_regime_observations"]
    min_walk_forward_folds: int = DEFAULTS["min_walk_forward_folds"]
    min_profit_factor: float = DEFAULTS["min_profit_factor"]
    min_expectancy_points: float = DEFAULTS["min_expectancy_points"]
    min_expectancy_r: float = DEFAULTS["min_expectancy_r"]
    max_drawdown_r: float = DEFAULTS["max_drawdown_r"]
    min_win_rate: float = DEFAULTS["min_win_rate"]
    min_positive_regimes: int = DEFAULTS["min_positive_regimes"]
    max_negative_regimes: int = DEFAULTS["max_negative_regimes"]
    bootstrap_iterations: int = DEFAULTS["bootstrap_iterations"]
    bootstrap_alpha: float = DEFAULTS["bootstrap_alpha"]
    fdr_alpha: float = DEFAULTS["fdr_alpha"]
    min_effective_trades_for_bootstrap: int = DEFAULTS[
        "min_effective_trades_for_bootstrap"
    ]
    walk_forward_train_bars: int = DEFAULTS["walk_forward_train_bars"]
    walk_forward_test_bars: int = DEFAULTS["walk_forward_test_bars"]
    walk_forward_step_bars: int = DEFAULTS["walk_forward_step_bars"]
    walk_forward_max_folds: int = DEFAULTS["walk_forward_max_folds"]
    default_holding_bars: int = DEFAULTS["default_holding_bars"]

    def validate(self) -> None:
        if self.entry_execution != "NEXT_BAR_OPEN":
            raise ValueError(
                "GSR replay currently supports only NEXT_BAR_OPEN execution "
                "to keep the no-lookahead contract explicit."
            )
        fractions = (
            self.train_fraction,
            self.validation_fraction,
            self.oos_fraction,
        )
        if any(x <= 0 for x in fractions):
            raise ValueError("Chronological split fractions must all be > 0.")
        if abs(sum(fractions) - 1.0) > 1e-9:
            raise ValueError("Train/validation/OOS fractions must sum to 1.")
        if self.purge_bars < 0 or self.embargo_bars < 0:
            raise ValueError("Purge/embargo bars cannot be negative.")
        if self.latency_bars < 0:
            raise ValueError("Latency bars cannot be negative.")
        if self.bootstrap_alpha <= 0 or self.bootstrap_alpha >= 1:
            raise ValueError("bootstrap_alpha must be between 0 and 1.")
        if self.fdr_alpha <= 0 or self.fdr_alpha >= 1:
            raise ValueError("fdr_alpha must be between 0 and 1.")


# ============================================================================
# 3. REPLAY DATA CONTRACTS
# ============================================================================

@dataclass(frozen=True)
class ReplayRule:
    """
    Explicit executable strategy rule.

    signal(history, index) is called with information available through
    history[index]. It returns:
        LONG, SHORT, or None.

    stop_points(snapshot, direction, context) and target_points(...) are
    optional. If both are supplied, the engine records target/stop outcomes
    with intrabar ambiguity handling.

    holding_bars is the maximum holding period if no target/stop is hit.
    """

    strategy_id: str
    version: str
    description: str
    signal: Callable[[Sequence[MarketSnapshot], int], Optional[str]]
    holding_bars: int = DEFAULTS["default_holding_bars"]
    stop_points: Optional[
        Callable[[Sequence[MarketSnapshot], int, str], Optional[float]]
    ] = None
    target_points: Optional[
        Callable[[Sequence[MarketSnapshot], int, str], Optional[float]]
    ] = None
    regime_filter: Optional[
        Callable[[Mapping[str, Any]], bool]
    ] = None
    source_type: str = "REPRODUCIBLE_RULE"

    def validate(self) -> None:
        if not self.strategy_id:
            raise ValueError("ReplayRule.strategy_id is required.")
        if not self.version:
            raise ValueError("ReplayRule.version is required.")
        if not self.description:
            raise ValueError("ReplayRule.description is required.")
        if self.holding_bars < 1:
            raise ValueError("ReplayRule.holding_bars must be >= 1.")
        if self.source_type not in {
            "REPRODUCIBLE_RULE",
            "BACKTESTABLE_RULE",
            "OOS_VERIFIED_RULE",
        }:
            raise ValueError(f"Unsupported rule source_type: {self.source_type}")


@dataclass
class TradeObservation:
    strategy_id: str
    rule_version: str
    symbol: str
    signal_timestamp: str
    entry_timestamp: str
    exit_timestamp: str
    direction: str
    entry_price: float
    exit_price: float
    gross_pnl_points: float
    net_pnl_points: float
    holding_bars: int
    outcome: str
    regime_at_entry: str
    mfe_points: float
    mae_points: float
    mfe_pct: float
    mae_pct: float
    ambiguous_path: bool
    target_points: Optional[float]
    stop_points: Optional[float]
    cost_points: float
    slippage_points: float
    latency_bars: int
    split: str = "UNASSIGNED"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SplitWindow:
    name: str
    start_index: int
    end_index: int
    purge_start: int
    purge_end: int
    embargo_start: int
    embargo_end: int

    @property
    def size(self) -> int:
        return max(0, self.end_index - self.start_index)


# ============================================================================
# 4. FILE / SOURCE LOADERS
# ============================================================================

class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Mapping[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    dict(record),
                    ensure_ascii=False,
                    default=str,
                    separators=(",", ":"),
                )
                + "\n"
            )

    def write_all(self, records: Iterable[Mapping[str, Any]]) -> None:
        with self.path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(
                    json.dumps(
                        dict(record),
                        ensure_ascii=False,
                        default=str,
                        separators=(",", ":"),
                    )
                    + "\n"
                )


def _coerce_csv_value(key: str, value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None

    numeric_fields = {
        "open", "high", "low", "close", "volume", "oi", "bid", "ask",
        "futures_close", "spot_close", "iv", "atm_iv", "iv_change",
        "iv_rank", "iv_percentile", "iv_skew", "iv_term_structure",
        "realized_vol", "iv_rv_spread", "pcr_oi", "pcr_volume",
        "ce_oi", "pe_oi", "ce_oi_change", "pe_oi_change", "atm_straddle",
        "delta", "gamma", "theta", "vega", "vanna", "charm", "dte",
        "strike", "moneyness", "spread_points", "spread_pct",
        "chain_completeness",
    }
    if key in numeric_fields:
        number = safe_float(text)
        return number if number is not None else text
    return text


def iter_csv(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header: {path}")
        for row in reader:
            yield {
                str(k): _coerce_csv_value(str(k), v)
                for k, v in row.items()
                if k is not None
            }


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                value = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSONL at {path}:{line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise ValueError(
                    f"JSONL record at {path}:{line_number} is not an object."
                )
            # Accept either a direct observation or a stored record containing
            # a nested "snapshot".
            if isinstance(value.get("snapshot"), dict):
                value = value["snapshot"]
            yield dict(value)


class SQLiteRawObservationReader:
    """
    Read immutable rows created by gsr_data_store.py.

    This reader intentionally does not modify the database. It is a replay
    source only. The exact column names match the GSR-1.1 raw store contract.
    """

    SELECT_FIELDS = (
        "timestamp,symbol,open,high,low,close,volume,oi,bid,ask,"
        "futures_close,spot_close,iv,atm_iv,iv_change,iv_rank,"
        "iv_percentile,iv_skew,iv_term_structure,realized_vol,iv_rv_spread,"
        "pcr_oi,pcr_volume,ce_oi,pe_oi,ce_oi_change,pe_oi_change,"
        "atm_straddle,chain_completeness,delta,gamma,theta,vega,vanna,charm,"
        "dte,strike,option_type,moneyness,expiry,exchange,market,"
        "instrument_type,asset_class,timeframe,session,stream,metadata_json"
    )

    def __init__(self, path: Path) -> None:
        self.path = path

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        if not self.path.exists():
            raise FileNotFoundError(self.path)

        connection = sqlite3.connect(str(self.path))
        connection.row_factory = sqlite3.Row
        try:
            cursor = connection.execute(
                f"SELECT {self.SELECT_FIELDS} "
                "FROM raw_observations ORDER BY timestamp ASC, symbol ASC"
            )
            for row in cursor:
                item = dict(row)
                metadata_raw = item.pop("metadata_json", None)
                if metadata_raw:
                    try:
                        metadata = json.loads(metadata_raw)
                        if isinstance(metadata, dict):
                            item.update(metadata)
                    except Exception:
                        item["metadata_json"] = metadata_raw
                yield {
                    key: value
                    for key, value in item.items()
                    if value is not None
                }
        finally:
            connection.close()


def load_observations(path: Path) -> Iterator[Dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from iter_csv(path)
        return
    if suffix in {".jsonl", ".ndjson"}:
        yield from iter_jsonl(path)
        return
    if suffix in {".sqlite", ".sqlite3", ".db"}:
        yield from SQLiteRawObservationReader(path)
        return
    raise ValueError(
        f"Unsupported historical source {path}. "
        "Use CSV, JSONL/NDJSON or the GSR SQLite raw store."
    )


# ============================================================================
# 5. CHRONOLOGY / LEAKAGE GUARDS
# ============================================================================

class ChronologyGuard:
    def __init__(self) -> None:
        self.last_by_symbol: Dict[str, float] = {}
        self.count = 0

    def validate(self, row: Mapping[str, Any]) -> None:
        if "timestamp" not in row or "symbol" not in row:
            raise ValueError("Every observation requires timestamp and symbol.")
        ts = timestamp_key(row["timestamp"])
        symbol = str(row["symbol"])
        previous = self.last_by_symbol.get(symbol)
        if previous is not None and ts < previous:
            raise ValueError(
                "Historical replay chronology violation: "
                f"{symbol} timestamp moved backwards."
            )
        self.last_by_symbol[symbol] = ts
        self.count += 1


def reject_external_opinions(row: Mapping[str, Any]) -> None:
    supplied = sorted(
        FORBIDDEN_EXTERNAL_OPINION_FIELDS.intersection(row.keys())
    )
    if supplied:
        raise ValueError(
            "GSR isolation violation in historical input: "
            + ", ".join(supplied)
        )


def validate_raw_row(row: Mapping[str, Any]) -> None:
    required = ("timestamp", "symbol", "open", "high", "low", "close")
    missing = [key for key in required if key not in row]
    if missing:
        raise ValueError(f"Missing required historical fields: {missing}")

    parse_timestamp(row["timestamp"])
    for key in ("open", "high", "low", "close"):
        value = safe_float(row.get(key))
        if value is None or value <= 0:
            raise ValueError(f"Invalid OHLC field {key}: {row.get(key)!r}")

    high = float(row["high"])
    low = float(row["low"])
    op = float(row["open"])
    close = float(row["close"])
    if high < max(op, close) or low > min(op, close) or high < low:
        raise ValueError("Invalid OHLC relationship.")

    reject_external_opinions(row)


def normalize_raw_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    result = dict(row)
    result["timestamp"] = parse_timestamp(row["timestamp"]).isoformat()
    result["symbol"] = str(row["symbol"]).strip()
    for key in ("open", "high", "low", "close"):
        result[key] = float(row[key])
    for key in (
        "volume", "oi", "bid", "ask", "futures_close", "spot_close"
    ):
        if key in result and result[key] is not None:
            value = safe_float(result[key])
            result[key] = value
    return result


# ============================================================================
# 6. CHRONOLOGICAL SPLITS / PURGE / EMBARGO
# ============================================================================

def build_chronological_splits(
    n: int,
    config: ReplayConfig,
) -> Dict[str, SplitWindow]:
    if n < 3:
        raise ValueError("At least 3 observations are required for splitting.")

    train_end = int(n * config.train_fraction)
    validation_end = int(
        n * (config.train_fraction + config.validation_fraction)
    )

    train_end = max(1, min(n - 2, train_end))
    validation_end = max(train_end + 1, min(n - 1, validation_end))

    purge = config.purge_bars
    embargo = config.embargo_bars

    train = SplitWindow(
        name="TRAIN",
        start_index=0,
        end_index=train_end,
        purge_start=max(0, train_end - purge),
        purge_end=train_end,
        embargo_start=train_end,
        embargo_end=min(n, train_end + embargo),
    )
    validation = SplitWindow(
        name="VALIDATION",
        start_index=min(n, train_end + embargo),
        end_index=validation_end,
        purge_start=max(0, validation_end - purge),
        purge_end=validation_end,
        embargo_start=validation_end,
        embargo_end=min(n, validation_end + embargo),
    )
    oos = SplitWindow(
        name="OOS",
        start_index=min(n, validation_end + embargo),
        end_index=n,
        purge_start=max(0, validation_end - purge),
        purge_end=validation_end,
        embargo_start=validation_end,
        embargo_end=min(n, validation_end + embargo),
    )
    return {
        "TRAIN": train,
        "VALIDATION": validation,
        "OOS": oos,
    }


def index_split(index: int, splits: Mapping[str, SplitWindow]) -> str:
    # OOS takes precedence at exact boundaries only through explicit ranges.
    for name in ("OOS", "VALIDATION", "TRAIN"):
        window = splits[name]
        if window.start_index <= index < window.end_index:
            return name
    return "UNASSIGNED"


def walk_forward_windows(
    n: int,
    config: ReplayConfig,
) -> List[Tuple[int, int, int, int]]:
    """
    Returns (train_start, train_end, test_start, test_end).

    Each test block occurs strictly after its training block.
    """
    train_len = max(1, config.walk_forward_train_bars)
    test_len = max(1, config.walk_forward_test_bars)
    step = max(1, config.walk_forward_step_bars)
    windows = []

    cursor = train_len
    while cursor < n and len(windows) < config.walk_forward_max_folds:
        test_start = cursor + config.embargo_bars
        test_end = min(n, test_start + test_len)
        if test_start >= n or test_end <= test_start:
            break
        train_start = max(0, cursor - train_len)
        train_end = cursor - config.purge_bars
        if train_end <= train_start:
            cursor += step
            continue
        windows.append((train_start, train_end, test_start, test_end))
        cursor += step

    return windows


# ============================================================================
# 7. REPLAY ENGINE
# ============================================================================

class HistoricalReplayEngine:
    """
    Coordinates chronological market replay and optional strategy evaluation.

    It deliberately keeps market replay and strategy evaluation separate:
    market replay can run even when zero executable rules are registered.
    """

    def __init__(self, config: Optional[ReplayConfig] = None) -> None:
        self.config = config or ReplayConfig()
        self.config.validate()
        self.config.replay_dir.mkdir(parents=True, exist_ok=True)

        gsr_config = GSRConfig.from_env()
        # Never mix replay artifacts into the live-shadow GSR directory unless
        # the caller explicitly sets GSR_DATA_DIR to the same path.
        gsr_config.data_dir = self.config.replay_dir / "gsr_engine"
        gsr_config.transaction_cost_points = self.config.transaction_cost_points
        gsr_config.slippage_points = self.config.slippage_points

        self.engine = GSREngine(gsr_config)
        self.registry = list(ATOMIC_STRATEGY_REGISTRY)
        self.registry_by_id = {
            str(item.get("atomic_strategy_id")): item
            for item in self.registry
        }
        self.rules: Dict[str, ReplayRule] = {}

        self.market_rows: List[Dict[str, Any]] = []
        self.snapshots: List[MarketSnapshot] = []
        self.replay_results: List[Dict[str, Any]] = []
        self.trades: List[TradeObservation] = []
        self.rejections: List[Dict[str, Any]] = []
        self.chronology = ChronologyGuard()

        self.market_writer = JsonlWriter(
            self.config.replay_dir / "replay_market_results.jsonl"
        )
        self.trade_writer = JsonlWriter(
            self.config.replay_dir / "replay_trade_observations.jsonl"
        )
        self.event_writer = JsonlWriter(
            self.config.replay_dir / "replay_events.jsonl"
        )
        self.validation_writer = JsonlWriter(
            self.config.replay_dir / "validation_results.jsonl"
        )

    def register_rule(self, rule: ReplayRule) -> None:
        rule.validate()
        if rule.strategy_id not in self.registry_by_id:
            raise KeyError(
                f"Strategy-DNA {rule.strategy_id!r} is not in the frozen registry."
            )
        self.rules[rule.strategy_id] = rule
        self.event_writer.append(
            {
                "event": "REPLAY_RULE_REGISTERED",
                "timestamp": utc_now(),
                "strategy_id": rule.strategy_id,
                "rule_version": rule.version,
                "source_type": rule.source_type,
                "description": rule.description,
            }
        )

    def load(self, source: Path, max_rows: Optional[int] = None) -> int:
        count = 0
        for row in load_observations(source):
            try:
                validate_raw_row(row)
                normalized = normalize_raw_row(row)
                self.chronology.validate(normalized)
                self.market_rows.append(normalized)
                count += 1
                if max_rows is not None and count >= max_rows:
                    break
            except Exception as exc:
                rejection = {
                    "event": "HISTORICAL_ROW_REJECTED",
                    "timestamp": utc_now(),
                    "error": str(exc),
                    "row_hash": content_hash(row),
                }
                self.rejections.append(rejection)
                self.event_writer.append(rejection)

        # Do NOT silently sort here. Sorting can hide a broken source and can
        # create false confidence around chronological correctness.
        return count

    def _grouped_indices(self) -> Dict[str, List[int]]:
        grouped: Dict[str, List[int]] = defaultdict(list)
        for index, row in enumerate(self.market_rows):
            grouped[str(row["symbol"])].append(index)
        return dict(grouped)

    def replay_market(self) -> Dict[str, Any]:
        if not self.market_rows:
            raise ValueError("No historical observations loaded.")

        total = 0
        successful = 0
        failed = 0

        # GSREngine itself maintains per-symbol chronology. We replay in the
        # source order, which was already checked by ChronologyGuard.
        for index, row in enumerate(self.market_rows):
            try:
                result = self.engine.ingest_snapshot(row)
                self.replay_results.append(result)

                regime = result.get("regime") or {}
                record = {
                    "schema_version": REPLAY_SCHEMA_VERSION,
                    "replay_version": REPLAY_VERSION,
                    "index": index,
                    "timestamp": row["timestamp"],
                    "symbol": row["symbol"],
                    "regime": regime,
                    "strategy_compatibility_count": len(
                        result.get("strategy_compatibility") or []
                    ),
                    "feature_keys": sorted(
                        (result.get("features") or {}).keys()
                    ),
                }
                self.market_writer.append(record)

                snapshot = MarketSnapshot.from_mapping(row)
                self.snapshots.append(snapshot)
                total += 1
                successful += 1
            except Exception as exc:
                failed += 1
                self.event_writer.append(
                    {
                        "event": "REPLAY_ENGINE_ERROR",
                        "timestamp": utc_now(),
                        "index": index,
                        "symbol": row.get("symbol"),
                        "error": str(exc),
                        "traceback": traceback.format_exc(limit=3),
                    }
                )
                # Historical replay is fail-closed. One malformed market
                # observation must not be converted into synthetic output.
                raise

        return {
            "status": "COMPLETED",
            "input_rows": len(self.market_rows),
            "replayed_rows": successful,
            "failed_rows": failed,
            "symbols": len({x["symbol"] for x in self.market_rows}),
            "rules_registered": len(self.rules),
        }

    def _regime_at_index(self, index: int) -> str:
        if index < 0 or index >= len(self.replay_results):
            return "UNKNOWN"
        regime = self.replay_results[index].get("regime") or {}
        value = regime.get("regime")
        return str(value) if value is not None else "UNKNOWN"

    def _entry_index(self, signal_index: int) -> int:
        return signal_index + 1 + self.config.latency_bars

    def _resolve_exit(
        self,
        rule: ReplayRule,
        signal_index: int,
        entry_index: int,
        direction: str,
        entry_price: float,
        snapshots: Sequence[MarketSnapshot],
    ) -> Tuple[int, float, str, float, float, bool, Optional[float], Optional[float]]:
        """
        Resolve target/stop/time exit using OHLC.

        If target and stop are both touched inside the same candle and the
        sequence cannot determine which came first, outcome is AMBIGUOUS.
        We do not choose the favorable path.
        """
        stop = None
        target = None
        if rule.stop_points is not None:
            stop = rule.stop_points(snapshots, signal_index, direction)
            stop = float(stop) if finite(stop) and stop > 0 else None
        if rule.target_points is not None:
            target = rule.target_points(snapshots, signal_index, direction)
            target = float(target) if finite(target) and target > 0 else None

        last_index = min(
            len(snapshots) - 1,
            entry_index + rule.holding_bars,
        )

        mfe = 0.0
        mae = 0.0
        ambiguous = False

        for j in range(entry_index, last_index + 1):
            bar = snapshots[j]
            if direction == "LONG":
                favorable = bar.high - entry_price
                adverse = bar.low - entry_price
                target_hit = target is not None and favorable >= target
                stop_hit = stop is not None and adverse <= -stop
            else:
                favorable = entry_price - bar.low
                adverse = entry_price - bar.high
                target_hit = target is not None and favorable >= target
                stop_hit = stop is not None and adverse <= -stop

            mfe = max(mfe, favorable)
            mae = min(mae, adverse)

            if target_hit and stop_hit:
                ambiguous = True
                exit_price = entry_price
                return (
                    j,
                    exit_price,
                    "AMBIGUOUS",
                    mfe,
                    mae,
                    True,
                    target,
                    stop,
                )

            if target_hit:
                exit_price = (
                    entry_price + target
                    if direction == "LONG"
                    else entry_price - target
                )
                return (
                    j,
                    exit_price,
                    "TARGET_FIRST",
                    mfe,
                    mae,
                    False,
                    target,
                    stop,
                )

            if stop_hit:
                exit_price = (
                    entry_price - stop
                    if direction == "LONG"
                    else entry_price + stop
                )
                return (
                    j,
                    exit_price,
                    "STOP_FIRST",
                    mfe,
                    mae,
                    False,
                    target,
                    stop,
                )

        exit_index = last_index
        exit_price = snapshots[exit_index].close
        return (
            exit_index,
            exit_price,
            "TIMEOUT",
            mfe,
            mae,
            ambiguous,
            target,
            stop,
        )

    def evaluate_rule(
        self,
        rule: ReplayRule,
        allowed_indices: Optional[Sequence[int]] = None,
        split_name: str = "ALL",
    ) -> List[TradeObservation]:
        rule.validate()

        if not self.snapshots:
            raise ValueError("Run replay_market() before evaluate_rule().")

        indices = (
            list(allowed_indices)
            if allowed_indices is not None
            else list(range(len(self.snapshots)))
        )

        trades: List[TradeObservation] = []
        occupied_until = -1

        for signal_index in indices:
            if signal_index >= len(self.snapshots):
                continue
            if signal_index >= len(self.replay_results):
                continue

            current_regime = (
                self.replay_results[signal_index].get("regime") or {}
            )
            if rule.regime_filter is not None:
                try:
                    if not rule.regime_filter(current_regime):
                        continue
                except Exception:
                    # Rule filters are research code. A failure is not an
                    # implicit pass.
                    continue

            try:
                direction = rule.signal(self.snapshots, signal_index)
            except Exception as exc:
                self.event_writer.append(
                    {
                        "event": "RULE_EVALUATION_ERROR",
                        "timestamp": utc_now(),
                        "strategy_id": rule.strategy_id,
                        "rule_version": rule.version,
                        "signal_index": signal_index,
                        "error": str(exc),
                    }
                )
                continue

            if direction not in {"LONG", "SHORT"}:
                continue

            entry_index = self._entry_index(signal_index)
            if entry_index >= len(self.snapshots):
                continue
            if entry_index <= occupied_until:
                continue

            # The signal can only use data <= signal_index. Execution is
            # explicitly delayed to a future bar.
            entry_snapshot = self.snapshots[entry_index]
            entry_price = entry_snapshot.open

            (
                exit_index,
                exit_price,
                outcome,
                mfe,
                mae,
                ambiguous,
                target,
                stop,
            ) = self._resolve_exit(
                rule,
                signal_index,
                entry_index,
                direction,
                entry_price,
                self.snapshots,
            )

            gross = (
                exit_price - entry_price
                if direction == "LONG"
                else entry_price - exit_price
            )

            cost = float(self.config.transaction_cost_points)
            slippage = float(self.config.slippage_points)
            net = gross - cost - slippage

            if stop is not None and stop > 0:
                r_multiple = net / stop
            else:
                r_multiple = None

            exit_snapshot = self.snapshots[exit_index]
            entry_close = max(abs(entry_price), 1e-12)

            trade = TradeObservation(
                strategy_id=rule.strategy_id,
                rule_version=rule.version,
                symbol=entry_snapshot.symbol,
                signal_timestamp=self.snapshots[signal_index].timestamp,
                entry_timestamp=entry_snapshot.timestamp,
                exit_timestamp=exit_snapshot.timestamp,
                direction=direction,
                entry_price=entry_price,
                exit_price=exit_price,
                gross_pnl_points=gross,
                net_pnl_points=net,
                holding_bars=exit_index - entry_index + 1,
                outcome=outcome,
                regime_at_entry=self._regime_at_index(signal_index),
                mfe_points=mfe,
                mae_points=mae,
                mfe_pct=safe_div(mfe, entry_close) * 100.0,
                mae_pct=safe_div(mae, entry_close) * 100.0,
                ambiguous_path=ambiguous,
                target_points=target,
                stop_points=stop,
                cost_points=cost,
                slippage_points=slippage,
                latency_bars=self.config.latency_bars,
                split=split_name,
                metadata={
                    "signal_index": signal_index,
                    "entry_index": entry_index,
                    "exit_index": exit_index,
                    "r_multiple": r_multiple,
                },
            )
            trades.append(trade)
            occupied_until = exit_index

        return trades

    def evaluate_registered_rules(self) -> Dict[str, Any]:
        if not self.rules:
            return {
                "status": "NO_EXECUTABLE_RULES",
                "message": (
                    "Market replay can run, but no strategy has been "
                    "promoted to an executable ReplayRule."
                ),
            }

        splits = build_chronological_splits(len(self.snapshots), self.config)
        all_indices = list(range(len(self.snapshots)))
        results = {}

        for strategy_id, rule in self.rules.items():
            strategy_trades: List[TradeObservation] = []

            # Evaluate the full chronology once to preserve path behavior.
            full = self.evaluate_rule(rule, all_indices, "ALL")

            # Re-label trades by the decision/signal index's chronological
            # split. A trade belongs to the split containing its signal.
            for trade in full:
                signal_index = int(trade.metadata["signal_index"])
                split = index_split(signal_index, splits)
                trade.split = split
                strategy_trades.append(trade)
                self.trades.append(trade)
                self.trade_writer.append(trade.to_dict())

            results[strategy_id] = self.validate_strategy(strategy_id)

        return results

    # ------------------------------------------------------------------------
    # Mathematical validation
    # ------------------------------------------------------------------------

    def _metrics(self, trades: Sequence[TradeObservation]) -> Dict[str, Any]:
        pnls = [float(t.net_pnl_points) for t in trades]
        r_values = [
            float(t.metadata["r_multiple"])
            for t in trades
            if finite(t.metadata.get("r_multiple"))
        ]
        wins = sum(1 for x in pnls if x > 0)
        losses = sum(1 for x in pnls if x < 0)

        return {
            "sample_size": len(trades),
            "wins": wins,
            "losses": losses,
            "win_rate": safe_div(wins, len(pnls)),
            "expectancy_points": mean(pnls),
            "median_pnl_points": statistics.median(pnls) if pnls else 0.0,
            "profit_factor": profit_factor(pnls),
            "max_drawdown_points": max_drawdown(pnls),
            "avg_mfe_points": mean([t.mfe_points for t in trades]),
            "avg_mae_points": mean([t.mae_points for t in trades]),
            "ambiguous_path_count": sum(
                1 for t in trades if t.ambiguous_path
            ),
            "ambiguous_path_rate": safe_div(
                sum(1 for t in trades if t.ambiguous_path),
                len(trades),
            ),
            "avg_holding_bars": mean(
                [float(t.holding_bars) for t in trades]
            ),
            "expectancy_r": mean(r_values) if r_values else None,
            "max_drawdown_r": max_drawdown(r_values) if r_values else None,
            "p_value_sign_test": sign_test_pvalue(pnls),
        }

    def _regime_metrics(
        self,
        trades: Sequence[TradeObservation],
    ) -> Dict[str, Dict[str, Any]]:
        groups: Dict[str, List[TradeObservation]] = defaultdict(list)
        for trade in trades:
            groups[trade.regime_at_entry].append(trade)

        output = {}
        for regime, rows in sorted(groups.items()):
            metric = self._metrics(rows)
            metric["regime"] = regime
            metric["negative_expectancy"] = (
                metric["expectancy_points"] < 0
            )
            output[regime] = metric
        return output

    def _bootstrap(self, strategy_id: str, trades: Sequence[TradeObservation]) -> Dict[str, Any]:
        values = [t.net_pnl_points for t in trades]
        if len(values) < self.config.min_effective_trades_for_bootstrap:
            return {
                "status": "INSUFFICIENT_SAMPLE",
                "sample_size": len(values),
                "ci": None,
            }
        seed_material = f"{REPLAY_VERSION}:{strategy_id}:{len(values)}"
        seed = int(hashlib.sha256(seed_material.encode()).hexdigest()[:8], 16)
        ci = bootstrap_mean_ci(
            values,
            self.config.bootstrap_iterations,
            self.config.bootstrap_alpha,
            seed,
        )
        return {
            "status": "OK",
            "sample_size": len(values),
            "ci": ci,
        }

    def _gate_report(
        self,
        total: Mapping[str, Any],
        oos: Mapping[str, Any],
        wf: Mapping[str, Any],
        regimes: Mapping[str, Mapping[str, Any]],
        bootstrap: Mapping[str, Any],
        multiple_testing: Optional[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        checks: Dict[str, bool] = {}

        checks["minimum_total_sample"] = (
            int(total["sample_size"]) >= self.config.min_total_observations
        )
        checks["minimum_oos_sample"] = (
            int(oos["sample_size"]) >= self.config.min_oos_observations
        )
        checks["oos_expectancy_positive"] = (
            finite(oos["expectancy_points"])
            and oos["expectancy_points"] > self.config.min_expectancy_points
        )
        checks["oos_profit_factor"] = (
            oos["profit_factor"] >= self.config.min_profit_factor
        )
        checks["oos_drawdown"] = (
            oos.get("max_drawdown_r") is None
            or oos["max_drawdown_r"] <= self.config.max_drawdown_r
        )
        checks["bootstrap_ci_not_negative"] = False
        ci = (bootstrap.get("ci") or {})
        if ci.get("lower") is not None:
            checks["bootstrap_ci_not_negative"] = (
                float(ci["lower"]) > self.config.min_expectancy_points
            )

        positive_regimes = sum(
            1
            for metric in regimes.values()
            if metric.get("sample_size", 0) >= self.config.min_regime_observations
            and metric.get("expectancy_points", 0) > 0
        )
        negative_regimes = sum(
            1
            for metric in regimes.values()
            if metric.get("sample_size", 0) >= self.config.min_regime_observations
            and metric.get("expectancy_points", 0) < 0
        )

        checks["positive_regime_count"] = (
            positive_regimes >= self.config.min_positive_regimes
        )
        checks["negative_regime_limit"] = (
            negative_regimes <= self.config.max_negative_regimes
        )
        checks["walk_forward_min_folds"] = (
            int(wf.get("fold_count", 0)) >= self.config.min_walk_forward_folds
        )

        if multiple_testing is None:
            checks["multiple_testing_control"] = False
        else:
            checks["multiple_testing_control"] = bool(
                multiple_testing.get("fdr_reject", False)
            )

        # Promotion is intentionally strict. Even if a single gate is true,
        # the strategy is not promoted unless all mandatory evidence layers pass.
        mandatory = {
            "minimum_total_sample",
            "minimum_oos_sample",
            "oos_expectancy_positive",
            "oos_profit_factor",
            "bootstrap_ci_not_negative",
            "walk_forward_min_folds",
            "multiple_testing_control",
        }
        eligible = all(checks[key] for key in mandatory)

        return {
            "checks": checks,
            "positive_regime_count": positive_regimes,
            "negative_regime_count": negative_regimes,
            "mandatory_gate_count": len(mandatory),
            "mandatory_gate_passed": sum(
                1 for key in mandatory if checks[key]
            ),
            "promotion_eligible_before_manual_review": eligible,
        }

    def _walk_forward_for_rule(
        self,
        rule: ReplayRule,
    ) -> Dict[str, Any]:
        windows = walk_forward_windows(len(self.snapshots), self.config)
        folds = []

        for fold_id, (
            train_start,
            train_end,
            test_start,
            test_end,
        ) in enumerate(windows, start=1):
            # The rule itself is fixed. We do not tune parameters inside the
            # replay engine. Train is therefore an evidence partition rather
            # than a hidden optimizer.
            train_indices = list(range(train_start, train_end))
            test_indices = list(range(test_start, test_end))

            train_trades = self.evaluate_rule(
                rule, train_indices, f"WF_TRAIN_{fold_id}"
            )
            test_trades = self.evaluate_rule(
                rule, test_indices, f"WF_TEST_{fold_id}"
            )

            folds.append(
                {
                    "fold": fold_id,
                    "train_window": [train_start, train_end],
                    "test_window": [test_start, test_end],
                    "train": self._metrics(train_trades),
                    "test": self._metrics(test_trades),
                }
            )

        test_expectancies = [
            fold["test"]["expectancy_points"]
            for fold in folds
            if fold["test"]["sample_size"] > 0
        ]
        positive_test_folds = sum(1 for x in test_expectancies if x > 0)

        return {
            "fold_count": len(folds),
            "folds": folds,
            "positive_test_folds": positive_test_folds,
            "test_expectancy_mean": mean(test_expectancies),
            "test_expectancy_median": (
                statistics.median(test_expectancies)
                if test_expectancies
                else 0.0
            ),
            "status": (
                "OK"
                if len(folds) >= self.config.min_walk_forward_folds
                else "INSUFFICIENT_FOLDS"
            ),
        }

    def validate_strategy(
        self,
        strategy_id: str,
        multiple_testing: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        if strategy_id not in self.rules:
            raise KeyError(f"No executable ReplayRule for {strategy_id}.")

        rule = self.rules[strategy_id]
        trades = [
            t for t in self.trades
            if t.strategy_id == strategy_id
        ]

        all_metrics = self._metrics(trades)
        train_trades = [t for t in trades if t.split == "TRAIN"]
        validation_trades = [t for t in trades if t.split == "VALIDATION"]
        oos_trades = [t for t in trades if t.split == "OOS"]

        train_metrics = self._metrics(train_trades)
        validation_metrics = self._metrics(validation_trades)
        oos_metrics = self._metrics(oos_trades)

        regime_metrics = self._regime_metrics(oos_trades)
        bootstrap = self._bootstrap(strategy_id, oos_trades)
        wf = self._walk_forward_for_rule(rule)

        gate = self._gate_report(
            all_metrics,
            oos_metrics,
            wf,
            regime_metrics,
            bootstrap,
            multiple_testing,
        )

        registry_record = self.registry_by_id.get(strategy_id, {})
        result = {
            "schema_version": VALIDATION_SCHEMA_VERSION,
            "replay_version": REPLAY_VERSION,
            "registry_version": REGISTRY_VERSION,
            "validated_at": utc_now(),
            "strategy_id": strategy_id,
            "strategy_name": registry_record.get("strategy_name"),
            "trader": registry_record.get("trader"),
            "rule_version": rule.version,
            "rule_source_type": rule.source_type,
            "rule_description": rule.description,
            "evidence_grade_before_validation": registry_record.get(
                "evidence_grade"
            ),
            "claimed_performance": registry_record.get("claimed_performance"),
            "claim_is_evidence": False,
            "all": all_metrics,
            "train": train_metrics,
            "validation": validation_metrics,
            "oos": oos_metrics,
            "oos_by_regime": regime_metrics,
            "bootstrap": bootstrap,
            "walk_forward": wf,
            "gates": gate,
            "promotion_status": (
                "CANDIDATE"
                if gate["promotion_eligible_before_manual_review"]
                else "HOLD"
            ),
            "manual_review_required": True,
            "do_not_infer_missing_rules": True,
            "negative_evidence_retained": True,
        }

        self.validation_writer.append(result)
        return result

    def validate_all_registered_rules(self) -> Dict[str, Any]:
        if not self.rules:
            return {"status": "NO_RULES", "results": {}}

        # First compute per-strategy raw p-values. FDR correction is applied
        # across the tested strategy family, not one strategy at a time.
        pvalues = {}
        raw_metrics = {}

        for strategy_id in self.rules:
            trades = [
                t for t in self.trades
                if t.strategy_id == strategy_id and t.split == "OOS"
            ]
            metrics = self._metrics(trades)
            raw_metrics[strategy_id] = metrics
            pvalues[strategy_id] = metrics["p_value_sign_test"]

        fdr = benjamini_hochberg(pvalues, self.config.fdr_alpha)

        results = {}
        for strategy_id in self.rules:
            result = self.validate_strategy(
                strategy_id,
                multiple_testing=fdr.get(strategy_id),
            )
            # Recompute the multiple-testing gate explicitly with the result.
            mt = fdr.get(strategy_id, {})
            result["multiple_testing"] = mt
            result["gates"]["checks"]["multiple_testing_control"] = bool(
                mt.get("fdr_reject", False)
            )
            mandatory = {
                "minimum_total_sample",
                "minimum_oos_sample",
                "oos_expectancy_positive",
                "oos_profit_factor",
                "bootstrap_ci_not_negative",
                "walk_forward_min_folds",
                "multiple_testing_control",
            }
            result["gates"][
                "promotion_eligible_before_manual_review"
            ] = all(
                result["gates"]["checks"].get(key, False)
                for key in mandatory
            )
            result["promotion_status"] = (
                "CANDIDATE"
                if result["gates"][
                    "promotion_eligible_before_manual_review"
                ]
                else "HOLD"
            )
            self.validation_writer.append(
                {
                    "event": "MULTIPLE_TESTING_FINALIZED",
                    "strategy_id": strategy_id,
                    "timestamp": utc_now(),
                    "fdr": mt,
                    "promotion_status": result["promotion_status"],
                }
            )
            results[strategy_id] = result

        return {
            "status": "COMPLETED",
            "tested_strategy_count": len(results),
            "fdr_alpha": self.config.fdr_alpha,
            "results": results,
        }

    # ------------------------------------------------------------------------
    # Research summaries
    # ------------------------------------------------------------------------

    def strategy_regime_matrix(self) -> List[Dict[str, Any]]:
        matrix: List[Dict[str, Any]] = []
        by_strategy_regime: Dict[Tuple[str, str], List[TradeObservation]] = (
            defaultdict(list)
        )
        for trade in self.trades:
            by_strategy_regime[
                (trade.strategy_id, trade.regime_at_entry)
            ].append(trade)

        for (strategy_id, regime), trades in sorted(
            by_strategy_regime.items()
        ):
            metric = self._metrics(trades)
            registry = self.registry_by_id.get(strategy_id, {})
            matrix.append(
                {
                    "strategy_id": strategy_id,
                    "strategy_name": registry.get("strategy_name"),
                    "trader": registry.get("trader"),
                    "regime": regime,
                    **metric,
                }
            )
        return matrix

    def export_matrix(self) -> Path:
        path = self.config.replay_dir / "strategy_regime_matrix.jsonl"
        JsonlWriter(path).write_all(self.strategy_regime_matrix())
        return path

    def research_summary(self) -> Dict[str, Any]:
        regimes = [
            x.get("regime")
            for x in self.replay_results
            if isinstance(x.get("regime"), dict)
        ]
        regime_counts: Dict[str, int] = defaultdict(int)
        for regime in regimes:
            regime_counts[str(regime.get("regime", "UNKNOWN"))] += 1

        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "replay_version": REPLAY_VERSION,
            "registry_version": REGISTRY_VERSION,
            "generated_at": utc_now(),
            "input_observations": len(self.market_rows),
            "replayed_observations": len(self.snapshots),
            "rejected_observations": len(self.rejections),
            "symbols": sorted({x["symbol"] for x in self.market_rows}),
            "strategy_dna_count": len(self.registry),
            "executable_rules_count": len(self.rules),
            "trade_observations": len(self.trades),
            "regime_observation_counts": dict(sorted(regime_counts.items())),
            "isolation": {
                "external_opinion_fields_rejected": True,
                "external_regime_consumed": False,
                "alpha_consumed": False,
                "confidence_consumed": False,
                "broker_dependency": False,
            },
            "validation_contract": {
                "chronological_split": True,
                "purge_bars": self.config.purge_bars,
                "embargo_bars": self.config.embargo_bars,
                "oos_required": True,
                "walk_forward_required": True,
                "bootstrap_ci_required": True,
                "cost_slippage_required": True,
                "mfe_mae_required": True,
                "ambiguous_path_recording": True,
                "negative_evidence_required": True,
                "multiple_testing_control": True,
            },
        }

    def finalize(self) -> Path:
        summary_path = self.config.replay_dir / "replay_summary.json"
        summary_path.write_text(
            json.dumps(
                self.research_summary(),
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        return summary_path


# ============================================================================
# 8. EXAMPLE RULE BUILDERS â€” DISABLED BY DEFAULT
# ============================================================================

def make_close_vs_sma_rule(
    strategy_id: str,
    period: int = 20,
    holding_bars: int = 5,
) -> ReplayRule:
    """
    Explicit example rule for testing the replay machinery.

    IMPORTANT:
    This is NOT assigned to any trader's DNA automatically.
    It is a generic research harness only. Do not label its result as a
    trader's strategy.

    It exists so the engine can be mechanically smoke-tested before real
    source-captured rules are added.
    """

    def signal(history: Sequence[MarketSnapshot], index: int) -> Optional[str]:
        if index + 1 >= len(history) or index + 1 < period:
            return None
        closes = [float(x.close) for x in history[index + 1 - period:index + 1]]
        sma = mean(closes)
        close = float(history[index].close)
        if close > sma:
            return "LONG"
        if close < sma:
            return "SHORT"
        return None

    return ReplayRule(
        strategy_id=strategy_id,
        version=f"SMOKE_SMA_{period}_V1",
        description=(
            "Generic replay smoke-test rule only; not trader-attributed."
        ),
        signal=signal,
        holding_bars=holding_bars,
        source_type="REPRODUCIBLE_RULE",
    )


# ============================================================================
# 9. SELF-TEST DATA â€” MECHANICS ONLY, NEVER RESEARCH EVIDENCE
# ============================================================================

def synthetic_observations(
    count: int = 200,
    symbol: str = "GSR_TEST",
) -> List[Dict[str, Any]]:
    """
    Deterministic synthetic candles for pipeline self-test.

    These rows MUST NOT be used as trading evidence.
    """
    rows = []
    base = datetime(2026, 1, 1, 9, 15, tzinfo=timezone.utc)
    price = 100.0

    for i in range(count):
        drift = 0.15 if (i // 25) % 2 == 0 else -0.10
        noise = ((i * 17) % 11 - 5) * 0.01
        op = price
        close = max(1.0, price + drift + noise)
        high = max(op, close) + 0.10
        low = min(op, close) - 0.10
        price = close

        rows.append(
            {
                "timestamp": (base.timestamp() + i * 180).__str__(),
                "symbol": symbol,
                "open": op,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000 + i,
            }
        )

    # Convert the timestamp strings into timezone-aware ISO strings.
    for row in rows:
        row["timestamp"] = datetime.fromtimestamp(
            float(row["timestamp"]),
            tz=timezone.utc,
        ).isoformat()
    return rows


def run_self_test() -> Dict[str, Any]:
    """
    Pipeline mechanics self-test.

    It deliberately does not assert strategy profitability.
    """
    config = ReplayConfig(
        replay_dir=Path("./gsr_data/replay_self_test"),
        min_total_observations=10,
        min_oos_observations=5,
        min_regime_observations=2,
        min_walk_forward_folds=1,
        walk_forward_train_bars=30,
        walk_forward_test_bars=10,
        walk_forward_step_bars=10,
        walk_forward_max_folds=3,
        bootstrap_iterations=100,
        min_effective_trades_for_bootstrap=5,
    )
    replay = HistoricalReplayEngine(config)

    for row in synthetic_observations():
        validate_raw_row(row)
        normalized = normalize_raw_row(row)
        replay.chronology.validate(normalized)
        replay.market_rows.append(normalized)

    replay.replay_market()

    # Use the first registry ID only to prove that explicit rule registration
    # is constrained by the frozen registry.
    strategy_id = str(replay.registry[0]["atomic_strategy_id"])
    replay.register_rule(
        make_close_vs_sma_rule(
            strategy_id=strategy_id,
            period=20,
            holding_bars=3,
        )
    )
    replay.evaluate_registered_rules()
    replay.export_matrix()
    replay.finalize()

    return {
        "status": "PASS",
        "observations": len(replay.snapshots),
        "trades": len(replay.trades),
        "registry_count": len(replay.registry),
        "rule_count": len(replay.rules),
        "warning": (
            "Synthetic data and the generic SMA rule are mechanics-only. "
            "They are NOT strategy evidence."
        ),
    }


# ============================================================================
# 10. CLI
# ============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "GSR-1.1.0 chronological historical replay and validation "
            "laboratory."
        )
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Historical CSV, JSONL/NDJSON or GSR SQLite raw-store path.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=str(DEFAULT_REPLAY_DIR),
        help="Replay output directory.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional maximum number of observations to replay.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run mechanics-only synthetic self-test.",
    )
    parser.add_argument(
        "--export-matrix",
        action="store_true",
        help="Export strategy Ã— GSR-regime matrix after replay.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.self_test:
        result = run_self_test()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if not args.source:
        parser.error("--source is required unless --self-test is used.")

    config = ReplayConfig(replay_dir=Path(args.out))
    replay = HistoricalReplayEngine(config)

    source = Path(args.source)
    loaded = replay.load(source, max_rows=args.max_rows)
    print(
        json.dumps(
            {
                "event": "SOURCE_LOADED",
                "source": str(source),
                "rows_loaded": loaded,
            },
            indent=2,
        )
    )

    replay_result = replay.replay_market()
    print(json.dumps(replay_result, indent=2, ensure_ascii=False))

    replay.evaluate_registered_rules()
    validation_result = replay.validate_all_registered_rules()
    print(
        json.dumps(
            {
                "event": "EXECUTABLE_RULE_VALIDATION",
                "status": validation_result.get("status"),
                "tested_strategy_count": validation_result.get(
                    "tested_strategy_count", 0
                ),
                "fdr_controlled": True,
            },
            indent=2,
            ensure_ascii=False,
        )
    )

    if args.export_matrix:
        matrix_path = replay.export_matrix()
        print(f"Strategy-regime matrix: {matrix_path}")

    summary_path = replay.finalize()
    print(f"Replay summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
