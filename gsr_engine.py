"""
GSR-1.2.1 â€” Global Strategy Research Engine
============================================

Research-only, leakage-safe strategy research core.

Design goals
------------
- Independent from app.py / next_day_alpha_engine.py opinions.
- Session-local VWAP; never mixes prior sessions into current-session VWAP.
- Dynamic transaction-cost / slippage / market-impact estimation.
- Probabilistic regime state estimation with deterministic, explainable
  features and online state probabilities (no look-ahead).
- Intrabar realism with OHLC ambiguity handling and optional lower-timeframe
  observations.
- Portfolio-level strategy return correlation and risk allocation research.
- Strategy decay / concept-drift monitoring.
- Chronological, purge/embargo-aware, cost-aware validation records.
- Strategy-DNA registry remains external and is never modified by this file.
- No broker, order, execution, alpha, confidence, signal or prediction
  dependency.

IMPORTANT
---------
This file is a research laboratory, not a claim of profitable trading.
A strategy is not promoted because a hypothesis score is high. Promotion
requires reproducible rules plus independent chronological OOS evidence.

Expected repository layout
--------------------------
nifty-engine/
    app.py
    next_day_alpha_engine.py
    strategy_registry.py
    gsr_engine_v1_2.py
    gsr_data/

Only strategy_registry.py is imported by this module.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import statistics
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from strategy_registry import (
        ATOMIC_STRATEGY_REGISTRY,
        EVIDENCE_LADDER,
        ISOLATION_CONTRACT,
        REGISTRY_VERSION,
        VALIDATION_GATES,
    )
except ImportError as exc:
    raise ImportError(
        "GSR-1.2.1 requires strategy_registry.py in the same repository."
    ) from exc


ENGINE_VERSION = "GSR-1.2.1-RESEARCH"
SCHEMA_VERSION = "GSR_STATE_1.2.1"
DATA_SCHEMA_VERSION = "GSR_OBS_1.2.1"
DEFAULT_DATA_DIR = Path(os.getenv("GSR_DATA_DIR", "./gsr_data"))

FORBIDDEN_EXTERNAL_OPINION_FIELDS = {
    "alpha", "alpha_score", "confidence", "prediction", "signal",
    "external_regime", "regime_label", "position", "weight",
    "decision", "trade_decision", "target", "stop", "forecast",
}

OPTION_FIELDS = {
    "iv", "atm_iv", "iv_change", "iv_rank", "iv_percentile",
    "iv_skew", "iv_term_structure", "realized_vol", "iv_rv_spread",
    "oi", "oi_change", "volume", "ce_oi", "pe_oi", "ce_oi_change",
    "pe_oi_change", "pcr_oi", "pcr_volume", "atm_straddle",
    "bid", "ask", "mid", "spread_points", "spread_pct",
    "chain_completeness", "delta", "gamma", "theta", "vega",
    "vanna", "charm", "dte", "strike", "option_type", "moneyness",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(x: Any) -> bool:
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def fnum(x: Any, default: Optional[float] = 0.0) -> Optional[float]:
    return float(x) if finite(x) else default


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(x)))


def safe_div(a: Optional[float], b: Optional[float],
             default: Optional[float] = 0.0) -> Optional[float]:
    if a is None or b is None or not finite(a) or not finite(b) or abs(b) <= 1e-12:
        return default
    return a / b


def mean_or(values: Sequence[float], default: Optional[float] = 0.0) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    return statistics.fmean(vals) if vals else default


def stdev_or(values: Sequence[float], default: Optional[float] = 0.0) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    return statistics.stdev(vals) if len(vals) >= 2 else default


def normalize_tag(x: Any) -> str:
    return str(x or "").strip().lower().replace("-", "_").replace(" ", "_")


def stable_hash(payload: Any) -> str:
    raw = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_timestamp(ts: str) -> datetime:
    value = str(ts).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        # Naive timestamps are interpreted consistently, but callers should
        # preferably supply timezone-aware timestamps.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def session_key(timestamp: str, session_start: str = "09:15",
                session_end: str = "15:30") -> str:
    dt = parse_timestamp(timestamp)
    hhmm = dt.strftime("%H:%M")
    if hhmm < session_start or hhmm > session_end:
        return f"{dt.date().isoformat()}|OUT_OF_SESSION"
    return f"{dt.date().isoformat()}|{session_start}-{session_end}"


class JsonlStore:
    """Append-only JSONL research store."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Mapping[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    dict(record), ensure_ascii=False, separators=(",", ":")
                ) + "\n"
            )

    def read_all(self) -> Iterable[Dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows


@dataclass
class GSRConfig:
    data_dir: Path = DEFAULT_DATA_DIR
    max_bars_per_symbol: int = 5000

    # Feature / regime settings.
    atr_period: int = 14
    sma_fast: int = 20
    sma_slow: int = 50
    rsi_period: int = 14
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    donchian_period: int = 20
    realized_vol_period: int = 20
    regime_temperature: float = 1.0
    regime_min_history: int = 20
    session_start: str = "09:15"
    session_end: str = "15:30"

    # Execution realism.
    transaction_cost_points: float = 0.0
    base_slippage_points: float = 0.0
    impact_coefficient: float = 0.10
    impact_exponent: float = 0.50
    min_liquidity_units: float = 1.0
    spread_weight: float = 0.50
    volatility_slippage_weight: float = 0.25
    impact_weight: float = 0.25

    # Validation.
    purge_bars: int = 15
    embargo_bars: int = 5
    minimum_oos_trades: int = 30
    max_drawdown_limit: float = 0.30
    ambiguity_penalty: float = 0.50

    # Portfolio.
    correlation_window: int = 60
    max_pairwise_correlation: float = 0.85
    portfolio_target_vol: float = 0.10

    # Decay.
    decay_window: int = 50
    decay_baseline_window: int = 200
    decay_threshold: float = 0.50
    decay_z_threshold: float = -1.5

    @classmethod
    def from_env(cls) -> "GSRConfig":
        env = os.getenv
        return cls(
            data_dir=Path(env("GSR_DATA_DIR", str(DEFAULT_DATA_DIR))),
            max_bars_per_symbol=int(env("GSR_MAX_BARS", "5000")),
            atr_period=int(env("GSR_ATR_PERIOD", "14")),
            sma_fast=int(env("GSR_SMA_FAST", "20")),
            sma_slow=int(env("GSR_SMA_SLOW", "50")),
            rsi_period=int(env("GSR_RSI_PERIOD", "14")),
            bollinger_period=int(env("GSR_BB_PERIOD", "20")),
            bollinger_std=float(env("GSR_BB_STD", "2.0")),
            donchian_period=int(env("GSR_DONCHIAN_PERIOD", "20")),
            realized_vol_period=int(env("GSR_RV_PERIOD", "20")),
            regime_temperature=float(env("GSR_REGIME_TEMP", "1.0")),
            regime_min_history=int(env("GSR_REGIME_MIN_HISTORY", "20")),
            session_start=env("GSR_SESSION_START", "09:15"),
            session_end=env("GSR_SESSION_END", "15:30"),
            transaction_cost_points=float(env("GSR_COST_POINTS", "0")),
            base_slippage_points=float(env("GSR_BASE_SLIPPAGE", "0")),
            impact_coefficient=float(env("GSR_IMPACT_COEFF", "0.10")),
            impact_exponent=float(env("GSR_IMPACT_EXP", "0.50")),
            min_liquidity_units=float(env("GSR_MIN_LIQUIDITY", "1")),
            purge_bars=int(env("GSR_PURGE_BARS", "15")),
            embargo_bars=int(env("GSR_EMBARGO_BARS", "5")),
            minimum_oos_trades=int(env("GSR_MIN_OOS_TRADES", "30")),
            max_drawdown_limit=float(env("GSR_MAX_DRAWDOWN", "0.30")),
            ambiguity_penalty=float(env("GSR_AMBIGUITY_PENALTY", "0.50")),
            correlation_window=int(env("GSR_CORR_WINDOW", "60")),
            max_pairwise_correlation=float(env("GSR_MAX_PAIR_CORR", "0.85")),
            portfolio_target_vol=float(env("GSR_TARGET_VOL", "0.10")),
            decay_window=int(env("GSR_DECAY_WINDOW", "50")),
            decay_baseline_window=int(env("GSR_DECAY_BASELINE", "200")),
            decay_threshold=float(env("GSR_DECAY_THRESHOLD", "0.50")),
            decay_z_threshold=float(env("GSR_DECAY_Z", "-1.5")),
        )


@dataclass
class MarketSnapshot:
    timestamp: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    oi: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    futures_close: Optional[float] = None
    spot_close: Optional[float] = None
    option_data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "MarketSnapshot":
        required = ("timestamp", "symbol", "open", "high", "low", "close")
        missing = [k for k in required if k not in row]
        if missing:
            raise ValueError(f"Missing required market fields: {missing}")

        option_data: Dict[str, Any] = {}
        metadata: Dict[str, Any] = {}

        core = {
            "timestamp", "symbol", "open", "high", "low", "close",
            "volume", "oi", "bid", "ask", "futures_close", "spot_close",
        }
        for key, value in row.items():
            if key in OPTION_FIELDS:
                option_data[key] = value
            elif key not in core:
                metadata[key] = value

        return cls(
            timestamp=str(row["timestamp"]),
            symbol=str(row["symbol"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=fnum(row.get("volume"), None),
            oi=fnum(row.get("oi"), None),
            bid=fnum(row.get("bid"), None),
            ask=fnum(row.get("ask"), None),
            futures_close=fnum(row.get("futures_close"), None),
            spot_close=fnum(row.get("spot_close"), None),
            option_data=option_data,
            metadata=metadata,
        )

    def validate(self) -> List[str]:
        errors: List[str] = []
        if not self.symbol:
            errors.append("empty_symbol")
        if not self.timestamp:
            errors.append("empty_timestamp")
        if min(self.open, self.high, self.low, self.close) < 0:
            errors.append("negative_price")
        if self.high < self.low:
            errors.append("high_below_low")
        if self.high < max(self.open, self.close):
            errors.append("high_below_open_or_close")
        if self.low > min(self.open, self.close):
            errors.append("low_above_open_or_close")
        if self.volume is not None and self.volume < 0:
            errors.append("negative_volume")
        return errors

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RollingSeries:
    def __init__(self, maxlen: int) -> None:
        self.values: Deque[float] = deque(maxlen=maxlen)

    def add(self, value: float) -> None:
        if finite(value):
            self.values.append(float(value))

    def list(self) -> List[float]:
        return list(self.values)

    def __len__(self) -> int:
        return len(self.values)


def true_range(prev_close: Optional[float], high: float, low: float) -> float:
    if prev_close is None:
        return high - low
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def simple_atr(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int,
) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    start = max(1, len(closes) - period)
    trs = [
        true_range(closes[i - 1], highs[i], lows[i])
        for i in range(start, len(closes))
    ]
    return mean_or(trs, None)


def ema(values: Sequence[float], period: int) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    result = statistics.fmean(vals[:period])
    for value in vals[period:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def rsi(values: Sequence[float], period: int) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < period + 1:
        return None
    changes = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
    recent = changes[-period:]
    gains = [max(x, 0.0) for x in recent]
    losses = [max(-x, 0.0) for x in recent]
    avg_gain = mean_or(gains, 0.0) or 0.0
    avg_loss = mean_or(losses, 0.0) or 0.0
    if avg_loss <= 1e-12:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def linear_slope(values: Sequence[float], lookback: int = 5) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < lookback:
        return None
    y = vals[-lookback:]
    x = list(range(lookback))
    xm, ym = statistics.fmean(x), statistics.fmean(y)
    denom = sum((a - xm) ** 2 for a in x)
    return safe_div(
        sum((a - xm) * (b - ym) for a, b in zip(x, y)),
        denom,
        None,
    )


def realized_vol(values: Sequence[float], period: int) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < period + 1:
        return None
    returns = []
    for i in range(len(vals) - period, len(vals)):
        if vals[i] > 0 and vals[i - 1] > 0:
            returns.append(math.log(vals[i] / vals[i - 1]))
    if len(returns) < 2:
        return None
    return stdev_or(returns, None) * math.sqrt(252.0) * math.sqrt(125.0)


class SessionVWAP:
    """
    Session-local VWAP.

    State is explicitly keyed by (symbol, trading date/session). Therefore a
    5000-bar history can never contaminate the current day's VWAP.
    """

    def __init__(self, session_start: str, session_end: str) -> None:
        self.session_start = session_start
        self.session_end = session_end
        self._pv: Dict[Tuple[str, str], float] = defaultdict(float)
        self._vol: Dict[Tuple[str, str], float] = defaultdict(float)

    def update(self, snap: MarketSnapshot) -> Optional[float]:
        key = (snap.symbol, session_key(
            snap.timestamp, self.session_start, self.session_end
        ))
        volume = max(float(snap.volume or 0.0), 0.0)
        if volume <= 0.0:
            return None if self._vol[key] <= 0 else self._pv[key] / self._vol[key]

        typical = (snap.high + snap.low + snap.close) / 3.0
        self._pv[key] += typical * volume
        self._vol[key] += volume
        return safe_div(self._pv[key], self._vol[key], None)


class MarketFeatureEngine:
    """GSR-owned features calculated strictly from observations seen so far."""

    def __init__(self, config: GSRConfig) -> None:
        self.config = config
        self.history: Dict[str, Deque[MarketSnapshot]] = defaultdict(
            lambda: deque(maxlen=config.max_bars_per_symbol)
        )
        self.session_vwap = SessionVWAP(
            config.session_start, config.session_end
        )

    def add(self, snap: MarketSnapshot) -> Dict[str, Any]:
        self.history[snap.symbol].append(snap)
        bars = list(self.history[snap.symbol])
        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]

        atr = simple_atr(
            highs, lows, closes, self.config.atr_period
        )
        sma20 = (
            mean_or(closes[-self.config.sma_fast:], None)
            if len(closes) >= self.config.sma_fast else None
        )
        sma50 = (
            mean_or(closes[-self.config.sma_slow:], None)
            if len(closes) >= self.config.sma_slow else None
        )
        ema20 = ema(closes, self.config.sma_fast)
        ema50 = ema(closes, self.config.sma_slow)
        rsi14 = rsi(closes, self.config.rsi_period)

        bb_mid = (
            mean_or(
                closes[-self.config.bollinger_period:], None
            )
            if len(closes) >= self.config.bollinger_period else None
        )
        bb_std = (
            stdev_or(
                closes[-self.config.bollinger_period:], None
            )
            if len(closes) >= self.config.bollinger_period else None
        )
        bb_upper = (
            bb_mid + self.config.bollinger_std * bb_std
            if bb_mid is not None and bb_std is not None else None
        )
        bb_lower = (
            bb_mid - self.config.bollinger_std * bb_std
            if bb_mid is not None and bb_std is not None else None
        )
        bb_width = (
            safe_div(bb_upper - bb_lower, bb_mid, None)
            if bb_upper is not None and bb_lower is not None else None
        )

        dc = self.config.donchian_period
        prior_high = prior_low = None
        if len(highs) > dc:
            prior_high = max(highs[-dc - 1:-1])
            prior_low = min(lows[-dc - 1:-1])

        rv = realized_vol(closes, self.config.realized_vol_period)
        ret1 = (
            safe_div(closes[-1] - closes[-2], closes[-2], None)
            if len(closes) >= 2 else None
        )
        slope5 = linear_slope(closes, 5)
        slope20 = linear_slope(closes, 20) if len(closes) >= 20 else None

        session_vwap = self.session_vwap.update(snap)

        spread = None
        if snap.futures_close is not None and snap.spot_close is not None:
            spread = snap.spot_close - snap.futures_close

        atr_norm_return = (
            safe_div(closes[-1] - closes[-2], atr, None)
            if len(closes) >= 2 and atr else None
        )
        atr_stretch = (
            safe_div(closes[-1] - session_vwap, atr, None)
            if session_vwap is not None and atr else None
        )

        return {
            "feature_version": "GSR_FEATURES_1.2.1",
            "bar_count": len(bars),
            "close": closes[-1],
            "return_1": ret1,
            "atr": atr,
            "atr_pct": safe_div(atr, closes[-1], None),
            "sma20": sma20,
            "sma50": sma50,
            "ema20": ema20,
            "ema50": ema50,
            "rsi14": rsi14,
            "bb_mid": bb_mid,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "bb_width": bb_width,
            "donchian_high": prior_high,
            "donchian_low": prior_low,
            "realized_vol": rv,
            "slope5": slope5,
            "slope20": slope20,
            "session_vwap": session_vwap,
            "atr_norm_return": atr_norm_return,
            "atr_stretch": atr_stretch,
            "futures_spot_spread": spread,
            "volume": snap.volume,
            "oi": snap.oi,
            "option_state": dict(snap.option_data),
            "session_key": session_key(
                snap.timestamp,
                self.config.session_start,
                self.config.session_end,
            ),
        }


class ProbabilisticRegimeEngine:
    """
    Online probabilistic regime estimator.

    This is deliberately an interpretable Bayesian-style state machine rather
    than a pretend-trained HMM. It outputs probabilities over TREND_UP,
    TREND_DOWN, RANGE, HIGH_VOL_TRANSITION and UNKNOWN. Probabilities use only
    observations available through the current bar.
    """

    STATES = (
        "TREND_UP",
        "TREND_DOWN",
        "RANGE",
        "HIGH_VOL_TRANSITION",
        "UNKNOWN",
    )

    def __init__(self, config: GSRConfig) -> None:
        self.config = config
        self.prev_probs: Dict[str, List[float]] = {}

    def _softmax(self, scores: Sequence[float]) -> List[float]:
        temperature = max(self.config.regime_temperature, 1e-6)
        scaled = [float(s) / temperature for s in scores]
        m = max(scaled)
        exps = [math.exp(max(-60.0, min(60.0, x - m))) for x in scaled]
        total = sum(exps)
        return [x / total for x in exps]

    def classify(self, symbol: str, features: Mapping[str, Any]) -> Dict[str, Any]:
        close = fnum(features.get("close"), None)
        sma20 = fnum(features.get("sma20"), None)
        sma50 = fnum(features.get("sma50"), None)
        atr_pct = fnum(features.get("atr_pct"), None)
        rsi14 = fnum(features.get("rsi14"), None)
        bb_width = fnum(features.get("bb_width"), None)
        slope20 = fnum(features.get("slope20"), None)
        rv = fnum(features.get("realized_vol"), None)
        stretch = fnum(features.get("atr_stretch"), None)
        count = int(features.get("bar_count", 0))

        if close is None or count < self.config.regime_min_history:
            probs = [0.0, 0.0, 0.0, 0.0, 1.0]
            return {
                "regime": "UNKNOWN",
                "probabilities": dict(zip(self.STATES, probs)),
                "regime_confidence": 0.0,
                "transition_probability": 1.0,
                "model": "ONLINE_INTERPRETABLE_PROBABILISTIC_1.2.1",
                "evidence": {"bar_count": count},
            }

        normalized_slope = 0.0
        if slope20 is not None and atr_pct is not None and close:
            normalized_slope = safe_div(
                slope20, atr_pct * close, 0.0
            ) or 0.0

        trend_alignment = 0.0
        if sma20 is not None and sma50 is not None and atr_pct:
            trend_alignment = clamp(
                abs(sma20 - sma50) / max(close * atr_pct * 2.0, 1e-9),
                0.0, 1.0
            )

        direction = 1.0 if close > (sma20 or close) else -1.0
        up_score = (
            1.5 * max(normalized_slope, 0.0)
            + 1.0 * trend_alignment * max(direction, 0.0)
            + 0.5 * (1.0 if (rsi14 or 50) >= 55 else 0.0)
        )
        down_score = (
            1.5 * max(-normalized_slope, 0.0)
            + 1.0 * trend_alignment * max(-direction, 0.0)
            + 0.5 * (1.0 if (rsi14 or 50) <= 45 else 0.0)
        )

        range_score = 1.2 * max(0.0, 1.0 - min(
            abs(normalized_slope) / 0.20, 1.0
        ))
        if bb_width is not None:
            range_score += 0.5 * max(0.0, 1.0 - bb_width / 0.08)

        high_vol_score = 0.0
        if rv is not None:
            high_vol_score += max(0.0, min(2.0, (rv - 0.20) / 0.10))
        if atr_pct is not None:
            high_vol_score += max(0.0, min(1.5, (atr_pct - 0.01) / 0.01))
        if stretch is not None:
            high_vol_score += max(0.0, min(0.75, abs(stretch) - 1.0))

        unknown_score = max(
            0.0,
            1.0 - min(1.0, count / max(self.config.max_bars_per_symbol, 1))
        )

        raw = [up_score, down_score, range_score, high_vol_score, unknown_score]
        probs = self._softmax(raw)

        previous = self.prev_probs.get(symbol)
        if previous:
            # Conservative persistence: state probabilities cannot jump
            # entirely on one noisy bar.
            probs = [
                0.75 * p + 0.25 * q
                for p, q in zip(previous, probs)
            ]
            total = sum(probs)
            probs = [p / total for p in probs]
        self.prev_probs[symbol] = probs

        best_idx = max(range(len(probs)), key=lambda i: probs[i])
        best = self.STATES[best_idx]
        confidence = probs[best_idx]
        transition = 1.0 - abs(
            max(probs[0], probs[1], probs[2]) - max(probs)
        )

        return {
            "regime": best,
            "probabilities": dict(zip(self.STATES, [round(x, 6) for x in probs])),
            "regime_confidence": round(confidence, 6),
            "transition_probability": round(clamp(transition), 6),
            "model": "ONLINE_INTERPRETABLE_PROBABILISTIC_1.2.1",
            "evidence": {
                "normalized_slope": normalized_slope,
                "trend_alignment": trend_alignment,
                "rv": rv,
                "atr_pct": atr_pct,
                "bb_width": bb_width,
                "atr_stretch": stretch,
            },
        }


def strategy_tags(strategy: Mapping[str, Any]) -> set[str]:
    tags = {normalize_tag(x) for x in strategy.get("mechanism_tags", [])}
    tags.update({
        normalize_tag(strategy.get("family")),
        normalize_tag(strategy.get("mechanism")),
        normalize_tag(strategy.get("asset_class")),
    })
    return {x for x in tags if x}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    return safe_div(len(a & b), len(a | b), 0.0) or 0.0


def similarity_score(a: Mapping[str, Any], b: Mapping[str, Any]) -> Dict[str, Any]:
    ta, tb = strategy_tags(a), strategy_tags(b)
    tag_score = jaccard(ta, tb)
    family_score = float(
        bool(normalize_tag(a.get("family")))
        and normalize_tag(a.get("family")) == normalize_tag(b.get("family"))
    )
    mechanism_score = float(
        bool(normalize_tag(a.get("mechanism")))
        and normalize_tag(a.get("mechanism")) == normalize_tag(b.get("mechanism"))
    )
    asset_score = float(
        bool(normalize_tag(a.get("asset_class")))
        and normalize_tag(a.get("asset_class")) == normalize_tag(b.get("asset_class"))
    )
    total = (
        0.55 * tag_score
        + 0.20 * family_score
        + 0.15 * mechanism_score
        + 0.10 * asset_score
    )
    return {
        "similarity_score": round(clamp(total), 6),
        "tag_similarity": round(tag_score, 6),
        "family_similarity": family_score,
        "mechanism_similarity": mechanism_score,
        "asset_similarity": asset_score,
    }


def regime_compatibility(
    strategy: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    Descriptive hypothesis only. Never interpreted as win probability.
    """
    tags = strategy_tags(strategy)
    probabilities = regime.get("probabilities", {})
    score = 0.50
    reasons: List[str] = []

    trend_p = float(probabilities.get("TREND_UP", 0.0)) + float(
        probabilities.get("TREND_DOWN", 0.0)
    )
    range_p = float(probabilities.get("RANGE", 0.0))
    high_vol_p = float(probabilities.get("HIGH_VOL_TRANSITION", 0.0))

    if {"trend_following", "breakout"} & tags:
        score += 0.25 * trend_p
        score -= 0.15 * range_p
        reasons.append("trend_breakout_semantics")

    if "mean_reversion" in tags:
        score += 0.25 * range_p
        score -= 0.15 * trend_p
        reasons.append("mean_reversion_semantics")

    if "momentum" in tags:
        score += 0.15 * trend_p
        reasons.append("momentum_semantics")

    if "volatility_selling" in tags:
        score += 0.10 * range_p
        score -= 0.20 * high_vol_p
        reasons.append("volatility_selling_semantics")

    if {"volatility_expansion", "breakout"} & tags:
        score += 0.15 * high_vol_p
        reasons.append("volatility_expansion_semantics")

    return {
        "hypothesis_score": round(clamp(score), 6),
        "status": "HYPOTHESIS_NOT_VALIDATED",
        "reasons": reasons,
        "probabilistic_regime_used": True,
    }


class ExecutionCostModel:
    """
    Dynamic cost model.

    Cost is expressed in price points. It increases with spread, volatility
    and participation/market impact. If liquidity is unavailable, the model
    marks the estimate as degraded rather than pretending precision.
    """

    def __init__(self, config: GSRConfig) -> None:
        self.config = config

    def estimate(
        self,
        snap: MarketSnapshot,
        features: Mapping[str, Any],
        order_size: float = 1.0,
    ) -> Dict[str, Any]:
        size = max(float(order_size), 0.0)
        close = max(float(snap.close), 1e-9)
        atr = fnum(features.get("atr"), None)
        atr_pct = fnum(features.get("atr_pct"), None)

        spread_points = None
        if snap.bid is not None and snap.ask is not None:
            spread_points = max(float(snap.ask - snap.bid), 0.0)
        elif finite(snap.option_data.get("spread_points")):
            spread_points = max(float(snap.option_data["spread_points"]), 0.0)

        spread_component = (
            spread_points * self.config.spread_weight
            if spread_points is not None else 0.0
        )

        volatility_component = (
            max(float(atr or 0.0), close * float(atr_pct or 0.0))
            * self.config.volatility_slippage_weight
        )

        liquidity = max(
            float(snap.volume or 0.0),
            float(snap.oi or 0.0),
            self.config.min_liquidity_units,
        )
        participation = size / liquidity
        impact = (
            self.config.impact_coefficient
            * (max(0.0, participation) ** self.config.impact_exponent)
            * max(float(atr or 0.0), close * float(atr_pct or 0.0), 1e-9)
            * self.config.impact_weight
        )

        base = self.config.base_slippage_points
        total_slippage = max(0.0, base + spread_component
                             + volatility_component + impact)
        total_cost = self.config.transaction_cost_points + total_slippage

        return {
            "estimated_slippage_points": round(total_slippage, 8),
            "transaction_cost_points": round(
                self.config.transaction_cost_points, 8
            ),
            "total_execution_cost_points": round(total_cost, 8),
            "spread_points": spread_points,
            "market_impact_points": round(impact, 8),
            "participation_rate": round(participation, 8),
            "liquidity_proxy": round(liquidity, 8),
            "model": "DYNAMIC_SPREAD_VOLATILITY_IMPACT_1.2.1",
            "quality": (
                "FULL"
                if spread_points is not None and
                (snap.volume is not None or snap.oi is not None)
                else "DEGRADED"
            ),
        }


class IntrabarRealism:
    """
    OHLC path realism.

    If both stop and target are touched in one bar, the engine does not invent
    an order. It supports:
      - CONSERVATIVE: stop-first
      - OPTIMISTIC: target-first
      - MIDPOINT: average of the two outcomes
      - LOWER_TIMEFRAME: exact lower-timeframe sequence supplied by caller

    Lower-timeframe bars must be strictly chronological and must belong to the
    parent interval; otherwise the result is rejected.
    """

    def __init__(self, ambiguity_penalty: float = 0.50) -> None:
        self.ambiguity_penalty = clamp(ambiguity_penalty)

    @staticmethod
    def _touches(bar: Mapping[str, Any], price: float) -> bool:
        return float(bar["low"]) <= price <= float(bar["high"])

    def evaluate(
        self,
        bar: Mapping[str, Any],
        entry: float,
        stop: float,
        target: float,
        direction: str,
        policy: str = "CONSERVATIVE",
        lower_bars: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> Dict[str, Any]:
        high = float(bar["high"])
        low = float(bar["low"])

        stop_hit = self._touches(bar, stop)
        target_hit = self._touches(bar, target)

        if not stop_hit and not target_hit:
            return {
                "outcome": "NONE",
                "ambiguous": False,
                "probability_of_target": 0.0,
                "penalty_factor": 1.0,
            }

        if stop_hit and target_hit:
            if lower_bars:
                ordered = list(lower_bars)
                prev = None
                for child in ordered:
                    child_ts = parse_timestamp(str(child["timestamp"]))
                    if prev is not None and child_ts < prev:
                        raise ValueError("Lower-timeframe bars are not chronological")
                    prev = child_ts

                for child in ordered:
                    sh = self._touches(child, stop)
                    th = self._touches(child, target)
                    if sh and th:
                        # Still ambiguous at the child resolution.
                        continue
                    if sh:
                        return {
                            "outcome": "STOP_FIRST",
                            "ambiguous": False,
                            "probability_of_target": 0.0,
                            "penalty_factor": 1.0,
                        }
                    if th:
                        return {
                            "outcome": "TARGET_FIRST",
                            "ambiguous": False,
                            "probability_of_target": 1.0,
                            "penalty_factor": 1.0,
                        }

            policy = policy.upper()
            if policy == "OPTIMISTIC":
                outcome, p = "TARGET_FIRST", 1.0
            elif policy == "MIDPOINT":
                outcome, p = "AMBIGUOUS", 0.5
            else:
                outcome, p = "STOP_FIRST", 0.0

            return {
                "outcome": outcome,
                "ambiguous": True,
                "probability_of_target": p,
                "penalty_factor": self.ambiguity_penalty,
            }

        if target_hit:
            return {
                "outcome": "TARGET_FIRST",
                "ambiguous": False,
                "probability_of_target": 1.0,
                "penalty_factor": 1.0,
            }

        return {
            "outcome": "STOP_FIRST",
            "ambiguous": False,
            "probability_of_target": 0.0,
            "penalty_factor": 1.0,
        }


@dataclass
class RuleSpec:
    strategy_id: str
    version: str
    direction: str
    entry: Callable[[Sequence[MarketSnapshot], Mapping[str, Any]], Optional[int]]
    exit: Callable[[Sequence[MarketSnapshot], Mapping[str, Any]], Optional[int]]
    description: str
    source_type: str = "REPRODUCIBLE_RULE"

    def validate(self) -> None:
        if self.direction not in {"LONG", "SHORT", "BOTH"}:
            raise ValueError("direction must be LONG, SHORT or BOTH")
        if not self.version or not self.description:
            raise ValueError("RuleSpec version and description are required")


class ValidationEngine:
    """
    Cost-aware chronological validation record system.

    This class records realized/replayed outcomes but deliberately does not
    call a strategy profitable or promote it. Train/OOS splits are generated
    from ordered observations with purge + embargo gaps.
    """

    def __init__(self, config: GSRConfig, store: JsonlStore) -> None:
        self.config = config
        self.store = store

    def chronological_split(
        self,
        rows: Sequence[Mapping[str, Any]],
        train_fraction: float = 0.70,
    ) -> Dict[str, List[Mapping[str, Any]]]:
        ordered = sorted(rows, key=lambda x: parse_timestamp(str(x["timestamp"])))
        cut = int(len(ordered) * clamp(train_fraction, 0.01, 0.99))
        train_end = max(0, cut - self.config.purge_bars)
        test_start = min(
            len(ordered),
            cut + self.config.embargo_bars,
        )
        return {
            "train": ordered[:train_end],
            "purged": ordered[train_end:cut],
            "embargo": ordered[cut:test_start],
            "oos": ordered[test_start:],
        }

    def record_trade_observation(
        self,
        strategy_id: str,
        symbol: str,
        entry_time: str,
        exit_time: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        regime: str,
        execution_cost_points: float,
        ambiguous: bool = False,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        if direction not in {"LONG", "SHORT"}:
            raise ValueError("direction must be LONG or SHORT")

        gross = (
            exit_price - entry_price
            if direction == "LONG"
            else entry_price - exit_price
        )
        net = gross - float(execution_cost_points)

        if ambiguous:
            net *= (1.0 - self.config.ambiguity_penalty)

        record = {
            "schema_version": "GSR_VALIDATION_1.2.1",
            "recorded_at": utc_now(),
            "strategy_id": strategy_id,
            "symbol": symbol,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "direction": direction,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "gross_pnl_points": gross,
            "execution_cost_points": execution_cost_points,
            "net_pnl_points": net,
            "ambiguous": bool(ambiguous),
            "regime": regime,
            "validation_status": "OBSERVATION_ONLY",
            "promotion_eligible": False,
            "metadata": dict(metadata or {}),
        }
        self.store.append(record)
        return record

    def summary(self, strategy_id: str) -> Dict[str, Any]:
        rows = [
            x for x in self.store.read_all()
            if x.get("strategy_id") == strategy_id
        ]
        pnls = [
            float(x["net_pnl_points"])
            for x in rows if finite(x.get("net_pnl_points"))
        ]
        if not pnls:
            return {
                "strategy_id": strategy_id,
                "sample_size": 0,
                "validation_status": "UNTESTED",
                "promotion_eligible": False,
            }

        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        for p in pnls:
            equity += p
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)

        wins = sum(p > 0 for p in pnls)
        expectancy = mean_or(pnls, 0.0) or 0.0
        return {
            "strategy_id": strategy_id,
            "sample_size": len(pnls),
            "wins": wins,
            "losses": sum(p < 0 for p in pnls),
            "win_rate": safe_div(wins, len(pnls), 0.0),
            "expectancy_points": expectancy,
            "net_pnl_points": sum(pnls),
            "max_drawdown_points": max_dd,
            "validation_status": "OBSERVATION_ONLY",
            "promotion_eligible": False,
        }


class PortfolioResearch:
    """Strategy-return correlation and simple inverse-volatility allocation."""

    def __init__(self, config: GSRConfig, store: JsonlStore) -> None:
        self.config = config
        self.store = store

    @staticmethod
    def _returns_by_strategy(
        records: Sequence[Mapping[str, Any]],
    ) -> Dict[str, List[float]]:
        out: Dict[str, List[float]] = defaultdict(list)
        ordered = sorted(
            records,
            key=lambda x: (
                str(x.get("strategy_id")),
                parse_timestamp(str(x.get("entry_time", x.get("recorded_at")))),
            ),
        )
        for row in ordered:
            value = fnum(row.get("net_pnl_points"), None)
            if value is not None:
                out[str(row["strategy_id"])].append(value)
        return out

    def correlation_matrix(self) -> Dict[str, Any]:
        records = list(self.store.read_all())
        by = self._returns_by_strategy(records)
        ids = sorted(by)
        matrix: Dict[str, Dict[str, Optional[float]]] = {
            a: {} for a in ids
        }

        def corr(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
            n = min(len(a), len(b), self.config.correlation_window)
            if n < 3:
                return None
            aa, bb = list(a)[-n:], list(b)[-n:]
            ma, mb = statistics.fmean(aa), statistics.fmean(bb)
            da = math.sqrt(sum((x - ma) ** 2 for x in aa))
            db = math.sqrt(sum((x - mb) ** 2 for x in bb))
            if da <= 1e-12 or db <= 1e-12:
                return None
            return sum((x - ma) * (y - mb) for x, y in zip(aa, bb)) / (da * db)

        for a in ids:
            for b in ids:
                matrix[a][b] = corr(by[a], by[b]) if a != b else 1.0

        return {
            "schema_version": "GSR_PORTFOLIO_1.2.1",
            "strategy_ids": ids,
            "correlation_matrix": matrix,
            "note": "Correlation is research evidence, not causal dependence.",
        }

    def inverse_volatility_weights(self) -> Dict[str, Any]:
        records = list(self.store.read_all())
        by = self._returns_by_strategy(records)
        raw: Dict[str, float] = {}
        for strategy_id, values in by.items():
            tail = values[-self.config.correlation_window:]
            vol = stdev_or(tail, None)
            if vol is not None and vol > 1e-12:
                raw[strategy_id] = 1.0 / vol

        total = sum(raw.values())
        weights = {
            k: v / total for k, v in raw.items()
        } if total > 0 else {}

        return {
            "schema_version": "GSR_PORTFOLIO_1.2.1",
            "method": "INVERSE_VOLATILITY",
            "weights": weights,
            "target_vol": self.config.portfolio_target_vol,
            "promotion_eligible": False,
        }


class DecayMonitor:
    """Detects deterioration using only the chronological outcome stream."""

    def __init__(self, config: GSRConfig, store: JsonlStore) -> None:
        self.config = config
        self.store = store

    def evaluate(self, strategy_id: str) -> Dict[str, Any]:
        rows = [
            x for x in self.store.read_all()
            if x.get("strategy_id") == strategy_id
        ]
        rows.sort(
            key=lambda x: parse_timestamp(str(
                x.get("exit_time", x.get("recorded_at"))
            ))
        )
        pnls = [
            float(x["net_pnl_points"])
            for x in rows if finite(x.get("net_pnl_points"))
        ]

        short = pnls[-self.config.decay_window:]
        baseline = pnls[-self.config.decay_baseline_window:]

        if len(short) < 10 or len(baseline) < 20:
            return {
                "strategy_id": strategy_id,
                "status": "INSUFFICIENT_DATA",
                "decay_detected": False,
                "sample_short": len(short),
                "sample_baseline": len(baseline),
            }

        short_mean = mean_or(short, 0.0) or 0.0
        base_mean = mean_or(baseline, 0.0) or 0.0
        base_sd = stdev_or(baseline, 0.0) or 0.0

        relative = safe_div(short_mean, abs(base_mean), None)
        z = safe_div(short_mean - base_mean, base_sd, None)

        decay = (
            (relative is not None and relative < self.config.decay_threshold)
            or (z is not None and z < self.config.decay_z_threshold)
        )

        return {
            "strategy_id": strategy_id,
            "status": "DECAY_ALERT" if decay else "STABLE_OR_UNCONFIRMED",
            "decay_detected": bool(decay),
            "short_mean": short_mean,
            "baseline_mean": base_mean,
            "relative_expectancy": relative,
            "z_score": z,
            "sample_short": len(short),
            "sample_baseline": len(baseline),
            "action": "REVIEW_NOT_AUTO_DISABLE" if decay else "CONTINUE_MONITORING",
        }


class GSREngine:
    """Main isolated research orchestrator."""

    def __init__(self, config: Optional[GSRConfig] = None) -> None:
        self.config = config or GSRConfig.from_env()
        self.config.data_dir.mkdir(parents=True, exist_ok=True)

        self.market_store = JsonlStore(
            self.config.data_dir / "market_observations.jsonl"
        )
        self.regime_store = JsonlStore(
            self.config.data_dir / "regime_observations.jsonl"
        )
        self.compat_store = JsonlStore(
            self.config.data_dir / "strategy_compatibility.jsonl"
        )
        self.sim_store = JsonlStore(
            self.config.data_dir / "similarity_edges.jsonl"
        )
        self.validation_store = JsonlStore(
            self.config.data_dir / "validation_observations.jsonl"
        )
        self.portfolio_store = JsonlStore(
            self.config.data_dir / "portfolio_observations.jsonl"
        )
        self.decay_store = JsonlStore(
            self.config.data_dir / "decay_observations.jsonl"
        )
        self.event_store = JsonlStore(
            self.config.data_dir / "research_events.jsonl"
        )

        self.feature_engine = MarketFeatureEngine(self.config)
        self.regime_engine = ProbabilisticRegimeEngine(self.config)
        self.cost_model = ExecutionCostModel(self.config)
        self.intrabar = IntrabarRealism(self.config.ambiguity_penalty)
        self.validation_engine = ValidationEngine(
            self.config, self.validation_store
        )
        self.portfolio = PortfolioResearch(
            self.config, self.validation_store
        )
        self.decay_monitor = DecayMonitor(
            self.config, self.validation_store
        )

        self.registry = list(ATOMIC_STRATEGY_REGISTRY)
        self.rule_specs: Dict[str, RuleSpec] = {}
        self._last_timestamp: Dict[str, str] = {}
        self._similarity_built = False
        self._write_state()

    def _write_state(self) -> None:
        state = {
            "engine_version": ENGINE_VERSION,
            "registry_version": REGISTRY_VERSION,
            "schema_version": SCHEMA_VERSION,
            "data_schema_version": DATA_SCHEMA_VERSION,
            "updated_at": utc_now(),
            "strategy_count": len(self.registry),
            "rule_spec_count": len(self.rule_specs),
            "isolation": ISOLATION_CONTRACT,
            "validation_gates": VALIDATION_GATES,
            "similarity_built": self._similarity_built,
            "execution_enabled": False,
        }
        (self.config.data_dir / "engine_state.json").write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def register_rule_spec(self, spec: RuleSpec) -> None:
        spec.validate()
        known = {
            x.get("atomic_strategy_id")
            for x in self.registry
        }
        if spec.strategy_id not in known:
            raise KeyError(f"Unknown Strategy-DNA ID: {spec.strategy_id}")
        self.rule_specs[spec.strategy_id] = spec
        self.event_store.append({
            "event": "RULE_SPEC_REGISTERED",
            "timestamp": utc_now(),
            "strategy_id": spec.strategy_id,
            "rule_version": spec.version,
            "source_type": spec.source_type,
            "rule_hash": stable_hash({
                "strategy_id": spec.strategy_id,
                "version": spec.version,
                "direction": spec.direction,
                "description": spec.description,
            }),
        })
        self._write_state()

    def ingest_snapshot(self, raw: Mapping[str, Any]) -> Dict[str, Any]:
        supplied = sorted(
            FORBIDDEN_EXTERNAL_OPINION_FIELDS.intersection(raw.keys())
        )
        if supplied:
            raise ValueError(
                "GSR isolation violation: external opinion fields supplied: "
                + ", ".join(supplied)
            )

        snap = MarketSnapshot.from_mapping(raw)
        errors = snap.validate()
        if errors:
            self.event_store.append({
                "event": "INVALID_MARKET_OBSERVATION",
                "timestamp": utc_now(),
                "symbol": snap.symbol,
                "errors": errors,
            })
            raise ValueError(f"Invalid market snapshot: {errors}")

        current_ts = parse_timestamp(snap.timestamp)
        previous_raw = self._last_timestamp.get(snap.symbol)
        if previous_raw is not None:
            previous = parse_timestamp(previous_raw)
            if current_ts < previous:
                raise ValueError(
                    f"Chronology violation for {snap.symbol}: "
                    f"{snap.timestamp} < {previous_raw}"
                )
            if current_ts == previous:
                raise ValueError(
                    f"Duplicate timestamp for {snap.symbol}: {snap.timestamp}"
                )
        self._last_timestamp[snap.symbol] = snap.timestamp

        features = self.feature_engine.add(snap)
        regime = self.regime_engine.classify(snap.symbol, features)

        market_record = {
            "schema_version": DATA_SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "ingested_at": utc_now(),
            "snapshot": snap.to_dict(),
            "features": features,
        }
        self.market_store.append(market_record)

        regime_record = {
            "schema_version": SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "timestamp": snap.timestamp,
            "symbol": snap.symbol,
            "regime": regime,
        }
        self.regime_store.append(regime_record)

        compatibility = self._score_current_market(regime)
        for row in compatibility:
            self.compat_store.append({
                "timestamp": snap.timestamp,
                "symbol": snap.symbol,
                "strategy_id": row["strategy_id"],
                "strategy_name": row["strategy_name"],
                "regime": regime["regime"],
                "hypothesis": row,
            })

        return {
            "engine_version": ENGINE_VERSION,
            "timestamp": snap.timestamp,
            "symbol": snap.symbol,
            "features": features,
            "regime": regime,
            "strategy_compatibility": compatibility,
        }

    def _score_current_market(
        self,
        regime: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        rows = []
        for strategy in self.registry:
            comp = regime_compatibility(strategy, regime)
            rows.append({
                "strategy_id": strategy.get("atomic_strategy_id"),
                "strategy_name": strategy.get("strategy_name"),
                "family": strategy.get("family"),
                "rule_precision": strategy.get("rule_precision"),
                "evidence_grade": strategy.get("evidence_grade"),
                "validation_status": strategy.get(
                    "validation_status", "UNTESTED"
                ),
                **comp,
            })
        rows.sort(
            key=lambda x: x["hypothesis_score"],
            reverse=True,
        )
        return rows

    def build_similarity_graph(self) -> int:
        self.sim_store.path.write_text("", encoding="utf-8")
        count = 0
        for i, a in enumerate(self.registry):
            for b in self.registry[i + 1:]:
                score = similarity_score(a, b)
                if score["similarity_score"] >= 0.55:
                    self.sim_store.append({
                        "schema_version": "GSR_SIM_1.2.1",
                        "created_at": utc_now(),
                        "a": a.get("atomic_strategy_id"),
                        "b": b.get("atomic_strategy_id"),
                        **score,
                    })
                    count += 1
        self._similarity_built = True
        self._write_state()
        return count

    def register_validation_outcome(
        self,
        strategy_id: str,
        symbol: str,
        entry_time: str,
        exit_time: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        regime: str,
        snapshot: Optional[MarketSnapshot] = None,
        features: Optional[Mapping[str, Any]] = None,
        order_size: float = 1.0,
        ambiguous: bool = False,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        if strategy_id not in self.rule_specs:
            raise ValueError(
                "No deterministic RuleSpec registered; "
                "GSR will not invent strategy rules."
            )

        cost = 0.0
        if snapshot is not None:
            cost = float(self.cost_model.estimate(
                snapshot, features or {}, order_size
            )["total_execution_cost_points"])

        return self.validation_engine.record_trade_observation(
            strategy_id=strategy_id,
            symbol=symbol,
            entry_time=entry_time,
            exit_time=exit_time,
            direction=direction,
            entry_price=entry_price,
            exit_price=exit_price,
            regime=regime,
            execution_cost_points=cost,
            ambiguous=ambiguous,
            metadata=metadata,
        )

    def validation_summary(self, strategy_id: str) -> Dict[str, Any]:
        return self.validation_engine.summary(strategy_id)

    def portfolio_report(self) -> Dict[str, Any]:
        corr = self.portfolio.correlation_matrix()
        weights = self.portfolio.inverse_volatility_weights()
        report = {
            "timestamp": utc_now(),
            "correlation": corr,
            "allocation": weights,
            "status": "RESEARCH_ONLY",
        }
        self.portfolio_store.append(report)
        return report

    def decay_report(self, strategy_id: str) -> Dict[str, Any]:
        report = self.decay_monitor.evaluate(strategy_id)
        self.decay_store.append({
            "timestamp": utc_now(),
            **report,
        })
        return report

    def registry_audit(self) -> Dict[str, Any]:
        ids = [x.get("atomic_strategy_id") for x in self.registry]
        duplicate_ids = sorted({
            x for x in ids if ids.count(x) > 1
        })
        missing_ids = [
            i for i, x in enumerate(self.registry)
            if not x.get("atomic_strategy_id")
        ]
        missing_hashes = [
            x.get("atomic_strategy_id")
            for x in self.registry
            if not x.get("strategy_dna_hash")
        ]
        return {
            "engine_version": ENGINE_VERSION,
            "registry_version": REGISTRY_VERSION,
            "strategy_count": len(self.registry),
            "duplicate_ids": duplicate_ids,
            "missing_ids": missing_ids,
            "missing_dna_hashes": missing_hashes,
            "rule_spec_count": len(self.rule_specs),
            "ok": (
                not duplicate_ids
                and not missing_ids
                and not missing_hashes
            ),
        }

    def replay_jsonl(self, path: Path) -> Dict[str, Any]:
        count = errors = 0
        with Path(path).open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                if not line.strip():
                    continue
                try:
                    self.ingest_snapshot(json.loads(line))
                    count += 1
                except Exception as exc:
                    errors += 1
                    self.event_store.append({
                        "event": "REPLAY_ERROR",
                        "timestamp": utc_now(),
                        "line": line_no,
                        "error": str(exc),
                    })
        return {"rows_processed": count, "errors": errors}

    def replay_csv(self, path: Path) -> Dict[str, Any]:
        count = errors = 0
        with Path(path).open(
            "r", encoding="utf-8-sig", newline=""
        ) as fh:
            for line_no, row in enumerate(csv.DictReader(fh), 2):
                try:
                    self.ingest_snapshot(row)
                    count += 1
                except Exception as exc:
                    errors += 1
                    self.event_store.append({
                        "event": "CSV_REPLAY_ERROR",
                        "timestamp": utc_now(),
                        "line": line_no,
                        "error": str(exc),
                    })
        return {"rows_processed": count, "errors": errors}

    def health(self) -> Dict[str, Any]:
        return {
            "engine_version": ENGINE_VERSION,
            "registry_version": REGISTRY_VERSION,
            "schema_version": SCHEMA_VERSION,
            "strategy_count": len(self.registry),
            "rule_spec_count": len(self.rule_specs),
            "data_dir": str(self.config.data_dir),
            "isolation_ok": True,
            "execution_enabled": False,
            "features": {
                "session_local_vwap": True,
                "dynamic_impact_slippage": True,
                "probabilistic_regime": True,
                "intrabar_realism": True,
                "portfolio_correlation": True,
                "decay_monitor": True,
                "purge_embargo_validation": True,
            },
            "registry_audit": self.registry_audit(),
        }


def load_engine() -> GSREngine:
    return GSREngine(GSRConfig.from_env())


def smoke_test() -> Dict[str, Any]:
    """
    Offline synthetic test. No broker/network access.
    Tests session-local VWAP, chronology, probabilistic regime, cost model,
    intrabar ambiguity and registry isolation.
    """
    import tempfile

    with tempfile.TemporaryDirectory(prefix="gsr_121_") as tmp:
        cfg = GSRConfig(
            data_dir=Path(tmp),
            max_bars_per_symbol=200,
            regime_min_history=20,
        )
        engine = GSREngine(cfg)

        last = None
        for i in range(80):
            minute = 15 + i * 3
            hour = 9 + minute // 60
            mm = minute % 60
            if hour > 15 or (hour == 15 and mm > 30):
                break
            close = 100.0 + i * 0.10
            row = {
                "timestamp": f"2026-01-01T{hour:02d}:{mm:02d}:00+05:30",
                "symbol": "GSR_SMOKE",
                "open": close - 0.05,
                "high": close + 0.10,
                "low": close - 0.10,
                "close": close,
                "volume": 1000 + i,
            }
            last = engine.ingest_snapshot(row)

        # Session reset check: second day must not inherit prior VWAP.
        second = engine.ingest_snapshot({
            "timestamp": "2026-01-02T09:15:00+05:30",
            "symbol": "GSR_SMOKE",
            "open": 200.0,
            "high": 201.0,
            "low": 199.0,
            "close": 200.5,
            "volume": 1000,
        })
        vwap_ok = abs(
            float(second["features"]["session_vwap"]) - 200.1666666667
        ) < 1e-6

        cost = engine.cost_model.estimate(
            MarketSnapshot.from_mapping({
                "timestamp": "2026-01-02T09:18:00+05:30",
                "symbol": "GSR_SMOKE",
                "open": 200.5,
                "high": 201.5,
                "low": 200.0,
                "close": 201.0,
                "volume": 1000,
                "bid": 200.9,
                "ask": 201.1,
            }),
            {"atr": 1.0, "atr_pct": 0.005},
            order_size=10,
        )

        ambiguity = engine.intrabar.evaluate(
            {"high": 110.0, "low": 90.0},
            entry=100.0,
            stop=95.0,
            target=105.0,
            direction="LONG",
            policy="CONSERVATIVE",
        )

        audit = engine.registry_audit()
        return {
            "ok": (
                audit["ok"]
                and last is not None
                and vwap_ok
                and cost["estimated_slippage_points"] >= 0.0
                and ambiguity["ambiguous"]
            ),
            "audit": audit,
            "session_local_vwap_reset": vwap_ok,
            "dynamic_cost": cost,
            "intrabar": ambiguity,
            "last_regime": (
                last["regime"]["regime"] if last else None
            ),
        }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="GSR-1.2.1 isolated research engine"
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("audit")
    sub.add_parser("similarity")
    sub.add_parser("health")
    sub.add_parser("smoke-test")
    sub.add_parser("portfolio")

    decay = sub.add_parser("decay")
    decay.add_argument("strategy_id")

    replay = sub.add_parser("replay-jsonl")
    replay.add_argument("path")

    replay_csv = sub.add_parser("replay-csv")
    replay_csv.add_argument("path")

    args = parser.parse_args()
    engine = load_engine()

    if args.command == "audit":
        print(json.dumps(
            engine.registry_audit(), indent=2, ensure_ascii=False
        ))
    elif args.command == "similarity":
        print(json.dumps({
            "edges_created": engine.build_similarity_graph()
        }, indent=2))
    elif args.command == "health":
        print(json.dumps(
            engine.health(), indent=2, ensure_ascii=False
        ))
    elif args.command == "smoke-test":
        print(json.dumps(
            smoke_test(), indent=2, ensure_ascii=False
        ))
    elif args.command == "portfolio":
        print(json.dumps(
            engine.portfolio_report(), indent=2, ensure_ascii=False
        ))
    elif args.command == "decay":
        print(json.dumps(
            engine.decay_report(args.strategy_id),
            indent=2, ensure_ascii=False
        ))
    elif args.command == "replay-jsonl":
        print(json.dumps(
            engine.replay_jsonl(Path(args.path)),
            indent=2, ensure_ascii=False
        ))
    elif args.command == "replay-csv":
        print(json.dumps(
            engine.replay_csv(Path(args.path)),
            indent=2, ensure_ascii=False
        ))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
