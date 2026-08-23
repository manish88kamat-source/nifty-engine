"""
GSR-1.1.0 â€” Global Strategy Research Engine
============================================

Research-only core engine for the isolated Global Strategy Research (GSR) layer.

ARCHITECTURAL RULES
-------------------
1. GSR consumes raw/normalized market observations only.
2. GSR does NOT consume alpha, confidence, regime, prediction, weights,
   signals, or opinions from app.py / next_day_alpha_engine.py.
3. Trader claims are metadata. Claims are never evidence.
4. Missing strategy rules remain UNKNOWN. This engine never invents them.
5. A strategy can be promoted only through chronological, cost-aware,
   out-of-sample and walk-forward validation.
6. Regime compatibility is descriptive until the strategy has reproducible
   executable rules.
7. This module has no broker/order/execution dependency.
8. Research variants are explicitly isolated from trader-attributed DNA.

EXPECTED REPOSITORY LAYOUT
--------------------------
nifty-engine/
    app.py
    next_day_alpha_engine.py
    strategy_registry.py
    GSR_1.1.0_MASTER_STRATEGY_REGISTRY.txt
    gsr_engine.py                  <-- this file
    gsr_data/                      <-- created automatically

The engine is intentionally standard-library-only.

INPUT
-----
Normalized JSONL or CSV observations. A minimum OHLC observation needs:
timestamp, symbol, open, high, low, close
Optional:
volume, oi, bid, ask, futures_close, spot_close, iv, pcr_oi, pcr_volume,
atm_iv, etc.

The engine can also receive snapshots directly with ingest_snapshot().

OUTPUT
------
gsr_data/
    market_observations.jsonl
    regime_observations.jsonl
    strategy_compatibility.jsonl
    similarity_edges.jsonl
    validation_observations.jsonl
    research_events.jsonl
    engine_state.json

IMPORTANT
---------
The current Strategy-DNA registry contains mechanisms and metadata, but not
complete executable rules for every strategy. Therefore this core engine
does NOT pretend to backtest all 113 strategies. It computes:
- market state/regime,
- strategy/mechanism similarity,
- regime compatibility hypotheses,
- deterministic research observation records,
- validation data when a reproducible rule is later supplied.

A strategy-specific evaluator can be attached later through RULE_SPECS without
changing the registry or the isolation boundary.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import statistics
import time
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
except ImportError:
    # Clear failure instead of silently using a different registry.
    raise ImportError(
        "GSR requires strategy_registry.py in the same directory. "
        "Do not rename or merge the registry into another engine."
    )


ENGINE_VERSION = "GSR-1.1.0-CORE-FROZEN"
SCHEMA_VERSION = "GSR_STATE_1.1"
DATA_SCHEMA_VERSION = "GSR_OBS_1.1"
DEFAULT_DATA_DIR = Path(os.getenv("GSR_DATA_DIR", "./gsr_data"))

# GSR reads these fields if supplied by a raw-data adapter.
# It does not import them from another engine's feature dictionary.
OPTION_FIELDS = {
    "iv", "atm_iv", "iv_change", "iv_rank", "iv_percentile",
    "iv_skew", "iv_term_structure", "realized_vol", "iv_rv_spread",
    "oi", "oi_change", "volume", "ce_oi", "pe_oi", "ce_oi_change",
    "pe_oi_change", "pcr_oi", "pcr_volume", "atm_straddle",
    "bid", "ask", "mid", "spread_points", "spread_pct",
    "chain_completeness", "delta", "gamma", "theta", "vega",
    "vanna", "charm", "dte", "strike", "option_type", "moneyness"
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(x: Any) -> bool:
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def fnum(x: Any, default: float = 0.0) -> float:
    return float(x) if finite(x) else default


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if finite(a) and finite(b) and abs(b) > 1e-12 else default


def mean_or(values: Sequence[float], default: float = 0.0) -> float:
    vals = [float(x) for x in values if finite(x)]
    return statistics.fmean(vals) if vals else default


def stdev_or(values: Sequence[float], default: float = 0.0) -> float:
    vals = [float(x) for x in values if finite(x)]
    return statistics.stdev(vals) if len(vals) >= 2 else default


def normalize_tag(x: Any) -> str:
    return str(x or "").strip().lower().replace("-", "_").replace(" ", "_")


def stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class JsonlStore:
    """Append-only research store. One JSON object per line."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Mapping[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(dict(record), ensure_ascii=False, separators=(",", ":")) + "\n")

    def read_all(self) -> Iterable[Dict[str, Any]]:
        if not self.path.exists():
            return []
        out: List[Dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return out


@dataclass
class GSRConfig:
    data_dir: Path = DEFAULT_DATA_DIR
    max_bars_per_symbol: int = 5000
    similarity_min_score: float = 0.55
    minimum_regime_bars: int = 20
    atr_period: int = 14
    sma_fast: int = 20
    sma_slow: int = 50
    rsi_period: int = 14
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    donchian_period: int = 20
    realized_vol_period: int = 20
    transaction_cost_points: float = 0.0
    slippage_points: float = 0.0
    annualization_bars: int = 252
    session_start: str = "09:15"
    session_end: str = "15:30"

    @classmethod
    def from_env(cls) -> "GSRConfig":
        return cls(
            data_dir=Path(os.getenv("GSR_DATA_DIR", str(DEFAULT_DATA_DIR))),
            max_bars_per_symbol=int(os.getenv("GSR_MAX_BARS", "5000")),
            similarity_min_score=float(os.getenv("GSR_SIM_MIN", "0.55")),
            minimum_regime_bars=int(os.getenv("GSR_MIN_REGIME_BARS", "20")),
            atr_period=int(os.getenv("GSR_ATR_PERIOD", "14")),
            sma_fast=int(os.getenv("GSR_SMA_FAST", "20")),
            sma_slow=int(os.getenv("GSR_SMA_SLOW", "50")),
            rsi_period=int(os.getenv("GSR_RSI_PERIOD", "14")),
            bollinger_period=int(os.getenv("GSR_BB_PERIOD", "20")),
            bollinger_std=float(os.getenv("GSR_BB_STD", "2.0")),
            donchian_period=int(os.getenv("GSR_DONCHIAN_PERIOD", "20")),
            realized_vol_period=int(os.getenv("GSR_RV_PERIOD", "20")),
            transaction_cost_points=float(os.getenv("GSR_COST_POINTS", "0")),
            slippage_points=float(os.getenv("GSR_SLIPPAGE_POINTS", "0")),
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

        option_data = {}
        metadata = {}
        for key, value in row.items():
            if key in OPTION_FIELDS:
                option_data[key] = value
            elif key not in {
                "timestamp", "symbol", "open", "high", "low", "close",
                "volume", "oi", "bid", "ask", "futures_close", "spot_close"
            }:
                metadata[key] = value

        return cls(
            timestamp=str(row["timestamp"]),
            symbol=str(row["symbol"]),
            open=fnum(row["open"]),
            high=fnum(row["high"]),
            low=fnum(row["low"]),
            close=fnum(row["close"]),
            volume=fnum(row["volume"]) if finite(row.get("volume")) else None,
            oi=fnum(row["oi"]) if finite(row.get("oi")) else None,
            bid=fnum(row["bid"]) if finite(row.get("bid")) else None,
            ask=fnum(row["ask"]) if finite(row.get("ask")) else None,
            futures_close=fnum(row["futures_close"]) if finite(row.get("futures_close")) else None,
            spot_close=fnum(row["spot_close"]) if finite(row.get("spot_close")) else None,
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
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            errors.append("invalid_ohlc")
        if self.high < self.low:
            errors.append("high_below_low")
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


def simple_atr(highs: Sequence[float], lows: Sequence[float],
               closes: Sequence[float], period: int) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    trs = []
    start = max(1, len(closes) - period)
    for i in range(start, len(closes)):
        trs.append(true_range(closes[i - 1], highs[i], lows[i]))
    return mean_or(trs, None)


def ema(values: Sequence[float], period: int) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    result = statistics.fmean(vals[:period])
    for x in vals[period:]:
        result = alpha * x + (1 - alpha) * result
    return result


def rsi(values: Sequence[float], period: int) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < period + 1:
        return None
    changes = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
    recent = changes[-period:]
    gains = [max(x, 0.0) for x in recent]
    losses = [max(-x, 0.0) for x in recent]
    avg_gain = mean_or(gains)
    avg_loss = mean_or(losses)
    if avg_loss <= 1e-12:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def linear_slope(values: Sequence[float], lookback: int = 5) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < lookback:
        return None
    y = vals[-lookback:]
    x = list(range(lookback))
    xm = statistics.fmean(x)
    ym = statistics.fmean(y)
    denom = sum((a - xm) ** 2 for a in x)
    return safe_div(sum((a - xm) * (b - ym) for a, b in zip(x, y)), denom, None)


def realized_vol(values: Sequence[float], period: int) -> Optional[float]:
    vals = [float(x) for x in values if finite(x)]
    if len(vals) < period + 1:
        return None
    returns = [
        math.log(vals[i] / vals[i - 1])
        for i in range(len(vals) - period, len(vals))
        if vals[i] > 0 and vals[i - 1] > 0
    ]
    if len(returns) < 2:
        return None
    return stdev_or(returns) * math.sqrt(252.0)


class MarketFeatureEngine:
    """Calculates GSR-owned features only from the supplied market history."""

    def __init__(self, config: GSRConfig) -> None:
        self.config = config
        self.history: Dict[str, Deque[MarketSnapshot]] = defaultdict(
            lambda: deque(maxlen=config.max_bars_per_symbol)
        )

    def add(self, snap: MarketSnapshot) -> Dict[str, Any]:
        self.history[snap.symbol].append(snap)
        bars = list(self.history[snap.symbol])

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars if b.volume is not None]

        atr = simple_atr(highs, lows, closes, self.config.atr_period)
        sma20 = mean_or(closes[-self.config.sma_fast:], None) if len(closes) >= self.config.sma_fast else None
        sma50 = mean_or(closes[-self.config.sma_slow:], None) if len(closes) >= self.config.sma_slow else None
        ema20 = ema(closes, self.config.sma_fast)
        ema50 = ema(closes, self.config.sma_slow)
        rsi14 = rsi(closes, self.config.rsi_period)

        bb_mid = sma20
        bb_std = stdev_or(closes[-self.config.bollinger_period:]) if len(closes) >= self.config.bollinger_period else None
        bb_upper = bb_mid + self.config.bollinger_std * bb_std if bb_mid is not None and bb_std is not None else None
        bb_lower = bb_mid - self.config.bollinger_std * bb_std if bb_mid is not None and bb_std is not None else None
        bb_width = safe_div(bb_upper - bb_lower, bb_mid, None) if bb_upper is not None and bb_lower is not None else None

        dc_n = self.config.donchian_period
        prior_high = max(highs[-dc_n:]) if len(highs) >= dc_n else None
        prior_low = min(lows[-dc_n:]) if len(lows) >= dc_n else None
        # "prior" breakout levels exclude the current bar.
        if len(highs) > dc_n:
            prior_high = max(highs[-dc_n-1:-1])
            prior_low = min(lows[-dc_n-1:-1])

        rv = realized_vol(closes, self.config.realized_vol_period)
        ret1 = safe_div(closes[-1] - closes[-2], closes[-2], None) if len(closes) >= 2 else None
        slope5 = linear_slope(closes, 5)
        slope20 = linear_slope(closes, min(20, len(closes))) if len(closes) >= 20 else None

        vwap = None
        if len(bars) >= 1 and volumes and sum(volumes) > 0:
            pv = sum(((b.high + b.low + b.close) / 3.0) * (b.volume or 0.0) for b in bars)
            vv = sum((b.volume or 0.0) for b in bars)
            vwap = safe_div(pv, vv, None)

        spread = None
        if snap.futures_close is not None and snap.spot_close is not None:
            spread = snap.spot_close - snap.futures_close

        atr_norm_return = safe_div(closes[-1] - closes[-2], atr, None) if len(closes) >= 2 and atr else None
        atr_stretch = safe_div(closes[-1] - vwap, atr, None) if vwap is not None and atr else None

        return {
            "feature_version": "GSR_FEATURES_1.1",
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
            "vwap": vwap,
            "atr_norm_return": atr_norm_return,
            "atr_stretch": atr_stretch,
            "futures_spot_spread": spread,
            "volume": snap.volume,
            "oi": snap.oi,
            "option_state": dict(snap.option_data),
        }


class RegimeEngine:
    """
    GSR-owned regime classifier.

    It is intentionally descriptive rather than predictive.
    No regime label from another engine is accepted.
    """

    def __init__(self, config: GSRConfig) -> None:
        self.config = config

    def classify(self, features: Mapping[str, Any]) -> Dict[str, Any]:
        close = fnum(features.get("close"), None)
        sma20 = features.get("sma20")
        sma50 = features.get("sma50")
        atr_pct = features.get("atr_pct")
        rsi14 = features.get("rsi14")
        bb_width = features.get("bb_width")
        slope20 = features.get("slope20")
        rv = features.get("realized_vol")
        atr_stretch = features.get("atr_stretch")

        if close is None:
            return {"regime": "UNKNOWN", "regime_confidence": 0.0, "components": {}}

        trend_direction = "NEUTRAL"
        if sma20 is not None and sma50 is not None:
            if close > sma20 > sma50:
                trend_direction = "UP"
            elif close < sma20 < sma50:
                trend_direction = "DOWN"

        trend_strength = "UNKNOWN"
        if atr_pct is not None and slope20 is not None:
            normalized_slope = abs(safe_div(slope20, atr_pct * close, 0.0))
            if normalized_slope >= 0.15:
                trend_strength = "STRONG"
            elif normalized_slope >= 0.05:
                trend_strength = "MODERATE"
            else:
                trend_strength = "WEAK"

        volatility_state = "UNKNOWN"
        if rv is not None:
            if rv < 0.12:
                volatility_state = "LOW"
            elif rv < 0.25:
                volatility_state = "NORMAL"
            else:
                volatility_state = "HIGH"

        range_vs_trend = "UNKNOWN"
        if trend_strength in {"STRONG", "MODERATE"} and trend_direction != "NEUTRAL":
            range_vs_trend = "TREND"
        elif trend_strength == "WEAK":
            range_vs_trend = "RANGE"

        location = "NEUTRAL"
        if atr_stretch is not None:
            if atr_stretch >= 1.0:
                location = "UPPER_STRETCH"
            elif atr_stretch <= -1.0:
                location = "LOWER_STRETCH"

        momentum_state = "NEUTRAL"
        if rsi14 is not None:
            if rsi14 >= 60:
                momentum_state = "POSITIVE"
            elif rsi14 <= 40:
                momentum_state = "NEGATIVE"

        compression = "UNKNOWN"
        if bb_width is not None:
            compression = "COMPRESSED" if bb_width < 0.04 else "EXPANDED"

        if range_vs_trend == "TREND" and volatility_state == "HIGH":
            composite = f"{trend_direction}_TREND_HIGH_VOL"
        elif range_vs_trend == "TREND":
            composite = f"{trend_direction}_TREND"
        elif range_vs_trend == "RANGE" and volatility_state == "LOW":
            composite = "RANGE_LOW_VOL"
        elif range_vs_trend == "RANGE":
            composite = "RANGE"
        else:
            composite = "TRANSITION"

        known = [
            trend_direction != "NEUTRAL",
            trend_strength != "UNKNOWN",
            volatility_state != "UNKNOWN",
            range_vs_trend != "UNKNOWN",
            momentum_state != "NEUTRAL",
        ]
        confidence = sum(known) / len(known)

        return {
            "regime": composite,
            "regime_confidence": round(clamp(confidence), 4),
            "trend_direction": trend_direction,
            "trend_strength": trend_strength,
            "volatility_state": volatility_state,
            "range_vs_trend": range_vs_trend,
            "location_state": location,
            "momentum_state": momentum_state,
            "compression_state": compression,
            "components": {
                "sma20": sma20,
                "sma50": sma50,
                "rsi14": rsi14,
                "atr_pct": atr_pct,
                "bb_width": bb_width,
                "realized_vol": rv,
                "atr_stretch": atr_stretch,
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
    union = a | b
    return safe_div(len(a & b), len(union), 0.0)


def similarity_score(a: Mapping[str, Any], b: Mapping[str, Any]) -> Dict[str, Any]:
    ta, tb = strategy_tags(a), strategy_tags(b)
    tag_score = jaccard(ta, tb)

    family_a = normalize_tag(a.get("family"))
    family_b = normalize_tag(b.get("family"))
    family_score = 1.0 if family_a and family_a == family_b else 0.0

    asset_a = normalize_tag(a.get("asset_class"))
    asset_b = normalize_tag(b.get("asset_class"))
    asset_score = 1.0 if asset_a and asset_a == asset_b else 0.0

    mech_a = normalize_tag(a.get("mechanism"))
    mech_b = normalize_tag(b.get("mechanism"))
    mechanism_score = 1.0 if mech_a and mech_a == mech_b else 0.0

    # Tag similarity is deliberately dominant; no performance leakage.
    total = (
        0.55 * tag_score +
        0.20 * family_score +
        0.15 * mechanism_score +
        0.10 * asset_score
    )

    return {
        "similarity_score": round(clamp(total), 6),
        "tag_similarity": round(tag_score, 6),
        "family_similarity": family_score,
        "mechanism_similarity": mechanism_score,
        "asset_similarity": asset_score,
    }


def regime_tag_compatibility(strategy: Mapping[str, Any], regime: Mapping[str, Any]) -> Dict[str, Any]:
    """
    This is a RESEARCH HYPOTHESIS score, not a validated win probability.
    It uses mechanism semantics only and is therefore marked HYPOTHESIS.
    """
    tags = strategy_tags(strategy)
    r = normalize_tag(regime.get("regime"))
    trend = normalize_tag(regime.get("trend_direction"))
    range_state = normalize_tag(regime.get("range_vs_trend"))
    vol = normalize_tag(regime.get("volatility_state"))

    score = 0.50
    reasons: List[str] = []

    if {"trend_following", "breakout"} & tags:
        if range_state == "trend":
            score += 0.20
            reasons.append("trend_following_breakout_vs_trend")
        elif range_state == "range":
            score -= 0.15
            reasons.append("trend_following_breakout_vs_range")

    if {"mean_reversion"} & tags:
        if range_state == "range":
            score += 0.20
            reasons.append("mean_reversion_vs_range")
        elif range_state == "trend":
            score -= 0.15
            reasons.append("mean_reversion_vs_trend")

    if {"volatility_selling"} & tags:
        if vol == "low":
            score += 0.15
            reasons.append("vol_selling_vs_low_vol")
        elif vol == "high":
            score -= 0.15
            reasons.append("vol_selling_vs_high_vol")

    if {"volatility_expansion"} & tags or "breakout" in tags:
        if "high_vol" in r or "transition" in r:
            score += 0.10
            reasons.append("expansion_or_breakout_vs_transition")

    if {"momentum"} & tags:
        if trend in {"up", "down"}:
            score += 0.10
            reasons.append("momentum_vs_directional_state")

    return {
        "hypothesis_score": round(clamp(score), 6),
        "status": "HYPOTHESIS_NOT_VALIDATED",
        "reasons": reasons,
    }


@dataclass
class RuleSpec:
    """
    Optional deterministic strategy evaluator.

    The registry is not modified. Exact rules can be supplied later as
    versioned RuleSpecs. A RuleSpec must be explicit and reproducible.
    """

    strategy_id: str
    version: str
    direction: str
    entry: Callable[[Sequence[MarketSnapshot], Mapping[str, Any]], Optional[int]]
    exit: Callable[[Sequence[MarketSnapshot], Mapping[str, Any]], Optional[int]]
    description: str
    source_type: str = "REPRODUCIBLE_RULE"

    def validate(self) -> None:
        if self.direction not in {"LONG", "SHORT", "BOTH"}:
            raise ValueError("RuleSpec.direction must be LONG, SHORT or BOTH")
        if not self.version:
            raise ValueError("RuleSpec.version is required")
        if not self.description:
            raise ValueError("RuleSpec.description is required")


class ValidationEngine:
    """Stores outcomes without promoting them."""

    def __init__(self, config: GSRConfig, store: JsonlStore) -> None:
        self.config = config
        self.store = store

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
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        gross = (exit_price - entry_price) if direction == "LONG" else (entry_price - exit_price)
        net = gross - self.config.transaction_cost_points - self.config.slippage_points
        r_multiple = None

        record = {
            "schema_version": "GSR_VALIDATION_1.1",
            "recorded_at": utc_now(),
            "strategy_id": strategy_id,
            "symbol": symbol,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "direction": direction,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "gross_pnl_points": gross,
            "net_pnl_points": net,
            "r_multiple": r_multiple,
            "regime": regime,
            "metadata": dict(metadata or {}),
            "validation_status": "OBSERVATION_ONLY",
            "promotion_eligible": False,
        }
        self.store.append(record)
        return record

    def summary(self, strategy_id: str) -> Dict[str, Any]:
        rows = [
            x for x in self.store.read_all()
            if x.get("strategy_id") == strategy_id
        ]
        pnls = [fnum(x.get("net_pnl_points")) for x in rows if finite(x.get("net_pnl_points"))]
        if not pnls:
            return {
                "strategy_id": strategy_id,
                "sample_size": 0,
                "validation_status": "UNTESTED",
                "promotion_eligible": False,
            }

        wins = sum(1 for x in pnls if x > 0)
        losses = sum(1 for x in pnls if x < 0)
        expectancy = mean_or(pnls)
        return {
            "strategy_id": strategy_id,
            "sample_size": len(pnls),
            "wins": wins,
            "losses": losses,
            "win_rate": safe_div(wins, len(pnls)),
            "expectancy_points": expectancy,
            "net_pnl_points": sum(pnls),
            "validation_status": "OBSERVATION_ONLY",
            "promotion_eligible": False,
        }


class GSREngine:
    """Main orchestrator. It owns state and never calls another engine's opinion."""

    def __init__(self, config: Optional[GSRConfig] = None) -> None:
        self.config = config or GSRConfig.from_env()
        self.config.data_dir.mkdir(parents=True, exist_ok=True)

        self.market_store = JsonlStore(self.config.data_dir / "market_observations.jsonl")
        self.regime_store = JsonlStore(self.config.data_dir / "regime_observations.jsonl")
        self.compat_store = JsonlStore(self.config.data_dir / "strategy_compatibility.jsonl")
        self.sim_store = JsonlStore(self.config.data_dir / "similarity_edges.jsonl")
        self.validation_store = JsonlStore(self.config.data_dir / "validation_observations.jsonl")
        self.event_store = JsonlStore(self.config.data_dir / "research_events.jsonl")

        self.feature_engine = MarketFeatureEngine(self.config)
        self.regime_engine = RegimeEngine(self.config)
        self.validation_engine = ValidationEngine(self.config, self.validation_store)

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
        }
        (self.config.data_dir / "engine_state.json").write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def register_rule_spec(self, spec: RuleSpec) -> None:
        spec.validate()
        known = {x.get("atomic_strategy_id") for x in self.registry}
        if spec.strategy_id not in known:
            raise KeyError(f"Unknown Strategy-DNA ID: {spec.strategy_id}")
        self.rule_specs[spec.strategy_id] = spec
        self.event_store.append({
            "event": "RULE_SPEC_REGISTERED",
            "timestamp": utc_now(),
            "strategy_id": spec.strategy_id,
            "rule_version": spec.version,
            "source_type": spec.source_type,
        })
        self._write_state()

    def ingest_snapshot(self, raw: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Main live/historical observation entry point.

        Only the raw snapshot is accepted. If callers include keys named
        confidence/alpha/prediction/external_regime, they are rejected rather
        than silently consumed.
        """
        forbidden = {
            "alpha", "alpha_score", "confidence", "prediction", "signal",
            "external_regime", "regime_label", "position", "weight",
            "decision", "trade_decision"
        }
        supplied_forbidden = sorted(forbidden.intersection(raw.keys()))
        if supplied_forbidden:
            raise ValueError(
                "GSR isolation violation: external opinion fields supplied: "
                + ", ".join(supplied_forbidden)
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

        previous = self._last_timestamp.get(snap.symbol)
        if previous is not None and snap.timestamp < previous:
            raise ValueError(
                f"Look-ahead/order violation for {snap.symbol}: "
                f"{snap.timestamp} < {previous}"
            )
        self._last_timestamp[snap.symbol] = snap.timestamp

        features = self.feature_engine.add(snap)
        regime = self.regime_engine.classify(features)

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

    def _score_current_market(self, regime: Mapping[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for strategy in self.registry:
            comp = regime_tag_compatibility(strategy, regime)
            rows.append({
                "strategy_id": strategy.get("atomic_strategy_id"),
                "strategy_name": strategy.get("strategy_name"),
                "family": strategy.get("family"),
                "rule_precision": strategy.get("rule_precision"),
                "evidence_grade": strategy.get("evidence_grade"),
                "validation_status": strategy.get("validation_status", "UNTESTED"),
                **comp,
            })
        rows.sort(key=lambda x: x["hypothesis_score"], reverse=True)
        return rows

    def build_similarity_graph(self) -> int:
        """Build pairwise mechanism similarity; no performance data is used."""
        count = 0
        self.sim_store.path.write_text("", encoding="utf-8")
        n = len(self.registry)
        for i in range(n):
            for j in range(i + 1, n):
                a, b = self.registry[i], self.registry[j]
                score = similarity_score(a, b)
                if score["similarity_score"] >= self.config.similarity_min_score:
                    self.sim_store.append({
                        "schema_version": "GSR_SIM_1.1",
                        "created_at": utc_now(),
                        "a": a.get("atomic_strategy_id"),
                        "b": b.get("atomic_strategy_id"),
                        **score,
                    })
                    count += 1
        self._similarity_built = True
        self._write_state()
        return count

    def top_compatible_strategies(self, regime: Mapping[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        rows = self._score_current_market(regime)
        return rows[:max(1, int(limit))]

    def registry_audit(self) -> Dict[str, Any]:
        ids = [x.get("atomic_strategy_id") for x in self.registry]
        duplicate_ids = sorted({x for x in ids if ids.count(x) > 1})
        missing_ids = [i for i, x in enumerate(self.registry) if not x.get("atomic_strategy_id")]
        missing_hashes = [x.get("atomic_strategy_id") for x in self.registry if not x.get("strategy_dna_hash")]
        research_variants = sum(
            1 for x in self.registry if x.get("rule_precision") == "research_variant"
        )
        claim_only = sum(
            1 for x in self.registry if x.get("evidence_grade") in {"D", "CLAIM"}
        )
        return {
            "engine_version": ENGINE_VERSION,
            "registry_version": REGISTRY_VERSION,
            "strategy_count": len(self.registry),
            "duplicate_ids": duplicate_ids,
            "missing_ids": missing_ids,
            "missing_dna_hashes": missing_hashes,
            "research_variant_count": research_variants,
            "claim_or_low_evidence_count": claim_only,
            "rule_spec_count": len(self.rule_specs),
            "ok": not duplicate_ids and not missing_ids and not missing_hashes,
        }

    def replay_jsonl(self, path: Path) -> Dict[str, Any]:
        """
        Historical replay. Input must be chronological normalized observations.
        No random shuffling is performed.
        """
        count = 0
        errors = 0
        with Path(path).open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    self.ingest_snapshot(row)
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
        count = 0
        errors = 0
        with Path(path).open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for line_no, row in enumerate(reader, 2):
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

    def record_rule_outcome(
        self,
        strategy_id: str,
        symbol: str,
        entry_time: str,
        exit_time: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        regime: str,
        mfe_points: Optional[float] = None,
        mae_points: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        if strategy_id not in self.rule_specs:
            raise ValueError(
                "No deterministic RuleSpec registered. "
                "GSR will not fabricate a strategy rule."
            )

        extra = dict(metadata or {})
        extra.update({
            "mfe_points": mfe_points,
            "mae_points": mae_points,
            "rule_version": self.rule_specs[strategy_id].version,
        })
        return self.validation_engine.record_trade_observation(
            strategy_id=strategy_id,
            symbol=symbol,
            entry_time=entry_time,
            exit_time=exit_time,
            direction=direction,
            entry_price=entry_price,
            exit_price=exit_price,
            regime=regime,
            metadata=extra,
        )

    def validation_summary(self, strategy_id: str) -> Dict[str, Any]:
        return self.validation_engine.summary(strategy_id)

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
            "registry_audit": self.registry_audit(),
        }


def load_engine() -> GSREngine:
    return GSREngine(GSRConfig.from_env())


def smoke_test() -> Dict[str, Any]:
    """
    Safe offline test. Does not contact a broker and does not alter another
    engine. Uses synthetic observations only.
    """
    import tempfile

    with tempfile.TemporaryDirectory(prefix="gsr_smoke_") as tmp:
        cfg = GSRConfig(data_dir=Path(tmp), max_bars_per_symbol=100)
        engine = GSREngine(cfg)

        base = 100.0
        last = None
        for i in range(80):
            close = base + i * 0.10
            row = {
                "timestamp": f"2026-01-01T09:{15 + i:02d}:00+05:30",
                "symbol": "GSR_SMOKE",
                "open": close - 0.05,
                "high": close + 0.10,
                "low": close - 0.10,
                "close": close,
                "volume": 1000 + i,
            }
            last = engine.ingest_snapshot(row)

        edges = engine.build_similarity_graph()
        audit = engine.registry_audit()

        return {
            "ok": audit["ok"] and bool(last) and edges >= 0,
            "audit": audit,
            "similarity_edges": edges,
            "last_regime": last["regime"]["regime"] if last else None,
            "compatibility_count": len(last["strategy_compatibility"]) if last else 0,
        }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="GSR-1.1.0 isolated research engine")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("audit", help="Audit strategy registry")
    sub.add_parser("similarity", help="Build strategy similarity graph")
    sub.add_parser("health", help="Show engine health")
    sub.add_parser("smoke-test", help="Run offline synthetic smoke test")

    replay = sub.add_parser("replay-jsonl", help="Replay normalized JSONL observations")
    replay.add_argument("path")

    replay_csv = sub.add_parser("replay-csv", help="Replay normalized CSV observations")
    replay_csv.add_argument("path")

    args = parser.parse_args()
    engine = load_engine()

    if args.command == "audit":
        print(json.dumps(engine.registry_audit(), indent=2, ensure_ascii=False))
    elif args.command == "similarity":
        print(json.dumps({"edges_created": engine.build_similarity_graph()}, indent=2))
    elif args.command == "health":
        print(json.dumps(engine.health(), indent=2, ensure_ascii=False))
    elif args.command == "smoke-test":
        print(json.dumps(smoke_test(), indent=2, ensure_ascii=False))
    elif args.command == "replay-jsonl":
        print(json.dumps(engine.replay_jsonl(Path(args.path)), indent=2))
    elif args.command == "replay-csv":
        print(json.dumps(engine.replay_csv(Path(args.path)), indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
