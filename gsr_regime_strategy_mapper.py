"""
GSR-1.1.0
Global Strategy Research Engine
Regime Strategy Mapper

Purpose
-------
Maps the CURRENT / OBSERVED market regime to historically validated
Strategy-DNA mechanisms.

IMPORTANT ARCHITECTURAL RULES
-----------------------------
1. This module is research-only. It is NOT an order/signal engine.
2. It does not consume opinions, confidence, regime labels, or signals
   produced by NIFTY Engine / Next-Day Engine / any other engine.
3. It accepts raw/derived market observations and independently builds
   its own regime fingerprint.
4. Historical validation evidence is treated as evidence, never as truth.
5. Claimed trader performance is never used as performance evidence.
6. No automatic promotion to live trading.
7. Chronology is preserved; future observations must never influence a
   past mapping decision.
8. Missing data reduces evidence quality; it is never silently invented.
9. Similarity and regime fit are separate concepts.
10. A high similarity score alone can never make a strategy eligible.

INPUT CONTRACT
--------------
The mapper can consume JSON/JSONL records produced by the GSR registry,
historical replay and validation layers. It intentionally supports
flexible schemas so that future schema versions can be added without
breaking the core mapper.

Expected strategy evidence fields (examples):
    strategy_id
    dna_id
    strategy_name
    mechanism_tags
    trend_tags
    momentum_tags
    location_tags
    volatility_tags
    volume_tags
    structure_tags
    option_tags
    timeframe_tags
    instrument_tags
    validation_status
    evidence_grade
    sample_size
    oos_expectancy_r
    oos_win_rate
    profit_factor
    max_drawdown_r
    sharpe_like
    sortino_like
    walk_forward_stability
    cost_robustness
    parameter_robustness
    statistical_confidence
    regime_results / regime_performance

Expected raw market observation fields may include:
    timestamp
    spot_close / close
    future_close
    spot_volume
    volume
    vwap
    atr
    realized_vol
    iv
    pcr_oi
    pcr_volume
    breadth
    advance_decline
    trend_strength
    momentum
    distance_from_vwap_atr
    gap_atr
    option_oi_change
    call_oi_change
    put_oi_change
    time_to_expiry
    expiry_flag

The mapper computes its own regime labels from these observations.

OUTPUT
------
For each observation:
    current_regime_fingerprint
    eligible_strategy rankings
    regime_fit
    DNA similarity
    evidence quality
    robustness
    composite research score
    exclusion reasons

The output is suitable for research dashboards and reports, NOT
direct order execution.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ENGINE_VERSION = "GSR-1.1.0"
MODULE_VERSION = "GSR-RSM-1.0.0"
SCHEMA_VERSION = "RSM-1.0"

DEFAULT_CONFIG: Dict[str, Any] = {
    "min_sample_size": 40,
    "min_oos_expectancy_r": 0.0,
    "min_walk_forward_stability": 0.50,
    "min_cost_robustness": 0.50,
    "min_parameter_robustness": 0.50,
    "min_statistical_confidence": 0.50,
    "max_drawdown_r": 999.0,
    "min_evidence_grade": "C",
    "similarity_threshold": 0.35,
    "regime_fit_threshold": 0.45,
    "max_results": 15,
    "regime_history_size": 30,
    "volatility_low_quantile": 0.33,
    "volatility_high_quantile": 0.67,
    "trend_threshold": 0.55,
    "momentum_threshold": 0.55,
    "range_threshold": 0.45,
    "breadth_threshold": 0.55,
    "vwap_stretch_threshold": 0.75,
    "gap_threshold_atr": 0.75,
    "expiry_days_threshold": 1.0,
    "min_feature_coverage": 0.50,
}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return default
        x = float(value)
        if not math.isfinite(x):
            return default
        return x
    except (TypeError, ValueError):
        return default


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def mean_or(values: Iterable[float], default: float = 0.0) -> float:
    vals = [v for v in values if v is not None and math.isfinite(v)]
    return statistics.fmean(vals) if vals else default


def norm_token(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_").replace(" ", "_")


def tokens(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        # Support comma-separated, pipe-separated and whitespace tags.
        raw = value.replace("|", ",").replace(";", ",")
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) == 1 and " " in parts[0]:
            parts = parts[0].split()
        return [norm_token(x) for x in parts if str(x).strip()]
    if isinstance(value, (list, tuple, set)):
        return [norm_token(x) for x in value if str(x).strip()]
    return [norm_token(value)]


def get_any(obj: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in obj:
            return obj[name]
    return default


def evidence_rank(grade: Any) -> int:
    g = norm_token(grade).upper()
    return {"A": 4, "B": 3, "C": 2, "D": 1}.get(g, 0)


def parse_timestamp(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        x = safe_float(value)
        return x
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return None


def stable_hash(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Market regime fingerprint
# ---------------------------------------------------------------------------

@dataclass
class RegimeFingerprint:
    trend_state: str = "UNKNOWN"
    momentum_state: str = "UNKNOWN"
    volatility_state: str = "UNKNOWN"
    location_state: str = "UNKNOWN"
    breadth_state: str = "UNKNOWN"
    market_structure_state: str = "UNKNOWN"
    option_state: str = "UNKNOWN"
    session_state: str = "UNKNOWN"
    regime_label: str = "UNKNOWN"

    trend_score: float = 0.0
    momentum_score: float = 0.0
    volatility_score: float = 0.0
    breadth_score: float = 0.0
    vwap_stretch_atr: float = 0.0

    feature_coverage: float = 0.0
    data_quality_score: float = 0.0
    timestamp: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RegimeDetector:
    """
    Independent regime detector.

    It deliberately does not accept a pre-computed 'regime' field from
    another engine. If a source record contains one, it is ignored.
    """

    def __init__(self, config: Optional[Mapping[str, Any]] = None):
        self.config = dict(DEFAULT_CONFIG)
        if config:
            self.config.update(config)

    def _value(self, row: Mapping[str, Any], *names: str) -> Optional[float]:
        return safe_float(get_any(row, *names))

    def _trend(self, row: Mapping[str, Any]) -> Tuple[str, float]:
        explicit = self._value(
            row, "trend_strength", "adx_normalized", "trend_score_raw"
        )
        slope = self._value(
            row, "trend_slope", "ema_slope", "price_slope", "vwap_slope"
        )
        adx = self._value(row, "adx")

        if explicit is not None:
            score = clamp(abs(explicit) if abs(explicit) <= 1 else abs(explicit) / 100.0)
        elif adx is not None:
            score = clamp((adx - 15.0) / 25.0)
        else:
            score = clamp(abs(slope) if slope is not None else 0.0)

        direction = 0.0
        for name in ("trend_direction", "slope_direction"):
            v = self._value(row, name)
            if v is not None:
                direction = v
                break

        if direction == 0.0 and slope is not None:
            direction = slope

        if score < self.config["range_threshold"]:
            return "RANGE", score
        if direction > 0:
            return "TREND_UP", score
        if direction < 0:
            return "TREND_DOWN", score
        return "TREND_UNDIRECTIONAL", score

    def _momentum(self, row: Mapping[str, Any]) -> Tuple[str, float]:
        rsi = self._value(row, "rsi")
        momentum = self._value(row, "momentum", "momentum_score")
        macd = self._value(row, "macd_hist", "macd_histogram")

        vals = []
        if momentum is not None:
            vals.append(clamp(abs(momentum) if abs(momentum) <= 1 else abs(momentum) / 100))
        if rsi is not None:
            vals.append(clamp(abs(rsi - 50.0) / 25.0))
        if macd is not None:
            vals.append(clamp(abs(macd)))

        score = mean_or(vals, 0.0)

        direction = momentum if momentum is not None else 0.0
        if direction == 0.0 and macd is not None:
            direction = macd
        if direction == 0.0 and rsi is not None:
            direction = rsi - 50.0

        if score < self.config["momentum_threshold"]:
            return "NEUTRAL", score
        return ("MOMENTUM_UP" if direction > 0 else "MOMENTUM_DOWN"), score

    def _volatility(self, row: Mapping[str, Any], history: Sequence[Mapping[str, Any]]) -> Tuple[str, float]:
        value = self._value(row, "realized_vol", "rv", "volatility", "iv")
        hist_values = []
        for r in history:
            v = self._value(r, "realized_vol", "rv", "volatility", "iv")
            if v is not None:
                hist_values.append(v)

        if value is None:
            return "UNKNOWN", 0.0

        if len(hist_values) >= 5:
            sorted_vals = sorted(hist_values)
            rank = sum(v <= value for v in sorted_vals) / len(sorted_vals)
        else:
            # If no distribution exists, only classify based on IV/RV level
            # relative to common normalized 0-1 inputs.
            rank = clamp(value if value <= 1 else value / 100.0)

        if rank <= self.config["volatility_low_quantile"]:
            return "LOW_VOL", rank
        if rank >= self.config["volatility_high_quantile"]:
            return "HIGH_VOL", rank
        return "MID_VOL", rank

    def _location(self, row: Mapping[str, Any]) -> Tuple[str, float]:
        stretch = self._value(
            row,
            "distance_from_vwap_atr",
            "vwap_stretch_atr",
            "normalized_stretch",
        )

        if stretch is None:
            close = self._value(row, "close", "spot_close", "future_close")
            vwap = self._value(row, "vwap", "future_vwap")
            atr = self._value(row, "atr", "atr_14_close", "atr_14_prev")
            if close is not None and vwap is not None and atr and atr > 0:
                stretch = (close - vwap) / atr

        if stretch is None:
            return "UNKNOWN", 0.0

        threshold = float(self.config["vwap_stretch_threshold"])
        score = clamp(abs(stretch) / max(threshold, 1e-9))
        if stretch > threshold:
            return "ABOVE_VWAP_STRETCHED", score
        if stretch < -threshold:
            return "BELOW_VWAP_STRETCHED", score
        if stretch > 0:
            return "ABOVE_VWAP", score
        if stretch < 0:
            return "BELOW_VWAP", score
        return "AT_VWAP", 0.0

    def _breadth(self, row: Mapping[str, Any]) -> Tuple[str, float]:
        breadth = self._value(row, "breadth", "advance_decline", "breadth_score")
        if breadth is None:
            return "UNKNOWN", 0.0
        if abs(breadth) > 1:
            breadth = breadth / 100.0
        score = clamp(abs(breadth))
        if score < self.config["breadth_threshold"]:
            return "MIXED", score
        return ("BROAD_POSITIVE" if breadth > 0 else "BROAD_NEGATIVE"), score

    def _structure(self, row: Mapping[str, Any]) -> str:
        if self._value(row, "range_compression", "compression_score") is not None:
            x = self._value(row, "range_compression", "compression_score") or 0.0
            if x >= 0.65:
                return "COMPRESSION"
        if self._value(row, "breakout", "breakout_state") is not None:
            x = self._value(row, "breakout", "breakout_state") or 0.0
            if x > 0:
                return "BREAKOUT_UP"
            if x < 0:
                return "BREAKOUT_DOWN"
        return "CONTINUOUS"

    def _options(self, row: Mapping[str, Any]) -> str:
        pcr = self._value(row, "pcr_oi", "pcr")
        ce = self._value(row, "call_oi_change", "ce_oi_change")
        pe = self._value(row, "put_oi_change", "pe_oi_change")
        iv_change = self._value(row, "iv_change")

        if pcr is None and ce is None and pe is None and iv_change is None:
            return "UNKNOWN"

        if iv_change is not None and iv_change > 0:
            if ce is not None and pe is not None and pe > ce:
                return "IV_UP_PUT_BUILDUP"
            return "IV_EXPANSION"

        if pcr is not None:
            if pcr > 1.20:
                return "PUT_HEAVY"
            if pcr < 0.80:
                return "CALL_HEAVY"

        if ce is not None and pe is not None:
            if pe > ce:
                return "PUT_BUILDUP"
            if ce > pe:
                return "CALL_BUILDUP"

        return "NEUTRAL_OPTION_STATE"

    def _session(self, row: Mapping[str, Any]) -> str:
        hour = self._value(row, "hour", "session_hour")
        minute = self._value(row, "minute", "session_minute")
        if hour is None:
            ts = parse_timestamp(get_any(row, "timestamp", "datetime", "time"))
            if ts is not None:
                dt = datetime.fromtimestamp(ts)
                hour, minute = dt.hour, dt.minute

        if hour is None:
            return "UNKNOWN"
        hm = hour * 60 + (minute or 0)
        if hm < 9 * 60 + 45:
            return "OPENING"
        if hm < 12 * 60:
            return "MORNING"
        if hm < 14 * 60:
            return "MIDDAY"
        return "CLOSING"

    def detect(
        self,
        row: Mapping[str, Any],
        history: Sequence[Mapping[str, Any]] = (),
    ) -> RegimeFingerprint:
        fields = [
            "trend_strength", "adx", "rsi", "momentum", "realized_vol",
            "iv", "vwap", "close", "breadth", "pcr_oi", "timestamp"
        ]
        present = sum(
            1 for f in fields
            if get_any(row, f) is not None
        )
        coverage = present / len(fields)

        trend_state, trend_score = self._trend(row)
        momentum_state, momentum_score = self._momentum(row)
        vol_state, vol_score = self._volatility(row, history)
        loc_state, stretch_score = self._location(row)
        breadth_state, breadth_score = self._breadth(row)

        structure_state = self._structure(row)
        option_state = self._options(row)
        session_state = self._session(row)

        # Composite regime label is deliberately descriptive rather than
        # a prediction. It is generated only from this module's calculations.
        label_parts = [
            trend_state,
            momentum_state,
            vol_state,
            loc_state,
            structure_state,
        ]
        label = "|".join(label_parts)

        return RegimeFingerprint(
            trend_state=trend_state,
            momentum_state=momentum_state,
            volatility_state=vol_state,
            location_state=loc_state,
            breadth_state=breadth_state,
            market_structure_state=structure_state,
            option_state=option_state,
            session_state=session_state,
            regime_label=label,
            trend_score=clamp(trend_score),
            momentum_score=clamp(momentum_score),
            volatility_score=clamp(vol_score),
            breadth_score=clamp(breadth_score),
            vwap_stretch_atr=self._value(
                row, "distance_from_vwap_atr", "vwap_stretch_atr",
                "normalized_stretch", default=0.0
            ) or 0.0,
            feature_coverage=coverage,
            data_quality_score=coverage,
            timestamp=str(get_any(row, "timestamp", "datetime", "time", default="")),
        )


# ---------------------------------------------------------------------------
# Strategy DNA normalization
# ---------------------------------------------------------------------------

TAG_GROUPS = {
    "mechanism": ("mechanism_tags", "mechanisms", "mechanism"),
    "trend": ("trend_tags", "trend"),
    "momentum": ("momentum_tags", "momentum"),
    "location": ("location_tags", "location"),
    "volatility": ("volatility_tags", "volatility"),
    "volume": ("volume_tags", "volume"),
    "structure": ("structure_tags", "structure"),
    "option": ("option_tags", "option_parameters", "options"),
    "timeframe": ("timeframe_tags", "timeframes", "timeframe"),
    "instrument": ("instrument_tags", "instruments", "instrument"),
    "session": ("session_tags", "session"),
}


@dataclass
class StrategyDNA:
    strategy_id: str
    dna_id: str
    name: str

    mechanism: List[str] = field(default_factory=list)
    trend: List[str] = field(default_factory=list)
    momentum: List[str] = field(default_factory=list)
    location: List[str] = field(default_factory=list)
    volatility: List[str] = field(default_factory=list)
    volume: List[str] = field(default_factory=list)
    structure: List[str] = field(default_factory=list)
    option: List[str] = field(default_factory=list)
    timeframe: List[str] = field(default_factory=list)
    instrument: List[str] = field(default_factory=list)
    session: List[str] = field(default_factory=list)

    def all_tags(self) -> List[str]:
        out: List[str] = []
        for group in (
            self.mechanism, self.trend, self.momentum, self.location,
            self.volatility, self.volume, self.structure, self.option,
            self.timeframe, self.instrument, self.session
        ):
            out.extend(group)
        return out


def parse_strategy_dna(row: Mapping[str, Any]) -> StrategyDNA:
    groups: Dict[str, List[str]] = {}
    for group, aliases in TAG_GROUPS.items():
        values = []
        for alias in aliases:
            if alias in row:
                values.extend(tokens(row[alias]))
        # Unique, deterministic ordering.
        groups[group] = sorted(set(values))

    strategy_id = str(get_any(row, "strategy_id", "id", "strategy", default="UNKNOWN"))
    dna_id = str(get_any(row, "dna_id", "strategy_dna_id", default=strategy_id))
    name = str(get_any(row, "strategy_name", "name", default=strategy_id))

    return StrategyDNA(
        strategy_id=strategy_id,
        dna_id=dna_id,
        name=name,
        **groups,
    )


# ---------------------------------------------------------------------------
# Similarity methodology
# ---------------------------------------------------------------------------

GROUP_WEIGHTS = {
    "mechanism": 0.22,
    "trend": 0.12,
    "momentum": 0.10,
    "location": 0.10,
    "volatility": 0.10,
    "volume": 0.07,
    "structure": 0.10,
    "option": 0.08,
    "timeframe": 0.04,
    "instrument": 0.04,
    "session": 0.03,
}


def jaccard(a: Sequence[str], b: Sequence[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def weighted_dna_similarity(dna: StrategyDNA, regime: RegimeFingerprint) -> Dict[str, float]:
    """
    Strategy-DNA â†” regime similarity.

    This is NOT a statistical performance score.
    It measures structural compatibility between what a strategy requires
    and what the market currently looks like.

    Synonyms are represented explicitly rather than fuzzy-matched so that
    accidental text similarity cannot create false compatibility.
    """
    regime_tags = {
        "mechanism": set(),
        "trend": {norm_token(regime.trend_state)},
        "momentum": {norm_token(regime.momentum_state)},
        "location": {norm_token(regime.location_state)},
        "volatility": {norm_token(regime.volatility_state)},
        "volume": set(),
        "structure": {norm_token(regime.market_structure_state)},
        "option": {norm_token(regime.option_state)},
        "timeframe": set(),
        "instrument": set(),
        "session": {norm_token(regime.session_state)},
    }

    dna_groups = {
        "mechanism": dna.mechanism,
        "trend": dna.trend,
        "momentum": dna.momentum,
        "location": dna.location,
        "volatility": dna.volatility,
        "volume": dna.volume,
        "structure": dna.structure,
        "option": dna.option,
        "timeframe": dna.timeframe,
        "instrument": dna.instrument,
        "session": dna.session,
    }

    scores: Dict[str, float] = {}
    for group, weight in GROUP_WEIGHTS.items():
        scores[group] = jaccard(dna_groups[group], list(regime_tags[group]))

    weighted = sum(scores[g] * GROUP_WEIGHTS[g] for g in GROUP_WEIGHTS)
    active_weight = sum(
        GROUP_WEIGHTS[g]
        for g in GROUP_WEIGHTS
        if dna_groups[g]
    )

    # Do not penalize strategies for groups they never specified, but
    # normalize by the groups actually specified.
    final = weighted / active_weight if active_weight else 0.0
    scores["weighted_similarity"] = clamp(final)
    return scores


# ---------------------------------------------------------------------------
# Historical evidence / regime-fit extraction
# ---------------------------------------------------------------------------

def _numeric(row: Mapping[str, Any], *names: str, default: float = 0.0) -> float:
    return safe_float(get_any(row, *names), default) or default


def extract_regime_key(regime: RegimeFingerprint) -> str:
    return regime.regime_label


def strategy_regime_evidence(
    strategy: Mapping[str, Any],
    regime: RegimeFingerprint,
) -> Dict[str, Any]:
    """
    Extracts the historical performance for the closest known regime.

    Supported formats:
      regime_results: {"TREND_UP|...": {...}}
      regime_performance: [...]
      regime_results: [{"regime": "...", ...}]
      regime_fit: {"TREND_UP": 0.8}
    """
    target = regime.regime_label
    target_tokens = set(norm_token(x) for x in target.split("|"))

    containers = [
        get_any(strategy, "regime_results"),
        get_any(strategy, "regime_performance"),
        get_any(strategy, "regime_fit"),
    ]

    candidates: List[Tuple[float, Mapping[str, Any]]] = []

    for container in containers:
        if isinstance(container, Mapping):
            for key, value in container.items():
                if isinstance(value, Mapping):
                    k = norm_token(key)
                    overlap = len(target_tokens & set(k.split("|")))
                    exact = 1.0 if k == norm_token(target) else 0.0
                    score = exact + overlap * 0.10
                    candidates.append((score, value))
                else:
                    v = safe_float(value)
                    if v is not None:
                        k = norm_token(key)
                        overlap = len(target_tokens & set(k.split("|")))
                        candidates.append((overlap * 0.10, {"regime_fit": v}))
        elif isinstance(container, list):
            for item in container:
                if not isinstance(item, Mapping):
                    continue
                k = norm_token(get_any(item, "regime", "regime_label", "label", default=""))
                overlap = len(target_tokens & set(k.split("|")))
                exact = 1.0 if k == norm_token(target) else 0.0
                candidates.append((exact + overlap * 0.10, item))

    if not candidates:
        return {}

    candidates.sort(key=lambda x: x[0], reverse=True)
    return dict(candidates[0][1])


def evidence_score(strategy: Mapping[str, Any]) -> float:
    grade = evidence_rank(get_any(strategy, "evidence_grade", "evidence", default="D"))
    sample = _numeric(strategy, "sample_size", "n", default=0)
    sample_score = clamp(math.log1p(sample) / math.log1p(1000))

    wf = _numeric(
        strategy,
        "walk_forward_stability",
        "wf_stability",
        "walk_forward_score",
        default=0.0,
    )
    stat = _numeric(
        strategy,
        "statistical_confidence",
        "confidence",
        "confidence_score",
        default=0.0,
    )

    return clamp(
        0.35 * ((grade - 1) / 3.0)
        + 0.25 * sample_score
        + 0.20 * clamp(wf)
        + 0.20 * clamp(stat)
    )


def robustness_score(strategy: Mapping[str, Any]) -> float:
    values = [
        _numeric(strategy, "walk_forward_stability", "wf_stability", default=0.0),
        _numeric(strategy, "cost_robustness", "cost_stability", default=0.0),
        _numeric(strategy, "parameter_robustness", "param_robustness", default=0.0),
    ]
    return clamp(mean_or(values, 0.0))


def normalized_expectancy(strategy: Mapping[str, Any], regime_ev: Mapping[str, Any]) -> float:
    return _numeric(
        regime_ev,
        "oos_expectancy_r",
        "expectancy_r",
        "expectancy",
        default=_numeric(
            strategy,
            "oos_expectancy_r",
            "expectancy_r",
            "expectancy",
            default=0.0,
        ),
    )


def regime_fit_score(
    strategy: Mapping[str, Any],
    regime_ev: Mapping[str, Any],
) -> float:
    explicit = safe_float(
        get_any(regime_ev, "regime_fit", "fit_score", "regime_score")
    )
    if explicit is not None:
        return clamp(explicit)

    win = safe_float(get_any(regime_ev, "oos_win_rate", "win_rate"))
    pf = safe_float(get_any(regime_ev, "profit_factor", "pf"))
    exp = safe_float(get_any(regime_ev, "oos_expectancy_r", "expectancy_r", "expectancy"))

    components = []
    if win is not None:
        if win > 1:
            win /= 100.0
        components.append(clamp(win))
    if pf is not None:
        # PF=1 is neutral, PF>=2 approaches 1.
        components.append(clamp((pf - 1.0) / 1.0))
    if exp is not None:
        # 0R neutral, +1R strong.
        components.append(clamp((exp + 0.25) / 1.25))

    return clamp(mean_or(components, 0.0))


# ---------------------------------------------------------------------------
# Eligibility gates
# ---------------------------------------------------------------------------

@dataclass
class EligibilityResult:
    eligible: bool
    reasons: List[str] = field(default_factory=list)


class ValidationGate:
    def __init__(self, config: Optional[Mapping[str, Any]] = None):
        self.config = dict(DEFAULT_CONFIG)
        if config:
            self.config.update(config)

    def check(self, strategy: Mapping[str, Any]) -> EligibilityResult:
        reasons: List[str] = []

        sample = _numeric(strategy, "sample_size", "n", default=0)
        if sample < self.config["min_sample_size"]:
            reasons.append("INSUFFICIENT_SAMPLE")

        exp = _numeric(strategy, "oos_expectancy_r", "expectancy_r", default=0.0)
        if exp < self.config["min_oos_expectancy_r"]:
            reasons.append("NEGATIVE_OR_INSUFFICIENT_OOS_EXPECTANCY")

        wf = _numeric(strategy, "walk_forward_stability", "wf_stability", default=0.0)
        if wf < self.config["min_walk_forward_stability"]:
            reasons.append("WEAK_WALK_FORWARD_STABILITY")

        cost = _numeric(strategy, "cost_robustness", "cost_stability", default=0.0)
        if cost < self.config["min_cost_robustness"]:
            reasons.append("WEAK_COST_ROBUSTNESS")

        param = _numeric(strategy, "parameter_robustness", "param_robustness", default=0.0)
        if param < self.config["min_parameter_robustness"]:
            reasons.append("WEAK_PARAMETER_ROBUSTNESS")

        stat = _numeric(strategy, "statistical_confidence", "confidence", default=0.0)
        if stat < self.config["min_statistical_confidence"]:
            reasons.append("LOW_STATISTICAL_CONFIDENCE")

        dd = abs(_numeric(strategy, "max_drawdown_r", "max_dd_r", default=0.0))
        if dd > self.config["max_drawdown_r"]:
            reasons.append("EXCESSIVE_DRAWDOWN")

        grade = get_any(strategy, "evidence_grade", "evidence", default="D")
        if evidence_rank(grade) < evidence_rank(self.config["min_evidence_grade"]):
            reasons.append("EVIDENCE_GRADE_BELOW_GATE")

        status = norm_token(get_any(strategy, "validation_status", "status", default=""))
        if status in {"failed", "reject", "rejected", "invalid"}:
            reasons.append("VALIDATION_STATUS_FAILED")

        return EligibilityResult(not reasons, reasons)


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

@dataclass
class StrategyMatch:
    strategy_id: str
    dna_id: str
    name: str

    eligible: bool
    exclusion_reasons: List[str]

    dna_similarity: float
    regime_fit: float
    evidence_score: float
    robustness_score: float

    oos_expectancy_r: float
    oos_win_rate: float
    profit_factor: float
    max_drawdown_r: float
    sample_size: int

    composite_score: float
    research_rank: int = 0

    similarity_breakdown: Dict[str, float] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


class StrategyRanker:
    """
    Research ranking only.

    Composite score deliberately separates:
      structural similarity
      historical regime fit
      evidence quality
      robustness
      expected value

    A strategy failing hard validation gates is not allowed to outrank an
    eligible strategy merely because its claimed similarity is high.
    """

    def __init__(self, config: Optional[Mapping[str, Any]] = None):
        self.config = dict(DEFAULT_CONFIG)
        if config:
            self.config.update(config)
        self.gate = ValidationGate(self.config)

    def score(
        self,
        strategy: Mapping[str, Any],
        regime: RegimeFingerprint,
    ) -> StrategyMatch:
        dna = parse_strategy_dna(strategy)
        sim = weighted_dna_similarity(dna, regime)
        dna_score = sim["weighted_similarity"]

        regime_ev = strategy_regime_evidence(strategy, regime)
        fit = regime_fit_score(strategy, regime_ev)
        ev_score = evidence_score(strategy)
        rob_score = robustness_score(strategy)

        exp = normalized_expectancy(strategy, regime_ev)
        win = _numeric(
            regime_ev,
            "oos_win_rate", "win_rate",
            default=_numeric(strategy, "oos_win_rate", "win_rate", default=0.0),
        )
        if win > 1:
            win /= 100.0

        pf = _numeric(
            regime_ev,
            "profit_factor", "pf",
            default=_numeric(strategy, "profit_factor", "pf", default=0.0),
        )
        dd = abs(_numeric(
            regime_ev,
            "max_drawdown_r", "max_dd_r",
            default=_numeric(strategy, "max_drawdown_r", "max_dd_r", default=0.0),
        ))
        sample = int(_numeric(
            regime_ev,
            "sample_size", "n",
            default=_numeric(strategy, "sample_size", "n", default=0),
        ))

        eligibility = self.gate.check(strategy)

        # Composite score:
        # 30% historical regime fit
        # 25% DNA similarity
        # 20% robustness
        # 15% evidence quality
        # 10% normalized expectancy
        exp_component = clamp((exp + 0.25) / 1.25)

        composite = (
            0.30 * fit
            + 0.25 * dna_score
            + 0.20 * rob_score
            + 0.15 * ev_score
            + 0.10 * exp_component
        )

        # Hard gates: failed validation cannot be a top candidate.
        if not eligibility.eligible:
            composite *= 0.25

        return StrategyMatch(
            strategy_id=dna.strategy_id,
            dna_id=dna.dna_id,
            name=dna.name,
            eligible=eligibility.eligible,
            exclusion_reasons=eligibility.reasons,
            dna_similarity=clamp(dna_score),
            regime_fit=clamp(fit),
            evidence_score=clamp(ev_score),
            robustness_score=clamp(rob_score),
            oos_expectancy_r=exp,
            oos_win_rate=clamp(win),
            profit_factor=pf,
            max_drawdown_r=dd,
            sample_size=sample,
            composite_score=clamp(composite),
            similarity_breakdown=sim,
        )

    def rank(
        self,
        strategies: Sequence[Mapping[str, Any]],
        regime: RegimeFingerprint,
    ) -> List[StrategyMatch]:
        results = [self.score(s, regime) for s in strategies]

        eligible = [
            r for r in results
            if r.eligible
            and r.dna_similarity >= self.config["similarity_threshold"]
            and r.regime_fit >= self.config["regime_fit_threshold"]
        ]
        ineligible = [
            r for r in results if r not in eligible
        ]

        eligible.sort(
            key=lambda x: (
                x.composite_score,
                x.regime_fit,
                x.dna_similarity,
                x.robustness_score,
            ),
            reverse=True,
        )
        ineligible.sort(key=lambda x: x.composite_score, reverse=True)

        combined = eligible + ineligible
        limit = int(self.config["max_results"])
        combined = combined[:limit]

        for idx, item in enumerate(combined, 1):
            item.research_rank = idx

        return combined


# ---------------------------------------------------------------------------
# Mapper
# ---------------------------------------------------------------------------

class RegimeStrategyMapper:
    def __init__(self, config: Optional[Mapping[str, Any]] = None):
        self.config = dict(DEFAULT_CONFIG)
        if config:
            self.config.update(config)
        self.detector = RegimeDetector(self.config)
        self.ranker = StrategyRanker(self.config)

    def map(
        self,
        market_row: Mapping[str, Any],
        strategies: Sequence[Mapping[str, Any]],
        history: Sequence[Mapping[str, Any]] = (),
    ) -> Dict[str, Any]:
        regime = self.detector.detect(market_row, history)
        matches = self.ranker.rank(strategies, regime)

        eligible = [m for m in matches if m.eligible]
        qualified = [
            m for m in eligible
            if m.dna_similarity >= self.config["similarity_threshold"]
            and m.regime_fit >= self.config["regime_fit_threshold"]
        ]

        return {
            "engine_version": ENGINE_VERSION,
            "module_version": MODULE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "timestamp": regime.timestamp,
            "regime": regime.as_dict(),
            "qualified_count": len(qualified),
            "eligible_count": len(eligible),
            "matches": [m.as_dict() for m in matches],
            "research_only": True,
            "live_order_permission": False,
            "hash": stable_hash({
                "regime": regime.as_dict(),
                "matches": [m.as_dict() for m in matches],
            }),
        }


# ---------------------------------------------------------------------------
# File IO
# ---------------------------------------------------------------------------

def iter_json_records(path: Path) -> Iterable[Dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    yield item
        elif isinstance(obj, dict):
            if isinstance(obj.get("records"), list):
                for item in obj["records"]:
                    if isinstance(item, dict):
                        yield item
            else:
                yield obj
        return

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSONL at {path}:{line_no}: {exc}"
                ) from exc
            if isinstance(obj, dict):
                yield obj


def load_records(path: str) -> List[Dict[str, Any]]:
    return list(iter_json_records(Path(path)))


def write_json(path: str, obj: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def write_jsonl(path: str, rows: Iterable[Mapping[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def write_csv(path: str, matches: Sequence[StrategyMatch]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "research_rank", "strategy_id", "dna_id", "name", "eligible",
        "dna_similarity", "regime_fit", "evidence_score",
        "robustness_score", "oos_expectancy_r", "oos_win_rate",
        "profit_factor", "max_drawdown_r", "sample_size",
        "composite_score", "exclusion_reasons",
    ]
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for m in matches:
            row = m.as_dict()
            row["exclusion_reasons"] = "|".join(m.exclusion_reasons)
            writer.writerow({k: row.get(k) for k in fields})


# ---------------------------------------------------------------------------
# Batch historical mapping
# ---------------------------------------------------------------------------

def chronological_sort(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    def key(row: Mapping[str, Any]) -> Tuple[int, str]:
        ts = parse_timestamp(get_any(row, "timestamp", "datetime", "time"))
        return (0 if ts is not None else 1, str(ts if ts is not None else ""))
    return [dict(r) for r in sorted(rows, key=key)]


def map_history(
    market_rows: Sequence[Mapping[str, Any]],
    strategies: Sequence[Mapping[str, Any]],
    config: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Walk forward chronologically.

    The current row is mapped using prior rows only. This is crucial:
    volatility ranks and other history-derived regime descriptors must not
    see future observations.
    """
    mapper = RegimeStrategyMapper(config)
    rows = chronological_sort(market_rows)
    out: List[Dict[str, Any]] = []

    history_size = int(
        (config or DEFAULT_CONFIG).get("regime_history_size", 30)
    )

    for idx, row in enumerate(rows):
        history = rows[max(0, idx - history_size):idx]
        result = mapper.map(row, strategies, history)
        result["observation_index"] = idx
        result["history_rows_used"] = len(history)
        out.append(result)

    return out


# ---------------------------------------------------------------------------
# Audit / diagnostics
# ---------------------------------------------------------------------------

def audit_strategy_registry(strategies: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    seen_ids = set()
    duplicate_ids = []
    missing_dna = []
    missing_tags = []
    missing_validation = []

    for s in strategies:
        sid = str(get_any(s, "strategy_id", "id", default=""))
        if sid in seen_ids and sid:
            duplicate_ids.append(sid)
        seen_ids.add(sid)

        dna = parse_strategy_dna(s)
        if not dna.dna_id:
            missing_dna.append(sid)
        if not dna.all_tags():
            missing_tags.append(sid)
        if not get_any(s, "validation_status", "status"):
            missing_validation.append(sid)

    return {
        "strategy_count": len(strategies),
        "duplicate_strategy_ids": sorted(set(duplicate_ids)),
        "missing_dna_ids": sorted(set(missing_dna)),
        "missing_strategy_tags": sorted(set(missing_tags)),
        "missing_validation_status": sorted(set(missing_validation)),
        "audit_pass": not (
            duplicate_ids or missing_dna or missing_tags
        ),
    }


# ---------------------------------------------------------------------------
# Synthetic self-test
# ---------------------------------------------------------------------------

def synthetic_strategies() -> List[Dict[str, Any]]:
    return [
        {
            "strategy_id": "S_TREND_BREAKOUT",
            "dna_id": "DNA_BREAKOUT_TREND",
            "strategy_name": "Trend Breakout",
            "mechanism_tags": ["breakout", "trend_following"],
            "trend_tags": ["trend_up", "trend_down"],
            "momentum_tags": ["momentum_up", "momentum_down"],
            "location_tags": ["above_vwap", "below_vwap"],
            "volatility_tags": ["high_vol", "expansion"],
            "structure_tags": ["breakout_up", "breakout_down"],
            "instrument_tags": ["equity", "index"],
            "timeframe_tags": ["intraday"],
            "validation_status": "validated",
            "evidence_grade": "B",
            "sample_size": 600,
            "oos_expectancy_r": 0.22,
            "oos_win_rate": 0.58,
            "profit_factor": 1.45,
            "max_drawdown_r": 9.0,
            "walk_forward_stability": 0.76,
            "cost_robustness": 0.72,
            "parameter_robustness": 0.70,
            "statistical_confidence": 0.78,
            "regime_results": {
                "TREND_UP|MOMENTUM_UP|HIGH_VOL|ABOVE_VWAP_STRETCHED|BREAKOUT_UP": {
                    "regime_fit": 0.91,
                    "oos_expectancy_r": 0.31,
                    "oos_win_rate": 0.64,
                    "profit_factor": 1.70,
                    "sample_size": 120,
                }
            },
        },
        {
            "strategy_id": "S_MEAN_REVERT",
            "dna_id": "DNA_MEAN_REVERSION",
            "strategy_name": "VWAP Mean Reversion",
            "mechanism_tags": ["mean_reversion"],
            "trend_tags": ["range"],
            "momentum_tags": ["neutral"],
            "location_tags": ["above_vwap_stretched", "below_vwap_stretched"],
            "volatility_tags": ["low_vol"],
            "structure_tags": ["compression", "continuous"],
            "instrument_tags": ["equity", "index"],
            "timeframe_tags": ["intraday"],
            "validation_status": "validated",
            "evidence_grade": "B",
            "sample_size": 500,
            "oos_expectancy_r": 0.14,
            "oos_win_rate": 0.61,
            "profit_factor": 1.32,
            "max_drawdown_r": 8.0,
            "walk_forward_stability": 0.71,
            "cost_robustness": 0.68,
            "parameter_robustness": 0.73,
            "statistical_confidence": 0.72,
            "regime_results": {
                "RANGE|NEUTRAL|LOW_VOL|ABOVE_VWAP_STRETCHED|COMPRESSION": {
                    "regime_fit": 0.89,
                    "oos_expectancy_r": 0.25,
                    "oos_win_rate": 0.67,
                    "profit_factor": 1.60,
                    "sample_size": 110,
                }
            },
        },
    ]


def synthetic_market_rows() -> List[Dict[str, Any]]:
    return [
        {
            "timestamp": "2026-01-01T09:30:00+00:00",
            "close": 100,
            "vwap": 99,
            "atr": 1,
            "trend_strength": 0.80,
            "trend_direction": 1,
            "momentum": 0.80,
            "realized_vol": 0.90,
            "breadth": 0.70,
            "pcr_oi": 0.75,
            "breakout": 1,
        },
        {
            "timestamp": "2026-01-01T09:33:00+00:00",
            "close": 101,
            "vwap": 99,
            "atr": 1,
            "trend_strength": 0.85,
            "trend_direction": 1,
            "momentum": 0.85,
            "realized_vol": 0.95,
            "breadth": 0.80,
            "pcr_oi": 0.70,
            "breakout": 1,
        },
        {
            "timestamp": "2026-01-01T09:36:00+00:00",
            "close": 102,
            "vwap": 99.5,
            "atr": 1,
            "trend_strength": 0.90,
            "trend_direction": 1,
            "momentum": 0.90,
            "realized_vol": 1.00,
            "breadth": 0.85,
            "pcr_oi": 0.68,
            "breakout": 1,
        },
    ]


def run_self_test() -> Dict[str, Any]:
    strategies = synthetic_strategies()
    market = synthetic_market_rows()

    audit = audit_strategy_registry(strategies)
    mapped = map_history(market, strategies)

    if not audit["audit_pass"]:
        raise AssertionError(f"Registry audit failed: {audit}")

    if len(mapped) != len(market):
        raise AssertionError("History mapping count mismatch")

    last = mapped[-1]
    if not last.get("matches"):
        raise AssertionError("No strategy matches produced")

    # Confirm the engine does not expose a permission to trade.
    if last.get("live_order_permission") is not False:
        raise AssertionError("Live order permission must be false")

    return {
        "passed": True,
        "engine_version": ENGINE_VERSION,
        "module_version": MODULE_VERSION,
        "strategy_count": len(strategies),
        "market_rows": len(market),
        "last_regime": last["regime"]["regime_label"],
        "top_strategy": last["matches"][0]["strategy_id"],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GSR Regime Strategy Mapper"
    )
    parser.add_argument("--market", help="Market JSON/JSONL input")
    parser.add_argument("--strategies", help="Strategy registry JSON/JSONL input")
    parser.add_argument(
        "--out",
        default="gsr_regime_strategy_map.jsonl",
        help="Output JSONL for chronological mapping",
    )
    parser.add_argument(
        "--summary",
        default="gsr_regime_strategy_latest.json",
        help="Latest mapping summary JSON",
    )
    parser.add_argument(
        "--csv",
        default="gsr_regime_strategy_latest.csv",
        help="Latest ranking CSV",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run synthetic self-test",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.self_test:
        result = run_self_test()
        print(json.dumps(result, indent=2))
        return 0

    if not args.market or not args.strategies:
        parser.error(
            "--market and --strategies are required unless --self-test is used"
        )

    strategies = load_records(args.strategies)
    market_rows = load_records(args.market)

    config = {}
    if args.max_results is not None:
        config["max_results"] = args.max_results

    audit = audit_strategy_registry(strategies)
    if not audit["audit_pass"]:
        print(
            "WARNING: strategy registry audit reported structural issues:",
            json.dumps(audit, indent=2),
            file=sys.stderr,
        )

    results = map_history(market_rows, strategies, config)

    write_jsonl(args.out, results)

    latest = results[-1] if results else {
        "error": "No market rows supplied",
        "engine_version": ENGINE_VERSION,
        "module_version": MODULE_VERSION,
    }
    latest["registry_audit"] = audit
    write_json(args.summary, latest)

    if latest.get("matches"):
        latest_matches = [
            StrategyMatch(**m) for m in latest["matches"]
        ]
        write_csv(args.csv, latest_matches)

    print(
        json.dumps(
            {
                "status": "ok",
                "engine_version": ENGINE_VERSION,
                "module_version": MODULE_VERSION,
                "market_rows": len(market_rows),
                "strategies": len(strategies),
                "output": args.out,
                "summary": args.summary,
                "csv": args.csv,
                "latest_regime": latest.get("regime", {}).get("regime_label"),
                "qualified_count": latest.get("qualified_count", 0),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
