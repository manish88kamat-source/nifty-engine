"""
GSR-1.1.0 â€” Raw Market Data Contract + Adapter
===============================================

Purpose
-------
Strict isolation boundary between external/raw market feeds and gsr_engine.py.

This module does NOT calculate alpha, confidence, prediction, trade signals,
or consume another engine's regime/opinion. It only:
1. normalizes raw market observations into the GSR observation contract,
2. rejects/strips non-market opinion fields,
3. validates chronology and required fields,
4. optionally preserves feed metadata,
5. forwards only the normalized market observation to GSREngine.

Expected repository layout
--------------------------
nifty-engine/
    app.py
    next_day_alpha_engine.py
    strategy_registry.py
    GSR_1.1.0_MASTER_STRATEGY_REGISTRY.txt
    gsr_engine.py
    gsr_data_adapter.py          <-- this file

Design rule
-----------
The adapter may sit physically near other engines, but it is NOT allowed to
read their calculated opinions. If a caller passes a dictionary containing
alpha/confidence/regime/signal/prediction fields, those fields are rejected by
default. A permissive "drop" mode exists only for a feed envelope that carries
mixed data; even in that mode, forbidden fields are removed before GSR sees
the observation.

This file is standard-library-only and has no broker dependency.
"""

from __future__ import annotations

import csv
import json
import math
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple


ADAPTER_VERSION = "GSR-1.1.0-DATA-ADAPTER"
DATA_CONTRACT_VERSION = "GSR_RAW_OBS_1.1"

# These are explicitly forbidden at the GSR boundary.
# They are opinions/outputs, not raw market observations.
FORBIDDEN_OPINION_FIELDS = frozenset({
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
})

# Canonical GSR fields and common aliases encountered in feeds/files.
ALIASES = {
    "time": "timestamp",
    "datetime": "timestamp",
    "date_time": "timestamp",
    "ts": "timestamp",
    "symbol_name": "symbol",
    "ticker": "symbol",
    "instrument": "symbol",
    "instrument_name": "symbol",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "last": "close",
    "ltp": "close",
    "last_price": "close",
    "vol": "volume",
    "qty": "volume",
    "quantity": "volume",
    "open_interest": "oi",
    "openinterest": "oi",
    "bid_price": "bid",
    "ask_price": "ask",
    "fut_close": "futures_close",
    "future_close": "futures_close",
    "futures_ltp": "futures_close",
    "spot": "spot_close",
    "spot_ltp": "spot_close",
    "spot_price": "spot_close",
    "underlying_price": "spot_close",
    "atm_iv": "atm_iv",
    "pcr": "pcr_oi",
    "pcr_oi_ratio": "pcr_oi",
    "pcr_volume_ratio": "pcr_volume",
    "implied_volatility": "iv",
    "volatility": "iv",
    "expiry_date": "expiry",
    "option_expiry": "expiry",
    "opt_type": "option_type",
    "type": "option_type",
}

# Fields allowed to pass through as raw market information.
CANONICAL_MARKET_FIELDS = frozenset({
    "timestamp", "symbol", "exchange", "market", "instrument_type",
    "asset_class", "timeframe", "session",
    "open", "high", "low", "close", "volume", "oi",
    "bid", "ask", "mid",
    "futures_close", "spot_close",
    "iv", "atm_iv", "iv_change", "iv_rank", "iv_percentile",
    "iv_skew", "iv_term_structure", "realized_vol", "iv_rv_spread",
    "pcr_oi", "pcr_volume",
    "ce_oi", "pe_oi", "ce_oi_change", "pe_oi_change",
    "atm_straddle", "chain_completeness",
    "delta", "gamma", "theta", "vega", "vanna", "charm",
    "dte", "strike", "option_type", "moneyness",
    "expiry",
})

# Metadata that is useful for audit/provenance but is not an opinion.
METADATA_FIELDS = frozenset({
    "source", "source_id", "feed", "feed_timestamp", "received_at",
    "sequence", "token", "instrument_token", "contract_id",
    "data_quality", "raw_event_type", "bar_closed",
    "is_snapshot", "currency", "lot_size", "tick_size",
    "expiry", "strike", "option_type",
})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _num(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        value_f = float(value)
        return value_f if math.isfinite(value_f) else default
    except (TypeError, ValueError):
        return default


def _clean_key(key: Any) -> str:
    return str(key).strip().lower().replace("-", "_").replace(" ", "_")


def _canonical_key(key: Any) -> str:
    cleaned = _clean_key(key)
    return ALIASES.get(cleaned, cleaned)


def _parse_timestamp(value: Any) -> str:
    """
    Return an ISO-8601 string.

    Accepted:
    - ISO strings, including trailing Z
    - Unix seconds
    - Unix milliseconds
    """
    if value is None or value == "":
        raise ValueError("timestamp is required")

    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        x = float(value)
        if x > 10_000_000_000:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc).isoformat()

    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    # Keep timezone-aware values as supplied, otherwise explicitly mark UTC.
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Unsupported timestamp format: {value!r}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.isoformat()


def _norm_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_option_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    mapping = {
        "CALL": "CE",
        "C": "CE",
        "CE": "CE",
        "PUT": "PE",
        "P": "PE",
        "PE": "PE",
    }
    return mapping.get(text, text)


@dataclass(frozen=True)
class GSRRawObservation:
    """
    Canonical raw observation.

    Minimum:
        timestamp, symbol, open, high, low, close

    For quote-only/option snapshots where OHLC does not exist, the adapter
    deliberately requires the caller to construct a valid bar/snapshot using
    a supplied close and matching OHLC fields. It never invents OHLC values.
    """

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
    mid: Optional[float] = None

    futures_close: Optional[float] = None
    spot_close: Optional[float] = None

    exchange: Optional[str] = None
    market: Optional[str] = None
    instrument_type: Optional[str] = None
    asset_class: Optional[str] = None
    timeframe: Optional[str] = None
    session: Optional[str] = None

    iv: Optional[float] = None
    atm_iv: Optional[float] = None
    iv_change: Optional[float] = None
    iv_rank: Optional[float] = None
    iv_percentile: Optional[float] = None
    iv_skew: Optional[float] = None
    iv_term_structure: Optional[float] = None
    realized_vol: Optional[float] = None
    iv_rv_spread: Optional[float] = None

    pcr_oi: Optional[float] = None
    pcr_volume: Optional[float] = None
    ce_oi: Optional[float] = None
    pe_oi: Optional[float] = None
    ce_oi_change: Optional[float] = None
    pe_oi_change: Optional[float] = None
    atm_straddle: Optional[float] = None
    chain_completeness: Optional[float] = None

    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    vanna: Optional[float] = None
    charm: Optional[float] = None
    dte: Optional[float] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None
    moneyness: Optional[float] = None
    expiry: Optional[str] = None

    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_mapping(self) -> Dict[str, Any]:
        data = asdict(self)
        return data

    def validate(self) -> List[str]:
        errors: List[str] = []

        if not self.symbol.strip():
            errors.append("empty_symbol")

        for name in ("open", "high", "low", "close"):
            value = getattr(self, name)
            if not _finite(value):
                errors.append(f"invalid_{name}")

        if _finite(self.high) and _finite(self.low) and self.high < self.low:
            errors.append("high_below_low")

        if _finite(self.open) and _finite(self.high) and _finite(self.low):
            if self.open > self.high or self.open < self.low:
                errors.append("open_outside_high_low")

        if _finite(self.close) and _finite(self.high) and _finite(self.low):
            if self.close > self.high or self.close < self.low:
                errors.append("close_outside_high_low")

        if self.volume is not None and self.volume < 0:
            errors.append("negative_volume")

        if self.oi is not None and self.oi < 0:
            errors.append("negative_oi")

        if self.bid is not None and self.ask is not None and self.ask < self.bid:
            errors.append("ask_below_bid")

        if self.chain_completeness is not None and not 0.0 <= self.chain_completeness <= 1.0:
            errors.append("chain_completeness_out_of_range")

        return errors


class GSRDataContract:
    """
    Pure normalization/validation layer.

    It never calls gsr_engine and therefore can be unit-tested independently.
    """

    def __init__(self, strict_opinion_rejection: bool = True) -> None:
        self.strict_opinion_rejection = bool(strict_opinion_rejection)

    def normalize(self, raw: Mapping[str, Any]) -> GSRRawObservation:
        if not isinstance(raw, Mapping):
            raise TypeError("GSR raw observation must be a mapping/dict")

        canonical: Dict[str, Any] = {}
        forbidden: List[str] = []
        extra_metadata: Dict[str, Any] = {}

        for raw_key, raw_value in raw.items():
            key = _canonical_key(raw_key)

            if key in FORBIDDEN_OPINION_FIELDS:
                forbidden.append(str(raw_key))
                continue

            if key in CANONICAL_MARKET_FIELDS:
                canonical[key] = raw_value
            else:
                # Preserve harmless provenance/feed fields only.
                if key in METADATA_FIELDS:
                    extra_metadata[key] = raw_value

        if forbidden and self.strict_opinion_rejection:
            raise ValueError(
                "GSR isolation violation: opinion/output fields supplied: "
                + ", ".join(sorted(set(forbidden)))
            )

        required = ("timestamp", "symbol", "open", "high", "low", "close")
        missing = [key for key in required if key not in canonical or canonical[key] in (None, "")]
        if missing:
            raise ValueError(f"Missing required GSR raw fields: {missing}")

        timestamp = _parse_timestamp(canonical["timestamp"])
        symbol = str(canonical["symbol"]).strip()

        numeric_fields = {
            "open", "high", "low", "close", "volume", "oi", "bid", "ask", "mid",
            "futures_close", "spot_close", "iv", "atm_iv", "iv_change",
            "iv_rank", "iv_percentile", "iv_skew", "iv_term_structure",
            "realized_vol", "iv_rv_spread", "pcr_oi", "pcr_volume",
            "ce_oi", "pe_oi", "ce_oi_change", "pe_oi_change", "atm_straddle",
            "chain_completeness", "delta", "gamma", "theta", "vega", "vanna",
            "charm", "dte", "strike", "moneyness",
        }

        normalized: Dict[str, Any] = {
            "timestamp": timestamp,
            "symbol": symbol,
        }

        for key in numeric_fields:
            if key in canonical:
                normalized[key] = _num(canonical[key])

        for key in (
            "exchange", "market", "instrument_type", "asset_class",
            "timeframe", "session", "expiry",
        ):
            if key in canonical:
                normalized[key] = _norm_text(canonical[key])

        if "option_type" in canonical:
            normalized["option_type"] = _normalize_option_type(canonical["option_type"])

        if normalized.get("mid") is None:
            bid = normalized.get("bid")
            ask = normalized.get("ask")
            if bid is not None and ask is not None:
                normalized["mid"] = (bid + ask) / 2.0

        # Only raw/provenance metadata survives. The original feed dictionary
        # is never passed to GSR wholesale.
        metadata = dict(extra_metadata)
        metadata["adapter_version"] = ADAPTER_VERSION
        metadata["data_contract_version"] = DATA_CONTRACT_VERSION

        # Keep the fact that forbidden fields were seen without preserving
        # their values.
        if forbidden:
            metadata["forbidden_fields_dropped"] = sorted(set(forbidden))

        normalized["metadata"] = metadata

        observation = GSRRawObservation(**normalized)
        errors = observation.validate()
        if errors:
            raise ValueError(f"Invalid GSR raw observation: {errors}")

        return observation


class ChronologyGuard:
    """Per-symbol monotonic timestamp guard. No random reordering."""

    def __init__(self) -> None:
        self._last: Dict[str, str] = {}

    def check(self, observation: GSRRawObservation) -> None:
        previous = self._last.get(observation.symbol)
        if previous is not None and observation.timestamp < previous:
            raise ValueError(
                f"Chronology violation for {observation.symbol}: "
                f"{observation.timestamp} < {previous}"
            )
        self._last[observation.symbol] = observation.timestamp

    def reset(self) -> None:
        self._last.clear()


class GSRDataAdapter:
    """
    Operational adapter.

    Usage:
        from gsr_engine import GSREngine
        from gsr_data_adapter import GSRDataAdapter

        engine = GSREngine()
        adapter = GSRDataAdapter(engine)
        result = adapter.ingest(raw_feed_dict)

    The adapter forwards ONLY the canonical GSRRawObservation mapping.
    """

    def __init__(
        self,
        engine: Any,
        *,
        strict_opinion_rejection: bool = True,
        enforce_chronology: bool = True,
    ) -> None:
        self.engine = engine
        self.contract = GSRDataContract(
            strict_opinion_rejection=strict_opinion_rejection
        )
        self.chronology = ChronologyGuard()
        self.enforce_chronology = enforce_chronology
        self.accepted = 0
        self.rejected = 0

        if not hasattr(engine, "ingest_snapshot"):
            raise TypeError(
                "engine must expose ingest_snapshot(raw_mapping)"
            )

    def normalize(self, raw: Mapping[str, Any]) -> GSRRawObservation:
        return self.contract.normalize(raw)

    def ingest(self, raw: Mapping[str, Any]) -> Dict[str, Any]:
        try:
            observation = self.normalize(raw)
            if self.enforce_chronology:
                self.chronology.check(observation)

            result = self.engine.ingest_snapshot(observation.to_mapping())
            self.accepted += 1
            return result
        except Exception:
            self.rejected += 1
            raise

    def ingest_many(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = True,
    ) -> Iterator[Dict[str, Any]]:
        for row in rows:
            try:
                yield self.ingest(row)
            except Exception:
                if stop_on_error:
                    raise

    def stats(self) -> Dict[str, Any]:
        return {
            "adapter_version": ADAPTER_VERSION,
            "data_contract_version": DATA_CONTRACT_VERSION,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "chronology_enforced": self.enforce_chronology,
            "strict_opinion_rejection": self.contract.strict_opinion_rejection,
        }


def load_jsonl(path: str | Path) -> Iterator[Dict[str, Any]]:
    """Read one JSON object per line without modifying order."""
    with Path(path).open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at line {line_no}: {exc}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"JSONL line {line_no} is not an object")
            yield row


def load_csv(path: str | Path) -> Iterator[Dict[str, Any]]:
    """Read CSV in source order."""
    with Path(path).open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header")
        for row in reader:
            yield dict(row)


def save_normalized_jsonl(
    rows: Iterable[Mapping[str, Any]],
    output_path: str | Path,
    *,
    strict_opinion_rejection: bool = True,
) -> Dict[str, int]:
    """
    Normalize a historical file without starting the GSR engine.

    Useful for preparing a clean research dataset before the one-year
    accumulation phase.
    """
    contract = GSRDataContract(
        strict_opinion_rejection=strict_opinion_rejection
    )
    guard = ChronologyGuard()
    accepted = 0
    rejected = 0

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as fh:
        for row in rows:
            try:
                obs = contract.normalize(row)
                guard.check(obs)
                fh.write(
                    json.dumps(
                        obs.to_mapping(),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                accepted += 1
            except Exception:
                rejected += 1

    return {"accepted": accepted, "rejected": rejected}


def build_from_gsr_engine(
    engine_factory: Callable[[], Any],
    *,
    strict_opinion_rejection: bool = True,
    enforce_chronology: bool = True,
) -> GSRDataAdapter:
    """
    Convenience factory. Importing gsr_engine is intentionally deferred until
    this function is called.
    """
    engine = engine_factory()
    return GSRDataAdapter(
        engine,
        strict_opinion_rejection=strict_opinion_rejection,
        enforce_chronology=enforce_chronology,
    )


def make_example_nifty_bar(
    timestamp: str,
    close: float,
    *,
    symbol: str = "NIFTY",
    open_price: Optional[float] = None,
    high: Optional[float] = None,
    low: Optional[float] = None,
    volume: Optional[float] = None,
    futures_close: Optional[float] = None,
    spot_close: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Test helper only.

    It may create OHLC from a supplied close because this is explicitly a
    synthetic test observation. It must NEVER be used to fabricate historical
    production data.
    """
    o = close if open_price is None else float(open_price)
    h = max(o, close) if high is None else float(high)
    l = min(o, close) if low is None else float(low)

    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "open": o,
        "high": h,
        "low": l,
        "close": float(close),
        "volume": volume,
        "futures_close": futures_close,
        "spot_close": spot_close,
        "source": "SYNTHETIC_TEST_ONLY",
        "bar_closed": True,
    }


def _self_test() -> None:
    """
    Lightweight contract test.

    Does not require broker credentials, internet, or historical files.
    """
    obs = make_example_nifty_bar(
        "2026-01-01T09:15:00+00:00",
        25000.0,
        futures_close=25008.0,
        spot_close=25000.0,
        volume=1000,
    )
    contract = GSRDataContract()
    normalized = contract.normalize(obs)

    assert normalized.symbol == "NIFTY"
    assert normalized.close == 25000.0
    assert normalized.futures_close == 25008.0
    assert normalized.spot_close == 25000.0

    # Isolation test: opinion fields must not cross the boundary.
    try:
        contract.normalize({
            **obs,
            "confidence": 0.99,
        })
    except ValueError:
        pass
    else:
        raise AssertionError("Opinion field crossed GSR boundary")

    # Alias normalization test.
    alias_obs = contract.normalize({
        "time": "2026-01-01T09:18:00+00:00",
        "ticker": "NIFTY",
        "o": 25000,
        "h": 25020,
        "l": 24990,
        "c": 25010,
        "open_interest": 12345,
        "pcr": 1.05,
        "bid_price": 25009,
        "ask_price": 25011,
    })
    assert alias_obs.symbol == "NIFTY"
    assert alias_obs.pcr_oi == 1.05
    assert alias_obs.mid == 25010.0

    guard = ChronologyGuard()
    guard.check(normalized)
    try:
        guard.check(normalized)
    except Exception as exc:
        # Equal timestamps are intentionally allowed for same timestamp
        # snapshots; only backward movement is forbidden.
        raise AssertionError(f"Unexpected equal timestamp rejection: {exc}")

    print("GSR DATA ADAPTER SELF-TEST: PASS")


if __name__ == "__main__":
    _self_test()
