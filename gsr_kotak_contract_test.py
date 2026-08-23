"""
GSR-KOTAK CONTRACT TEST v1.0.0
===============================

Purpose
-------
Validate the REAL Kotak Neo raw market-data contract before long-term
GSR shadow/history accumulation begins.

IMPORTANT:
- This module does NOT login to Kotak Neo.
- This module does NOT place, modify, or cancel orders.
- It does NOT consume alpha/confidence/regime/prediction/trade decisions.
- It only validates raw market-data payloads and their mapping into the
  GSR normalized observation contract.
- It can run against saved raw JSON files OR payloads imported from the
  existing NIFTY engine.

Designed around the current NIFTY engine's observed Kotak quote fields:
    tk, v, oi, op, h, lo, ap, iv
and tolerated naming variants for OI/volume.
Option identity is represented by strike + CE/PE when available.

Recommended file location:
    Global Strategy Research Engine/
        gsr_kotak_contract_test.py

CLI examples:
    python gsr_kotak_contract_test.py --self-test
    python gsr_kotak_contract_test.py --input kotak_raw_sample.json
    python gsr_kotak_contract_test.py --input kotak_raw_sample.json \
        --output kotak_contract_report.json

The test intentionally does NOT assume bid/ask are mandatory because the
current uploaded NIFTY engine does not establish them as guaranteed raw
Kotak fields. They are optional if present.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import re
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


VERSION = "GSR-KOTAK-CONTRACT-1.0.0"

# Fields which must never cross the raw-data boundary into GSR.
FORBIDDEN_OPINION_FIELDS = {
    "alpha",
    "alpha_score",
    "confidence",
    "confidence_score",
    "prediction",
    "predicted_return",
    "predicted_direction",
    "signal",
    "trade_signal",
    "trade_decision",
    "decision",
    "regime",
    "regime_label",
    "regime_score",
    "model_score",
    "entry_signal",
    "exit_signal",
    "position",
    "position_size",
    "target",
    "stop_loss",
    "stop",
    "take_profit",
    "pnl",
    "strategy_score",
}

# Current Kotak parser fields observed in the supplied NIFTY engine.
CANONICAL_RAW_ALIASES = {
    "token": ("tk", "token", "instrument_token", "instrumentToken"),
    "volume": ("v", "volume", "traded_volume", "pTradedVolume"),
    "oi": ("oi", "openInterest", "pOpenInterest"),
    "open": ("op", "open", "o"),
    "high": ("h", "high"),
    "low": ("lo", "low", "l"),
    "close": ("c", "close", "ltp", "last_price", "lastPrice"),
    "vwap": ("ap", "vwap", "average_price", "averagePrice"),
    "iv": ("iv", "implied_volatility", "impliedVolatility"),
    "timestamp": (
        "timestamp",
        "time",
        "ts",
        "exchange_timestamp",
        "exchangeTime",
        "ltt",
    ),
    "strike": (
        "strike",
        "strike_price",
        "strikePrice",
        "stk",
        "strprc",
    ),
    "option_type": (
        "option_type",
        "optionType",
        "opt_type",
        "instrument_type",
        "optt",
        "type",
    ),
    "symbol": ("symbol", "trading_symbol", "tradingSymbol", "name"),
    "exchange_segment": (
        "exchange_segment",
        "exchangeSegment",
        "exSeg",
        "segment",
    ),
    "bid": ("bid", "bid_price", "bidPrice"),
    "ask": ("ask", "ask_price", "askPrice"),
    "oi_change": (
        "oi_change",
        "oiChange",
        "pChangeInOI",
        "changeInOI",
    ),
}

REQUIRED_BASE = ("timestamp", "close")
OPTION_REQUIRED = ("strike", "option_type", "close")


@dataclass
class Check:
    name: str
    status: str
    severity: str = "INFO"
    message: str = ""
    count: int = 0


@dataclass
class NormalizedObservation:
    timestamp: Optional[str]
    symbol: Optional[str]
    token: Optional[str]
    exchange_segment: Optional[str]
    instrument_kind: str
    option_type: Optional[str]
    strike: Optional[float]

    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]
    oi: Optional[float]
    oi_change: Optional[float]
    vwap: Optional[float]
    iv: Optional[float]
    bid: Optional[float]
    ask: Optional[float]

    raw_hash: str
    schema_version: str = VERSION


@dataclass
class ContractReport:
    version: str
    status: str
    input_records: int
    accepted_records: int
    rejected_records: int
    duplicate_records: int
    checks: List[Check] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    observations: List[Dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        name: str,
        status: str,
        message: str = "",
        severity: str = "INFO",
        count: int = 0,
    ) -> None:
        self.checks.append(
            Check(
                name=name,
                status=status,
                severity=severity,
                message=message,
                count=count,
            )
        )
        if status == "FAIL":
            self.errors.append(f"{name}: {message}")
        elif status == "WARN":
            self.warnings.append(f"{name}: {message}")


def safe_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        x = float(value)
        if not math.isfinite(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def clean_key(key: Any) -> str:
    return str(key).strip()


def first_value(row: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    for key in aliases:
        if key in row and row[key] not in (None, ""):
            return row[key]
    # Case-insensitive fallback.
    lower = {str(k).lower(): v for k, v in row.items()}
    for key in aliases:
        if key.lower() in lower and lower[key.lower()] not in (None, ""):
            return lower[key.lower()]
    return None


def flatten_candidate_rows(payload: Any) -> List[Mapping[str, Any]]:
    """
    Extract quote-like dictionaries without inventing a Kotak response shape.

    Accepted forms:
      {"data": [{...}, {...}]}
      {"data": {"data": [...]}}
      {"success": true, "data": {...}}
      [{...}, {...}]
      {...single quote...}

    Unknown nested structures are searched conservatively.
    """
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, Mapping)]

    if not isinstance(payload, Mapping):
        return []

    for key in ("data", "result", "results", "quotes", "quote", "response"):
        value = payload.get(key)
        if isinstance(value, list):
            rows = [x for x in value if isinstance(x, Mapping)]
            if rows:
                return rows
        if isinstance(value, Mapping):
            nested = flatten_candidate_rows(value)
            if nested:
                return nested

    # If the object itself looks like a quote row, retain it.
    quote_markers = {"tk", "v", "oi", "op", "h", "lo", "ap", "iv", "ltp", "close"}
    if any(k in payload for k in quote_markers):
        return [payload]

    # Conservative recursive search one level deeper.
    for value in payload.values():
        if isinstance(value, Mapping):
            nested = flatten_candidate_rows(value)
            if nested:
                return nested
        elif isinstance(value, list):
            nested = [x for x in value if isinstance(x, Mapping)]
            if nested and any(
                any(k in x for k in quote_markers) for x in nested
            ):
                return nested

    return []


def normalize_option_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"CE", "CALL", "C"} or text.endswith("CE"):
        return "CE"
    if text in {"PE", "PUT", "P"} or text.endswith("PE"):
        return "PE"
    return None


def parse_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Treat large values as epoch milliseconds, smaller as seconds.
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000.0
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    # ISO-like string.
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        pass

    # Numeric string epoch.
    try:
        number = float(text)
        if number > 10_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def canonical_raw_hash(row: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(sorted((str(k), v) for k, v in row.items())),
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def detect_instrument_kind(
    row: Mapping[str, Any],
    option_type: Optional[str],
    strike: Optional[float],
) -> str:
    if option_type is not None or strike is not None:
        return "OPTION"

    segment = first_value(row, CANONICAL_RAW_ALIASES["exchange_segment"])
    symbol = first_value(row, CANONICAL_RAW_ALIASES["symbol"])

    text = f"{segment or ''} {symbol or ''}".upper()
    if "NSE_FO" in text or "FUT" in text:
        return "FUTURE"

    return "SPOT_OR_OTHER"


def find_forbidden_fields(obj: Any, path: str = "") -> List[str]:
    found: List[str] = []

    if isinstance(obj, Mapping):
        for key, value in obj.items():
            key_text = clean_key(key)
            normalized = re.sub(r"[^a-z0-9_]", "_", key_text.lower())
            current = f"{path}.{key_text}" if path else key_text

            if normalized in FORBIDDEN_OPINION_FIELDS:
                found.append(current)

            found.extend(find_forbidden_fields(value, current))

    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            found.extend(find_forbidden_fields(value, f"{path}[{idx}]"))

    return found


def normalize_row(row: Mapping[str, Any]) -> NormalizedObservation:
    timestamp = parse_timestamp(
        first_value(row, CANONICAL_RAW_ALIASES["timestamp"])
    )
    symbol = first_value(row, CANONICAL_RAW_ALIASES["symbol"])
    token = first_value(row, CANONICAL_RAW_ALIASES["token"])
    exchange_segment = first_value(
        row, CANONICAL_RAW_ALIASES["exchange_segment"]
    )

    option_type = normalize_option_type(
        first_value(row, CANONICAL_RAW_ALIASES["option_type"])
    )
    strike = safe_float(first_value(row, CANONICAL_RAW_ALIASES["strike"]))

    raw_hash = canonical_raw_hash(row)

    return NormalizedObservation(
        timestamp=timestamp,
        symbol=str(symbol) if symbol is not None else None,
        token=str(token) if token is not None else None,
        exchange_segment=(
            str(exchange_segment) if exchange_segment is not None else None
        ),
        instrument_kind=detect_instrument_kind(row, option_type, strike),
        option_type=option_type,
        strike=strike,
        open=safe_float(first_value(row, CANONICAL_RAW_ALIASES["open"])),
        high=safe_float(first_value(row, CANONICAL_RAW_ALIASES["high"])),
        low=safe_float(first_value(row, CANONICAL_RAW_ALIASES["low"])),
        close=safe_float(first_value(row, CANONICAL_RAW_ALIASES["close"])),
        volume=safe_float(first_value(row, CANONICAL_RAW_ALIASES["volume"])),
        oi=safe_float(first_value(row, CANONICAL_RAW_ALIASES["oi"])),
        oi_change=safe_float(
            first_value(row, CANONICAL_RAW_ALIASES["oi_change"])
        ),
        vwap=safe_float(first_value(row, CANONICAL_RAW_ALIASES["vwap"])),
        iv=safe_float(first_value(row, CANONICAL_RAW_ALIASES["iv"])),
        bid=safe_float(first_value(row, CANONICAL_RAW_ALIASES["bid"])),
        ask=safe_float(first_value(row, CANONICAL_RAW_ALIASES["ask"])),
        raw_hash=raw_hash,
    )


def validate_normalized(
    obs: NormalizedObservation,
) -> Tuple[bool, List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    if not obs.timestamp:
        errors.append("missing_or_invalid_timestamp")

    if obs.close is None or obs.close <= 0:
        errors.append("missing_or_invalid_close")

    if obs.instrument_kind == "OPTION":
        if obs.strike is None or obs.strike <= 0:
            errors.append("option_missing_or_invalid_strike")
        if obs.option_type not in {"CE", "PE"}:
            errors.append("option_missing_or_invalid_type")

    if obs.volume is None:
        warnings.append("volume_unavailable")
    elif obs.volume < 0:
        errors.append("negative_volume")

    if obs.oi is None:
        warnings.append("oi_unavailable")
    elif obs.oi < 0:
        errors.append("negative_oi")

    if obs.iv is None and obs.instrument_kind == "OPTION":
        warnings.append("option_iv_unavailable")

    if obs.bid is not None and obs.bid < 0:
        errors.append("negative_bid")
    if obs.ask is not None and obs.ask < 0:
        errors.append("negative_ask")
    if obs.bid is not None and obs.ask is not None and obs.ask < obs.bid:
        errors.append("ask_below_bid")

    # OHLC sanity is checked only when all fields are available.
    if all(x is not None for x in (obs.open, obs.high, obs.low, obs.close)):
        if obs.high < obs.low:
            errors.append("high_below_low")
        if obs.high < max(obs.open, obs.close):
            errors.append("high_below_open_or_close")
        if obs.low > min(obs.open, obs.close):
            errors.append("low_above_open_or_close")

    return not errors, errors, warnings


def validate_payload(
    payload: Any,
    expected_kind: Optional[str] = None,
) -> Tuple[ContractReport, List[NormalizedObservation]]:
    rows = flatten_candidate_rows(payload)

    report = ContractReport(
        version=VERSION,
        status="PASS",
        input_records=len(rows),
        accepted_records=0,
        rejected_records=0,
        duplicate_records=0,
    )

    if not rows:
        report.status = "FAIL"
        report.add(
            "payload_shape",
            "FAIL",
            "No quote-like rows could be extracted from payload.",
            severity="ERROR",
        )
        return report, []

    forbidden = find_forbidden_fields(payload)
    if forbidden:
        report.status = "FAIL"
        report.add(
            "opinion_field_isolation",
            "FAIL",
            "Forbidden opinion fields found: " + ", ".join(forbidden[:20]),
            severity="ERROR",
            count=len(forbidden),
        )
    else:
        report.add(
            "opinion_field_isolation",
            "PASS",
            "No forbidden strategy/opinion fields found.",
        )

    observations: List[NormalizedObservation] = []
    hashes: set[str] = set()
    timestamps: List[str] = []

    for idx, row in enumerate(rows):
        obs = normalize_row(row)

        if obs.raw_hash in hashes:
            report.duplicate_records += 1
            report.add(
                f"duplicate_record_{idx}",
                "WARN",
                "Duplicate raw payload hash.",
                severity="WARNING",
                count=1,
            )
            continue

        hashes.add(obs.raw_hash)

        ok, errors, warnings = validate_normalized(obs)

        if expected_kind and obs.instrument_kind != expected_kind:
            errors.append(
                f"unexpected_instrument_kind:{obs.instrument_kind}"
            )

        if ok:
            report.accepted_records += 1
            observations.append(obs)
            if obs.timestamp:
                timestamps.append(obs.timestamp)
        else:
            report.rejected_records += 1
            report.status = "FAIL"
            report.add(
                f"row_{idx}_validation",
                "FAIL",
                "; ".join(errors),
                severity="ERROR",
            )

        for warning in warnings:
            report.add(
                f"row_{idx}_{warning}",
                "WARN",
                warning,
                severity="WARNING",
            )

    if report.rejected_records:
        report.status = "FAIL"

    if timestamps:
        ordered = all(
            timestamps[i] <= timestamps[i + 1]
            for i in range(len(timestamps) - 1)
        )
        if ordered:
            report.add(
                "chronology",
                "PASS",
                "Accepted timestamps are non-decreasing.",
            )
        else:
            report.add(
                "chronology",
                "WARN",
                "Input timestamps are not ordered; replay must sort/reject according to its chronology policy.",
                severity="WARNING",
            )

    if report.accepted_records:
        report.add(
            "normalization",
            "PASS",
            f"{report.accepted_records} records normalized successfully.",
            count=report.accepted_records,
        )
    else:
        report.status = "FAIL"

    report.observations = [asdict(x) for x in observations]

    return report, observations


def representative_payload() -> Dict[str, Any]:
    """
    Synthetic shape based on fields observed in the supplied NIFTY engine.
    Values are deliberately fake. This is only a contract-shape test.
    """
    return {
        "success": True,
        "data": [
            {
                "tk": "26000",
                "v": "125430",
                "oi": "0",
                "op": "25100.0",
                "h": "25128.5",
                "lo": "25091.2",
                "c": "25120.3",
                "ap": "25112.8",
                "iv": None,
                "timestamp": "2026-08-24T10:15:00+05:30",
                "symbol": "NIFTY",
                "exchange_segment": "nse_cm",
            },
            {
                "tk": "NIFTY-FUT",
                "v": "84210",
                "oi": "1250000",
                "op": "25108.0",
                "h": "25142.0",
                "lo": "25100.0",
                "c": "25132.0",
                "ap": "25125.5",
                "iv": None,
                "timestamp": "2026-08-24T10:15:00+05:30",
                "symbol": "NIFTY FUT",
                "exchange_segment": "nse_fo",
            },
            {
                "tk": "OPT25100CE",
                "v": "18200",
                "oi": "325000",
                "op": "138.0",
                "h": "146.0",
                "lo": "135.0",
                "c": "142.5",
                "ap": "141.7",
                "iv": "12.8",
                "timestamp": "2026-08-24T10:15:00+05:30",
                "symbol": "NIFTY 25100 CE",
                "exchange_segment": "nse_fo",
                "strike": "25100",
                "option_type": "CE",
                "oi_change": "18500",
            },
            {
                "tk": "OPT25100PE",
                "v": "21100",
                "oi": "301000",
                "op": "128.0",
                "h": "136.0",
                "lo": "124.0",
                "c": "131.5",
                "ap": "130.2",
                "iv": "13.1",
                "timestamp": "2026-08-24T10:15:00+05:30",
                "symbol": "NIFTY 25100 PE",
                "exchange_segment": "nse_fo",
                "strike": "25100",
                "option_type": "PE",
                "oi_change": "-9200",
            },
        ],
    }


def run_self_test() -> Dict[str, Any]:
    """
    Self-test suite. No network and no Kotak credentials.
    """
    results: Dict[str, Any] = {
        "version": VERSION,
        "status": "PASS",
        "tests": {},
    }

    payload = representative_payload()
    report, observations = validate_payload(payload)

    results["tests"]["representative_payload"] = (
        report.status == "PASS" and len(observations) == 4
    )

    # Test forbidden opinion field rejection.
    poisoned = copy.deepcopy(payload)
    poisoned["data"][0]["confidence"] = 0.99
    poison_report, _ = validate_payload(poisoned)
    results["tests"]["forbidden_opinion_rejection"] = (
        poison_report.status == "FAIL"
        and any(
            "confidence" in error
            for error in poison_report.errors
        )
    )

    # Test duplicate detection.
    duplicate_payload = {
        "data": [
            payload["data"][0],
            payload["data"][0],
        ]
    }
    duplicate_report, _ = validate_payload(duplicate_payload)
    results["tests"]["duplicate_detection"] = (
        duplicate_report.duplicate_records == 1
    )

    # Test invalid OHLC rejection.
    bad = copy.deepcopy(payload)
    bad["data"][0]["h"] = "1"
    bad_report, _ = validate_payload(bad)
    results["tests"]["ohlc_sanity_rejection"] = bad_report.status == "FAIL"

    # Test option identity.
    option_obs = [x for x in observations if x.instrument_kind == "OPTION"]
    results["tests"]["option_identity"] = (
        len(option_obs) == 2
        and {x.option_type for x in option_obs} == {"CE", "PE"}
        and all(x.strike == 25100.0 for x in option_obs)
    )

    # Test optional bid/ask: absence must not fail the contract.
    no_bid_ask = copy.deepcopy(payload)
    for row in no_bid_ask["data"]:
        row.pop("bid", None)
        row.pop("ask", None)
    no_ba_report, _ = validate_payload(no_bid_ask)
    results["tests"]["bid_ask_optional"] = no_ba_report.status == "PASS"

    # Test aliases used by the current parser.
    alias_payload = {
        "data": [
            {
                "token": "X",
                "volume": 100,
                "openInterest": 500,
                "open": 100,
                "high": 105,
                "low": 99,
                "ltp": 103,
                "averagePrice": 102,
                "impliedVolatility": 14.2,
                "timestamp": "2026-08-24T10:18:00+05:30",
                "strikePrice": 25100,
                "optionType": "PE",
            }
        ]
    }
    alias_report, alias_obs = validate_payload(alias_payload)
    results["tests"]["alias_normalization"] = (
        alias_report.status == "PASS"
        and len(alias_obs) == 1
        and alias_obs[0].option_type == "PE"
        and alias_obs[0].iv == 14.2
    )

    failed = [k for k, v in results["tests"].items() if not v]
    if failed:
        results["status"] = "FAIL"
        results["failed_tests"] = failed

    return results


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(obj, handle, indent=2, ensure_ascii=False, default=str)


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Kotak Neo raw payload against the GSR contract."
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run offline contract self-tests.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Path to saved Kotak raw JSON response.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON report output path.",
    )
    parser.add_argument(
        "--expected-kind",
        choices=["SPOT_OR_OTHER", "FUTURE", "OPTION"],
        help="Require all extracted records to be of one instrument kind.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    if args.self_test or not args.input:
        result = run_self_test()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if args.self_test:
            return 0 if result["status"] == "PASS" else 1

    if not args.input:
        return 0

    try:
        payload = load_json(args.input)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "error": f"Unable to read JSON: {exc}",
                },
                indent=2,
            )
        )
        return 1

    report, _ = validate_payload(
        payload,
        expected_kind=args.expected_kind,
    )

    output = asdict(report)
    print(json.dumps(output, indent=2, ensure_ascii=False, default=str))

    if args.output:
        write_json(args.output, output)

    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
