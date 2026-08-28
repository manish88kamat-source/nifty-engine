#!/usr/bin/env python3
"""
COMMON RAW DATA PRODUCER
=======================
Standalone Streamlit application for the three isolated engines:

    Kotak/NSE/Yahoo raw sources
              |
              v
      COMMON RAW DATA STORE
              |
       +------+------+------+
       |             |      |
     NIFTY        ALPHA    GSR

STRICT CONTRACT
---------------
This application is a DATA PRODUCER only.
It publishes raw observations and source/transport metadata.
It MUST NOT publish indicators, scores, signals, regimes, predictions,
confidence, rankings, labels, trade decisions, or engine opinions.

The producer owns broker credentials. Consumer engines do not.
The store is append-only for raw observations plus a latest-state index.
Consumers can independently read the same observations and compute their
own features.

Deployment:
    streamlit run raw_data_producer_app.py

Required secrets/env for Kotak live feed:
    KOTAK_CONSUMER_KEY
    KOTAK_MOBILE
    KOTAK_UCC
    KOTAK_TOTP
    KOTAK_MPIN

Optional:
    KOTAK_ENVIRONMENT=prod
    SHARED_RAW_CACHE_DIR=./shared_raw_data
    RAW_PRODUCER_POLL_SECONDS=3
    RAW_MAX_JSONL_MB=128

Historical raw capture:
    optional yfinance package; no credentials required.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import struct
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

try:
    import streamlit as st
except Exception:
    st = None

try:
    from neo_api_client import NeoAPI
except Exception:
    NeoAPI = None

try:
    import yfinance as yf
except Exception:
    yf = None

IST = ZoneInfo("Asia/Kolkata")

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
PRODUCER_VERSION = "RAW_PRODUCER_1.0.0"
RAW_SCHEMA_VERSION = "RAW_OBSERVATION_1.0"
HEARTBEAT_VERSION = "RAW_CONSUMER_HEARTBEAT_1.0"
ROOT = Path(os.getenv("SHARED_RAW_CACHE_DIR", "./shared_raw_data"))
ROOT.mkdir(parents=True, exist_ok=True)
OBS_DIR = ROOT / "observations"
OBS_DIR.mkdir(parents=True, exist_ok=True)
LATEST_FILE = ROOT / "latest.json"
HEALTH_FILE = ROOT / "producer_health.json"
CONSUMER_DIR = ROOT / "consumers"
CONSUMER_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_FILE = ROOT / "producer_audit.jsonl"

POLL_SECONDS = max(1, int(os.getenv("RAW_PRODUCER_POLL_SECONDS", "3")))
MAX_JSONL_BYTES = max(10_000_000, int(os.getenv("RAW_MAX_JSONL_MB", "128")) * 1024 * 1024)
KOTAK_ENVIRONMENT = os.getenv("KOTAK_ENVIRONMENT", "prod")
NIFTY_SPOT_TOKEN = "Nifty 50"
NIFTY_FUT_FALLBACK = os.getenv("NIFTY_FUT_TOKEN", "53000").strip()

HEAVYWEIGHT_TOKENS = {
    "HDFCBANK": "1333", "RELIANCE": "2885", "ICICIBANK": "4963",
    "INFY": "1594", "ITC": "1660", "TCS": "11536",
    "LT": "11483", "AXISBANK": "5900", "KOTAKBANK": "1922", "SBIN": "3045",
}

FORBIDDEN_INTELLIGENCE_FIELDS = {
    "alpha", "alpha_score", "score", "selection_score", "ranking", "rank",
    "signal", "signals", "bias", "market_bias", "regime", "regime_score",
    "prediction", "probability", "confidence", "label", "trade_decision",
    "decision", "recommendation", "thesis", "invalidation", "target",
    "stop_loss", "entry", "final_2", "final_1", "final_candidates",
    "day_ahead_score", "setup_score", "quality_score", "composite_score",
}

# Raw market fields only. The raw broker payload is also retained for audit.
RAW_CANONICAL_FIELDS = {
    "ltp", "open", "high", "low", "close", "prev_close", "volume",
    "oi", "bid", "ask", "bid_qty", "ask_qty", "vwap", "upper_circuit",
    "lower_circuit", "price_band", "last_traded_time", "strike", "option_type",
    "expiry", "exchange", "exchange_segment", "instrument_token", "symbol",
}

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def now_ist() -> datetime:
    return datetime.now(IST)


def iso_now() -> str:
    return now_ist().isoformat()


def safe_float(value: Any, default: float = float("nan")) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if not value:
                return default
        value = float(value)
        return value if math.isfinite(value) else default
    except Exception:
        return default


def is_valid_number(value: Any) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except Exception:
        return False


def safe_json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(k): safe_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe_json_value(v) for v in value]
    return value


def env_or_secret(name: str) -> str:
    value = os.getenv(name, "")
    if value:
        return str(value).strip()
    if st is not None:
        try:
            value = st.secrets.get(name, "")
            if value:
                return str(value).strip()
        except Exception:
            pass
    return ""


def normalize_mobile(value: str) -> str:
    raw = str(value or "").strip()
    digits = "".join(c for c in raw if c.isdigit())
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    return "+91" + digits if len(digits) == 10 and digits[0] in "6789" else raw


def generate_totp(secret_or_otp: str) -> str:
    raw = str(secret_or_otp or "").strip().replace(" ", "").upper()
    if raw.isdigit() and len(raw) == 6:
        return raw
    try:
        if len(raw) % 8:
            raw += "=" * (8 - len(raw) % 8)
        key = base64.b32decode(raw, casefold=True)
        counter = int(time.time() // 30)
        digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
        offset = digest[-1] & 15
        code = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff) % 1_000_000
        return f"{code:06d}"
    except Exception:
        return raw


def token_from_record(record: Mapping[str, Any]) -> str:
    for key in ("exchange_token", "pSymbol", "pSymbolToken", "instrument_token",
                "instrumentToken", "tok", "token", "pToken", "tk"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def extract_ltp(record: Mapping[str, Any]) -> float:
    for key in ("ltp", "lp", "last_price", "last_traded_price", "c", "close", "lastPrice"):
        value = safe_float(record.get(key))
        if is_valid_number(value) and value > 0:
            return value
    return float("nan")


def extract_oi(record: Mapping[str, Any]) -> float:
    keys = ("oi", "open_interest", "openInterest", "OpenInterest", "oI", "OI",
            "open_int", "opnInterest", "openInt", "dOpenInterest")
    for key in keys:
        value = safe_float(record.get(key))
        if is_valid_number(value) and value >= 0:
            return value
    for wrapper in ("data", "quote", "marketDepth", "depth", "ohlc"):
        nested = record.get(wrapper)
        if isinstance(nested, dict):
            for key in keys:
                value = safe_float(nested.get(key))
                if is_valid_number(value) and value >= 0:
                    return value
    return float("nan")


def record_list(response: Any) -> List[Dict[str, Any]]:
    if isinstance(response, list):
        return [x for x in response if isinstance(x, dict)]
    if not isinstance(response, dict):
        return []
    for key in ("data", "result", "records", "data_list", "scrips", "list", "message"):
        value = response.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            for nested_key in ("data", "records", "result", "scrips"):
                nested = value.get(nested_key)
                if isinstance(nested, list):
                    return [x for x in nested if isinstance(x, dict)]
    return []


def parse_expiry(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(IST) if value.tzinfo else value.replace(tzinfo=IST)
    try:
        x = float(value)
        if x > 10_000_000_000:
            return datetime.fromtimestamp(x / 1000, tz=IST)
        if x > 1_000_000_000:
            return datetime.fromtimestamp(x, tz=IST)
    except Exception:
        pass
    text = str(value).strip().upper()
    for fmt in ("%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=IST)
        except Exception:
            continue
    return None


def option_type(record: Mapping[str, Any]) -> str:
    value = str(record.get("pOptionType") or record.get("optType") or record.get("option_type") or "").upper()
    if "CE" in value or "CALL" in value:
        return "CE"
    if "PE" in value or "PUT" in value:
        return "PE"
    symbol = str(record.get("pTrdSymbol") or record.get("ts") or "").upper()
    return "CE" if symbol.endswith("CE") else ("PE" if symbol.endswith("PE") else "")


def strike(record: Mapping[str, Any]) -> float:
    for key in ("dStrikePrice", "dStrikePrice;", "strike_price", "strikePrice", "dStrike", "strike", "pStrikePrice"):
        value = safe_float(record.get(key))
        if is_valid_number(value) and value > 0:
            return value / 100.0 if value > 1_000_000 else value
    return float("nan")


def detect_forbidden(payload: Mapping[str, Any]) -> List[str]:
    found = []
    for key in payload:
        if str(key).strip().lower() in FORBIDDEN_INTELLIGENCE_FIELDS:
            found.append(str(key))
    return sorted(set(found))

# -----------------------------------------------------------------------------
# Shared raw-data store
# -----------------------------------------------------------------------------
class SharedRawStore:
    """Append-only raw observation store with atomic latest index."""

    def __init__(self, root: Path = ROOT):
        self.root = Path(root)
        self.obs_dir = self.root / "observations"
        self.obs_dir.mkdir(parents=True, exist_ok=True)
        self.latest_path = self.root / "latest.json"
        self.audit_path = self.root / "producer_audit.jsonl"
        self.lock = threading.RLock()
        self.sequence = self._load_sequence()
        self.latest: Dict[str, Dict[str, Any]] = self._load_latest()

    def _load_sequence(self) -> int:
        try:
            return int((self.root / "sequence.txt").read_text(encoding="utf-8").strip())
        except Exception:
            return 0

    def _persist_sequence(self) -> None:
        tmp = self.root / "sequence.txt.tmp"
        tmp.write_text(str(self.sequence), encoding="utf-8")
        os.replace(tmp, self.root / "sequence.txt")

    def _load_latest(self) -> Dict[str, Dict[str, Any]]:
        try:
            data = json.loads(self.latest_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _day_file(ts: str) -> Path:
        try:
            day = ts[:10]
        except Exception:
            day = now_ist().strftime("%Y-%m-%d")
        return OBS_DIR / f"raw_{day}.jsonl"

    def publish(self, raw: Mapping[str, Any], *, source: str, source_type: str = "live") -> Dict[str, Any]:
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError("Raw observation must be a non-empty mapping")
        forbidden = detect_forbidden(raw)
        if forbidden:
            raise ValueError("RAW boundary violation: " + ", ".join(forbidden))

        received = iso_now()
        observation_ts = raw.get("observation_timestamp") or raw.get("timestamp") or received
        symbol = str(raw.get("symbol") or raw.get("instrument_token") or "UNKNOWN")
        exchange = str(raw.get("exchange") or raw.get("exchange_segment") or "UNKNOWN")
        key = f"{exchange}:{symbol}"

        with self.lock:
            self.sequence += 1
            event = {
                "schema_version": RAW_SCHEMA_VERSION,
                "producer_version": PRODUCER_VERSION,
                "event_id": f"raw-{self.sequence:012d}-{uuid.uuid4().hex[:10]}",
                "sequence": self.sequence,
                "source": source,
                "source_type": source_type,
                "observation_timestamp": str(observation_ts),
                "received_at": received,
                "symbol": symbol,
                "exchange": exchange,
                "instrument_token": str(raw.get("instrument_token") or ""),
                "raw": safe_json_value(dict(raw)),
            }
            line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
            day_file = self._day_file(received)
            with day_file.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            self.latest[key] = event
            self._atomic_write_json(self.latest_path, self.latest)
            self._persist_sequence()
            self._audit("RAW_PUBLISHED", event_id=event["event_id"], symbol=symbol, source=source)
            return event

    def publish_many(self, rows: Iterable[Mapping[str, Any]], *, source: str, source_type: str = "live") -> int:
        count = 0
        for row in rows:
            try:
                self.publish(row, source=source, source_type=source_type)
                count += 1
            except Exception as exc:
                self._audit("RAW_REJECTED", source=source, reason=str(exc))
        return count

    def _atomic_write_json(self, path: Path, value: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(safe_json_value(value), ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)

    def _audit(self, event: str, **kwargs: Any) -> None:
        row = {"timestamp": iso_now(), "event": event, **safe_json_value(kwargs)}
        with self.audit_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def status(self) -> Dict[str, Any]:
        try:
            latest_mtime = self.latest_path.stat().st_mtime if self.latest_path.exists() else 0
            age = max(0.0, time.time() - latest_mtime) if latest_mtime else None
        except Exception:
            age = None
        return {
            "schema_version": RAW_SCHEMA_VERSION,
            "producer_version": PRODUCER_VERSION,
            "root": str(self.root),
            "latest_symbols": len(self.latest),
            "sequence": self.sequence,
            "latest_file_age_sec": age,
        }

# -----------------------------------------------------------------------------
# Kotak raw adapter
# -----------------------------------------------------------------------------
class KotakRawProducer:
    def __init__(self, store: SharedRawStore):
        self.store = store
        self.client = None
        self.connected = False
        self.authenticated = False
        self.streaming = False
        self.last_error = ""
        self.last_poll = None
        self.last_tick = None
        self.poll_count = 0
        self.publish_count = 0
        self.lock = threading.RLock()
        self.spot_token = NIFTY_SPOT_TOKEN
        self.future_token = NIFTY_FUT_FALLBACK
        self.future_symbol = ""
        self.heavy_tokens = dict(HEAVYWEIGHT_TOKENS)
        self.pcr_tokens: List[str] = []
        self.pcr_records: Dict[str, Dict[str, Any]] = {}
        self.active_expiry: Optional[datetime] = None
        self.latest: Dict[str, Dict[str, Any]] = {}

    def _credentials(self) -> Dict[str, str]:
        return {
            "KOTAK_CONSUMER_KEY": env_or_secret("KOTAK_CONSUMER_KEY"),
            "KOTAK_MOBILE": normalize_mobile(env_or_secret("KOTAK_MOBILE")),
            "KOTAK_UCC": env_or_secret("KOTAK_UCC"),
            "KOTAK_TOTP": env_or_secret("KOTAK_TOTP"),
            "KOTAK_MPIN": env_or_secret("KOTAK_MPIN"),
        }

    def credentials_status(self) -> Dict[str, Any]:
        c = self._credentials()
        return {"credentials_present": all(bool(v) for v in c.values()),
                "missing": [k for k, v in c.items() if not v]}

    def login(self) -> bool:
        if NeoAPI is None:
            raise RuntimeError("neo_api_client is not installed")
        c = self._credentials()
        missing = [k for k, v in c.items() if not v]
        if missing:
            raise RuntimeError("Missing credentials: " + ", ".join(missing))

        self.client = NeoAPI(environment=KOTAK_ENVIRONMENT, access_token=None, neo_fin_key=None,
                             consumer_key=c["KOTAK_CONSUMER_KEY"])
        self.client.on_message = self.on_message
        self.client.on_error = self.on_error
        self.client.on_close = self.on_close
        self.client.on_open = self.on_open

        step1 = self.client.totp_login(
            mobile_number=c["KOTAK_MOBILE"], ucc=c["KOTAK_UCC"], totp=generate_totp(c["KOTAK_TOTP"])
        )
        if isinstance(step1, dict) and step1.get("error"):
            raise RuntimeError(str(step1))
        step2 = self.client.totp_validate(mpin=c["KOTAK_MPIN"])
        if isinstance(step2, dict) and step2.get("error"):
            raise RuntimeError(str(step2))
        self.connected = True
        self.authenticated = True
        return True

    def on_open(self, message=None):
        self.connected = True

    def on_close(self, message=None):
        self.streaming = False
        self.connected = False

    def on_error(self, error):
        self.last_error = str(error or "")

    def on_message(self, message):
        try:
            if isinstance(message, str):
                message = json.loads(message)
            rows = message if isinstance(message, list) else [message]
            for row in rows:
                if not isinstance(row, dict):
                    continue
                self._publish_kotak_record(row, source_type="websocket")
        except Exception as exc:
            self.last_error = f"websocket: {exc}"

    def _publish_kotak_record(self, record: Mapping[str, Any], source_type: str) -> Optional[Dict[str, Any]]:
        token = token_from_record(record)
        if not token:
            return None
        symbol = str(record.get("display_symbol") or record.get("pTrdSymbol") or record.get("ts") or "")
        if not symbol:
            symbol = self._symbol_for_token(token)
        segment = str(record.get("exchange_segment") or self._segment_for_token(token))
        exchange = "NSE"
        raw = {
            "symbol": symbol or token,
            "instrument_token": token,
            "exchange": exchange,
            "exchange_segment": segment,
            "ltp": extract_ltp(record),
            "open": safe_float(record.get("o") or record.get("open") or record.get("openPrice")),
            "high": safe_float(record.get("h") or record.get("high") or record.get("highPrice")),
            "low": safe_float(record.get("l") or record.get("low") or record.get("lowPrice")),
            "close": safe_float(record.get("c") or record.get("close") or record.get("closePrice")),
            "prev_close": safe_float(record.get("pdc") or record.get("prev_close") or record.get("previousClose")),
            "volume": safe_float(record.get("v") or record.get("volume") or record.get("vol")),
            "oi": extract_oi(record),
            "bid": safe_float(record.get("bp") or record.get("bid") or record.get("best_bid")),
            "ask": safe_float(record.get("sp") or record.get("ask") or record.get("best_ask")),
            "bid_qty": safe_float(record.get("bq") or record.get("bid_qty")),
            "ask_qty": safe_float(record.get("sq") or record.get("ask_qty")),
            "vwap": safe_float(record.get("v") if False else record.get("vwap") or record.get("avp") or record.get("averagePrice")),
            "upper_circuit": safe_float(record.get("upper_circuit") or record.get("upperCircuit")),
            "lower_circuit": safe_float(record.get("lower_circuit") or record.get("lowerCircuit")),
            "last_traded_time": str(record.get("lstup_time") or record.get("ltt") or record.get("ft") or ""),
            "observation_timestamp": iso_now(),
        }
        if token in self.pcr_records:
            meta = self.pcr_records[token]
            raw.update({"strike": meta.get("strike"), "option_type": meta.get("option_type"),
                        "expiry": meta.get("expiry").isoformat() if isinstance(meta.get("expiry"), datetime) else meta.get("expiry")})
        event = self.store.publish(raw, source="kotak_neo", source_type=source_type)
        with self.lock:
            self.latest[token] = event
            self.last_tick = now_ist()
            self.publish_count += 1
        return event

    def _symbol_for_token(self, token: str) -> str:
        if token == self.spot_token:
            return "NIFTY_SPOT"
        if token == self.future_token:
            return self.future_symbol or "NIFTY_FUT"
        for symbol, tok in self.heavy_tokens.items():
            if str(tok) == str(token):
                return symbol
        meta = self.pcr_records.get(str(token))
        return str(meta.get("symbol") if meta else token)

    def _segment_for_token(self, token: str) -> str:
        return "nse_fo" if str(token) in {str(self.future_token), *map(str, self.pcr_tokens)} else "nse_cm"

    def resolve_future(self) -> str:
        if not self.client:
            return self.future_token
        try:
            rows = record_list(self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY"))
            candidates = []
            for r in rows:
                sym = str(r.get("pTrdSymbol") or r.get("ts") or r.get("symbol") or "").upper()
                inst = str(r.get("pInstType") or "").upper()
                if not sym.startswith("NIFTY") or not ("FUT" in sym or "FUTIDX" in inst):
                    continue
                exp = parse_expiry(r.get("pExpiryDate") or r.get("lExpiryDate") or r.get("expiryDate") or r.get("expiry"))
                tok = token_from_record(r)
                if exp and tok and exp.date() >= now_ist().date():
                    candidates.append((exp, tok, sym))
            if candidates:
                candidates.sort(key=lambda x: x[0])
                exp, tok, sym = candidates[0]
                self.future_token, self.future_symbol = tok, sym
                return tok
        except Exception as exc:
            self.last_error = f"future discovery: {exc}"
        return self.future_token

    def discover_options(self, center: Optional[float] = None, count: int = 5, step: float = 50.0) -> int:
        if not self.client:
            return 0
        try:
            if not is_valid_number(center):
                center = extract_ltp(self.latest.get(self.spot_token, {}).get("raw", {}))
            if not is_valid_number(center):
                center = 25000.0
            atm = round(center / step) * step
            strikes = {atm + i * step for i in range(-count, count + 1)}
            rows = record_list(self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY"))
            options = []
            for r in rows:
                sym = str(r.get("pTrdSymbol") or r.get("ts") or r.get("symbol") or "").upper()
                if "NIFTY" not in sym or not (sym.endswith("CE") or sym.endswith("PE")):
                    continue
                op = option_type(r)
                exp = parse_expiry(r.get("pExpiryDate") or r.get("lExpiryDate") or r.get("expiryDate") or r.get("expiry"))
                st = strike(r)
                tok = token_from_record(r)
                if op in ("CE", "PE") and exp and tok and st in strikes and exp.date() >= now_ist().date():
                    options.append((exp, tok, st, op, sym))
            if not options:
                return 0
            target = min(x[0] for x in options)
            self.active_expiry = target
            self.pcr_tokens = []
            self.pcr_records = {}
            for exp, tok, st, op, sym in options:
                if exp.date() == target.date():
                    self.pcr_tokens.append(tok)
                    self.pcr_records[tok] = {"strike": st, "option_type": op, "expiry": exp, "symbol": sym}
            self.pcr_tokens = sorted(set(self.pcr_tokens))
            return len(self.pcr_tokens)
        except Exception as exc:
            self.last_error = f"option discovery: {exc}"
            return 0

    def discover(self) -> Dict[str, Any]:
        self.resolve_future()
        self.discover_options()
        return {"future_token": self.future_token, "future_symbol": self.future_symbol,
                "option_tokens": len(self.pcr_tokens), "heavyweights": len(self.heavy_tokens)}

    def _quote_batch(self) -> List[Dict[str, str]]:
        tokens = [(self.spot_token, "nse_cm"), (str(self.future_token), "nse_fo")]
        tokens += [(str(t), "nse_cm") for t in self.heavy_tokens.values()]
        tokens += [(str(t), "nse_fo") for t in self.pcr_tokens]
        seen = set()
        result = []
        for token, segment in tokens:
            key = (str(token), segment)
            if key not in seen:
                result.append({"instrument_token": str(token), "exchange_segment": segment})
                seen.add(key)
        return result

    def poll(self) -> int:
        if not self.client or not self.authenticated:
            raise RuntimeError("Kotak is not authenticated")
        self.last_poll = now_ist()
        count = 0
        try:
            response = self.client.quotes(instrument_tokens=self._quote_batch(), quote_type="all")
            rows = record_list(response)
            for row in rows:
                event = self._publish_kotak_record(row, source_type="poll")
                if event:
                    count += 1
            self.poll_count += 1
            self.last_error = ""
            return count
        except Exception as exc:
            self.last_error = f"quote poll: {exc}"
            raise

    def subscribe(self) -> int:
        if not self.client or not self.authenticated:
            raise RuntimeError("Kotak is not authenticated")
        tokens = self._quote_batch()
        try:
            self.client.subscribe(instrument_tokens=[{"instrument_token": self.spot_token, "exchange_segment": "nse_cm"}], isIndex=True)
            rest = [x for x in tokens if x["instrument_token"] != self.spot_token]
            if rest:
                self.client.subscribe(instrument_tokens=rest, isIndex=False)
            self.streaming = True
            return len(tokens)
        except Exception as exc:
            self.last_error = f"subscribe: {exc}"
            raise

    def status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "authenticated": self.authenticated,
                "connected": self.connected,
                "streaming": self.streaming,
                "last_poll": self.last_poll.isoformat() if self.last_poll else None,
                "last_tick": self.last_tick.isoformat() if self.last_tick else None,
                "poll_count": self.poll_count,
                "published_count": self.publish_count,
                "last_error": self.last_error,
                "future_token": self.future_token,
                "future_symbol": self.future_symbol,
                "option_tokens": len(self.pcr_tokens),
            }

# -----------------------------------------------------------------------------
# Yahoo raw historical producer
# -----------------------------------------------------------------------------
class YahooRawProducer:
    """Historical raw OHLCV producer. No indicator calculation."""
    def __init__(self, store: SharedRawStore):
        self.store = store
        self.last_error = ""
        self.last_run = None
        self.rows_published = 0

    def publish_dataframe(self, symbol: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        count = 0
        for idx, row in df.iterrows():
            ts = pd.Timestamp(idx)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            ts = ts.tz_convert(IST)
            raw = {
                "symbol": symbol,
                "instrument_token": symbol,
                "exchange": "NSE",
                "exchange_segment": "nse_cm",
                "observation_timestamp": ts.isoformat(),
                "open": safe_float(row.get("Open")),
                "high": safe_float(row.get("High")),
                "low": safe_float(row.get("Low")),
                "close": safe_float(row.get("Close")),
                "volume": safe_float(row.get("Volume")),
            }
            try:
                self.store.publish(raw, source="yahoo_finance", source_type="historical")
                count += 1
            except Exception as exc:
                self.last_error = str(exc)
        self.rows_published += count
        return count

    def fetch(self, tickers: List[str], period: str = "1y") -> Dict[str, Any]:
        if yf is None:
            raise RuntimeError("yfinance is not installed")
        self.last_run = now_ist()
        published = 0
        successful = 0
        for ticker in tickers:
            try:
                df = yf.download(ticker, period=period, interval="1d", auto_adjust=False,
                                 progress=False, threads=False)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                n = self.publish_dataframe(ticker, df)
                if n:
                    successful += 1
                    published += n
            except Exception as exc:
                self.last_error = f"{ticker}: {exc}"
        return {"tickers_requested": len(tickers), "tickers_with_data": successful, "rows_published": published}

# -----------------------------------------------------------------------------
# Consumer health
# -----------------------------------------------------------------------------
def read_consumer_health() -> Dict[str, Any]:
    result = {}
    for path in sorted(CONSUMER_DIR.glob("*.json")):
        try:
            result[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            result[path.stem] = {"status": "INVALID_HEARTBEAT"}
    return result


def consumer_age(heartbeat: Mapping[str, Any]) -> Optional[float]:
    value = heartbeat.get("heartbeat_timestamp") or heartbeat.get("timestamp")
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return max(0.0, (now_ist() - dt.astimezone(IST)).total_seconds())
    except Exception:
        return None


def write_producer_health(store: SharedRawStore, kotak: KotakRawProducer, yahoo: YahooRawProducer) -> None:
    payload = {
        "schema_version": HEARTBEAT_VERSION,
        "producer_version": PRODUCER_VERSION,
        "timestamp": iso_now(),
        "producer_status": store.status(),
        "kotak": kotak.status(),
        "yahoo": {"last_run": yahoo.last_run.isoformat() if yahoo.last_run else None,
                  "rows_published": yahoo.rows_published, "last_error": yahoo.last_error},
    }
    tmp = HEALTH_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(safe_json_value(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, HEALTH_FILE)

# -----------------------------------------------------------------------------
# Streamlit UI
# -----------------------------------------------------------------------------
def main() -> None:
    if st is None:
        raise RuntimeError("This application requires Streamlit")
    st.set_page_config(page_title="Common Raw Data Producer", page_icon="📡", layout="wide")

    if "store" not in st.session_state:
        st.session_state.store = SharedRawStore()
    if "kotak" not in st.session_state:
        st.session_state.kotak = KotakRawProducer(st.session_state.store)
    if "yahoo" not in st.session_state:
        st.session_state.yahoo = YahooRawProducer(st.session_state.store)

    store: SharedRawStore = st.session_state.store
    kotak: KotakRawProducer = st.session_state.kotak
    yahoo: YahooRawProducer = st.session_state.yahoo

    st.title("COMMON RAW DATA PRODUCER")
    st.caption("Raw market observations only • independent consumer boundary • no engine intelligence")

    # Auto health file refresh on every rerun.
    write_producer_health(store, kotak, yahoo)

    c1, c2, c3, c4 = st.columns(4)
    ks = kotak.status()
    ps = store.status()
    with c1:
        st.metric("RAW PRODUCER", "READY")
    with c2:
        st.metric("KOTAK", "CONNECTED" if ks["authenticated"] else "NOT CONNECTED")
    with c3:
        st.metric("RAW EVENTS", f"{ps['sequence']:,}")
    with c4:
        st.metric("RAW SYMBOLS", f"{ps['latest_symbols']:,}")

    st.divider()
    left, right = st.columns([1, 1])
    with left:
        st.subheader("1. Source Authentication")
        cred = kotak.credentials_status()
        if cred["credentials_present"]:
            st.success("Kotak credentials present")
        else:
            st.warning("Kotak credentials not configured")
            if cred["missing"]:
                st.caption("Missing: " + ", ".join(cred["missing"]))
        b1, b2, b3 = st.columns(3)
        if b1.button("Login Kotak", use_container_width=True):
            try:
                kotak.login()
                st.success("Authentication successful")
            except Exception as exc:
                st.error(str(exc))
        if b2.button("Discover Instruments", use_container_width=True):
            try:
                st.json(kotak.discover())
            except Exception as exc:
                st.error(str(exc))
        if b3.button("Subscribe Feed", use_container_width=True):
            try:
                n = kotak.subscribe()
                st.success(f"Subscribed to {n} raw instruments")
            except Exception as exc:
                st.error(str(exc))

        st.subheader("Kotak Health")
        st.json(ks)

    with right:
        st.subheader("2. Raw Store")
        st.code(str(ROOT), language="text")
        st.write("**Boundary:** RAW ONLY")
        st.write("**Schema:**", RAW_SCHEMA_VERSION)
        st.write("**Producer:**", PRODUCER_VERSION)
        st.write("**Consumer intelligence accepted:** NO")
        st.subheader("Consumer Health")
        consumers = read_consumer_health()
        if not consumers:
            st.info("No consumer heartbeat files found yet.")
        for name, hb in consumers.items():
            age = consumer_age(hb)
            healthy = hb.get("status") in {"HEALTHY", "CONSUMING", "OK"} and (age is None or age <= 60)
            st.write(f"**{name}** — {'🟢 HEALTHY' if healthy else '🟠 STALE/UNKNOWN'}")
            st.caption(f"Last heartbeat: {hb.get('heartbeat_timestamp', hb.get('timestamp', 'n/a'))} | age={age:.1f}s" if age is not None else "No heartbeat age")

    st.divider()
    st.subheader("3. Manual Raw Poll")
    if st.button("Fetch + Publish Current Raw Snapshot", type="primary"):
        try:
            if not kotak.authenticated:
                kotak.login()
            if not kotak.future_symbol:
                kotak.discover()
            n = kotak.poll()
            write_producer_health(store, kotak, yahoo)
            st.success(f"Published {n} raw observations")
        except Exception as exc:
            st.error(str(exc))

    st.subheader("4. Historical Raw Capture (Yahoo)")
    st.caption("This downloads raw daily OHLCV only. No indicators or rankings are computed here.")
    default_tickers = "^NSEI,RELIANCE.NS,HDFCBANK.NS,ICICIBANK.NS,INFY.NS,TCS.NS,ITC.NS,LT.NS,AXISBANK.NS,KOTAKBANK.NS,SBIN.NS"
    ticker_text = st.text_area("Yahoo tickers (comma separated)", default_tickers, height=80)
    period = st.selectbox("Historical period", ["1mo", "3mo", "6mo", "1y", "2y"], index=3)
    if st.button("Fetch + Publish Historical Raw", use_container_width=True):
        try:
            tickers = [x.strip() for x in ticker_text.split(",") if x.strip()]
            result = yahoo.fetch(tickers, period=period)
            write_producer_health(store, kotak, yahoo)
            st.success(f"Published {result['rows_published']:,} raw daily rows from {result['tickers_with_data']}/{result['tickers_requested']} tickers")
            st.json(result)
        except Exception as exc:
            st.error(str(exc))

    st.divider()
    st.subheader("5. Latest Raw Observations")
    latest = store.latest
    if latest:
        rows = []
        for key, event in list(latest.items())[-50:]:
            raw = event.get("raw", {})
            rows.append({
                "key": key,
                "symbol": event.get("symbol"),
                "source": event.get("source"),
                "received_at": event.get("received_at"),
                "ltp": raw.get("ltp"),
                "open": raw.get("open"),
                "high": raw.get("high"),
                "low": raw.get("low"),
                "close": raw.get("close"),
                "volume": raw.get("volume"),
                "oi": raw.get("oi"),
                "bid": raw.get("bid"),
                "ask": raw.get("ask"),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No raw observations published yet.")

    st.caption("The producer never consumes engine output. It only publishes raw observations.")


if __name__ == "__main__":
    main()
