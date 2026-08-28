#!/usr/bin/env python3
"""
COMMON RAW DATA PRODUCER
========================
One standalone Streamlit application that owns market-data credentials and
publishes RAW observations to a common remote Supabase raw-data bus.

Architecture:
    Kotak / Yahoo
         |
         v
    RAW PRODUCER
         |
         v
    SUPABASE RAW BUS
       /    |    \
    NIFTY ALPHA  GSR

STRICT CONTRACT
---------------
This app is a producer only. It never consumes engine opinions and never
calculates indicators, scores, signals, regimes, predictions, rankings,
labels, entries, targets, stops or trade decisions.

Cross-app source of truth:
    Supabase tables created by common_raw_schema.sql

Local disk is only an optional audit/cache mirror. It is NOT the shared bus.

Secrets:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY
    KOTAK_CONSUMER_KEY
    KOTAK_MOBILE
    KOTAK_UCC
    KOTAK_TOTP
    KOTAK_MPIN

Optional:
    KOTAK_ENVIRONMENT=prod
    RAW_PRODUCER_POLL_SECONDS=3
    RAW_LOCAL_CACHE_DIR=./raw_producer_cache
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import struct
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

try:
    from neo_api_client import NeoAPI
except Exception:
    NeoAPI = None

try:
    import yfinance as yf
except Exception:
    yf = None

IST = ZoneInfo("Asia/Kolkata")
PRODUCER_VERSION = "RAW_PRODUCER_2.0.0"
RAW_SCHEMA_VERSION = "RAW_OBSERVATION_2.0"
HEARTBEAT_VERSION = "RAW_HEALTH_1.0"

LOCAL_ROOT = Path(os.getenv("RAW_LOCAL_CACHE_DIR", "./raw_producer_cache"))
LOCAL_ROOT.mkdir(parents=True, exist_ok=True)
LOCAL_OBS = LOCAL_ROOT / "observations"
LOCAL_OBS.mkdir(parents=True, exist_ok=True)

POLL_SECONDS = max(1, int(os.getenv("RAW_PRODUCER_POLL_SECONDS", "3")))
KOTAK_ENVIRONMENT = os.getenv("KOTAK_ENVIRONMENT", "prod")

HEAVYWEIGHT_TOKENS = {
    "HDFCBANK": "1333", "RELIANCE": "2885", "ICICIBANK": "4963",
    "INFY": "1594", "ITC": "1660", "TCS": "11536",
    "LT": "11483", "AXISBANK": "5900", "KOTAKBANK": "1922",
    "SBIN": "3045",
}

FORBIDDEN_FIELDS = {
    "alpha", "alpha_score", "score", "selection_score", "ranking", "rank",
    "signal", "signals", "bias", "market_bias", "regime", "regime_score",
    "prediction", "probability", "confidence", "label", "trade_decision",
    "decision", "recommendation", "thesis", "invalidation", "target",
    "stop_loss", "entry", "final_2", "final_1", "final_candidates",
    "day_ahead_score", "setup_score", "quality_score", "composite_score",
}

def now_ist() -> datetime:
    return datetime.now(IST)

def iso_now() -> str:
    return now_ist().isoformat()

def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        x = float(str(value).replace(",", "").strip())
        return x if math.isfinite(x) else None
    except Exception:
        return None

def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return str(value)

def secret(name: str) -> str:
    value = os.getenv(name, "")
    if value:
        return str(value).strip()
    try:
        value = st.secrets.get(name, "")
        return str(value).strip() if value else ""
    except Exception:
        return ""

def normalize_mobile(value: str) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    return "+91" + digits if len(digits) == 10 and digits[0] in "6789" else str(value or "").strip()

def generate_totp(value: str) -> str:
    raw = str(value or "").replace(" ", "").upper()
    if raw.isdigit() and len(raw) == 6:
        return raw
    try:
        padded = raw + "=" * ((8 - len(raw) % 8) % 8)
        key = base64.b32decode(padded, casefold=True)
        counter = int(time.time() // 30)
        digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
        offset = digest[-1] & 15
        code = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000
        return f"{code:06d}"
    except Exception:
        return raw

def token_from_record(row: Mapping[str, Any]) -> str:
    for key in ("exchange_token", "pSymbol", "pSymbolToken", "instrument_token",
                "instrumentToken", "tok", "token", "pToken", "tk"):
        if row.get(key) not in (None, ""):
            return str(row[key]).strip()
    return ""

def extract_ltp(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("ltp", "lp", "last_price", "last_traded_price", "c", "close", "lastPrice"):
        x = safe_float(row.get(key))
        if x is not None and x > 0:
            return x
    return None

def extract_oi(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("oi", "open_interest", "openInterest", "OpenInterest", "oI", "OI",
                "open_int", "opnInterest", "openInt", "dOpenInterest"):
        x = safe_float(row.get(key))
        if x is not None and x >= 0:
            return x
    return None

def record_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if not isinstance(value, dict):
        return []
    for key in ("data", "result", "records", "data_list", "scrips", "list", "message"):
        item = value.get(key)
        if isinstance(item, list):
            return [x for x in item if isinstance(x, dict)]
        if isinstance(item, dict):
            for nested in ("data", "records", "result", "scrips"):
                child = item.get(nested)
                if isinstance(child, list):
                    return [x for x in child if isinstance(x, dict)]
    return []

def reject_intelligence(payload: Mapping[str, Any]) -> None:
    keys = {str(k).strip().lower() for k in payload}
    bad = sorted(keys.intersection(FORBIDDEN_FIELDS))
    if bad:
        raise ValueError("RAW boundary violation: " + ", ".join(bad))

def canonical_raw(payload: Mapping[str, Any]) -> Dict[str, Any]:
    reject_intelligence(payload)
    allowed = {
        "symbol", "instrument_token", "exchange", "exchange_segment",
        "observation_timestamp", "open", "high", "low", "close", "ltp",
        "prev_close", "volume", "oi", "bid", "ask", "bid_qty", "ask_qty",
        "vwap", "upper_circuit", "lower_circuit", "price_band",
        "last_traded_time", "strike", "option_type", "expiry", "source_sequence",
        "source_status",
    }
    return {k: json_safe(v) for k, v in payload.items() if k in allowed}

class SupabaseRawBus:
    """Minimal REST client. Producer uses service-role key; consumers do not."""

    def __init__(self) -> None:
        self.url = secret("SUPABASE_URL").rstrip("/")
        self.key = secret("SUPABASE_SERVICE_ROLE_KEY")
        self.enabled = bool(self.url and self.key)
        self.last_error = ""
        self.last_publish = None
        self.published = 0

    def _request(self, method: str, table: str, payload: Any = None,
                 query: str = "", prefer: str = "return=minimal") -> Any:
        if not self.enabled:
            raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not configured")
        url = f"{self.url}/rest/v1/{table}"
        if query:
            url += "?" + query
        body = None if payload is None else json.dumps(json_safe(payload)).encode()
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        }
        req = Request(url, data=body, headers=headers, method=method.upper())
        try:
            with urlopen(req, timeout=20) as response:
                raw = response.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase HTTP {exc.code}: {detail[:500]}") from exc
        except URLError as exc:
            raise RuntimeError(f"Supabase network error: {exc}") from exc

    def publish(self, event: Mapping[str, Any]) -> None:
        self._request("POST", "raw_observations", dict(event), prefer="return=minimal")
        self.last_publish = iso_now()
        self.published += 1

    def latest(self, limit: int = 100) -> List[Dict[str, Any]]:
        query = f"select=*&order=received_at.desc&limit={max(1, min(limit, 1000))}"
        result = self._request("GET", "raw_observations", query=query)
        return result if isinstance(result, list) else []

    def heartbeat(self, consumer_name: str, status: str, **extra: Any) -> None:
        row = {
            "consumer_name": consumer_name,
            "status": status,
            "heartbeat_timestamp": iso_now(),
            "producer_version": PRODUCER_VERSION,
            **json_safe(extra),
        }
        self._request(
            "POST", "consumer_heartbeats", row,
            prefer="resolution=merge-duplicates,return=minimal",
        )

    def producer_health(self, payload: Mapping[str, Any]) -> None:
        self._request(
            "POST", "producer_health",
            dict(payload),
            prefer="resolution=merge-duplicates,return=minimal",
        )

    def health(self) -> Dict[str, Any]:
        if not self.enabled:
            return {"configured": False, "reachable": False, "error": "missing Supabase secrets"}
        try:
            self._request("GET", "raw_observations", query="select=id&limit=1")
            self.last_error = ""
            return {"configured": True, "reachable": True}
        except Exception as exc:
            self.last_error = str(exc)
            return {"configured": True, "reachable": False, "error": str(exc)}

class LocalAuditMirror:
    """Local audit only. Never advertised as the cross-app source of truth."""

    def __init__(self) -> None:
        self.sequence = 0
        self.lock = threading.RLock()

    def write(self, event: Mapping[str, Any]) -> None:
        with self.lock:
            self.sequence += 1
            day = now_ist().strftime("%Y-%m-%d")
            path = LOCAL_OBS / f"raw_{day}.jsonl"
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(json_safe(event), ensure_ascii=False) + "\n")

class KotakRawProducer:
    def __init__(self, bus: SupabaseRawBus, mirror: LocalAuditMirror) -> None:
        self.bus, self.mirror = bus, mirror
        self.client = None
        self.authenticated = False
        self.connected = False
        self.streaming = False
        self.last_error = ""
        self.last_tick = None
        self.last_poll = None
        self.poll_count = 0
        self.publish_count = 0
        self.future_token = secret("KOTAK_NIFTY_FUT_TOKEN")
        self.future_symbol = ""
        self.pcr_tokens: List[str] = []
        self.pcr_meta: Dict[str, Dict[str, Any]] = {}

    def credentials_status(self) -> Dict[str, Any]:
        names = ("KOTAK_CONSUMER_KEY", "KOTAK_MOBILE", "KOTAK_UCC", "KOTAK_TOTP", "KOTAK_MPIN")
        present = {n: bool(secret(n)) for n in names}
        return {"credentials_present": all(present.values()), "missing": [n for n, ok in present.items() if not ok]}

    def login(self) -> bool:
        if NeoAPI is None:
            raise RuntimeError("neo_api_client is not installed")
        status = self.credentials_status()
        if not status["credentials_present"]:
            raise RuntimeError("Missing credentials: " + ", ".join(status["missing"]))
        c = {n: secret(n) for n in ("KOTAK_CONSUMER_KEY","KOTAK_MOBILE","KOTAK_UCC","KOTAK_TOTP","KOTAK_MPIN")}
        self.client = NeoAPI(
            environment=KOTAK_ENVIRONMENT,
            access_token=None,
            neo_fin_key=None,
            consumer_key=c["KOTAK_CONSUMER_KEY"],
        )
        self.client.on_message = self.on_message
        self.client.on_error = self.on_error
        self.client.on_close = self.on_close
        self.client.on_open = self.on_open
        login = self.client.totp_login(
            mobile_number=normalize_mobile(c["KOTAK_MOBILE"]),
            ucc=c["KOTAK_UCC"],
            totp=generate_totp(c["KOTAK_TOTP"]),
        )
        if isinstance(login, dict) and login.get("error"):
            raise RuntimeError(str(login))
        validated = self.client.totp_validate(mpin=c["KOTAK_MPIN"])
        if isinstance(validated, dict) and validated.get("error"):
            raise RuntimeError(str(validated))
        self.authenticated = True
        self.connected = True
        self.last_error = ""
        return True

    def on_open(self, _message=None) -> None:
        self.connected = True

    def on_error(self, error=None) -> None:
        self.connected = False
        self.last_error = str(error or "")

    def on_close(self, _message=None) -> None:
        self.connected = False
        self.streaming = False

    def _symbol_for_token(self, token: str) -> str:
        if token == "Nifty 50":
            return "NIFTY_SPOT"
        if token == self.future_token:
            return self.future_symbol or "NIFTY_FUT"
        for symbol, tok in HEAVYWEIGHT_TOKENS.items():
            if str(tok) == str(token):
                return symbol
        meta = self.pcr_meta.get(str(token))
        return str(meta.get("symbol")) if meta else token

    def _normalize_quote(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        token = token_from_record(row)
        if not token:
            return None
        symbol = str(row.get("display_symbol") or row.get("pTrdSymbol") or row.get("ts") or "")
        symbol = symbol or self._symbol_for_token(token)
        return canonical_raw({
            "symbol": symbol,
            "instrument_token": token,
            "exchange": "NSE",
            "exchange_segment": str(row.get("exchange_segment") or "nse_cm"),
            "observation_timestamp": str(row.get("timestamp") or row.get("ft") or iso_now()),
            "open": safe_float(row.get("o") or row.get("open") or row.get("openPrice")),
            "high": safe_float(row.get("h") or row.get("high") or row.get("highPrice")),
            "low": safe_float(row.get("l") or row.get("low") or row.get("lowPrice")),
            "close": safe_float(row.get("c") or row.get("close") or row.get("closePrice")),
            "ltp": extract_ltp(row),
            "prev_close": safe_float(row.get("pdc") or row.get("prev_close") or row.get("previousClose")),
            "volume": safe_float(row.get("v") or row.get("volume") or row.get("vol")),
            "oi": extract_oi(row),
            "bid": safe_float(row.get("bp") or row.get("bid")),
            "ask": safe_float(row.get("sp") or row.get("ask")),
            "bid_qty": safe_float(row.get("bq") or row.get("bid_qty")),
            "ask_qty": safe_float(row.get("sq") or row.get("ask_qty")),
            "vwap": safe_float(row.get("vwap") or row.get("avp") or row.get("averagePrice")),
            "upper_circuit": safe_float(row.get("upper_circuit") or row.get("upperCircuit")),
            "lower_circuit": safe_float(row.get("lower_circuit") or row.get("lowerCircuit")),
            "last_traded_time": str(row.get("ltt") or row.get("ft") or ""),
            "source_sequence": row.get("sequence") or row.get("seq"),
            "source_status": "LIVE",
        })

    def publish_row(self, row: Mapping[str, Any], source_type: str = "kotak_live") -> bool:
        raw = self._normalize_quote(row)
        if not raw:
            return False
        event = {
            "schema_version": RAW_SCHEMA_VERSION,
            "event_id": str(uuid.uuid4()),
            "source": "kotak_neo",
            "source_type": source_type,
            "symbol": raw["symbol"],
            "instrument_token": raw.get("instrument_token"),
            "exchange": raw.get("exchange"),
            "observation_timestamp": raw.get("observation_timestamp") or iso_now(),
            "received_at": iso_now(),
            "raw": raw,
        }
        self.mirror.write(event)
        self.bus.publish(event)
        self.publish_count += 1
        self.last_tick = now_ist()
        return True

    def on_message(self, message=None) -> None:
        try:
            if isinstance(message, str):
                message = json.loads(message)
            for row in (message if isinstance(message, list) else [message]):
                if isinstance(row, dict):
                    self.publish_row(row, "kotak_websocket")
        except Exception as exc:
            self.last_error = f"websocket: {exc}"
                def quote_tokens(self) -> List[Dict[str, str]]:
        result = [
            {
                "instrument_token": "Nifty 50",
                "exchange_segment": "nse_cm",
            }
        ]

        result += [
            {
                "instrument_token": str(token),
                "exchange_segment": "nse_cm",
            }
            for token in HEAVYWEIGHT_TOKENS.values()
        ]

        if self.future_token:
            result.append(
                {
                    "instrument_token": str(self.future_token),
                    "exchange_segment": "nse_fo",
                }
            )

        result += [
            {
                "instrument_token": str(token),
                "exchange_segment": "nse_fo",
            }
            for token in self.pcr_tokens
        ]

        unique = []
        seen = set()

        for item in result:
            key = (
                item["instrument_token"],
                item["exchange_segment"],
            )

            if key not in seen:
                unique.append(item)
                seen.add(key)

        return unique

    def discover_future(self) -> None:
        if not self.client:
            return

        try:
            rows = record_list(
                self.client.search_scrip(
                    exchange_segment="nse_fo",
                    symbol="NIFTY",
                )
            )

            candidates = []

            for row in rows:
                symbol = str(
                    row.get("pTrdSymbol")
                    or row.get("ts")
                    or row.get("symbol")
                    or ""
                ).upper()

                token = token_from_record(row)

                expiry = str(
                    row.get("pExpiryDate")
                    or row.get("lExpiryDate")
                    or row.get("expiry")
                    or ""
                )

                if (
                    token
                    and symbol.startswith("NIFTY")
                    and "FUT" in symbol
                ):
                    candidates.append(
                        (
                            expiry,
                            token,
                            symbol,
                        )
                    )

            if candidates:
                candidates.sort(
                    key=lambda x: x[0]
                )

                _, self.future_token, self.future_symbol = (
                    candidates[0]
                )

        except Exception as exc:
            self.last_error = (
                f"future discovery: {exc}"
            )

    def discover_options(
        self,
        count: int = 5,
        step: float = 50.0,
    ) -> int:
        if not self.client:
            return 0

        try:
            rows = record_list(
                self.client.search_scrip(
                    exchange_segment="nse_fo",
                    symbol="NIFTY",
                )
            )

            # Option discovery is deliberately raw.
            # If no live spot is available yet, the producer does not
            # invent a market-derived ATM. It simply skips option discovery.
            center = None

            if center is None:
                self.pcr_tokens = []
                self.pcr_meta = {}
                return 0

            atm = round(center / step) * step

            wanted = {
                atm + i * step
                for i in range(-count, count + 1)
            }

            found = []

            for row in rows:
                symbol = str(
                    row.get("pTrdSymbol")
                    or row.get("ts")
                    or row.get("symbol")
                    or ""
                ).upper()

                token = token_from_record(row)

                option_type = (
                    "CE"
                    if symbol.endswith("CE")
                    else (
                        "PE"
                        if symbol.endswith("PE")
                        else ""
                    )
                )

                raw_strike = safe_float(
                    row.get("dStrikePrice")
                    or row.get("strikePrice")
                    or row.get("strike")
                )

                if (
                    raw_strike is not None
                    and raw_strike > 100000
                ):
                    raw_strike /= 100.0

                if (
                    token
                    and option_type
                    and raw_strike in wanted
                    and "NIFTY" in symbol
                ):
                    found.append(
                        (
                            token,
                            raw_strike,
                            option_type,
                            symbol,
                        )
                    )

            self.pcr_tokens = [
                item[0]
                for item in found
            ]

            self.pcr_meta = {
                item[0]: {
                    "strike": item[1],
                    "option_type": item[2],
                    "symbol": item[3],
                }
                for item in found
            }

            return len(self.pcr_tokens)

        except Exception as exc:
            self.last_error = (
                f"option discovery: {exc}"
            )
            return 0

    def discover(self) -> Dict[str, Any]:
        self.discover_future()

        option_count = self.discover_options()

        return {
            "future_token": self.future_token,
            "future_symbol": self.future_symbol,
            "option_tokens": option_count,
        }

    def poll(self) -> int:
        if not self.client or not self.authenticated:
            raise RuntimeError(
                "Kotak is not authenticated"
            )

        self.last_poll = now_ist()

        response = self.client.quotes(
            instrument_tokens=self.quote_tokens(),
            quote_type="all",
        )

        rows = record_list(response)

        count = 0

        for row in rows:
            try:
                if self.publish_row(
                    row,
                    "kotak_poll",
                ):
                    count += 1

            except Exception as exc:
                self.last_error = str(exc)

        self.poll_count += 1
        self.connected = True

        return count

    def subscribe(self) -> int:
        if not self.client or not self.authenticated:
            raise RuntimeError(
                "Kotak is not authenticated"
            )

        tokens = self.quote_tokens()

        self.client.subscribe(
            instrument_tokens=[
                {
                    "instrument_token": "Nifty 50",
                    "exchange_segment": "nse_cm",
                }
            ],
            isIndex=True,
        )

        rest = [
            item
            for item in tokens
            if item["instrument_token"] != "Nifty 50"
        ]

        if rest:
            self.client.subscribe(
                instrument_tokens=rest,
                isIndex=False,
            )

        self.streaming = True

        return len(tokens)

    def status(self) -> Dict[str, Any]:
        return {
            "authenticated": self.authenticated,
            "connected": self.connected,
            "streaming": self.streaming,
            "last_tick": (
                self.last_tick.isoformat()
                if self.last_tick
                else None
            ),
            "last_poll": (
                self.last_poll.isoformat()
                if self.last_poll
                else None
            ),
            "poll_count": self.poll_count,
            "published_count": self.publish_count,
            "last_error": self.last_error,
            "future_token": self.future_token,
            "future_symbol": self.future_symbol,
            "option_tokens": len(
                self.pcr_tokens
            ),
        }


class YahooRawProducer:
    """
    Historical Yahoo Finance producer.

    Only raw OHLCV observations are published.
    No indicators, rankings or alpha calculations happen here.
    """

    def __init__(
        self,
        bus: SupabaseRawBus,
        mirror: LocalAuditMirror,
    ) -> None:
        self.bus = bus
        self.mirror = mirror

        self.last_error = ""
        self.last_run = None
        self.rows_published = 0

    def publish_dataframe(
        self,
        ticker: str,
        frame: pd.DataFrame,
    ) -> int:

        if frame is None or frame.empty:
            return 0

        count = 0

        for idx, row in frame.iterrows():

            ts = pd.Timestamp(idx)

            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")

            ts = ts.tz_convert(IST)

            raw = canonical_raw(
                {
                    "symbol": ticker,
                    "instrument_token": ticker,
                    "exchange": "NSE",
                    "exchange_segment": "nse_cm",
                    "observation_timestamp": ts.isoformat(),
                    "open": safe_float(
                        row.get("Open")
                    ),
                    "high": safe_float(
                        row.get("High")
                    ),
                    "low": safe_float(
                        row.get("Low")
                    ),
                    "close": safe_float(
                        row.get("Close")
                    ),
                    "volume": safe_float(
                        row.get("Volume")
                    ),
                    "source_status": "HISTORICAL",
                }
            )

            event = {
                "schema_version": RAW_SCHEMA_VERSION,
                "event_id": str(uuid.uuid4()),
                "source": "yahoo_finance",
                "source_type": "historical_daily",
                "symbol": ticker,
                "instrument_token": ticker,
                "exchange": "NSE",
                "observation_timestamp": raw[
                    "observation_timestamp"
                ],
                "received_at": iso_now(),
                "raw": raw,
            }

            try:
                self.mirror.write(event)

                self.bus.publish(event)

                count += 1

            except Exception as exc:
                self.last_error = str(exc)

        self.rows_published += count

        return count

    def fetch(
        self,
        tickers: List[str],
        period: str = "1y",
    ) -> Dict[str, Any]:

        if yf is None:
            raise RuntimeError(
                "yfinance is not installed"
            )

        self.last_run = now_ist()

        successful = 0
        published = 0

        for ticker in tickers:

            try:
                frame = yf.download(
                    ticker,
                    period=period,
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )

                if isinstance(
                    frame.columns,
                    pd.MultiIndex,
                ):
                    frame.columns = (
                        frame.columns
                        .get_level_values(0)
                    )

                n = self.publish_dataframe(
                    ticker,
                    frame,
                )

                if n:
                    successful += 1
                    published += n

            except Exception as exc:
                self.last_error = (
                    f"{ticker}: {exc}"
                )

        return {
            "tickers_requested": len(tickers),
            "tickers_with_data": successful,
            "rows_published": published,
        }


def publish_producer_health(
    bus: SupabaseRawBus,
    kotak: KotakRawProducer,
    yahoo: YahooRawProducer,
) -> Dict[str, Any]:

    payload = {
        "producer_name": "raw_data_producer",
        "producer_version": PRODUCER_VERSION,
        "status": "READY",
        "heartbeat_timestamp": iso_now(),

        "kotak_authenticated": (
            kotak.authenticated
        ),

        "kotak_connected": (
            kotak.connected
        ),

        "kotak_streaming": (
            kotak.streaming
        ),

        "kotak_last_tick": (
            kotak.last_tick.isoformat()
            if kotak.last_tick
            else None
        ),

        "kotak_published": (
            kotak.publish_count
        ),

        "yahoo_last_run": (
            yahoo.last_run.isoformat()
            if yahoo.last_run
            else None
        ),

        "yahoo_rows_published": (
            yahoo.rows_published
        ),

        "last_error": (
            kotak.last_error
            or yahoo.last_error
            or bus.last_error
        ),
    }

    if bus.enabled:

        try:
            bus.producer_health(
                payload
            )

        except Exception as exc:
            payload[
                "remote_health_error"
            ] = str(exc)

    return payload
    def main() -> None:
    st.set_page_config(
        page_title="Common Raw Data Producer",
        page_icon="📡",
        layout="wide",
    )

    if "bus" not in st.session_state:
        st.session_state.bus = SupabaseRawBus()

    if "mirror" not in st.session_state:
        st.session_state.mirror = LocalAuditMirror()

    if "kotak" not in st.session_state:
        st.session_state.kotak = KotakRawProducer(
            st.session_state.bus,
            st.session_state.mirror,
        )

    if "yahoo" not in st.session_state:
        st.session_state.yahoo = YahooRawProducer(
            st.session_state.bus,
            st.session_state.mirror,
        )

    bus = st.session_state.bus
    kotak = st.session_state.kotak
    yahoo = st.session_state.yahoo

    st.title("COMMON RAW DATA PRODUCER")

    st.caption(
        "One producer • one remote raw bus • "
        "NIFTY / Next-Day Alpha / GSR consume independently"
    )

    health = bus.health()

    a, b, c, d = st.columns(4)

    a.metric(
        "PRODUCER",
        "READY",
    )

    b.metric(
        "SUPABASE",
        "🟢 ONLINE"
        if health.get("reachable")
        else "🔴 OFFLINE",
    )

    c.metric(
        "KOTAK",
        "🟢 AUTH"
        if kotak.authenticated
        else "⚪ NOT AUTH",
    )

    d.metric(
        "RAW PUBLISHED",
        f"{bus.published:,}",
    )

    if not bus.enabled:

        st.error(
            "Supabase raw bus is NOT configured. "
            "Set SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY."
        )

    elif not health.get("reachable"):

        st.warning(
            health.get(
                "error",
                "Supabase is unreachable.",
            )
        )

    left, right = st.columns(2)

    # -----------------------------------------------------------------
    # KOTAK
    # -----------------------------------------------------------------
    with left:

        st.subheader(
            "KOTAK — credentials stay here"
        )

        cred = kotak.credentials_status()

        if cred["credentials_present"]:

            st.success(
                "All Kotak credentials present"
            )

        else:

            st.warning(
                "Missing: "
                + ", ".join(
                    cred["missing"]
                )
            )

        x1, x2, x3 = st.columns(3)

        if x1.button(
            "Login",
            use_container_width=True,
        ):

            try:

                kotak.login()

                st.success(
                    "Kotak authentication successful"
                )

            except Exception as exc:

                st.error(str(exc))

        if x2.button(
            "Discover",
            use_container_width=True,
        ):

            try:

                st.json(
                    kotak.discover()
                )

            except Exception as exc:

                st.error(str(exc))

        if x3.button(
            "Subscribe",
            use_container_width=True,
        ):

            try:

                st.success(
                    f"Subscribed: "
                    f"{kotak.subscribe()} instruments"
                )

            except Exception as exc:

                st.error(str(exc))

        st.json(
            kotak.status()
        )

    # -----------------------------------------------------------------
    # COMMON REMOTE RAW BUS
    # -----------------------------------------------------------------
    with right:

        st.subheader(
            "COMMON REMOTE RAW BUS"
        )

        st.write(
            "**Source of truth:** "
            "Supabase `raw_observations`"
        )

        st.write(
            "**Local disk:** "
            "audit/cache only"
        )

        st.write(
            "**Engine intelligence accepted:** NO"
        )

        st.write(
            "**Producer credentials exposed "
            "to consumers:** NO"
        )

        if st.button(
            "Test Remote Raw Bus",
            use_container_width=True,
        ):

            st.json(
                bus.health()
            )

        if st.button(
            "Show Latest Remote Raw",
            use_container_width=True,
        ):

            try:

                rows = bus.latest(50)

                if rows:

                    display = []

                    for item in rows:

                        raw = (
                            item.get("raw")
                            or {}
                        )

                        display.append(
                            {
                                "received_at":
                                    item.get(
                                        "received_at"
                                    ),

                                "source":
                                    item.get(
                                        "source"
                                    ),

                                "symbol":
                                    item.get(
                                        "symbol"
                                    ),

                                "observation_timestamp":
                                    item.get(
                                        "observation_timestamp"
                                    ),

                                "ltp":
                                    raw.get("ltp"),

                                "close":
                                    raw.get("close"),

                                "volume":
                                    raw.get("volume"),

                                "oi":
                                    raw.get("oi"),
                            }
                        )

                    st.dataframe(
                        pd.DataFrame(display),
                        use_container_width=True,
                        hide_index=True,
                    )

                else:

                    st.info(
                        "Remote raw bus has "
                        "no observations yet."
                    )

            except Exception as exc:

                st.error(str(exc))

    st.divider()

    # -----------------------------------------------------------------
    # YFINANCE
    # -----------------------------------------------------------------
    st.subheader(
        "YFINANCE — raw historical capture"
    )

    default = (
        "^NSEI,"
        "RELIANCE.NS,"
        "HDFCBANK.NS,"
        "ICICIBANK.NS,"
        "INFY.NS,"
        "TCS.NS,"
        "ITC.NS,"
        "LT.NS,"
        "AXISBANK.NS,"
        "KOTAKBANK.NS,"
        "SBIN.NS"
    )

    tickers_text = st.text_area(
        "Yahoo tickers",
        default,
    )

    period = st.selectbox(
        "Period",
        [
            "1mo",
            "3mo",
            "6mo",
            "1y",
            "2y",
        ],
        index=3,
    )

    if st.button(
        "Fetch + Publish Historical Raw",
        type="primary",
    ):

        try:

            tickers = [
                x.strip()
                for x in tickers_text.split(",")
                if x.strip()
            ]

            result = yahoo.fetch(
                tickers,
                period,
            )

            st.success(
                "Published "
                f"{result['rows_published']:,} "
                "raw rows"
            )

            st.json(result)

        except Exception as exc:

            st.error(str(exc))

    # -----------------------------------------------------------------
    # LIVE SNAPSHOT
    # -----------------------------------------------------------------
    st.subheader(
        "LIVE RAW SNAPSHOT"
    )

    if st.button(
        "Fetch + Publish Current Kotak Snapshot"
    ):

        try:

            if not kotak.authenticated:

                kotak.login()

            if not kotak.future_symbol:

                kotak.discover()

            published = kotak.poll()

            st.success(
                f"Published "
                f"{published} raw observations"
            )

        except Exception as exc:

            st.error(str(exc))

    # -----------------------------------------------------------------
    # CONSUMER HEALTH
    # -----------------------------------------------------------------
    st.subheader(
        "CONSUMER HEALTH"
    )

    if bus.enabled:

        try:

            health_rows = bus._request(
                "GET",
                "consumer_heartbeats",
                query=(
                    "select="
                    "consumer_name,"
                    "status,"
                    "heartbeat_timestamp,"
                    "updated_at"
                    "&order=updated_at.desc"
                    "&limit=50"
                ),
            )

            if health_rows:

                st.dataframe(
                    pd.DataFrame(
                        health_rows
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

            else:

                st.info(
                    "No consumer heartbeats yet."
                )

        except Exception as exc:

            st.warning(
                f"Consumer health unavailable: {exc}"
            )

    # -----------------------------------------------------------------
    # PRODUCER HEARTBEAT
    # -----------------------------------------------------------------
    payload = publish_producer_health(
        bus,
        kotak,
        yahoo,
    )

    st.caption(
        "Producer heartbeat: "
        f"{payload['heartbeat_timestamp']} "
        "• Version "
        f"{PRODUCER_VERSION}"
    )


if __name__ == "__main__":
    main()
