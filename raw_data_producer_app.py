#!/usr/bin/env python3
"""
COMMON RAW DATA PRODUCER
========================

Standalone Streamlit application for the three isolated engines:

                    KOTAK LIVE
                        |
                        |
                    YAHOO RAW
                        |
                        v
              +-------------------+
              |   RAW PRODUCER    |
              |                   |
              | RAW ONLY          |
              | NO ENGINE LOGIC   |
              +---------+---------+
                        |
                        v
                SUPABASE RAW BUS
                  /      |      \
                 /       |       \
                v        v        v
             NIFTY     ALPHA      GSR

STRICT ARCHITECTURAL CONTRACT
-----------------------------

This application is a DATA PRODUCER only.

It may:
    - authenticate with Kotak
    - receive Kotak raw market observations
    - poll Kotak raw quotes
    - subscribe to Kotak raw feed
    - discover raw instruments
    - fetch Yahoo historical OHLCV
    - publish raw observations to Supabase
    - maintain a local audit/cache mirror
    - publish producer health

It MUST NOT:
    - calculate indicators
    - calculate alpha
    - calculate scores
    - calculate rankings
    - calculate regimes
    - calculate predictions
    - calculate confidence
    - calculate signals
    - calculate labels
    - calculate entries
    - calculate targets
    - calculate stop losses
    - calculate trade decisions
    - consume NIFTY engine output
    - consume Alpha engine output
    - consume GSR output

ENGINE ISOLATION
----------------

NIFTY, Next-Day Alpha and GSR are independent consumers.

Only RAW OBSERVATIONS may cross the common boundary.

Producer credentials are never published to consumers.

REMOTE SOURCE OF TRUTH
----------------------

Supabase:
    raw_observations
    consumer_heartbeats
    producer_health

Local disk is only an audit/cache mirror.

Required secrets:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY

Kotak secrets:
    KOTAK_CONSUMER_KEY
    KOTAK_MOBILE
    KOTAK_UCC
    KOTAK_TOTP
    KOTAK_MPIN

Optional:
    KOTAK_ENVIRONMENT=prod
    KOTAK_NIFTY_FUT_TOKEN
    RAW_LOCAL_CACHE_DIR=./raw_producer_cache
    RAW_PRODUCER_POLL_SECONDS=3

Run:
    streamlit run raw_data_producer_app.py
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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st


# ============================================================================
# OPTIONAL DEPENDENCIES
# ============================================================================

try:
    from neo_api_client import NeoAPI
except Exception:
    NeoAPI = None

try:
    import yfinance as yf
except Exception:
    yf = None


# ============================================================================
# GLOBAL CONFIGURATION
# ============================================================================

IST = ZoneInfo("Asia/Kolkata")

PRODUCER_VERSION = "RAW_PRODUCER_2.1.0"
RAW_SCHEMA_VERSION = "RAW_OBSERVATION_2.0"
HEALTH_SCHEMA_VERSION = "RAW_HEALTH_1.0"

KOTAK_ENVIRONMENT = os.getenv("KOTAK_ENVIRONMENT", "prod")

POLL_SECONDS = max(
    1,
    int(os.getenv("RAW_PRODUCER_POLL_SECONDS", "3")),
)

LOCAL_ROOT = Path(
    os.getenv(
        "RAW_LOCAL_CACHE_DIR",
        "./raw_producer_cache",
    )
)

LOCAL_ROOT.mkdir(
    parents=True,
    exist_ok=True,
)

LOCAL_OBS = LOCAL_ROOT / "observations"

LOCAL_OBS.mkdir(
    parents=True,
    exist_ok=True,
)


# ============================================================================
# KOTAK INSTRUMENTS
# ============================================================================

HEAVYWEIGHT_TOKENS: Dict[str, str] = {
    "HDFCBANK": "1333",
    "RELIANCE": "2885",
    "ICICIBANK": "4963",
    "INFY": "1594",
    "ITC": "1660",
    "TCS": "11536",
    "LT": "11483",
    "AXISBANK": "5900",
    "KOTAKBANK": "1922",
    "SBIN": "3045",
}

NIFTY_INDEX_TOKEN = "Nifty 50"

DEFAULT_YAHOO_TICKERS = [
    "^NSEI",
    "RELIANCE.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
    "INFY.NS",
    "TCS.NS",
    "ITC.NS",
    "LT.NS",
    "AXISBANK.NS",
    "KOTAKBANK.NS",
    "SBIN.NS",
]


# ============================================================================
# RAW BOUNDARY
# ============================================================================

FORBIDDEN_FIELDS = {
    "alpha",
    "alpha_score",
    "score",
    "selection_score",
    "ranking",
    "rank",
    "signal",
    "signals",
    "bias",
    "market_bias",
    "regime",
    "regime_score",
    "prediction",
    "predicted",
    "probability",
    "confidence",
    "label",
    "trade_decision",
    "decision",
    "recommendation",
    "thesis",
    "invalidation",
    "target",
    "stop",
    "stop_loss",
    "entry",
    "final_2",
    "final_1",
    "final_candidates",
    "day_ahead_score",
    "setup_score",
    "quality_score",
    "composite_score",
    "direction",
    "strategy",
    "strategy_id",
}

RAW_ALLOWED_FIELDS = {
    "symbol",
    "instrument_token",
    "exchange",
    "exchange_segment",
    "observation_timestamp",
    "received_timestamp",
    "open",
    "high",
    "low",
    "close",
    "ltp",
    "prev_close",
    "volume",
    "oi",
    "open_interest",
    "bid",
    "ask",
    "bid_qty",
    "ask_qty",
    "vwap",
    "upper_circuit",
    "lower_circuit",
    "price_band",
    "last_traded_time",
    "strike",
    "option_type",
    "expiry",
    "source_sequence",
    "source_status",
}


# ============================================================================
# TIME / VALUE HELPERS
# ============================================================================

def now_ist() -> datetime:
    return datetime.now(IST)


def iso_now() -> str:
    return now_ist().isoformat()


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None

        if isinstance(value, bool):
            return None

        text = str(value).strip()

        if not text:
            return None

        text = text.replace(",", "")

        number = float(text)

        if not math.isfinite(number):
            return None

        return number

    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None

        return int(float(str(value).replace(",", "").strip()))

    except Exception:
        return None


def json_safe(value: Any) -> Any:

    if value is None:
        return None

    if isinstance(value, (str, int, bool)):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()

    if isinstance(value, Mapping):
        return {
            str(key): json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [
            json_safe(item)
            for item in value
        ]

    return str(value)


# ============================================================================
# SECRETS
# ============================================================================

def secret(name: str) -> str:

    value = os.getenv(name, "")

    if value:
        return str(value).strip()

    try:
        value = st.secrets.get(name, "")

        if value:
            return str(value).strip()

    except Exception:
        pass

    return ""


def normalize_mobile(value: str) -> str:

    raw = str(value or "").strip()

    digits = "".join(
        character
        for character in raw
        if character.isdigit()
    )

    if digits.startswith("00"):
        digits = digits[2:]

    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]

    if (
        len(digits) == 10
        and digits[0] in "6789"
    ):
        return "+91" + digits

    return raw


# ============================================================================
# TOTP
# ============================================================================

def generate_totp(value: str) -> str:

    raw = str(value or "").replace(
        " ",
        "",
    ).upper()

    # If the secret itself is already a 6 digit OTP,
    # allow it as-is.
    if raw.isdigit() and len(raw) == 6:
        return raw

    try:
        padded = raw + (
            "=" * ((8 - len(raw) % 8) % 8)
        )

        key = base64.b32decode(
            padded,
            casefold=True,
        )

        counter = int(
            time.time() // 30
        )

        digest = hmac.new(
            key,
            struct.pack(
                ">Q",
                counter,
            ),
            hashlib.sha1,
        ).digest()

        offset = digest[-1] & 15

        binary_code = (
            struct.unpack(
                ">I",
                digest[
                    offset:
                    offset + 4
                ],
            )[0]
            & 0x7FFFFFFF
        )

        otp = binary_code % 1000000

        return f"{otp:06d}"

    except Exception:
        return raw


# ============================================================================
# RAW VALIDATION
# ============================================================================

def reject_intelligence(
    payload: Mapping[str, Any],
) -> None:

    keys = {
        str(key).strip().lower()
        for key in payload.keys()
    }

    forbidden = sorted(
        keys.intersection(
            FORBIDDEN_FIELDS
        )
    )

    if forbidden:
        raise ValueError(
            "RAW boundary violation: "
            + ", ".join(forbidden)
        )


def canonical_raw(
    payload: Mapping[str, Any],
) -> Dict[str, Any]:

    if not isinstance(
        payload,
        Mapping,
    ):
        raise ValueError(
            "Raw payload must be a mapping"
        )

    reject_intelligence(payload)

    result: Dict[str, Any] = {}

    for key, value in payload.items():

        key_text = str(key)

        if key_text in RAW_ALLOWED_FIELDS:
            result[key_text] = json_safe(value)

    return result


# ============================================================================
# KOTAK RECORD HELPERS
# ============================================================================

def token_from_record(
    row: Mapping[str, Any],
) -> str:

    keys = (
        "exchange_token",
        "pSymbol",
        "pSymbolToken",
        "instrument_token",
        "instrumentToken",
        "tok",
        "token",
        "pToken",
        "tk",
    )

    for key in keys:

        value = row.get(key)

        if value not in (
            None,
            "",
        ):
            return str(value).strip()

    return ""


def extract_ltp(
    row: Mapping[str, Any],
) -> Optional[float]:

    keys = (
        "ltp",
        "lp",
        "last_price",
        "last_traded_price",
        "lastPrice",
        "c",
        "close",
    )

    for key in keys:

        value = safe_float(
            row.get(key)
        )

        if value is not None and value > 0:
            return value

    return None


def extract_oi(
    row: Mapping[str, Any],
) -> Optional[float]:

    keys = (
        "oi",
        "open_interest",
        "openInterest",
        "OpenInterest",
        "oI",
        "OI",
        "open_int",
        "opnInterest",
        "openInt",
        "dOpenInterest",
    )

    for key in keys:

        value = safe_float(
            row.get(key)
        )

        if value is not None and value >= 0:
            return value

    return None


def record_list(
    value: Any,
) -> List[Dict[str, Any]]:

    if isinstance(value, list):

        return [
            item
            for item in value
            if isinstance(item, dict)
        ]

    if not isinstance(
        value,
        dict,
    ):
        return []

    possible_keys = (
        "data",
        "result",
        "records",
        "data_list",
        "scrips",
        "list",
        "message",
    )

    for key in possible_keys:

        item = value.get(key)

        if isinstance(item, list):

            return [
                row
                for row in item
                if isinstance(row, dict)
            ]

        if isinstance(item, dict):

            for nested_key in (
                "data",
                "records",
                "result",
                "scrips",
            ):

                child = item.get(
                    nested_key
                )

                if isinstance(
                    child,
                    list,
                ):

                    return [
                        row
                        for row in child
                        if isinstance(row, dict)
                    ]

    return []


# ============================================================================
# SUPABASE RAW BUS
# ============================================================================

class SupabaseRawBus:
    """
    Minimal REST client for the common remote raw bus.

    The producer owns the service-role credential.

    Consumers only read raw observations and never receive this credential.
    """

    def __init__(self) -> None:

        self.url = secret(
            "SUPABASE_URL"
        ).rstrip("/")

        self.key = secret(
            "SUPABASE_SERVICE_ROLE_KEY"
        )

        self.enabled = bool(
            self.url and self.key
        )

        self.last_error = ""

        self.last_publish = None

        self.published = 0

    def _request(
        self,
        method: str,
        table: str,
        payload: Any = None,
        query: str = "",
        prefer: str = "return=minimal",
    ) -> Any:

        if not self.enabled:

            raise RuntimeError(
                "SUPABASE_URL / "
                "SUPABASE_SERVICE_ROLE_KEY "
                "not configured"
            )

        table_name = quote(
            table,
            safe="",
        )

        url = (
            f"{self.url}/rest/v1/"
            f"{table_name}"
        )

        if query:
            url += "?" + query

        body = None

        if payload is not None:

            body = json.dumps(
                json_safe(payload)
            ).encode(
                "utf-8"
            )

        headers = {
            "apikey": self.key,
            "Authorization": (
                f"Bearer {self.key}"
            ),
            "Content-Type": (
                "application/json"
            ),
            "Prefer": prefer,
        }

        request = Request(
            url,
            data=body,
            headers=headers,
            method=method.upper(),
        )

        try:

            with urlopen(
                request,
                timeout=20,
            ) as response:

                data = response.read()

                if not data:
                    return None

                text = data.decode(
                    "utf-8"
                )

                try:
                    return json.loads(text)
                except Exception:
                    return text

        except HTTPError as exc:

            detail = exc.read().decode(
                "utf-8",
                errors="replace",
            )

            raise RuntimeError(
                f"Supabase HTTP {exc.code}: "
                f"{detail[:800]}"
            ) from exc

        except URLError as exc:

            raise RuntimeError(
                f"Supabase network error: "
                f"{exc}"
            ) from exc

    def health(self) -> Dict[str, Any]:

        if not self.enabled:

            return {
                "configured": False,
                "reachable": False,
                "error": (
                    "Supabase secrets missing"
                ),
            }

        try:

            self._request(
                "GET",
                "raw_observations",
                query=(
                    "select=id"
                    "&limit=1"
                ),
            )

            self.last_error = ""

            return {
                "configured": True,
                "reachable": True,
            }

        except Exception as exc:

            self.last_error = str(exc)

            return {
                "configured": True,
                "reachable": False,
                "error": str(exc),
            }

    def publish(
        self,
        event: Mapping[str, Any],
    ) -> None:

        self._request(
            "POST",
            "raw_observations",
            dict(event),
            prefer="return=minimal",
        )

        self.last_publish = iso_now()

        self.published += 1

    def latest(
        self,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:

        limit = max(
            1,
            min(
                int(limit),
                1000,
            ),
        )

        query = (
            "select=*"
            "&order=received_at.desc"
            f"&limit={limit}"
        )

        result = self._request(
            "GET",
            "raw_observations",
            query=query,
        )

        if isinstance(
            result,
            list,
        ):
            return result

        return []

    def heartbeat(
        self,
        consumer_name: str,
        status: str,
        **extra: Any,
    ) -> None:

        row = {
            "consumer_name": consumer_name,
            "status": status,
            "heartbeat_timestamp": iso_now(),
            "producer_version": PRODUCER_VERSION,
        }

        row.update(
            json_safe(extra)
        )

        self._request(
            "POST",
            "consumer_heartbeats",
            row,
            prefer=(
                "resolution=merge-duplicates,"
                "return=minimal"
            ),
        )

    def producer_health(
        self,
        payload: Mapping[str, Any],
    ) -> None:

        self._request(
            "POST",
            "producer_health",
            dict(payload),
            prefer=(
                "resolution=merge-duplicates,"
                "return=minimal"
            ),
        )


# ============================================================================
# LOCAL AUDIT MIRROR
# ============================================================================

class LocalAuditMirror:
    """
    Local audit/cache only.

    It is deliberately NOT the cross-app source of truth.
    """

    def __init__(self) -> None:

        self.sequence = 0

        self.lock = threading.RLock()

    def write(
        self,
        event: Mapping[str, Any],
    ) -> None:

        with self.lock:

            self.sequence += 1

            day = now_ist().strftime(
                "%Y-%m-%d"
            )

            path = (
                LOCAL_OBS
                / f"raw_{day}.jsonl"
            )

            with path.open(
                "a",
                encoding="utf-8",
            ) as handle:

                handle.write(
                    json.dumps(
                        json_safe(event),
                        ensure_ascii=False,
                        separators=(
                            ",",
                            ":",
                        ),
                    )
                    + "\n"
                )


# ============================================================================
# KOTAK RAW PRODUCER
# ============================================================================

class KotakRawProducer:

    def __init__(
        self,
        bus: SupabaseRawBus,
        mirror: LocalAuditMirror,
    ) -> None:

        self.bus = bus

        self.mirror = mirror

        self.client = None

        self.authenticated = False

        self.connected = False

        self.streaming = False

        self.last_error = ""

        self.last_tick: Optional[datetime] = None

        self.last_poll: Optional[datetime] = None

        self.poll_count = 0

        self.publish_count = 0

        self.future_token = secret(
            "KOTAK_NIFTY_FUT_TOKEN"
        )

        self.future_symbol = ""

        self.pcr_tokens: List[str] = []

        self.pcr_meta: Dict[
            str,
            Dict[str, Any],
        ] = {}

        self.latest_by_token: Dict[
            str,
            Dict[str, Any],
        ] = {}

        self.lock = threading.RLock()

    # ------------------------------------------------------------------------
    # Credentials
    # ------------------------------------------------------------------------

    def credentials_status(
        self,
    ) -> Dict[str, Any]:

        names = (
            "KOTAK_CONSUMER_KEY",
            "KOTAK_MOBILE",
            "KOTAK_UCC",
            "KOTAK_TOTP",
            "KOTAK_MPIN",
        )

        present = {
            name: bool(
                secret(name)
            )
            for name in names
        }

        return {
            "credentials_present": all(
                present.values()
            ),
            "missing": [
                name
                for name, ok in present.items()
                if not ok
            ],
        }

    # ------------------------------------------------------------------------
    # Login
    # ------------------------------------------------------------------------

    def login(self) -> bool:

        if NeoAPI is None:

            raise RuntimeError(
                "neo_api_client is not installed"
            )

        status = (
            self.credentials_status()
        )

        if not status[
            "credentials_present"
        ]:

            raise RuntimeError(
                "Missing credentials: "
                + ", ".join(
                    status["missing"]
                )
            )

        consumer_key = secret(
            "KOTAK_CONSUMER_KEY"
        )

        mobile = normalize_mobile(
            secret("KOTAK_MOBILE")
        )

        ucc = secret(
            "KOTAK_UCC"
        )

        totp_secret = secret(
            "KOTAK_TOTP"
        )

        mpin = secret(
            "KOTAK_MPIN"
        )

        try:

            self.client = NeoAPI(
                environment=KOTAK_ENVIRONMENT,
                access_token=None,
                neo_fin_key=None,
                consumer_key=consumer_key,
            )

        except TypeError:

            # Compatibility with older SDK constructor.
            self.client = NeoAPI(
                environment=KOTAK_ENVIRONMENT,
                access_token=None,
                consumer_key=consumer_key,
            )

        self.client.on_message = (
            self.on_message
        )

        self.client.on_error = (
            self.on_error
        )

        self.client.on_close = (
            self.on_close
        )

        self.client.on_open = (
            self.on_open
        )

        # Current Kotak Neo SDK flow.
        if hasattr(
            self.client,
            "totp_login",
        ):

            login_response = (
                self.client.totp_login(
                    mobile_number=mobile,
                    ucc=ucc,
                    totp=generate_totp(
                        totp_secret
                    ),
                )
            )

            if (
                isinstance(
                    login_response,
                    Mapping,
                )
                and login_response.get(
                    "error"
                )
            ):

                raise RuntimeError(
                    str(login_response)
                )

            validation = (
                self.client.totp_validate(
                    mpin=mpin
                )
            )

            if (
                isinstance(
                    validation,
                    Mapping,
                )
                and validation.get(
                    "error"
                )
            ):

                raise RuntimeError(
                    str(validation)
                )

        # Compatibility fallback.
        elif hasattr(
            self.client,
            "login",
        ):

            login_response = (
                self.client.login(
                    mobile_number=mobile,
                    ucc=ucc,
                    totp=generate_totp(
                        totp_secret
                    ),
                )
            )

            if (
                isinstance(
                    login_response,
                    Mapping,
                )
                and login_response.get(
                    "error"
                )
            ):

                raise RuntimeError(
                    str(login_response)
                )

            if hasattr(
                self.client,
                "session_2fa",
            ):

                try:

                    self.client.session_2fa(
                        OTP=mpin
                    )

                except Exception:

                    self.client.session_2fa(
                        mpin=mpin
                    )

        else:

            raise RuntimeError(
                "Installed neo_api_client "
                "does not expose a supported "
                "Kotak login method"
            )

        self.authenticated = True

        self.connected = True

        self.last_error = ""

        return True

    # ------------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------------

    def on_open(
        self,
        _message: Any = None,
    ) -> None:

        self.connected = True

    def on_error(
        self,
        error: Any = None,
    ) -> None:

        self.connected = False

        self.last_error = str(
            error or ""
        )

    def on_close(
        self,
        _message: Any = None,
    ) -> None:

        self.connected = False

        self.streaming = False

    def on_message(
        self,
        message: Any = None,
    ) -> None:

        try:

            if isinstance(
                message,
                str,
            ):

                message = json.loads(
                    message
                )

            rows = (
                message
                if isinstance(
                    message,
                    list,
                )
                else [message]
            )

            for row in rows:

                if isinstance(
                    row,
                    Mapping,
                ):

                    self.publish_row(
                        row,
                        source_type=(
                            "kotak_websocket"
                        ),
                    )

        except Exception as exc:

            self.last_error = (
                f"websocket: {exc}"
            )

    # ------------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------------

    def symbol_for_token(
        self,
        token: str,
    ) -> str:

        token = str(token)

        if token == NIFTY_INDEX_TOKEN:
            return "NIFTY_SPOT"

        if (
            self.future_token
            and token
            == str(self.future_token)
        ):

            return (
                self.future_symbol
                or "NIFTY_FUT"
            )

        for (
            symbol,
            instrument_token,
        ) in HEAVYWEIGHT_TOKENS.items():

            if (
                str(instrument_token)
                == token
            ):

                return symbol

        metadata = self.pcr_meta.get(
            token
        )

        if metadata:

            return str(
                metadata.get(
                    "symbol",
                    token,
                )
            )

        return token

    # ------------------------------------------------------------------------
    # Raw quote normalization
    # ------------------------------------------------------------------------

    def normalize_quote(
        self,
        row: Mapping[str, Any],
    ) -> Optional[Dict[str, Any]]:

        token = token_from_record(
            row
        )

        if not token:

            return None

        symbol = str(
            row.get(
                "display_symbol"
            )
            or row.get(
                "pTrdSymbol"
            )
            or row.get(
                "ts"
            )
            or row.get(
                "symbol"
            )
            or ""
        ).strip()

        if not symbol:

            symbol = self.symbol_for_token(
                token
            )

        segment = str(
            row.get(
                "exchange_segment"
            )
            or (
                "nse_fo"
                if token == str(
                    self.future_token
                )
                else "nse_cm"
            )
        )

        metadata = self.pcr_meta.get(
            str(token),
            {},
        )

        return canonical_raw(
            {
                "symbol": symbol,
                "instrument_token": token,
                "exchange": "NSE",
                "exchange_segment": segment,
                "observation_timestamp": str(
                    row.get(
                        "timestamp"
                    )
                    or row.get(
                        "exchange_timestamp"
                    )
                    or row.get(
                        "ft"
                    )
                    or iso_now()
                ),
                "open": safe_float(
                    row.get("o")
                    or row.get("open")
                    or row.get(
                        "openPrice"
                    )
                ),
                "high": safe_float(
                    row.get("h")
                    or row.get("high")
                    or row.get(
                        "highPrice"
                    )
                ),
                "low": safe_float(
                    row.get("l")
                    or row.get("low")
                    or row.get(
                        "lowPrice"
                    )
                ),
                "close": safe_float(
                    row.get("c")
                    or row.get("close")
                    or row.get(
                        "closePrice"
                    )
                ),
                "ltp": extract_ltp(
                    row
                ),
                "prev_close": safe_float(
                    row.get("pdc")
                    or row.get(
                        "prev_close"
                    )
                    or row.get(
                        "previousClose"
                    )
                ),
                "volume": safe_float(
                    row.get("v")
                    or row.get("volume")
                    or row.get("vol")
                ),
                "oi": extract_oi(
                    row
                ),
                "bid": safe_float(
                    row.get("bp")
                    or row.get("bid")
                    or row.get(
                        "best_bid"
                    )
                ),
                "ask": safe_float(
                    row.get("sp")
                    or row.get("ask")
                    or row.get(
                        "best_ask"
                    )
                ),
                "bid_qty": safe_float(
                    row.get("bq")
                    or row.get(
                        "bid_qty"
                    )
                    or row.get(
                        "bid_quantity"
                    )
                ),
                "ask_qty": safe_float(
                    row.get("sq")
                    or row.get(
                        "ask_qty"
                    )
                    or row.get(
                        "ask_quantity"
                    )
                ),
                "vwap": safe_float(
                    row.get("vwap")
                    or row.get("avp")
                    or row.get(
                        "averagePrice"
                    )
                ),
                "upper_circuit": safe_float(
                    row.get(
                        "upper_circuit"
                    )
                    or row.get(
                        "upperCircuit"
                    )
                ),
                "lower_circuit": safe_float(
                    row.get(
                        "lower_circuit"
                    )
                    or row.get(
                        "lowerCircuit"
                    )
                ),
                "price_band": str(
                    row.get(
                        "price_band"
                    )
                    or ""
                ),
                "last_traded_time": str(
                    row.get("ltt")
                    or row.get("lstup_time")
                    or row.get("ft")
                    or ""
                ),
                "strike": metadata.get(
                    "strike"
                ),
                "option_type": metadata.get(
                    "option_type"
                ),
                "expiry": metadata.get(
                    "expiry"
                ),
                "source_sequence": (
                    row.get("sequence")
                    or row.get("seq")
                ),
                "source_status": "LIVE",
            }
        )

    # ------------------------------------------------------------------------
    # Publish one raw observation
    # ------------------------------------------------------------------------

    def publish_row(
        self,
        row: Mapping[str, Any],
        source_type: str = "kotak_poll",
    ) -> bool:

        raw = self.normalize_quote(
            row
        )

        if not raw:

            return False

        event = {
            "schema_version": (
                RAW_SCHEMA_VERSION
            ),
            "producer_version": (
                PRODUCER_VERSION
            ),
            "event_id": str(
                uuid.uuid4()
            ),
            "source": "kotak_neo",
            "source_type": source_type,
            "symbol": raw.get(
                "symbol"
            ),
            "instrument_token": raw.get(
                "instrument_token"
            ),
            "exchange": raw.get(
                "exchange"
            ),
            "observation_timestamp": raw.get(
                "observation_timestamp"
            )
            or iso_now(),
            "received_at": iso_now(),
            "raw": raw,
        }

        # Local audit first.
        self.mirror.write(
            event
        )

        # Remote source of truth.
        self.bus.publish(
            event
        )

        token = str(
            raw.get(
                "instrument_token",
                "",
            )
        )

        self.latest_by_token[
            token
        ] = event

        self.publish_count += 1

        self.last_tick = now_ist()

        self.connected = True

        return True

    # ------------------------------------------------------------------------
    # Quote token list
    # ------------------------------------------------------------------------

    def quote_tokens(
        self,
    ) -> List[Dict[str, str]]:

        result: List[
            Dict[str, str]
        ] = []

        result.append(
            {
                "instrument_token":
                    NIFTY_INDEX_TOKEN,
                "exchange_segment":
                    "nse_cm",
            }
        )

        for token in (
            HEAVYWEIGHT_TOKENS.values()
        ):

            result.append(
                {
                    "instrument_token":
                        str(token),
                    "exchange_segment":
                        "nse_cm",
                }
            )

        if self.future_token:

            result.append(
                {
                    "instrument_token":
                        str(
                            self.future_token
                        ),
                    "exchange_segment":
                        "nse_fo",
                }
            )

        for token in self.pcr_tokens:

            result.append(
                {
                    "instrument_token":
                        str(token),
                    "exchange_segment":
                        "nse_fo",
                }
            )

        unique: List[
            Dict[str, str]
        ] = []

        seen = set()

        for item in result:

            key = (
                item[
                    "instrument_token"
                ],
                item[
                    "exchange_segment"
                ],
            )

            if key in seen:
                continue

            seen.add(key)

            unique.append(
                item
            )

        return unique

    # ------------------------------------------------------------------------
    # Future discovery
    # ------------------------------------------------------------------------

    def discover_future(
        self,
    ) -> None:

        if not self.client:

            return

        search = getattr(
            self.client,
            "search_scrip",
            None,
        )

        if not callable(search):

            return

        try:

            response = search(
                exchange_segment="nse_fo",
                symbol="NIFTY",
            )

            rows = record_list(
                response
            )

            candidates = []

            for row in rows:

                symbol = str(
                    row.get(
                        "pTrdSymbol"
                    )
                    or row.get(
                        "ts"
                    )
                    or row.get(
                        "symbol"
                    )
                    or ""
                ).upper()

                token = token_from_record(
                    row
                )

                expiry = str(
                    row.get(
                        "pExpiryDate"
                    )
                    or row.get(
                        "lExpiryDate"
                    )
                    or row.get(
                        "expiryDate"
                    )
                    or row.get(
                        "expiry"
                    )
                    or ""
                )

                if (
                    token
                    and symbol.startswith(
                        "NIFTY"
                    )
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
                    key=lambda item: (
                        item[0]
                    )
                )

                (
                    _expiry,
                    token,
                    symbol,
                ) = candidates[0]

                self.future_token = str(
                    token
                )

                self.future_symbol = (
                    symbol
                )

        except Exception as exc:

            self.last_error = (
                f"future discovery: {exc}"
            )

    # ------------------------------------------------------------------------
    # Option discovery
    # ------------------------------------------------------------------------

    def discover_options(
        self,
        count: int = 5,
        step: float = 50.0,
    ) -> int:

        if not self.client:

            return 0

        # We deliberately use only the latest
        # raw NIFTY spot observation.
        spot_event = self.latest_by_token.get(
            NIFTY_INDEX_TOKEN
        )

        if not spot_event:

            self.pcr_tokens = []

            self.pcr_meta = {}

            return 0

        spot_raw = (
            spot_event.get(
                "raw"
            )
            or {}
        )

        center = safe_float(
            spot_raw.get("ltp")
            or spot_raw.get("close")
        )

        if center is None:

            self.pcr_tokens = []

            self.pcr_meta = {}

            return 0

        search = getattr(
            self.client,
            "search_scrip",
            None,
        )

        if not callable(search):

            return 0

        try:

            response = search(
                exchange_segment="nse_fo",
                symbol="NIFTY",
            )

            rows = record_list(
                response
            )

            atm = round(
                center / step
            ) * step

            wanted = {
                atm + (
                    i * step
                )
                for i in range(
                    -count,
                    count + 1,
                )
            }

            found = []

            for row in rows:

                symbol = str(
                    row.get(
                        "pTrdSymbol"
                    )
                    or row.get(
                        "ts"
                    )
                    or row.get(
                        "symbol"
                    )
                    or ""
                ).upper()

                token = token_from_record(
                    row
                )

                option_type = ""

                if symbol.endswith(
                    "CE"
                ):

                    option_type = "CE"

                elif symbol.endswith(
                    "PE"
                ):

                    option_type = "PE"

                raw_strike = safe_float(
                    row.get(
                        "dStrikePrice"
                    )
                    or row.get(
                        "strikePrice"
                    )
                    or row.get(
                        "strike"
                    )
                )

                if (
                    raw_strike is not None
                    and raw_strike > 100000
                ):

                    raw_strike /= 100.0

                if (
                    token
                    and option_type
                    and raw_strike is not None
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

            self.pcr_tokens = sorted(
                {
                    item[0]
                    for item in found
                }
            )

            self.pcr_meta = {
                item[0]: {
                    "strike": item[1],
                    "option_type": item[2],
                    "symbol": item[3],
                }
                for item in found
            }

            return len(
                self.pcr_tokens
            )

        except Exception as exc:

            self.last_error = (
                f"option discovery: {exc}"
            )

            return 0

    # ------------------------------------------------------------------------
    # Complete discovery
    # ------------------------------------------------------------------------

    def discover(
        self,
    ) -> Dict[str, Any]:

        if not self.authenticated:

            raise RuntimeError(
                "Kotak is not authenticated"
            )

        self.discover_future()

        option_count = (
            self.discover_options()
        )

        return {
            "future_token": (
                self.future_token
            ),
            "future_symbol": (
                self.future_symbol
            ),
            "option_tokens": (
                option_count
            ),
            "heavyweights": len(
                HEAVYWEIGHT_TOKENS
            ),
        }

    # ------------------------------------------------------------------------
    # Quote poll
    # ------------------------------------------------------------------------

    def poll(self) -> int:

        if (
            not self.client
            or not self.authenticated
        ):

            raise RuntimeError(
                "Kotak is not authenticated"
            )

        quotes_method = getattr(
            self.client,
            "quotes",
            None,
        )

        if not callable(
            quotes_method
        ):

            raise RuntimeError(
                "Kotak SDK quotes() "
                "method is unavailable"
            )

        self.last_poll = now_ist()

        count = 0

        try:

            tokens = self.quote_tokens()

            response = quotes_method(
                instrument_tokens=tokens,
                quote_type="all",
            )

            rows = record_list(
                response
            )

            for row in rows:

                try:

                    if self.publish_row(
                        row,
                        source_type=(
                            "kotak_poll"
                        ),
                    ):

                        count += 1

                except Exception as exc:

                    self.last_error = (
                        f"poll publish: {exc}"
                    )

            self.poll_count += 1

            self.connected = True

            if count:

                self.last_tick = (
                    now_ist()
                )

            return count

        except Exception as exc:

            self.connected = False

            self.last_error = (
                f"quote poll: {exc}"
            )

            raise

    # ------------------------------------------------------------------------
    # Websocket subscribe
    # ------------------------------------------------------------------------

    def subscribe(self) -> int:

        if (
            not self.client
            or not self.authenticated
        ):

            raise RuntimeError(
                "Kotak is not authenticated"
            )

        subscribe_method = getattr(
            self.client,
            "subscribe",
            None,
        )

        if not callable(
            subscribe_method
        ):

            raise RuntimeError(
                "Kotak SDK subscribe() "
                "method is unavailable"
            )

        tokens = self.quote_tokens()

        index_tokens = [
            {
                "instrument_token":
                    NIFTY_INDEX_TOKEN,
                "exchange_segment":
                    "nse_cm",
            }
        ]

        subscribe_method(
            instrument_tokens=index_tokens,
            isIndex=True,
        )

        rest = [
            item
            for item in tokens
            if item[
                "instrument_token"
            ] != NIFTY_INDEX_TOKEN
        ]

        if rest:

            subscribe_method(
                instrument_tokens=rest,
                isIndex=False,
            )

        self.streaming = True

        self.connected = True

        return len(tokens)

    # ------------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------------

    def status(
        self,
    ) -> Dict[str, Any]:

        return {
            "authenticated": (
                self.authenticated
            ),
            "connected": (
                self.connected
            ),
            "streaming": (
                self.streaming
            ),
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
            "poll_count": (
                self.poll_count
            ),
            "published_count": (
                self.publish_count
            ),
            "last_error": (
                self.last_error
            ),
            "future_token": (
                self.future_token
            ),
            "future_symbol": (
                self.future_symbol
            ),
            "option_tokens": len(
                self.pcr_tokens
            ),
        }


# ============================================================================
# YAHOO RAW PRODUCER
# ============================================================================

class YahooRawProducer:
    """
    Yahoo Finance historical raw producer.

    Only daily OHLCV is fetched.

    No indicators.
    No rankings.
    No alpha.
    No scores.
    """

    def __init__(
        self,
        bus: SupabaseRawBus,
        mirror: LocalAuditMirror,
    ) -> None:

        self.bus = bus

        self.mirror = mirror

        self.last_error = ""

        self.last_run: Optional[
            datetime
        ] = None

        self.rows_published = 0

        self.tickers_with_data = 0

    def publish_dataframe(
        self,
        ticker: str,
        frame: pd.DataFrame,
    ) -> int:

        if (
            frame is None
            or frame.empty
        ):

            return 0

        count = 0

        for index, row in frame.iterrows():

            try:

                timestamp = pd.Timestamp(
                    index
                )

                if timestamp.tzinfo is None:

                    timestamp = (
                        timestamp.tz_localize(
                            "UTC"
                        )
                    )

                timestamp = (
                    timestamp.tz_convert(
                        IST
                    )
                )

                raw = canonical_raw(
                    {
                        "symbol": ticker,
                        "instrument_token":
                            ticker,
                        "exchange": "NSE",
                        "exchange_segment":
                            "nse_cm",
                        "observation_timestamp":
                            timestamp.isoformat(),
                        "open": safe_float(
                            row.get(
                                "Open"
                            )
                        ),
                        "high": safe_float(
                            row.get(
                                "High"
                            )
                        ),
                        "low": safe_float(
                            row.get(
                                "Low"
                            )
                        ),
                        "close": safe_float(
                            row.get(
                                "Close"
                            )
                        ),
                        "volume": safe_float(
                            row.get(
                                "Volume"
                            )
                        ),
                        "source_status":
                            "HISTORICAL",
                    }
                )

                event = {
                    "schema_version":
                        RAW_SCHEMA_VERSION,
                    "producer_version":
                        PRODUCER_VERSION,
                    "event_id": str(
                        uuid.uuid4()
                    ),
                    "source":
                        "yahoo_finance",
                    "source_type":
                        "historical_daily",
                    "symbol":
                        ticker,
                    "instrument_token":
                        ticker,
                    "exchange":
                        "NSE",
                    "observation_timestamp":
                        raw.get(
                            "observation_timestamp"
                        ),
                    "received_at":
                        iso_now(),
                    "raw":
                        raw,
                }

                self.mirror.write(
                    event
                )

                self.bus.publish(
                    event
                )

                count += 1

            except Exception as exc:

                self.last_error = (
                    f"{ticker}: {exc}"
                )

        self.rows_published += count

        return count

    def fetch(
        self,
        tickers: List[str],
        period: str = "1y",
    ) -> Dict[str, Any]:

        if yf is None:

            raise RuntimeError(
                "yfinance is not installed. "
                "Add yfinance to requirements.txt."
            )

        self.last_run = now_ist()

        successful = 0

        published = 0

        errors: Dict[
            str,
            str,
        ] = {}

        for ticker in tickers:

            ticker = ticker.strip()

            if not ticker:
                continue

            try:

                frame = yf.download(
                    ticker,
                    period=period,
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )

                if frame is None:
                    continue

                if isinstance(
                    frame.columns,
                    pd.MultiIndex,
                ):

                    frame.columns = (
                        frame.columns
                        .get_level_values(0)
                    )

                rows = (
                    self.publish_dataframe(
                        ticker,
                        frame,
                    )
                )

                if rows > 0:

                    successful += 1

                    published += rows

            except Exception as exc:

                error_text = str(exc)

                errors[ticker] = (
                    error_text
                )

                self.last_error = (
                    f"{ticker}: "
                    f"{error_text}"
                )

        self.tickers_with_data = (
            successful
        )

        return {
            "tickers_requested": len(
                tickers
            ),
            "tickers_with_data": (
                successful
            ),
            "rows_published": (
                published
            ),
            "errors": errors,
        }

    def status(
        self,
    ) -> Dict[str, Any]:

        return {
            "installed": yf is not None,
            "last_run": (
                self.last_run.isoformat()
                if self.last_run
                else None
            ),
            "tickers_with_data": (
                self.tickers_with_data
            ),
            "rows_published": (
                self.rows_published
            ),
            "last_error": (
                self.last_error
            ),
        }


# ============================================================================
# PRODUCER HEALTH
# ============================================================================

def publish_producer_health(
    bus: SupabaseRawBus,
    kotak: KotakRawProducer,
    yahoo: YahooRawProducer,
) -> Dict[str, Any]:

    payload = {
        "producer_name":
            "raw_data_producer",

        "producer_version":
            PRODUCER_VERSION,

        "schema_version":
            HEALTH_SCHEMA_VERSION,

        "status":
            "READY",

        "heartbeat_timestamp":
            iso_now(),

        "kotak_authenticated":
            kotak.authenticated,

        "kotak_connected":
            kotak.connected,

        "kotak_streaming":
            kotak.streaming,

        "kotak_last_tick":
            (
                kotak.last_tick.isoformat()
                if kotak.last_tick
                else None
            ),

        "kotak_last_poll":
            (
                kotak.last_poll.isoformat()
                if kotak.last_poll
                else None
            ),

        "kotak_published":
            kotak.publish_count,

        "yahoo_last_run":
            (
                yahoo.last_run.isoformat()
                if yahoo.last_run
                else None
            ),

        "yahoo_rows_published":
            yahoo.rows_published,

        "last_error":
            (
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


# ============================================================================
# STREAMLIT UI
# ============================================================================

def main() -> None:

    st.set_page_config(
        page_title=(
            "Common Raw Data Producer"
        ),
        page_icon="📡",
        layout="wide",
    )

    # ------------------------------------------------------------------------
    # SESSION OBJECTS
    # ------------------------------------------------------------------------

    if "raw_bus" not in st.session_state:

        st.session_state.raw_bus = (
            SupabaseRawBus()
        )

    if "raw_mirror" not in st.session_state:

        st.session_state.raw_mirror = (
            LocalAuditMirror()
        )

    if "kotak_producer" not in st.session_state:

        st.session_state.kotak_producer = (
            KotakRawProducer(
                st.session_state.raw_bus,
                st.session_state.raw_mirror,
            )
        )

    if "yahoo_producer" not in st.session_state:

        st.session_state.yahoo_producer = (
            YahooRawProducer(
                st.session_state.raw_bus,
                st.session_state.raw_mirror,
            )
        )

    bus: SupabaseRawBus = (
        st.session_state.raw_bus
    )

    mirror: LocalAuditMirror = (
        st.session_state.raw_mirror
    )

    kotak: KotakRawProducer = (
        st.session_state.kotak_producer
    )

    yahoo: YahooRawProducer = (
        st.session_state.yahoo_producer
    )

    # ------------------------------------------------------------------------
    # HEADER
    # ------------------------------------------------------------------------

    st.title(
        "COMMON RAW DATA PRODUCER"
    )

    st.caption(
        "RAW market observations only • "
        "Supabase source of truth • "
        "NIFTY / Next-Day Alpha / GSR isolated consumers"
    )

    # ------------------------------------------------------------------------
    # TOP STATUS
    # ------------------------------------------------------------------------

    supabase_health = bus.health()

    c1, c2, c3, c4 = st.columns(4)

    with c1:

        st.metric(
            "PRODUCER",
            "READY",
        )

    with c2:

        st.metric(
            "SUPABASE",
            (
                "ONLINE"
                if supabase_health.get(
                    "reachable"
                )
                else "OFFLINE"
            ),
        )

    with c3:

        st.metric(
            "KOTAK",
            (
                "AUTHENTICATED"
                if kotak.authenticated
                else "NOT AUTH"
            ),
        )

    with c4:

        st.metric(
            "RAW PUBLISHED",
            f"{bus.published:,}",
        )

    # ------------------------------------------------------------------------
    # SUPABASE WARNING
    # ------------------------------------------------------------------------

    if not bus.enabled:

        st.error(
            "Supabase raw bus is not configured. "
            "Set SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY "
            "in Streamlit Secrets."
        )

    elif not supabase_health.get(
        "reachable"
    ):

        st.warning(
            supabase_health.get(
                "error",
                "Supabase is unreachable.",
            )
        )

    # ------------------------------------------------------------------------
    # KOTAK + BUS
    # ------------------------------------------------------------------------

    left, right = st.columns(
        [1, 1]
    )

    # ========================================================================
    # KOTAK PANEL
    # ========================================================================

    with left:

        st.subheader(
            "1. KOTAK SOURCE AUTHENTICATION"
        )

        credentials = (
            kotak.credentials_status()
        )

        if credentials[
            "credentials_present"
        ]:

            st.success(
                "All Kotak credentials present"
            )

        else:

            st.warning(
                "Kotak credentials incomplete"
            )

            if credentials[
                "missing"
            ]:

                st.caption(
                    "Missing: "
                    + ", ".join(
                        credentials[
                            "missing"
                        ]
                    )
                )

        b1, b2, b3 = st.columns(3)

        with b1:

            if st.button(
                "Login Kotak",
                use_container_width=True,
            ):

                try:

                    kotak.login()

                    st.success(
                        "Kotak authentication successful"
                    )

                except Exception as exc:

                    st.error(
                        str(exc)
                    )

        with b2:

            if st.button(
                "Discover Instruments",
                use_container_width=True,
            ):

                try:

                    result = (
                        kotak.discover()
                    )

                    st.json(
                        result
                    )

                except Exception as exc:

                    st.error(
                        str(exc)
                    )

        with b3:

            if st.button(
                "Subscribe Feed",
                use_container_width=True,
            ):

                try:

                    count = (
                        kotak.subscribe()
                    )

                    st.success(
                        f"Subscribed to "
                        f"{count} raw instruments"
                    )

                except Exception as exc:

                    st.error(
                        str(exc)
                    )

        st.subheader(
            "Kotak Health"
        )

        st.json(
            kotak.status()
        )

    # ========================================================================
    # REMOTE RAW BUS PANEL
    # ========================================================================

    with right:

        st.subheader(
            "2. COMMON REMOTE RAW BUS"
        )

        st.write(
            "**Remote source of truth:** "
            "`raw_observations`"
        )

        st.write(
            "**Local disk:** audit/cache only"
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

                rows = bus.latest(
                    50
                )

                if not rows:

                    st.info(
                        "No raw observations "
                        "found in Supabase."
                    )

                else:

                    display = []

                    for item in rows:

                        raw = (
                            item.get(
                                "raw"
                            )
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
                                    raw.get(
                                        "ltp"
                                    ),
                                "close":
                                    raw.get(
                                        "close"
                                    ),
                                "volume":
                                    raw.get(
                                        "volume"
                                    ),
                                "oi":
                                    raw.get(
                                        "oi"
                                    ),
                            }
                        )

                    st.dataframe(
                        pd.DataFrame(
                            display
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )

            except Exception as exc:

                st.error(
                    str(exc)
                )

    st.divider()

    # ------------------------------------------------------------------------
    # YAHOO HISTORICAL RAW
    # ------------------------------------------------------------------------

    st.subheader(
        "3. YFINANCE — HISTORICAL RAW CAPTURE"
    )

    st.caption(
        "Downloads raw daily OHLCV only. "
        "No indicators, scores, rankings or "
        "engine opinions are calculated here."
    )

    default_tickers = ",".join(
        DEFAULT_YAHOO_TICKERS
    )

    ticker_text = st.text_area(
        "Yahoo tickers "
        "(comma separated)",
        default_tickers,
        height=100,
    )

    period = st.selectbox(
        "Historical period",
        [
            "1mo",
            "3mo",
            "6mo",
            "1y",
            "2y",
            "5y",
        ],
        index=3,
    )

    if st.button(
        "Fetch + Publish Historical Raw",
        type="primary",
        use_container_width=True,
    ):

        try:

            tickers = [
                ticker.strip()
                for ticker
                in ticker_text.split(",")
                if ticker.strip()
            ]

            if not tickers:

                st.error(
                    "At least one Yahoo ticker "
                    "is required."
                )

            else:

                with st.spinner(
                    "Downloading Yahoo raw OHLCV..."
                ):

                    result = yahoo.fetch(
                        tickers,
                        period,
                    )

                st.success(
                    "Published "
                    f"{result['rows_published']:,} "
                    "raw daily rows"
                )

                st.json(
                    result
                )

        except Exception as exc:

            st.error(
                str(exc)
            )

    st.caption(
        "Yahoo installed: "
        + (
            "YES"
            if yf is not None
            else "NO — add yfinance to requirements.txt"
        )
    )

    # ------------------------------------------------------------------------
    # KOTAK MANUAL RAW POLL
    # ------------------------------------------------------------------------

    st.subheader(
        "4. KOTAK — MANUAL RAW SNAPSHOT"
    )

    st.caption(
        "This publishes the current raw Kotak "
        "snapshot. No calculations are performed."
    )

    if st.button(
        "Fetch + Publish Current Kotak Snapshot",
        use_container_width=True,
    ):

        try:

            if not kotak.authenticated:

                kotak.login()

            if not kotak.future_symbol:

                kotak.discover()

            count = kotak.poll()

            st.success(
                f"Published {count} "
                "raw observations"
            )

        except Exception as exc:

            st.error(
                str(exc)
            )

    # ------------------------------------------------------------------------
    # LOCAL AUDIT
    # ------------------------------------------------------------------------

    st.subheader(
        "5. LOCAL RAW AUDIT CACHE"
    )

    st.code(
        str(LOCAL_ROOT),
        language="text",
    )

    st.caption(
        "Local files are audit/cache only. "
        "They are NOT the common cross-app source of truth."
    )

    # ------------------------------------------------------------------------
    # LOCAL LATEST DISPLAY
    # ------------------------------------------------------------------------

    latest_local_rows = []

    for path in sorted(
        LOCAL_OBS.glob(
            "raw_*.jsonl"
        ),
        reverse=True,
    )[:3]:

        try:

            with path.open(
                "r",
                encoding="utf-8",
            ) as handle:

                lines = handle.readlines()

            for line in lines[-20:]:

                try:

                    event = json.loads(
                        line
                    )

                except Exception:

                    continue

                raw = (
                    event.get(
                        "raw"
                    )
                    or {}
                )

                latest_local_rows.append(
                    {
                        "source":
                            event.get(
                                "source"
                            ),
                        "symbol":
                            event.get(
                                "symbol"
                            ),
                        "received_at":
                            event.get(
                                "received_at"
                            ),
                        "ltp":
                            raw.get(
                                "ltp"
                            ),
                        "close":
                            raw.get(
                                "close"
                            ),
                        "volume":
                            raw.get(
                                "volume"
                            ),
                        "oi":
                            raw.get(
                                "oi"
                            ),
                    }
                )

        except Exception:
            continue

    if latest_local_rows:

        st.dataframe(
            pd.DataFrame(
                latest_local_rows
            ),
            use_container_width=True,
            hide_index=True,
        )

    else:

        st.info(
            "No local raw audit rows yet."
        )

    # ------------------------------------------------------------------------
    # CONSUMER HEALTH
    # ------------------------------------------------------------------------

    st.subheader(
        "6. CONSUMER HEALTH"
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
                "Consumer health unavailable: "
                + str(exc)
            )

    else:

        st.info(
            "Consumer health requires "
            "Supabase configuration."
        )

    # ------------------------------------------------------------------------
    # PRODUCER HEARTBEAT
    # ------------------------------------------------------------------------

    health_payload = (
        publish_producer_health(
            bus,
            kotak,
            yahoo,
        )
    )

    st.divider()

    st.caption(
        "Producer heartbeat: "
        f"{health_payload['heartbeat_timestamp']}"
        " • Producer version: "
        f"{PRODUCER_VERSION}"
        " • Raw schema: "
        f"{RAW_SCHEMA_VERSION}"
    )

    st.caption(
        "STRICT ISOLATION: "
        "Producer publishes raw observations only. "
        "NIFTY, Next-Day Alpha and GSR compute their "
        "own independent intelligence."
    )


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
