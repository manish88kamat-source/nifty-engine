#!/usr/bin/env python3
"""
COMMON RAW DATA PRODUCER
========================

Three-machine RAW market-data producer.

                         KOTAK LIVE
                             |
                         YAHOO RAW
                             |
                             v
                  +----------------------+
                  |   RAW DATA PRODUCER  |
                  |                      |
                  |   RAW ONLY           |
                  |   NO INTELLIGENCE    |
                  +----------+-----------+
                             |
                             v
                    SUPABASE RAW BUS
                      /      |      \
                     /       |       \
                    v        v        v
                 NIFTY     ALPHA      GSR


STRICT ARCHITECTURAL CONTRACT
-----------------------------

This application is a DATA PRODUCER ONLY.

It may:
    - authenticate with Kotak
    - receive Kotak raw market observations
    - poll Kotak raw quotes
    - subscribe to Kotak raw feed
    - discover NIFTY futures/options
    - fetch Yahoo historical OHLCV
    - publish raw observations
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

ONLY RAW OBSERVATIONS CROSS THE COMMON BOUNDARY.

REMOTE SOURCE OF TRUTH
----------------------

Supabase tables:

    raw_observations
    consumer_heartbeats
    producer_health

LOCAL DISK
----------

Local disk is an audit/cache mirror only.

It is NEVER advertised as the cross-machine source of truth.

SECRETS
-------

Required:

    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY

Kotak:

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

RUN
---

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


# ============================================================================
# GLOBAL CONFIGURATION
# ============================================================================

IST = ZoneInfo("Asia/Kolkata")

PRODUCER_VERSION = "RAW_PRODUCER_3.0.0"
RAW_SCHEMA_VERSION = "RAW_OBSERVATION_2.1"
HEALTH_SCHEMA_VERSION = "RAW_HEALTH_1.1"

KOTAK_ENVIRONMENT = os.getenv(
    "KOTAK_ENVIRONMENT",
    "prod",
)

POLL_SECONDS = max(
    1,
    int(
        os.getenv(
            "RAW_PRODUCER_POLL_SECONDS",
            "3",
        )
    ),
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

LOCAL_HEALTH = LOCAL_ROOT / "producer_health.json"

LOCAL_AUDIT = LOCAL_ROOT / "producer_audit.jsonl"


# ============================================================================
# INSTRUMENTS
# ============================================================================

NIFTY_INDEX_TOKEN = "Nifty 50"

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

FORBIDDEN_INTELLIGENCE_FIELDS = {
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
    "targets",
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
    "strategy",
    "strategy_id",
    "market_direction",
}

RAW_ALLOWED_FIELDS = {
    "symbol",
    "instrument_token",
    "exchange",
    "exchange_segment",
    "instrument_type",
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
# TIME / JSON HELPERS
# ============================================================================

def now_ist() -> datetime:
    return datetime.now(IST)


def iso_now() -> str:
    return now_ist().isoformat()


def safe_float(
    value: Any,
) -> Optional[float]:

    try:

        if value is None:
            return None

        if isinstance(
            value,
            bool,
        ):
            return None

        text = str(
            value
        ).strip()

        if not text:
            return None

        text = text.replace(
            ",",
            "",
        )

        number = float(
            text
        )

        if not math.isfinite(
            number
        ):
            return None

        return number

    except Exception:
        return None


def safe_json(
    value: Any,
) -> Any:

    if value is None:
        return None

    if isinstance(
        value,
        (str, int, bool),
    ):
        return value

    if isinstance(
        value,
        float,
    ):

        return (
            value
            if math.isfinite(value)
            else None
        )

    if isinstance(
        value,
        (datetime, pd.Timestamp),
    ):

        return value.isoformat()

    if isinstance(
        value,
        Mapping,
    ):

        return {
            str(k): safe_json(v)
            for k, v in value.items()
        }

    if isinstance(
        value,
        (list, tuple, set),
    ):

        return [
            safe_json(v)
            for v in value
        ]

    return str(value)


# ============================================================================
# SECRETS
# ============================================================================

def secret(
    name: str,
) -> str:

    value = os.getenv(
        name,
        "",
    )

    if value:
        return str(
            value
        ).strip()

    if st is not None:

        try:

            value = st.secrets.get(
                name,
                "",
            )

            if value:
                return str(
                    value
                ).strip()

        except Exception:
            pass

    return ""


def normalize_mobile(
    value: str,
) -> str:

    raw = str(
        value or ""
    ).strip()

    digits = "".join(
        ch
        for ch in raw
        if ch.isdigit()
    )

    if digits.startswith(
        "00"
    ):
        digits = digits[2:]

    if (
        digits.startswith("91")
        and len(digits) == 12
    ):
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

def generate_totp(
    secret_or_otp: str,
) -> str:

    raw = (
        str(
            secret_or_otp or ""
        )
        .replace(" ", "")
        .strip()
        .upper()
    )

    if (
        raw.isdigit()
        and len(raw) == 6
    ):
        return raw

    try:

        padded = raw + (
            "="
            * (
                (
                    8
                    - len(raw) % 8
                )
                % 8
            )
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

        offset = (
            digest[-1]
            & 15
        )

        code = (
            struct.unpack(
                ">I",
                digest[
                    offset:
                    offset + 4
                ],
            )[0]
            & 0x7FFFFFFF
        )

        code %= 1_000_000

        return f"{code:06d}"

    except Exception:

        return raw


# ============================================================================
# RAW VALIDATION
# ============================================================================

def reject_intelligence(
    payload: Mapping[str, Any],
) -> None:

    keys = {
        str(key)
        .strip()
        .lower()
        for key in payload.keys()
    }

    forbidden = sorted(
        keys.intersection(
            FORBIDDEN_INTELLIGENCE_FIELDS
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

    reject_intelligence(
        payload
    )

    result: Dict[
        str,
        Any,
    ] = {}

    for key, value in payload.items():

        key_text = str(
            key
        )

        if (
            key_text
            in RAW_ALLOWED_FIELDS
        ):

            result[
                key_text
            ] = safe_json(value)

    return result


# ============================================================================
# KOTAK RECORD HELPERS
# ============================================================================

def token_from_record(
    record: Mapping[str, Any],
) -> str:

    keys = (
        "exchange_token",
        "pSymbol",
        "pSymbolToken",
        "instrument_token",
        "instrumentToken",
        "instrumentTokenId",
        "tok",
        "token",
        "pToken",
        "tk",
        "i",
    )

    for key in keys:

        value = record.get(
            key
        )

        if value not in (
            None,
            "",
        ):

            return str(
                value
            ).strip()

    return ""


def extract_ltp(
    record: Mapping[str, Any],
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
            record.get(
                key
            )
        )

        if (
            value is not None
            and value > 0
        ):

            return value

    return None


def extract_oi(
    record: Mapping[str, Any],
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
            record.get(
                key
            )
        )

        if (
            value is not None
            and value >= 0
        ):

            return value

    return None


def record_list(
    response: Any,
) -> List[
    Dict[str, Any]
]:

    if isinstance(
        response,
        list,
    ):

        return [
            item
            for item in response
            if isinstance(
                item,
                dict,
            )
        ]

    if not isinstance(
        response,
        dict,
    ):

        return []

    wrapper_keys = (
        "data",
        "result",
        "records",
        "data_list",
        "scrips",
        "list",
        "message",
    )

    for key in wrapper_keys:

        value = response.get(
            key
        )

        if isinstance(
            value,
            list,
        ):

            return [
                item
                for item in value
                if isinstance(
                    item,
                    dict,
                )
            ]

        if isinstance(
            value,
            dict,
        ):

            for nested_key in (
                "data",
                "records",
                "result",
                "scrips",
            ):

                nested = value.get(
                    nested_key
                )

                if isinstance(
                    nested,
                    list,
                ):

                    return [
                        item
                        for item in nested
                        if isinstance(
                            item,
                            dict,
                        )
                    ]

    return []


def parse_expiry(
    value: Any,
) -> Optional[datetime]:

    if value is None:
        return None

    if isinstance(
        value,
        datetime,
    ):

        if value.tzinfo:
            return value.astimezone(
                IST
            )

        return value.replace(
            tzinfo=IST
        )

    try:

        number = float(
            value
        )

        if number > 10_000_000_000:

            return datetime.fromtimestamp(
                number / 1000,
                tz=IST,
            )

        if number > 1_000_000_000:

            return datetime.fromtimestamp(
                number,
                tz=IST,
            )

    except Exception:
        pass

    text = str(
        value
    ).strip().upper()

    formats = (
        "%d%b%Y",
        "%d%b%y",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y%m%d",
    )

    for fmt in formats:

        try:

            return datetime.strptime(
                text,
                fmt,
            ).replace(
                tzinfo=IST
            )

        except Exception:
            continue

    return None


def extract_option_type(
    record: Mapping[str, Any],
) -> str:

    value = str(
        record.get(
            "pOptionType"
        )
        or record.get(
            "optType"
        )
        or record.get(
            "option_type"
        )
        or ""
    ).upper()

    if (
        "CE" in value
        or "CALL" in value
    ):

        return "CE"

    if (
        "PE" in value
        or "PUT" in value
    ):

        return "PE"

    symbol = str(
        record.get(
            "pTrdSymbol"
        )
        or record.get(
            "ts"
        )
        or record.get(
            "symbol"
        )
        or ""
    ).upper()

    if symbol.endswith(
        "CE"
    ):

        return "CE"

    if symbol.endswith(
        "PE"
    ):

        return "PE"

    return ""


def extract_strike(
    record: Mapping[str, Any],
) -> Optional[float]:

    keys = (
        "dStrikePrice",
        "dStrikePrice;",
        "strike_price",
        "strikePrice",
        "dStrike",
        "strike",
        "pStrikePrice",
    )

    for key in keys:

        value = safe_float(
            record.get(
                key
            )
        )

        if (
            value is not None
            and value > 0
        ):

            if value > 1_000_000:
                value /= 100.0

            elif (
                value > 100_000
            ):
                value /= 100.0

            return value

    return None


# ============================================================================
# SUPABASE RAW BUS
# ============================================================================

class SupabaseRawBus:
    """
    Remote source of truth.

    Uses service-role credentials only inside producer.
    """

    def __init__(
        self,
    ) -> None:

        self.url = secret(
            "SUPABASE_URL"
        ).rstrip("/")

        self.key = secret(
            "SUPABASE_SERVICE_ROLE_KEY"
        )

        self.enabled = bool(
            self.url
            and self.key
        )

        self.last_error = ""

        self.last_publish = None

        self.published = 0

        self.lock = threading.RLock()

    # ------------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------------

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
                safe_json(payload)
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

                raw = response.read()

                if not raw:
                    return None

                text = raw.decode(
                    "utf-8"
                )

                try:
                    return json.loads(
                        text
                    )
                except Exception:
                    return text

        except HTTPError as exc:

            detail = exc.read().decode(
                "utf-8",
                errors="replace",
            )

            raise RuntimeError(
                f"Supabase HTTP "
                f"{exc.code}: "
                f"{detail[:1000]}"
            ) from exc

        except URLError as exc:

            raise RuntimeError(
                f"Supabase network error: "
                f"{exc}"
            ) from exc

    # ------------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------------

    def health(
        self,
    ) -> Dict[str, Any]:

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

            self.last_error = str(
                exc
            )

            return {
                "configured": True,
                "reachable": False,
                "error": str(exc),
            }

    # ------------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------------

    def publish(
        self,
        event: Mapping[str, Any],
    ) -> bool:

        if not self.enabled:

            raise RuntimeError(
                "Remote raw bus is not configured"
            )

        try:

            self._request(
                "POST",
                "raw_observations",
                dict(event),
                prefer="return=minimal",
            )

            with self.lock:

                self.last_publish = (
                    iso_now()
                )

                self.published += 1

            self.last_error = ""

            return True

        except Exception as exc:

            self.last_error = str(
                exc
            )

            raise

    # ------------------------------------------------------------------------
    # Latest
    # ------------------------------------------------------------------------

    def latest(
        self,
        limit: int = 100,
    ) -> List[
        Dict[str, Any]
    ]:

        if not self.enabled:
            return []

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

        return (
            result
            if isinstance(
                result,
                list,
            )
            else []
        )

    # ------------------------------------------------------------------------
    # Producer health
    # ------------------------------------------------------------------------

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

    # ------------------------------------------------------------------------
    # Consumer heartbeat
    # ------------------------------------------------------------------------

    def consumer_health(
        self,
    ) -> List[
        Dict[str, Any]
    ]:

        if not self.enabled:
            return []

        query = (
            "select="
            "consumer_name,"
            "status,"
            "heartbeat_timestamp,"
            "updated_at"
            "&order=updated_at.desc"
            "&limit=50"
        )

        result = self._request(
            "GET",
            "consumer_heartbeats",
            query=query,
        )

        return (
            result
            if isinstance(
                result,
                list,
            )
            else []
        )


# ============================================================================
# LOCAL AUDIT MIRROR
# ============================================================================

class LocalAuditMirror:
    """
    Local audit/cache only.

    NEVER used as the three-machine source of truth.
    """

    def __init__(
        self,
    ) -> None:

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
                        safe_json(event),
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

        self.last_tick: Optional[
            datetime
        ] = None

        self.last_poll: Optional[
            datetime
        ] = None

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
                for name, ok
                in present.items()
                if not ok
            ],
        }

    # ------------------------------------------------------------------------
    # Login
    # ------------------------------------------------------------------------

    def login(
        self,
    ) -> bool:

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
                "Missing Kotak credentials: "
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

            try:

                self.client = NeoAPI(
                    environment=(
                        KOTAK_ENVIRONMENT
                    ),
                    access_token=None,
                    neo_fin_key=None,
                    consumer_key=(
                        consumer_key
                    ),
                )

            except TypeError:

                self.client = NeoAPI(
                    environment=(
                        KOTAK_ENVIRONMENT
                    ),
                    access_token=None,
                    consumer_key=(
                        consumer_key
                    ),
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

            if hasattr(
                self.client,
                "totp_login",
            ):

                response = (
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
                        response,
                        Mapping,
                    )
                    and response.get(
                        "error"
                    )
                ):

                    raise RuntimeError(
                        str(response)
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

            else:

                raise RuntimeError(
                    "Installed Kotak SDK does not "
                    "provide totp_login()."
                )

            self.authenticated = True

            self.connected = True

            self.last_error = ""

            return True

        except Exception as exc:

            self.authenticated = False

            self.connected = False

            self.last_error = str(
                exc
            )

            raise

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
    # Symbol
    # ------------------------------------------------------------------------

    def symbol_for_token(
        self,
        token: str,
    ) -> str:

        token = str(
            token
        )

        if token == NIFTY_INDEX_TOKEN:
            return "NIFTY_SPOT"

        if (
            self.future_token
            and token
            == str(
                self.future_token
            )
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
                str(
                    instrument_token
                )
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
    # Segment
    # ------------------------------------------------------------------------

    def segment_for_token(
        self,
        token: str,
    ) -> str:

        token = str(
            token
        )

        fo_tokens = {
            str(
                self.future_token
            )
        }

        fo_tokens.update(
            str(token)
            for token
            in self.pcr_tokens
        )

        return (
            "nse_fo"
            if token in fo_tokens
            else "nse_cm"
        )

    # ------------------------------------------------------------------------
    # Normalize raw quote
    # ------------------------------------------------------------------------

    def normalize_quote(
        self,
        row: Mapping[str, Any],
    ) -> Optional[
        Dict[str, Any]
    ]:

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

            symbol = (
                self.symbol_for_token(
                    token
                )
            )

        metadata = self.pcr_meta.get(
            token,
            {},
        )

        raw = canonical_raw(
            {
                "symbol": (
                    symbol
                    or token
                ),
                "instrument_token": (
                    token
                ),
                "exchange": "NSE",
                "exchange_segment": (
                    str(
                        row.get(
                            "exchange_segment"
                        )
                        or self.segment_for_token(
                            token
                        )
                    )
                ),
                "instrument_type": (
                    "OPTION"
                    if metadata
                    else (
                        "FUTURE"
                        if (
                            token
                            == str(
                                self.future_token
                            )
                        )
                        else (
                            "INDEX"
                            if token
                            == NIFTY_INDEX_TOKEN
                            else "EQUITY"
                        )
                    )
                ),
                "observation_timestamp": (
                    str(
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
                    )
                ),
                "received_timestamp": (
                    iso_now()
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
                    or row.get(
                        "volume"
                    )
                    or row.get(
                        "vol"
                    )
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
                    row.get(
                        "lstup_time"
                    )
                    or row.get(
                        "ltt"
                    )
                    or row.get(
                        "ft"
                    )
                    or ""
                ),
                "strike": metadata.get(
                    "strike"
                ),
                "option_type": metadata.get(
                    "option_type"
                ),
                "expiry": (
                    metadata.get(
                        "expiry"
                    ).isoformat()
                    if isinstance(
                        metadata.get(
                            "expiry"
                        ),
                        datetime,
                    )
                    else metadata.get(
                        "expiry"
                    )
                ),
                "source_sequence": (
                    row.get(
                        "sequence"
                    )
                    or row.get(
                        "seq"
                    )
                ),
                "source_status": "LIVE",
            }
        )

        return raw

    # ------------------------------------------------------------------------
    # Publish
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
            "observation_timestamp": (
                raw.get(
                    "observation_timestamp"
                )
                or iso_now()
            ),
            "received_at": iso_now(),
            "raw": raw,
        }

        # Local audit is best-effort.
        try:

            self.mirror.write(
                event
            )

        except Exception as exc:

            self.last_error = (
                f"local audit: {exc}"
            )

        # Remote bus is authoritative.
        self.bus.publish(
            event
        )

        token = str(
            raw.get(
                "instrument_token",
                "",
            )
        )

        with self.lock:

            self.latest_by_token[
                token
            ] = event

            self.publish_count += 1

            self.last_tick = now_ist()

            self.connected = True

        return True

    # ------------------------------------------------------------------------
    # Quote tokens
    # ------------------------------------------------------------------------

    def quote_tokens(
        self,
    ) -> List[
        Dict[str, str]
    ]:

        result: List[
            Dict[str, str]
        ] = [
            {
                "instrument_token":
                    NIFTY_INDEX_TOKEN,
                "exchange_segment":
                    "nse_cm",
            }
        ]

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

        for token in (
            self.pcr_tokens
        ):

            result.append(
                {
                    "instrument_token":
                        str(token),
                    "exchange_segment":
                        "nse_fo",
                }
            )

        unique = []

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

        if not callable(
            search
        ):
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

                expiry = parse_expiry(
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
                )

                if (
                    token
                    and symbol.startswith(
                        "NIFTY"
                    )
                    and "FUT" in symbol
                    and expiry is not None
                    and expiry.date()
                    >= now_ist().date()
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

                expiry, token, symbol = (
                    candidates[0]
                )

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

        spot_event = (
            self.latest_by_token.get(
                NIFTY_INDEX_TOKEN
            )
        )

        center = None

        if spot_event:

            raw = (
                spot_event.get(
                    "raw"
                )
                or {}
            )

            center = (
                safe_float(
                    raw.get(
                        "ltp"
                    )
                    or raw.get(
                        "close"
                    )
                )
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

        if not callable(
            search
        ):

            return 0

        try:

            response = search(
                exchange_segment="nse_fo",
                symbol="NIFTY",
            )

            rows = record_list(
                response
            )

            atm = (
                round(
                    center / step
                )
                * step
            )

            wanted = {
                atm + (
                    index * step
                )
                for index
                in range(
                    -count,
                    count + 1,
                )
            }

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

                if (
                    not token
                    or "NIFTY"
                    not in symbol
                ):

                    continue

                option = (
                    extract_option_type(
                        row
                    )
                )

                expiry = parse_expiry(
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
                )

                strike = extract_strike(
                    row
                )

                if (
                    option
                    and expiry
                    and strike
                    and strike in wanted
                    and expiry.date()
                    >= now_ist().date()
                ):

                    candidates.append(
                        (
                            expiry,
                            token,
                            strike,
                            option,
                            symbol,
                        )
                    )

            if not candidates:

                self.pcr_tokens = []

                self.pcr_meta = {}

                return 0

            active_expiry = min(
                item[0]
                for item
                in candidates
            )

            self.pcr_tokens = []

            self.pcr_meta = {}

            for (
                expiry,
                token,
                strike,
                option,
                symbol,
            ) in candidates:

                if (
                    expiry.date()
                    != active_expiry.date()
                ):
                    continue

                self.pcr_tokens.append(
                    str(token)
                )

                self.pcr_meta[
                    str(token)
                ] = {
                    "strike": strike,
                    "option_type": option,
                    "expiry": expiry,
                    "symbol": symbol,
                }

            self.pcr_tokens = sorted(
                set(
                    self.pcr_tokens
                )
            )

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
    # Poll
    # ------------------------------------------------------------------------

    def poll(
        self,
    ) -> int:

        if (
            not self.client
            or not self.authenticated
        ):

            raise RuntimeError(
                "Kotak is not authenticated"
            )

        quotes = getattr(
            self.client,
            "quotes",
            None,
        )

        if not callable(
            quotes
        ):

            raise RuntimeError(
                "Kotak SDK quotes() "
                "method is unavailable"
            )

        self.last_poll = now_ist()

        try:

            response = quotes(
                instrument_tokens=(
                    self.quote_tokens()
                ),
                quote_type="all",
            )

            rows = record_list(
                response
            )

            count = 0

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
                        f"poll row: {exc}"
                    )

            self.poll_count += 1

            self.connected = True

            return count

        except Exception as exc:

            self.connected = False

            self.last_error = (
                f"quote poll: {exc}"
            )

            raise

    # ------------------------------------------------------------------------
    # Subscribe
    # ------------------------------------------------------------------------

    def subscribe(
        self,
    ) -> int:

        if (
            not self.client
            or not self.authenticated
        ):

            raise RuntimeError(
                "Kotak is not authenticated"
            )

        subscribe = getattr(
            self.client,
            "subscribe",
            None,
        )

        if not callable(
            subscribe
        ):

            raise RuntimeError(
                "Kotak SDK subscribe() "
                "method is unavailable"
            )

        tokens = self.quote_tokens()

        try:

            subscribe(
                instrument_tokens=[
                    {
                        "instrument_token":
                            NIFTY_INDEX_TOKEN,
                        "exchange_segment":
                            "nse_cm",
                    }
                ],
                isIndex=True,
            )

            rest = [
                item
                for item in tokens
                if item[
                    "instrument_token"
                ]
                != NIFTY_INDEX_TOKEN
            ]

            if rest:

                subscribe(
                    instrument_tokens=rest,
                    isIndex=False,
                )

            self.streaming = True

            self.connected = True

            return len(
                tokens
            )

        except Exception as exc:

            self.last_error = (
                f"subscribe: {exc}"
            )

            raise

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
            "last_poll": (
                self.last_poll.isoformat()
                if self.last_poll
                else None
            ),
            "last_tick": (
                self.last_tick.isoformat()
                if self.last_tick
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
                        "instrument_token": (
                            ticker
                        ),
                        "exchange": "NSE",
                        "exchange_segment": (
                            "nse_index"
                            if ticker
                            == "^NSEI"
                            else "nse_cm"
                        ),
                        "instrument_type": (
                            "INDEX"
                            if ticker
                            == "^NSEI"
                            else "EQUITY"
                        ),
                        "observation_timestamp": (
                            timestamp.isoformat()
                        ),
                        "received_timestamp": (
                            iso_now()
                        ),
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
                        "source_status": (
                            "HISTORICAL"
                        ),
                    }
                )

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
                    "source": (
                        "yahoo_finance"
                    ),
                    "source_type": (
                        "historical_daily"
                    ),
                    "symbol": ticker,
                    "instrument_token": ticker,
                    "exchange": "NSE",
                    "observation_timestamp": (
                        raw.get(
                            "observation_timestamp"
                        )
                    ),
                    "received_at": iso_now(),
                    "raw": raw,
                }

                try:

                    self.mirror.write(
                        event
                    )

                except Exception as exc:

                    self.last_error = (
                        f"local audit: "
                        f"{exc}"
                    )

                self.bus.publish(
                    event
                )

                count += 1

            except Exception as exc:

                self.last_error = (
                    f"{ticker}: "
                    f"{exc}"
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

        if not self.bus.enabled:

            raise RuntimeError(
                "Supabase raw bus is not configured."
            )

        self.last_run = now_ist()

        successful = 0

        published = 0

        errors: Dict[
            str,
            str,
        ] = {}

        unique_tickers = list(
            dict.fromkeys(
                ticker.strip()
                for ticker
                in tickers
                if ticker.strip()
            )
        )

        for ticker in unique_tickers:

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

                if rows:

                    successful += 1

                    published += rows

            except Exception as exc:

                errors[
                    ticker
                ] = str(exc)

                self.last_error = (
                    f"{ticker}: "
                    f"{exc}"
                )

        self.tickers_with_data = (
            successful
        )

        return {
            "tickers_requested": (
                len(unique_tickers)
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
            "installed": (
                yf is not None
            ),
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
        "producer_name": (
            "raw_data_producer"
        ),
        "producer_version": (
            PRODUCER_VERSION
        ),
        "schema_version": (
            HEALTH_SCHEMA_VERSION
        ),
        "status": "READY",
        "heartbeat_timestamp": (
            iso_now()
        ),
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
        "kotak_last_poll": (
            kotak.last_poll.isoformat()
            if kotak.last_poll
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

    try:

        LOCAL_HEALTH.write_text(
            json.dumps(
                safe_json(payload),
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    except Exception:
        pass

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
# UI HELPERS
# ============================================================================

def parse_tickers(
    text: str,
) -> List[str]:

    text = (
        str(text or "")
        .replace(
            "\n",
            ",",
        )
        .replace(
            ";",
            ",",
        )
    )

    return list(
        dict.fromkeys(
            item.strip()
            for item
            in text.split(",")
            if item.strip()
        )
    )


def local_latest(
    limit: int = 50,
) -> List[
    Dict[str, Any]
]:

    rows = []

    files = sorted(
        LOCAL_OBS.glob(
            "raw_*.jsonl"
        ),
        reverse=True,
    )

    for path in files[:3]:

        try:

            with path.open(
                "r",
                encoding="utf-8",
            ) as handle:

                lines = handle.readlines()

            for line in reversed(
                lines
            ):

                if len(rows) >= limit:
                    break

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

                rows.append(
                    {
                        "source": (
                            event.get(
                                "source"
                            )
                        ),
                        "symbol": (
                            event.get(
                                "symbol"
                            )
                        ),
                        "received_at": (
                            event.get(
                                "received_at"
                            )
                        ),
                        "ltp": (
                            raw.get(
                                "ltp"
                            )
                        ),
                        "close": (
                            raw.get(
                                "close"
                            )
                        ),
                        "volume": (
                            raw.get(
                                "volume"
                            )
                        ),
                        "oi": (
                            raw.get(
                                "oi"
                            )
                        ),
                    }
                )

        except Exception:
            continue

    return rows


# ============================================================================
# STREAMLIT MAIN
# ============================================================================

def main() -> None:

    if st is None:

        raise RuntimeError(
            "This application requires Streamlit."
        )

    st.set_page_config(
        page_title=(
            "Common Raw Data Producer"
        ),
        page_icon="📡",
        layout="wide",
    )

    # ------------------------------------------------------------------------
    # SESSION STATE
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
        "Supabase is the three-machine source of truth • "
        "NIFTY / Next-Day Alpha / GSR remain isolated"
    )

    # ------------------------------------------------------------------------
    # TOP HEALTH
    # ------------------------------------------------------------------------

    remote_health = bus.health()

    c1, c2, c3, c4 = st.columns(
        4
    )

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
                if remote_health.get(
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
            "REMOTE RAW",
            f"{bus.published:,}",
        )

    # ------------------------------------------------------------------------
    # REMOTE BUS WARNING
    # ------------------------------------------------------------------------

    if not bus.enabled:

        st.error(
            "SUPABASE RAW BUS IS NOT CONFIGURED. "
            "Set SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY. "
            "Local-only mode is intentionally NOT "
            "treated as a valid three-machine bus."
        )

    elif not remote_health.get(
        "reachable"
    ):

        st.warning(
            "Supabase is configured but currently "
            "unreachable: "
            + str(
                remote_health.get(
                    "error",
                    "unknown error",
                )
            )
        )

    else:

        st.success(
            "Supabase raw bus is ONLINE. "
            "This is the cross-machine source of truth."
        )

    # ------------------------------------------------------------------------
    # KOTAK / SUPABASE
    # ------------------------------------------------------------------------

    left, right = st.columns(
        [1, 1]
    )

    # ========================================================================
    # KOTAK
    # ========================================================================

    with left:

        st.subheader(
            "1. KOTAK RAW SOURCE"
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

        b1, b2, b3 = st.columns(
            3
        )

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
                "Discover",
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
                "Subscribe",
                use_container_width=True,
            ):

                try:

                    count = (
                        kotak.subscribe()
                    )

                    st.success(
                        "Subscribed to "
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
    # REMOTE BUS
    # ========================================================================

    with right:

        st.subheader(
            "2. COMMON SUPABASE RAW BUS"
        )

        st.write(
            "**Source of truth:** "
            "`raw_observations`"
        )

        st.write(
            "**Local disk:** "
            "audit/cache only"
        )

        st.write(
            "**Engine intelligence accepted:** "
            "NO"
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
    # YAHOO
    # ------------------------------------------------------------------------

    st.subheader(
        "3. YFINANCE — HISTORICAL RAW CAPTURE"
    )

    st.caption(
        "Raw daily OHLCV only. "
        "No indicators, scores, rankings, "
        "signals or engine opinions."
    )

    ticker_text = st.text_area(
        "Yahoo tickers",
        ",".join(
            DEFAULT_YAHOO_TICKERS
        ),
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

            tickers = parse_tickers(
                ticker_text
            )

            if not tickers:

                st.error(
                    "Enter at least one Yahoo ticker."
                )

            elif not bus.enabled:

                st.error(
                    "Supabase raw bus is not configured."
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
                    "raw rows"
                )

                st.json(
                    result
                )

        except Exception as exc:

            st.error(
                str(exc)
            )

    st.caption(
        "yfinance installed: "
        + (
            "YES"
            if yf is not None
            else "NO"
        )
    )

    # ------------------------------------------------------------------------
    # KOTAK SNAPSHOT
    # ------------------------------------------------------------------------

    st.subheader(
        "4. KOTAK — CURRENT RAW SNAPSHOT"
    )

    st.caption(
        "Manual snapshot publishes raw market "
        "observations only."
    )

    if st.button(
        "Fetch + Publish Current Kotak Snapshot",
        use_container_width=True,
    ):

        try:

            if not bus.enabled:

                raise RuntimeError(
                    "Supabase raw bus is not configured."
                )

            if not kotak.authenticated:

                kotak.login()

            if not kotak.future_symbol:

                kotak.discover()

            count = kotak.poll()

            publish_producer_health(
                bus,
                kotak,
                yahoo,
            )

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
        "5. LOCAL AUDIT / CACHE"
    )

    st.code(
        str(LOCAL_ROOT),
        language="text",
    )

    st.caption(
        "This directory is only a local audit mirror. "
        "It is NOT the shared three-machine bus."
    )

    local_rows = local_latest(
        50
    )

    if local_rows:

        st.dataframe(
            pd.DataFrame(
                local_rows
            ),
            use_container_width=True,
            hide_index=True,
        )

    else:

        st.info(
            "No local audit rows yet."
        )

    # ------------------------------------------------------------------------
    # CONSUMER HEALTH
    # ------------------------------------------------------------------------

    st.subheader(
        "6. CONSUMER HEALTH"
    )

    if bus.enabled:

        try:

            health_rows = (
                bus.consumer_health()
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
    # PRODUCER HEALTH
    # ------------------------------------------------------------------------

    payload = publish_producer_health(
        bus,
        kotak,
        yahoo,
    )

    st.divider()

    st.caption(
        "Producer heartbeat: "
        f"{payload['heartbeat_timestamp']}"
        " • Producer: "
        f"{PRODUCER_VERSION}"
        " • Raw schema: "
        f"{RAW_SCHEMA_VERSION}"
    )

    st.caption(
        "STRICT ISOLATION: "
        "This application publishes RAW observations only. "
        "NIFTY, Next-Day Alpha and GSR independently calculate "
        "their own intelligence."
    )


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
