#!/usr/bin/env python3
"""
COMMON RAW DATA PRODUCER
========================
Single Streamlit producer for the isolated NIFTY 3-Min, Next-Day Alpha and
GSR machines.

CONTRACT
--------
This process ONLY captures/publishes raw market observations. It never
calculates indicators, scores, signals, regimes, predictions, rankings,
labels, entries, targets, stops or trade decisions.

DATA FLOW
---------
    Kotak Neo / Yahoo Finance
             |
             v
       RAW PRODUCER
             |
             +--> Supabase raw_observations   <-- shared source of truth
             |
             +--> local audit/cache           <-- diagnostics only
             |
       +-----+----------+
       |        |       |
     NIFTY    ALPHA    GSR

REQUIRED FOR THREE-MACHINE SHARING
-----------------------------------
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY

KOTAK LIVE (optional until credentials are configured)
-------------------------------------------------------
KOTAK_CONSUMER_KEY
KOTAK_MOBILE
KOTAK_UCC
KOTAK_TOTP
KOTAK_MPIN

OPTIONAL
--------
KOTAK_ENVIRONMENT=prod
SHARED_RAW_CACHE_DIR=./shared_raw_data
RAW_PRODUCER_POLL_SECONDS=3
RAW_ALLOW_LOCAL_ONLY=0
RAW_MAX_JSONL_MB=128

YAHOO
-----
No credentials required. Yahoo is used here only for historical raw OHLCV.
The producer does NOT calculate any Alpha/NIFTY feature from Yahoo data.
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

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


# =============================================================================
# CONFIGURATION
# =============================================================================

IST = ZoneInfo("Asia/Kolkata")

PRODUCER_VERSION = "RAW_PRODUCER_2.1.0"
RAW_SCHEMA_VERSION = "RAW_OBSERVATION_2.0"
HEALTH_SCHEMA_VERSION = "RAW_PRODUCER_HEALTH_2.0"

ROOT = Path(
    os.getenv(
        "SHARED_RAW_CACHE_DIR",
        "./shared_raw_data",
    )
)

OBS_DIR = ROOT / "observations"
CONSUMER_DIR = ROOT / "consumers"

ROOT.mkdir(
    parents=True,
    exist_ok=True,
)

OBS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

CONSUMER_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

LATEST_FILE = ROOT / "latest.json"
SEQUENCE_FILE = ROOT / "sequence.txt"
HEALTH_FILE = ROOT / "producer_health.json"
AUDIT_FILE = ROOT / "producer_audit.jsonl"

POLL_SECONDS = max(
    1,
    int(
        os.getenv(
            "RAW_PRODUCER_POLL_SECONDS",
            "3",
        )
    ),
)

MAX_JSONL_BYTES = max(
    10_000_000,
    int(
        os.getenv(
            "RAW_MAX_JSONL_MB",
            "128",
        )
    )
    * 1024
    * 1024,
)

KOTAK_ENVIRONMENT = os.getenv(
    "KOTAK_ENVIRONMENT",
    "prod",
)

ALLOW_LOCAL_ONLY = (
    os.getenv(
        "RAW_ALLOW_LOCAL_ONLY",
        "0",
    )
    .strip()
    .lower()
    in {"1", "true", "yes"}
)

NIFTY_SPOT_TOKEN = "Nifty 50"

HEAVYWEIGHT_TOKENS = {
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


# =============================================================================
# RAW BOUNDARY
# =============================================================================

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
    "probability",
    "confidence",
    "label",
    "trade_decision",
    "decision",
    "recommendation",
    "thesis",
    "invalidation",
    "target",
    "stop_loss",
    "entry",
    "final_2",
    "final_1",
    "final_candidates",
    "day_ahead_score",
    "setup_score",
    "quality_score",
    "composite_score",
}


RAW_FIELDS = {
    "symbol",
    "instrument_token",
    "exchange",
    "exchange_segment",
    "observation_timestamp",
    "open",
    "high",
    "low",
    "close",
    "ltp",
    "prev_close",
    "volume",
    "oi",
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


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def now_ist() -> datetime:
    return datetime.now(IST)


def iso_now() -> str:
    return now_ist().isoformat()


def finite_float(
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

        if isinstance(
            value,
            str,
        ):
            value = value.replace(
                ",",
                "",
            ).strip()

            if not value:
                return None

        x = float(value)

        return (
            x
            if math.isfinite(x)
            else None
        )

    except Exception:
        return None


def json_safe(
    value: Any,
) -> Any:

    if value is None:
        return None

    if isinstance(
        value,
        (
            str,
            int,
            bool,
        ),
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
        (
            datetime,
            pd.Timestamp,
        ),
    ):
        return value.isoformat()

    if isinstance(
        value,
        Mapping,
    ):
        return {
            str(k): json_safe(v)
            for k, v in value.items()
        }

    if isinstance(
        value,
        (
            list,
            tuple,
            set,
        ),
    ):
        return [
            json_safe(v)
            for v in value
        ]

    return str(value)


def env_or_secret(
    name: str,
) -> str:

    value = os.getenv(
        name,
        "",
    )

    if value:
        return str(value).strip()

    try:
        value = st.secrets.get(
            name,
            "",
        )

        if value:
            return str(value).strip()

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
        c
        for c in raw
        if c.isdigit()
    )

    if digits.startswith("00"):
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


def generate_totp(
    secret_or_otp: str,
) -> str:

    raw = (
        str(
            secret_or_otp or ""
        )
        .strip()
        .replace(
            " ",
            "",
        )
        .upper()
    )

    if (
        raw.isdigit()
        and len(raw) == 6
    ):
        return raw

    try:

        padded = (
            raw
            + "="
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
                    offset:offset + 4
                ],
            )[0]
            & 0x7FFFFFFF
        ) % 1_000_000

        return f"{code:06d}"

    except Exception:
        return raw


def token_from_record(
    record: Mapping[str, Any],
) -> str:

    for key in (
        "exchange_token",
        "pSymbol",
        "pSymbolToken",
        "instrument_token",
        "instrumentToken",
        "tok",
        "token",
        "pToken",
        "tk",
    ):

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

    for key in (
        "ltp",
        "lp",
        "last_price",
        "last_traded_price",
        "c",
        "close",
        "lastPrice",
    ):

        x = finite_float(
            record.get(key)
        )

        if (
            x is not None
            and x > 0
        ):
            return x

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

        x = finite_float(
            record.get(key)
        )

        if (
            x is not None
            and x >= 0
        ):
            return x

    for wrapper in (
        "data",
        "quote",
        "marketDepth",
        "depth",
        "ohlc",
    ):

        nested = record.get(
            wrapper
        )

        if isinstance(
            nested,
            Mapping,
        ):

            for key in keys:

                x = finite_float(
                    nested.get(key)
                )

                if (
                    x is not None
                    and x >= 0
                ):
                    return x

    return None


def record_list(
    response: Any,
) -> List[Dict[str, Any]]:

    if isinstance(
        response,
        list,
    ):
        return [
            x
            for x in response
            if isinstance(
                x,
                dict,
            )
        ]

    if not isinstance(
        response,
        dict,
    ):
        return []

    for key in (
        "data",
        "result",
        "records",
        "data_list",
        "scrips",
        "list",
        "message",
    ):

        value = response.get(
            key
        )

        if isinstance(
            value,
            list,
        ):
            return [
                x
                for x in value
                if isinstance(
                    x,
                    dict,
                )
            ]

        if isinstance(
            value,
            dict,
        ):

            for nested in (
                "data",
                "records",
                "result",
                "scrips",
                "list",
            ):

                child = value.get(
                    nested
                )

                if isinstance(
                    child,
                    list,
                ):
                    return [
                        x
                        for x in child
                        if isinstance(
                            x,
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

        x = float(value)

        if x > 10_000_000_000:
            return datetime.fromtimestamp(
                x / 1000,
                tz=IST,
            )

        if x > 1_000_000_000:
            return datetime.fromtimestamp(
                x,
                tz=IST,
            )

    except Exception:
        pass

    text = str(
        value
    ).strip().upper()

    for fmt in (
        "%d%b%Y",
        "%d%b%y",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y%m%d",
    ):

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


def get_option_type(
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

    if symbol.endswith("CE"):
        return "CE"

    if symbol.endswith("PE"):
        return "PE"

    return ""


def get_strike(
    record: Mapping[str, Any],
) -> Optional[float]:

    for key in (
        "dStrikePrice",
        "dStrikePrice;",
        "strike_price",
        "strikePrice",
        "dStrike",
        "strike",
        "pStrikePrice",
    ):

        x = finite_float(
            record.get(key)
        )

        if (
            x is not None
            and x > 0
        ):

            if x > 1_000_000:
                return x / 100.0

            return x

    return None


def forbidden_fields(
    payload: Mapping[str, Any],
) -> List[str]:

    return sorted(
        {
            str(k)
            .strip()
            .lower()
            for k in payload
            if (
                str(k)
                .strip()
                .lower()
                in FORBIDDEN_INTELLIGENCE_FIELDS
            )
        }
    )


def canonical_raw(
    payload: Mapping[str, Any],
) -> Dict[str, Any]:

    bad = forbidden_fields(
        payload
    )

    if bad:
        raise ValueError(
            "RAW boundary violation: "
            + ", ".join(bad)
        )

    return {
        key: json_safe(value)
        for key, value in payload.items()
        if key in RAW_FIELDS
    }


# =============================================================================
# SUPABASE RAW BUS
# =============================================================================

class SupabaseRawBus:
    """
    Minimal REST client.

    Producer owns service-role key.
    Consumer engines only need read access appropriate to their contract.
    """

    def __init__(self) -> None:

        self.url = (
            env_or_secret(
                "SUPABASE_URL"
            )
            .rstrip("/")
        )

        self.key = env_or_secret(
            "SUPABASE_SERVICE_ROLE_KEY"
        )

        self.enabled = bool(
            self.url
            and self.key
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

        url = (
            f"{self.url}"
            f"/rest/v1/"
            f"{table}"
        )

        if query:
            url += "?" + query

        body = (
            None
            if payload is None
            else json.dumps(
                json_safe(payload)
            ).encode(
                "utf-8"
            )
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

                return json.loads(
                    raw.decode(
                        "utf-8"
                    )
                )

        except HTTPError as exc:

            detail = (
                exc.read()
                .decode(
                    "utf-8",
                    errors="replace",
                )
            )

            raise RuntimeError(
                f"Supabase HTTP "
                f"{exc.code}: "
                f"{detail[:500]}"
            ) from exc

        except URLError as exc:

            raise RuntimeError(
                f"Supabase network error: "
                f"{exc}"
            ) from exc

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
            **json_safe(extra),
        }

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

    def health(self) -> Dict[str, Any]:

        if not self.enabled:

            return {
                "configured": False,
                "reachable": False,
                "error": (
                    "missing Supabase secrets"
                ),
            }

        try:

            self._request(
                "GET",
                "raw_observations",
                query=(
                    "select=id&limit=1"
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


# =============================================================================
# LOCAL AUDIT MIRROR
# =============================================================================

class LocalAuditMirror:
    """
    Local diagnostics only.

    This is NOT the cross-machine source of truth.
    """

    def __init__(self) -> None:

        self.sequence = 0

        self.lock = (
            threading.RLock()
        )

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
                OBS_DIR
                / f"raw_{day}.jsonl"
            )

            with path.open(
                "a",
                encoding="utf-8",
            ) as fh:

                fh.write(
                    json.dumps(
                        json_safe(event),
                        ensure_ascii=False,
                    )
                    + "\n"
                )


# =============================================================================
# SHARED RAW STORE
# =============================================================================

class SharedRawStore:
    """
    Local latest cache + remote Supabase shared bus.

    Remote Supabase is authoritative for cross-machine sharing.
    """

    def __init__(
        self,
        bus: SupabaseRawBus,
    ) -> None:

        self.bus = bus

        self.root = ROOT
        self.obs_dir = OBS_DIR
        self.latest_file = LATEST_FILE
        self.sequence_file = SEQUENCE_FILE
        self.audit_file = AUDIT_FILE

        self.lock = (
            threading.RLock()
        )

        self.sequence = (
            self._load_sequence()
        )

        self.latest: Dict[
            str,
            Dict[str, Any],
        ] = self._load_latest()

    def _load_sequence(
        self,
    ) -> int:

        try:

            return int(
                self.sequence_file.read_text(
                    encoding="utf-8"
                ).strip()
            )

        except Exception:
            return 0

    def _persist_sequence(
        self,
    ) -> None:

        tmp = (
            self.sequence_file
            .with_suffix(".tmp")
        )

        tmp.write_text(
            str(self.sequence),
            encoding="utf-8",
        )

        os.replace(
            tmp,
            self.sequence_file,
        )

    def _load_latest(
        self,
    ) -> Dict[str, Dict[str, Any]]:

        try:

            data = json.loads(
                self.latest_file.read_text(
                    encoding="utf-8"
                )
            )

            if isinstance(
                data,
                dict,
            ):
                return data

        except Exception:
            pass

        return {}

    def _atomic_write_json(
        self,
        path: Path,
        value: Any,
    ) -> None:

        tmp = path.with_suffix(
            path.suffix + ".tmp"
        )

        tmp.write_text(
            json.dumps(
                json_safe(value),
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        os.replace(
            tmp,
            path,
        )

    def _day_file(
        self,
        ts: str,
    ) -> Path:

        day = (
            str(ts)[:10]
            if ts
            else now_ist().strftime(
                "%Y-%m-%d"
            )
        )

        path = (
            self.obs_dir
            / f"raw_{day}.jsonl"
        )

        if (
            path.exists()
            and path.stat().st_size
            >= MAX_JSONL_BYTES
        ):

            stamp = now_ist().strftime(
                "%Y%m%d_%H%M%S"
            )

            rotated = (
                self.obs_dir
                / (
                    f"raw_{day}_"
                    f"{stamp}.jsonl"
                )
            )

            os.replace(
                path,
                rotated,
            )

        return path

    def _audit(
        self,
        event: str,
        **kwargs: Any,
    ) -> None:

        row = {
            "timestamp": iso_now(),
            "event": event,
            **json_safe(kwargs),
        }

        with self.audit_file.open(
            "a",
            encoding="utf-8",
        ) as fh:

            fh.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                )
                + "\n"
            )

    def publish(
        self,
        raw: Mapping[str, Any],
        source: str,
        source_type: str = "live",
    ) -> Dict[str, Any]:

        if not isinstance(
            raw,
            Mapping,
        ) or not raw:

            raise ValueError(
                "Raw observation must be "
                "a non-empty mapping"
            )

        clean = canonical_raw(
            raw
        )

        symbol = str(
            clean.get(
                "symbol"
            )
            or ""
        ).strip()

        if not symbol:

            raise ValueError(
                "Raw observation requires symbol"
            )

        observation_timestamp = str(
            clean.get(
                "observation_timestamp"
            )
            or iso_now()
        )

        with self.lock:

            self.sequence += 1

            event = {
                "schema_version":
                    RAW_SCHEMA_VERSION,

                "producer_version":
                    PRODUCER_VERSION,

                "event_id":
                    (
                        f"raw-"
                        f"{self.sequence:012d}-"
                        f"{uuid.uuid4().hex[:10]}"
                    ),

                "sequence":
                    self.sequence,

                "source":
                    str(source),

                "source_type":
                    str(source_type),

                "symbol":
                    symbol,

                "instrument_token":
                    str(
                        clean.get(
                            "instrument_token"
                        )
                        or ""
                    ),

                "exchange":
                    str(
                        clean.get(
                            "exchange"
                        )
                        or ""
                    ),

                "observation_timestamp":
                    observation_timestamp,

                "received_at":
                    iso_now(),

                "raw":
                    clean,
            }

            # ---------------------------------------------------------
            # Remote shared bus FIRST.
            # ---------------------------------------------------------
            remote_published = False

            if self.bus.enabled:

                self.bus.publish(
                    event
                )

                remote_published = True

            elif not ALLOW_LOCAL_ONLY:

                raise RuntimeError(
                    "Shared Supabase raw bus "
                    "is not configured. "
                    "Set SUPABASE_URL and "
                    "SUPABASE_SERVICE_ROLE_KEY."
                )

            # ---------------------------------------------------------
            # Local mirror only after remote success.
            # ---------------------------------------------------------
            day_file = self._day_file(
                iso_now()
            )

            with day_file.open(
                "a",
                encoding="utf-8",
            ) as fh:

                fh.write(
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

                fh.flush()

            key = (
                f"{event['exchange']}:"
                f"{event['symbol']}"
            )

            self.latest[key] = event

            self._atomic_write_json(
                self.latest_file,
                self.latest,
            )

            self._persist_sequence()

            self._audit(
                "RAW_PUBLISHED",
                event_id=event[
                    "event_id"
                ],
                symbol=event[
                    "symbol"
                ],
                source=event[
                    "source"
                ],
                remote_published=(
                    remote_published
                ),
            )

            return event

    def publish_many(
        self,
        rows: Iterable[
            Mapping[str, Any]
        ],
        source: str,
        source_type: str = "live",
    ) -> int:

        count = 0

        for row in rows:

            try:

                self.publish(
                    row,
                    source=source,
                    source_type=source_type,
                )

                count += 1

            except Exception as exc:

                self._audit(
                    "RAW_REJECTED",
                    source=source,
                    reason=str(exc),
                )

        return count

    def status(
        self,
    ) -> Dict[str, Any]:

        try:

            age = (
                max(
                    0.0,
                    time.time()
                    - self.latest_file.stat().st_mtime,
                )
                if self.latest_file.exists()
                else None
            )

        except Exception:
            age = None

        return {
            "schema_version":
                RAW_SCHEMA_VERSION,

            "producer_version":
                PRODUCER_VERSION,

            "root":
                str(self.root),

            "latest_symbols":
                len(self.latest),

            "sequence":
                self.sequence,

            "latest_file_age_sec":
                age,

            "remote_bus_enabled":
                self.bus.enabled,

            "remote_published":
                self.bus.published,
        }


# =============================================================================
# KOTAK RAW PRODUCER
# =============================================================================

class KotakRawProducer:
    """
    Broker-facing raw producer.

    Credentials exist only here.
    No engine opinion is accepted or generated.
    """

    def __init__(
        self,
        store: SharedRawStore,
    ) -> None:

        self.store = store

        self.client = None

        self.authenticated = False
        self.connected = False
        self.streaming = False

        self.last_error = ""

        self.last_tick = None
        self.last_poll = None

        self.poll_count = 0
        self.publish_count = 0

        self.future_token = (
            env_or_secret(
                "KOTAK_NIFTY_FUT_TOKEN"
            )
        )

        self.future_symbol = ""

        self.pcr_tokens: List[
            str
        ] = []

        self.pcr_meta: Dict[
            str,
            Dict[str, Any],
        ] = {}

        self.lock = (
            threading.RLock()
        )

    # -----------------------------------------------------------------
    # Credentials
    # -----------------------------------------------------------------

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
                env_or_secret(name)
            )
            for name in names
        }

        return {
            "credentials_present":
                all(
                    present.values()
                ),

            "missing":
                [
                    name
                    for name, ok
                    in present.items()
                    if not ok
                ],
        }

    # -----------------------------------------------------------------
    # Login
    # -----------------------------------------------------------------

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
                "Missing credentials: "
                + ", ".join(
                    status["missing"]
                )
            )

        c = {
            name: env_or_secret(name)
            for name in (
                "KOTAK_CONSUMER_KEY",
                "KOTAK_MOBILE",
                "KOTAK_UCC",
                "KOTAK_TOTP",
                "KOTAK_MPIN",
            )
        }

        self.client = NeoAPI(
            environment=KOTAK_ENVIRONMENT,
            access_token=None,
            neo_fin_key=None,
            consumer_key=c[
                "KOTAK_CONSUMER_KEY"
            ],
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

        login_response = (
            self.client.totp_login(
                mobile_number=
                    normalize_mobile(
                        c["KOTAK_MOBILE"]
                    ),

                ucc=
                    c["KOTAK_UCC"],

                totp=
                    generate_totp(
                        c["KOTAK_TOTP"]
                    ),
            )
        )

        if (
            isinstance(
                login_response,
                dict,
            )
            and login_response.get(
                "error"
            )
        ):

            raise RuntimeError(
                str(login_response)
            )

        validated = (
            self.client.totp_validate(
                mpin=c["KOTAK_MPIN"]
            )
        )

        if (
            isinstance(
                validated,
                dict,
            )
            and validated.get(
                "error"
            )
        ):

            raise RuntimeError(
                str(validated)
            )

        self.authenticated = True
        self.connected = True
        self.last_error = ""

        return True

    # -----------------------------------------------------------------
    # Callbacks
    # -----------------------------------------------------------------

    def on_open(
        self,
        _message=None,
    ) -> None:

        self.connected = True

    def on_error(
        self,
        error=None,
    ) -> None:

        self.connected = False
        self.last_error = str(
            error or ""
        )

    def on_close(
        self,
        _message=None,
    ) -> None:

        self.connected = False
        self.streaming = False

    # -----------------------------------------------------------------
    # Symbol mapping
    # -----------------------------------------------------------------

    def _symbol_for_token(
        self,
        token: str,
    ) -> str:

        if token == NIFTY_SPOT_TOKEN:
            return "NIFTY_SPOT"

        if (
            self.future_token
            and token
            == self.future_token
        ):

            return (
                self.future_symbol
                or "NIFTY_FUT"
            )

        for (
            symbol,
            mapped_token,
        ) in HEAVYWEIGHT_TOKENS.items():

            if (
                str(mapped_token)
                == str(token)
            ):
                return symbol

        meta = self.pcr_meta.get(
            str(token)
        )

        if meta:
            return str(
                meta.get(
                    "symbol",
                    token,
                )
            )

        return token

    # -----------------------------------------------------------------
    # Normalize broker quote
    # -----------------------------------------------------------------

    def _normalize_quote(
        self,
        row: Mapping[str, Any],
    ) -> Optional[
        Dict[str, Any]
    ]:

        token = token_from_record(
            row
        )

        if not token:

            # Index packets may use the
            # actual token string directly.
            token = str(
                row.get(
                    "instrument_token"
                )
                or row.get(
                    "tk"
                )
                or ""
            ).strip()

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
            or ""
        ).strip()

        if not symbol:
            symbol = (
                self._symbol_for_token(
                    token
                )
            )

        segment = str(
            row.get(
                "exchange_segment"
            )
            or (
                "nse_fo"
                if (
                    token
                    == self.future_token
                    or token
                    in self.pcr_tokens
                )
                else "nse_cm"
            )
        )

        option_meta = (
            self.pcr_meta.get(
                str(token)
            )
            or {}
        )

        expiry_value = (
            option_meta.get(
                "expiry"
            )
        )

        if isinstance(
            expiry_value,
            datetime,
        ):
            expiry_value = (
                expiry_value.isoformat()
            )

        raw = {
            "symbol":
                symbol,

            "instrument_token":
                token,

            "exchange":
                "NSE",

            "exchange_segment":
                segment,

            "observation_timestamp":
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
                ),

            "open":
                finite_float(
                    row.get("o")
                    or row.get("open")
                    or row.get(
                        "openPrice"
                    )
                ),

            "high":
                finite_float(
                    row.get("h")
                    or row.get("high")
                    or row.get(
                        "highPrice"
                    )
                ),

            "low":
                finite_float(
                    row.get("l")
                    or row.get("low")
                    or row.get(
                        "lowPrice"
                    )
                ),

            "close":
                finite_float(
                    row.get("c")
                    or row.get("close")
                    or row.get(
                        "closePrice"
                    )
                ),

            "ltp":
                extract_ltp(row),

            "prev_close":
                finite_float(
                    row.get("pdc")
                    or row.get(
                        "prev_close"
                    )
                    or row.get(
                        "previousClose"
                    )
                ),

            "volume":
                finite_float(
                    row.get("v")
                    or row.get("volume")
                    or row.get("vol")
                ),

            "oi":
                extract_oi(row),

            "bid":
                finite_float(
                    row.get("bp")
                    or row.get("bid")
                ),

            "ask":
                finite_float(
                    row.get("sp")
                    or row.get("ask")
                ),

            "bid_qty":
                finite_float(
                    row.get("bq")
                    or row.get("bid_qty")
                ),

            "ask_qty":
                finite_float(
                    row.get("sq")
                    or row.get("ask_qty")
                ),

            "vwap":
                finite_float(
                    row.get("vwap")
                    or row.get("avp")
                    or row.get(
                        "averagePrice"
                    )
                ),

            "upper_circuit":
                finite_float(
                    row.get(
                        "upper_circuit"
                    )
                    or row.get(
                        "upperCircuit"
                    )
                ),

            "lower_circuit":
                finite_float(
                    row.get(
                        "lower_circuit"
                    )
                    or row.get(
                        "lowerCircuit"
                    )
                ),

            "last_traded_time":
                str(
                    row.get(
                        "ltt"
                    )
                    or row.get(
                        "lstup_time"
                    )
                    or row.get(
                        "ft"
                    )
                    or ""
                ),

            "strike":
                option_meta.get(
                    "strike"
                ),

            "option_type":
                option_meta.get(
                    "option_type"
                ),

            "expiry":
                expiry_value,

            "source_sequence":
                row.get(
                    "sequence"
                )
                or row.get(
                    "seq"
                ),

            "source_status":
                "LIVE",
        }

        return canonical_raw(
            raw
        )

    # -----------------------------------------------------------------
    # Publish broker row
    # -----------------------------------------------------------------

    def publish_row(
        self,
        row: Mapping[str, Any],
        source_type: str = "kotak_live",
    ) -> bool:

        raw = self._normalize_quote(
            row
        )

        if not raw:
            return False

        event = {
            "schema_version":
                RAW_SCHEMA_VERSION,

            "event_id":
                str(
                    uuid.uuid4()
                ),

            "source":
                "kotak_neo",

            "source_type":
                source_type,

            "symbol":
                raw[
                    "symbol"
                ],

            "instrument_token":
                raw.get(
                    "instrument_token"
                ),

            "exchange":
                raw.get(
                    "exchange"
                ),

            "observation_timestamp":
                raw.get(
                    "observation_timestamp"
                )
                or iso_now(),

            "received_at":
                iso_now(),

            "raw":
                raw,
        }

        self.store.publish(
            raw,
            source="kotak_neo",
            source_type=source_type,
        )

        self.publish_count += 1
        self.last_tick = now_ist()

        return True

    # -----------------------------------------------------------------
    # Websocket callback
    # -----------------------------------------------------------------

    def on_message(
        self,
        message=None,
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
                    dict,
                ):

                    self.publish_row(
                        row,
                        "kotak_websocket",
                    )

        except Exception as exc:

            self.last_error = (
                f"websocket: {exc}"
            )

    # -----------------------------------------------------------------
    # Quote token list
    # -----------------------------------------------------------------

    def quote_tokens(
        self,
    ) -> List[
        Dict[str, str]
    ]:

        result = [
            {
                "instrument_token":
                    NIFTY_SPOT_TOKEN,
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

            if key not in seen:

                unique.append(
                    item
                )

                seen.add(
                    key
                )

        return unique

    # -----------------------------------------------------------------
    # Future discovery
    # -----------------------------------------------------------------

    def discover_future(
        self,
    ) -> None:

        if not self.client:
            return

        try:

            rows = record_list(
                self.client.search_scrip(
                    exchange_segment=
                        "nse_fo",

                    symbol=
                        "NIFTY",
                )
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

                token = (
                    token_from_record(
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

                if (
                    token
                    and symbol.startswith(
                        "NIFTY"
                    )
                    and (
                        "FUT"
                        in symbol
                    )
                ):

                    if (
                        expiry is None
                        or expiry.date()
                        >= now_ist().date()
                    ):

                        candidates.append(
                            (
                                expiry
                                or datetime.max.replace(
                                    tzinfo=IST
                                ),
                                token,
                                symbol,
                            )
                        )

            if candidates:

                candidates.sort(
                    key=lambda x: x[0]
                )

                (
                    _expiry,
                    self.future_token,
                    self.future_symbol,
                ) = candidates[0]

        except Exception as exc:

            self.last_error = (
                f"future discovery: "
                f"{exc}"
            )

    # -----------------------------------------------------------------
    # Option discovery
    # -----------------------------------------------------------------

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
                    exchange_segment=
                        "nse_fo",

                    symbol=
                        "NIFTY",
                )
            )

            # IMPORTANT:
            # Never invent an ATM value.
            # We need actual spot LTP from our own latest raw cache.
            center = None

            for event in (
                reversed(
                    list(
                        self.store.latest.values()
                    )
                )
            ):

                if (
                    event.get(
                        "symbol"
                    )
                    == "NIFTY_SPOT"
                ):

                    raw = event.get(
                        "raw"
                    ) or {}

                    center = extract_ltp(
                        raw
                    )

                    if center:
                        break

            if center is None:

                self.pcr_tokens = []
                self.pcr_meta = {}

                return 0

            atm = (
                round(
                    center / step
                )
                * step
            )

            wanted = {
                atm + i * step
                for i in range(
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

                option_type = (
                    get_option_type(
                        row
                    )
                )

                strike = get_strike(
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
                    and option_type
                    and strike is not None
                    and strike in wanted
                    and "NIFTY" in symbol
                    and (
                        expiry is None
                        or expiry.date()
                        >= now_ist().date()
                    )
                ):

                    candidates.append(
                        (
                            expiry,
                            token,
                            strike,
                            option_type,
                            symbol,
                        )
                    )

            if not candidates:

                self.pcr_tokens = []
                self.pcr_meta = {}

                return 0

            dated = [
                x
                for x in candidates
                if x[0] is not None
            ]

            if dated:

                target_expiry = min(
                    x[0]
                    for x in dated
                )

                candidates = [
                    x
                    for x in candidates
                    if (
                        x[0] is None
                        or x[0].date()
                        == target_expiry.date()
                    )
                ]

            self.pcr_tokens = []
            self.pcr_meta = {}

            for (
                expiry,
                token,
                strike_value,
                option_type_value,
                symbol,
            ) in candidates:

                self.pcr_tokens.append(
                    str(token)
                )

                self.pcr_meta[
                    str(token)
                ] = {
                    "strike":
                        strike_value,

                    "option_type":
                        option_type_value,

                    "symbol":
                        symbol,

                    "expiry":
                        expiry,
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
                f"option discovery: "
                f"{exc}"
            )

            return 0

    # -----------------------------------------------------------------
    # Discovery
    # -----------------------------------------------------------------

    def discover(
        self,
    ) -> Dict[str, Any]:

        self.discover_future()

        option_count = (
            self.discover_options()
        )

        return {
            "future_token":
                self.future_token,

            "future_symbol":
                self.future_symbol,

            "option_tokens":
                option_count,

            "heavyweights":
                len(
                    HEAVYWEIGHT_TOKENS
                ),
        }

    # -----------------------------------------------------------------
    # Poll
    # -----------------------------------------------------------------

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

        self.last_poll = now_ist()

        try:

            response = (
                self.client.quotes(
                    instrument_tokens=
                        self.quote_tokens(),

                    quote_type=
                        "all",
                )
            )

            rows = record_list(
                response
            )

            count = 0

            for row in rows:

                try:

                    if self.publish_row(
                        row,
                        "kotak_poll",
                    ):
                        count += 1

                except Exception as exc:

                    self.last_error = str(
                        exc
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
                f"quote poll: "
                f"{exc}"
            )

            raise

    # -----------------------------------------------------------------
    # Subscribe
    # -----------------------------------------------------------------

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

        tokens = (
            self.quote_tokens()
        )

        try:

            self.client.subscribe(
                instrument_tokens=[
                    {
                        "instrument_token":
                            NIFTY_SPOT_TOKEN,

                        "exchange_segment":
                            "nse_cm",
                    }
                ],
                isIndex=True,
            )

            rest = [
                item
                for item in tokens
                if (
                    item[
                        "instrument_token"
                    ]
                    != NIFTY_SPOT_TOKEN
                )
            ]

            if rest:

                self.client.subscribe(
                    instrument_tokens=
                        rest,
                    isIndex=False,
                )

            self.streaming = True

            return len(tokens)

        except Exception as exc:

            self.streaming = False

            self.last_error = (
                f"subscribe: "
                f"{exc}"
            )

            raise

    # -----------------------------------------------------------------
    # Status
    # -----------------------------------------------------------------

    def status(
        self,
    ) -> Dict[str, Any]:

        return {
            "authenticated":
                self.authenticated,

            "connected":
                self.connected,

            "streaming":
                self.streaming,

            "last_tick":
                (
                    self.last_tick.isoformat()
                    if self.last_tick
                    else None
                ),

            "last_poll":
                (
                    self.last_poll.isoformat()
                    if self.last_poll
                    else None
                ),

            "poll_count":
                self.poll_count,

            "published_count":
                self.publish_count,

            "last_error":
                self.last_error,

            "future_token":
                self.future_token,

            "future_symbol":
                self.future_symbol,

            "option_tokens":
                len(
                    self.pcr_tokens
                ),
        }


# =============================================================================
# YAHOO RAW PRODUCER
# =============================================================================

class YahooRawProducer:
    """
    Historical Yahoo Finance producer.

    Only raw OHLCV observations are published.
    """

    def __init__(
        self,
        store: SharedRawStore,
    ) -> None:

        self.store = store

        self.last_error = ""
        self.last_run = None

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

        for idx, row in frame.iterrows():

            try:

                ts = pd.Timestamp(
                    idx
                )

                if ts.tzinfo is None:

                    ts = ts.tz_localize(
                        "UTC"
                    )

                ts = ts.tz_convert(
                    IST
                )

                raw = canonical_raw(
                    {
                        "symbol":
                            ticker,

                        "instrument_token":
                            ticker,

                        "exchange":
                            "NSE",

                        "exchange_segment":
                            "nse_cm",

                        "observation_timestamp":
                            ts.isoformat(),

                        "open":
                            finite_float(
                                row.get(
                                    "Open"
                                )
                            ),

                        "high":
                            finite_float(
                                row.get(
                                    "High"
                                )
                            ),

                        "low":
                            finite_float(
                                row.get(
                                    "Low"
                                )
                            ),

                        "close":
                            finite_float(
                                row.get(
                                    "Close"
                                )
                            ),

                        "volume":
                            finite_float(
                                row.get(
                                    "Volume"
                                )
                            ),

                        "source_status":
                            "HISTORICAL",
                    }
                )

                self.store.publish(
                    raw,
                    source="yahoo_finance",
                    source_type="historical_daily",
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
                "yfinance is not installed"
            )

        cleaned = list(
            dict.fromkeys(
                ticker.strip()
                for ticker in tickers
                if str(
                    ticker
                ).strip()
            )
        )

        self.last_run = now_ist()

        successful = 0
        published = 0
        errors: Dict[
            str,
            str,
        ] = {}

        for ticker in cleaned:

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

                n = (
                    self.publish_dataframe(
                        ticker,
                        frame,
                    )
                )

                if n:

                    successful += 1
                    published += n

                else:

                    errors[
                        ticker
                    ] = (
                        "No usable rows"
                    )

            except Exception as exc:

                errors[
                    ticker
                ] = str(exc)

        self.tickers_with_data = (
            successful
        )

        if errors:

            self.last_error = (
                "; ".join(
                    f"{k}: {v}"
                    for k, v
                    in list(
                        errors.items()
                    )[:5]
                )
            )

        return {
            "tickers_requested":
                len(cleaned),

            "tickers_with_data":
                successful,

            "rows_published":
                published,

            "errors":
                errors,
        }


# =============================================================================
# CONSUMER HEALTH
# =============================================================================

def read_consumer_health() -> Dict[str, Any]:

    result = {}

    for path in sorted(
        CONSUMER_DIR.glob(
            "*.json"
        )
    ):

        try:

            result[
                path.stem
            ] = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )

        except Exception:

            result[
                path.stem
            ] = {
                "status":
                    "INVALID_HEARTBEAT"
            }

    return result


def consumer_age(
    heartbeat: Mapping[str, Any],
) -> Optional[float]:

    value = (
        heartbeat.get(
            "heartbeat_timestamp"
        )
        or heartbeat.get(
            "timestamp"
        )
    )

    if not value:
        return None

    try:

        dt = datetime.fromisoformat(
            str(value)
        )

        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=IST
            )

        return max(
            0.0,
            (
                now_ist()
                - dt.astimezone(IST)
            ).total_seconds(),
        )

    except Exception:

        return None


# =============================================================================
# PRODUCER HEALTH
# =============================================================================

def write_producer_health(
    store: SharedRawStore,
    kotak: KotakRawProducer,
    yahoo: YahooRawProducer,
) -> Dict[str, Any]:

    payload = {
        "schema_version":
            HEALTH_SCHEMA_VERSION,

        "producer_version":
            PRODUCER_VERSION,

        "heartbeat_timestamp":
            iso_now(),

        "producer_status":
            store.status(),

        "kotak":
            kotak.status(),

        "yahoo": {
            "last_run":
                (
                    yahoo.last_run.isoformat()
                    if yahoo.last_run
                    else None
                ),

            "rows_published":
                yahoo.rows_published,

            "tickers_with_data":
                yahoo.tickers_with_data,

            "last_error":
                yahoo.last_error,
        },
    }

    tmp = (
        HEALTH_FILE
        .with_suffix(".tmp")
    )

    tmp.write_text(
        json.dumps(
            json_safe(payload),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    os.replace(
        tmp,
        HEALTH_FILE,
    )

    if store.bus.enabled:

        try:

            store.bus.producer_health(
                payload
            )

        except Exception as exc:

            payload[
                "remote_health_error"
            ] = str(exc)

    return payload


# =============================================================================
# TICKER PARSING
# =============================================================================

def parse_ticker_text(
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
            x.strip()
            for x in text.split(",")
            if x.strip()
        )
    )


# =============================================================================
# STREAMLIT UI
# =============================================================================

def main() -> None:

    st.set_page_config(
        page_title=
            "Common Raw Data Producer",

        page_icon=
            "📡",

        layout=
            "wide",
    )

    # -----------------------------------------------------------------
    # SESSION OBJECTS
    # -----------------------------------------------------------------

    if "bus" not in st.session_state:

        st.session_state.bus = (
            SupabaseRawBus()
        )

    if "store" not in st.session_state:

        st.session_state.store = (
            SharedRawStore(
                st.session_state.bus
            )
        )

    if "kotak" not in st.session_state:

        st.session_state.kotak = (
            KotakRawProducer(
                st.session_state.store
            )
        )

    if "yahoo" not in st.session_state:

        st.session_state.yahoo = (
            YahooRawProducer(
                st.session_state.store
            )
        )

    bus: SupabaseRawBus = (
        st.session_state.bus
    )

    store: SharedRawStore = (
        st.session_state.store
    )

    kotak: KotakRawProducer = (
        st.session_state.kotak
    )

    yahoo: YahooRawProducer = (
        st.session_state.yahoo
    )

    # -----------------------------------------------------------------
    # HEADER
    # -----------------------------------------------------------------

    st.title(
        "COMMON RAW DATA PRODUCER"
    )

    st.caption(
        "RAW only • shared bus • "
        "NIFTY / Alpha / GSR remain isolated"
    )

    remote = (
        bus.health()
        if bus.enabled
        else {
            "configured": False,
            "reachable": False,
            "error":
                "Supabase not configured",
        }
    )

    ks = kotak.status()
    ps = store.status()

    c1, c2, c3, c4 = (
        st.columns(4)
    )

    c1.metric(
        "PRODUCER",
        "READY",
    )

    c2.metric(
        "SUPABASE",
        (
            "🟢 ONLINE"
            if remote.get(
                "reachable"
            )
            else "🔴 OFFLINE"
        ),
    )

    c3.metric(
        "KOTAK",
        (
            "🟢 AUTH"
            if ks[
                "authenticated"
            ]
            else "⚪ NOT AUTH"
        ),
    )

    c4.metric(
        "RAW EVENTS",
        f"{ps['sequence']:,}",
    )

    # -----------------------------------------------------------------
    # SHARED BUS STATUS
    # -----------------------------------------------------------------

    if not bus.enabled:

        if ALLOW_LOCAL_ONLY:

            st.warning(
                "Supabase is not configured. "
                "LOCAL-ONLY mode is enabled. "
                "This is NOT suitable for "
                "three-machine sharing."
            )

        else:

            st.error(
                "Three-machine shared bus "
                "is NOT configured. "
                "Set SUPABASE_URL and "
                "SUPABASE_SERVICE_ROLE_KEY."
            )

    elif not remote.get(
        "reachable"
    ):

        st.warning(
            remote.get(
                "error",
                "Supabase is unreachable.",
            )
        )

    else:

        st.success(
            "Shared Supabase raw bus is "
            "reachable. This is the "
            "cross-machine source of truth."
        )

    # -----------------------------------------------------------------
    # TWO COLUMN CONTROL PANEL
    # -----------------------------------------------------------------

    left, right = (
        st.columns(2)
    )

    # =================================================================
    # KOTAK
    # =================================================================

    with left:

        st.subheader(
            "1. Kotak Raw Source"
        )

        cred = (
            kotak.credentials_status()
        )

        if cred[
            "credentials_present"
        ]:

            st.success(
                "Kotak credentials present"
            )

        else:

            st.warning(
                "Missing: "
                + ", ".join(
                    cred[
                        "missing"
                    ]
                )
            )

        b1, b2, b3 = (
            st.columns(3)
        )

        if b1.button(
            "Login Kotak",
            use_container_width=True,
        ):

            try:

                kotak.login()

                st.success(
                    "Kotak authentication "
                    "successful"
                )

            except Exception as exc:

                st.error(
                    str(exc)
                )

        if b2.button(
            "Discover",
            use_container_width=True,
        ):

            try:

                st.json(
                    kotak.discover()
                )

            except Exception as exc:

                st.error(
                    str(exc)
                )

        if b3.button(
            "Subscribe",
            use_container_width=True,
        ):

            try:

                subscribed = (
                    kotak.subscribe()
                )

                st.success(
                    f"Subscribed: "
                    f"{subscribed} "
                    f"instruments"
                )

            except Exception as exc:

                st.error(
                    str(exc)
                )

        st.json(
            kotak.status()
        )

    # =================================================================
    # COMMON BUS
    # =================================================================

    with right:

        st.subheader(
            "2. Shared Raw Bus"
        )

        st.write(
            "**Remote source of truth:** "
            "Supabase `raw_observations`"
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
            "Test Shared Raw Bus",
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

                if rows:

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

                else:

                    st.info(
                        "No remote raw "
                        "observations yet."
                    )

            except Exception as exc:

                st.error(
                    str(exc)
                )

    # -----------------------------------------------------------------
    # LIVE POLL
    # -----------------------------------------------------------------

    st.divider()

    st.subheader(
        "3. Manual Live Raw Poll"
    )

    if st.button(
        "Fetch + Publish Current Kotak Snapshot",
        type="primary",
        use_container_width=True,
    ):

        try:

            if not kotak.authenticated:

                kotak.login()

            if (
                not kotak.future_token
            ):

                kotak.discover()

            n = kotak.poll()

            write_producer_health(
                store,
                kotak,
                yahoo,
            )

            st.success(
                f"Published {n} "
                "raw observations "
                "to the shared bus"
            )

        except Exception as exc:

            st.error(
                str(exc)
            )

    # -----------------------------------------------------------------
    # YAHOO
    # -----------------------------------------------------------------

    st.divider()

    st.subheader(
        "4. Yahoo Historical Raw Capture"
    )

    st.caption(
        "Daily OHLCV only. "
        "No indicators, scores, rankings "
        "or Alpha/NIFTY decisions are "
        "computed here."
    )

    default_tickers = (
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

    ticker_text = st.text_area(
        "Yahoo tickers — "
        "comma/newline separated",
        default_tickers,
        height=120,
    )

    upload = st.file_uploader(
        "Optional ticker universe CSV/TXT",
        type=[
            "csv",
            "txt",
        ],
    )

    if upload is not None:

        try:

            content = (
                upload.getvalue()
                .decode(
                    "utf-8",
                    errors="ignore",
                )
            )

            uploaded_tickers = (
                parse_ticker_text(
                    content
                )
            )

            if uploaded_tickers:

                ticker_text = (
                    ",".join(
                        uploaded_tickers
                    )
                )

                st.info(
                    f"Loaded "
                    f"{len(uploaded_tickers):,} "
                    f"tickers from "
                    f"{upload.name}"
                )

        except Exception as exc:

            st.warning(
                "Could not read ticker "
                f"file: {exc}"
            )

    period = st.selectbox(
        "Historical period",
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
        use_container_width=True,
    ):

        try:

            tickers = (
                parse_ticker_text(
                    ticker_text
                )
            )

            if not tickers:

                st.warning(
                    "No Yahoo tickers "
                    "were supplied."
                )

            else:

                result = (
                    yahoo.fetch(
                        tickers,
                        period=period,
                    )
                )

                write_producer_health(
                    store,
                    kotak,
                    yahoo,
                )

                st.success(
                    f"Published "
                    f"{result['rows_published']:,} "
                    "raw daily rows from "
                    f"{result['tickers_with_data']}/"
                    f"{result['tickers_requested']} "
                    "tickers"
                )

                st.json(
                    result
                )

        except Exception as exc:

            st.error(
                str(exc)
            )

    # -----------------------------------------------------------------
    # LOCAL LATEST CACHE
    # -----------------------------------------------------------------

    st.divider()

    st.subheader(
        "5. Local Latest Raw Cache"
    )

    if store.latest:

        rows = []

        for key, event in list(
            store.latest.items()
        )[-50:]:

            raw = (
                event.get(
                    "raw"
                )
                or {}
            )

            rows.append(
                {
                    "key":
                        key,

                    "symbol":
                        event.get(
                            "symbol"
                        ),

                    "source":
                        event.get(
                            "source"
                        ),

                    "received_at":
                        event.get(
                            "received_at"
                        ),

                    "ltp":
                        raw.get(
                            "ltp"
                        ),

                    "open":
                        raw.get(
                            "open"
                        ),

                   
