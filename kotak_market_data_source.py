"""
Kotak Neo -> Market Data Hub
============================

Version:
    MDH_KOTAK_1.0.1

Purpose:
    Independent raw market-data producer for the common
    Market Data Hub.

Architecture:

    Kotak Neo
        |
        v
    KotakMarketDataSource
        |
        v
    MarketDataHub
        |
        +---- NIFTY Engine
        +---- GSR
        +---- Next-Day Alpha

IMPORTANT:
    - No strategy logic.
    - No BUY/SELL decisions.
    - No regime classification.
    - No alpha scoring.
    - Only raw market-data ingestion.
    - Credentials are loaded through kotak_credentials.py.
    - Credentials are never printed.

FIXES IN 1.0.1:
    1. NIFTY 50 is explicitly subscribed with isIndex=True.
    2. WebSocket ACK/control messages are NOT counted as ticks.
    3. Nested/list WebSocket payloads are handled.
    4. More Kotak field aliases are supported.
    5. Safe rejection diagnostics are exposed without credentials.
    6. Duplicate return True removed.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None

from kotak_credentials import (
    KotakCredentials,
    load_kotak_credentials,
)

from market_data_hub import MarketDataHub


VERSION = "1.0.1"

KOTAK_ENVIRONMENT = "prod"

DEFAULT_MAX_TICK_AGE_SECONDS = 30.0
DEFAULT_RECONNECT_SECONDS = 5.0


# ============================================================
# GENERIC HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def safe_float(value: Any) -> Optional[float]:
    try:
        result = float(value)

        if result != result:
            return None

        if result in (float("inf"), float("-inf")):
            return None

        return result

    except (TypeError, ValueError):
        return None


def first_value(
    record: Dict[str, Any],
    keys: Iterable[str],
) -> Any:
    for key in keys:
        if key in record:
            value = record.get(key)

            if value is not None:
                return value

    return None


def first_float(
    record: Dict[str, Any],
    keys: Iterable[str],
) -> Optional[float]:
    return safe_float(first_value(record, keys))


def lower_key_map(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Case-insensitive helper.

    Kotak payloads can differ slightly between SDK/feed versions.
    We preserve the original dictionary and use this only for
    tolerant field discovery.
    """
    result: Dict[str, Any] = {}

    for key, value in record.items():
        result[str(key).lower()] = value

    return result


def first_value_tolerant(
    record: Dict[str, Any],
    keys: Iterable[str],
) -> Any:
    value = first_value(record, keys)

    if value is not None:
        return value

    lowered = lower_key_map(record)

    for key in keys:
        value = lowered.get(str(key).lower())

        if value is not None:
            return value

    return None


def first_float_tolerant(
    record: Dict[str, Any],
    keys: Iterable[str],
) -> Optional[float]:
    return safe_float(
        first_value_tolerant(record, keys)
    )


# ============================================================
# TIMESTAMP
# ============================================================

def parse_timestamp(
    record: Dict[str, Any],
) -> datetime:
    """
    Extract source timestamp from a Kotak raw tick.

    Source timestamp is deliberately required.
    Receive time is NOT silently substituted.
    """

    keys = (
        "lstup_time",
        "ft",
        "exch_tm",
        "exchange_timestamp",
        "exchangeTime",
        "timestamp",
        "ltt",
        "last_traded_time",
        "lastTradedTime",
        "time",
        "lastUpdateTime",
    )

    value = first_value_tolerant(record, keys)

    if value is None:
        raise ValueError(
            "tick has no source timestamp"
        )

    if isinstance(value, datetime):
        dt = value

    elif isinstance(value, (int, float)):
        numeric = float(value)

        # Epoch milliseconds.
        if numeric > 10_000_000_000:
            numeric /= 1000.0

        dt = datetime.fromtimestamp(
            numeric,
            tz=timezone.utc,
        )

    else:
        text = str(value).strip()

        if not text:
            raise ValueError(
                "empty source timestamp"
            )

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(text)

        except ValueError:
            try:
                numeric = float(text)

                if numeric > 10_000_000_000:
                    numeric /= 1000.0

                dt = datetime.fromtimestamp(
                    numeric,
                    tz=timezone.utc,
                )

            except Exception as exc:
                raise ValueError(
                    "unsupported Kotak timestamp"
                ) from exc

    if dt.tzinfo is None:
        from zoneinfo import ZoneInfo

        dt = dt.replace(
            tzinfo=ZoneInfo("Asia/Kolkata")
        )

    return dt.astimezone(timezone.utc)


# ============================================================
# SYMBOL / TOKEN
# ============================================================

def extract_token(
    record: Dict[str, Any],
) -> str:
    value = first_value_tolerant(
        record,
        (
            "instrument_token",
            "instrumentToken",
            "token",
            "tk",
            "exchange_token",
            "exchangeToken",
            "pSymbol",
        ),
    )

    if value is None:
        return ""

    return str(value).strip()


def extract_symbol(
    record: Dict[str, Any],
) -> str:
    value = first_value_tolerant(
        record,
        (
            "display_symbol",
            "displaySymbol",
            "trading_symbol",
            "tradingSymbol",
            "pTrdSymbol",
            "ts",
            "symbol",
            "name",
            "scrip",
            "pSymbolName",
        ),
    )

    if value is None:
        return ""

    return str(value).strip()


def extract_exchange_segment(
    record: Dict[str, Any],
) -> str:
    value = first_value_tolerant(
        record,
        (
            "exchange_segment",
            "exchangeSegment",
            "exchange",
            "exch_seg",
            "es",
            "pExchSeg",
        ),
    )

    if value is None:
        return ""

    return str(value).strip()


# ============================================================
# MARKET FIELDS
# ============================================================

def extract_ltp(
    record: Dict[str, Any],
) -> Optional[float]:
    return first_float_tolerant(
        record,
        (
            "ltp",
            "last_price",
            "lastPrice",
            "last_traded_price",
            "lastTradedPrice",
            "lp",
            "LTP",
            "price",
            "close",
            "lastTradedPrice",
        ),
    )


def extract_open(
    record: Dict[str, Any],
) -> Optional[float]:
    return first_float_tolerant(
        record,
        ("open", "o", "Open"),
    )


def extract_high(
    record: Dict[str, Any],
) -> Optional[float]:
    return first_float_tolerant(
        record,
        ("high", "h", "High"),
    )


def extract_low(
    record: Dict[str, Any],
) -> Optional[float]:
    return first_float_tolerant(
        record,
        ("low", "l", "Low"),
    )


def extract_close(
    record: Dict[str, Any],
) -> Optional[float]:
    return first_float_tolerant(
        record,
        ("close", "c", "Close"),
    )


def extract_volume(
    record: Dict[str, Any],
) -> Optional[float]:
    return first_float_tolerant(
        record,
        (
            "volume",
            "vol",
            "v",
            "Volume",
            "last_volume",
            "lastVolume",
        ),
    )


def extract_oi(
    record: Dict[str, Any],
) -> Optional[float]:
    return first_float_tolerant(
        record,
        (
            "oi",
            "open_interest",
            "openInterest",
            "OpenInterest",
            "oI",
        ),
    )


# ============================================================
# MESSAGE NORMALIZATION
# ============================================================

def normalize_message_records(
    message: Any,
) -> List[Dict[str, Any]]:
    """
    Convert a Kotak websocket callback payload into candidate
    dictionaries.

    Control/ACK payloads are allowed through this layer, but are
    filtered later because they do not contain a valid LTP.
    """

    if isinstance(message, str):
        text = message.strip()

        if not text:
            return []

        try:
            message = json.loads(text)

        except Exception:
            return []

    if isinstance(message, dict):
        records: List[Dict[str, Any]] = [message]

        # Some SDK/feed versions wrap data in "data".
        nested = message.get("data")

        if isinstance(nested, dict):
            records.append(nested)

        elif isinstance(nested, list):
            records.extend(
                item
                for item in nested
                if isinstance(item, dict)
            )

        # Some payloads use "data" as a JSON string.
        elif isinstance(nested, str):
            try:
                parsed = json.loads(nested)

                if isinstance(parsed, dict):
                    records.append(parsed)

                elif isinstance(parsed, list):
                    records.extend(
                        item
                        for item in parsed
                        if isinstance(item, dict)
                    )

            except Exception:
                pass

        return records

    if isinstance(message, list):
        records: List[Dict[str, Any]] = []

        for item in message:
            records.extend(
                normalize_message_records(item)
            )

        return records

    return []


# ============================================================
# KOTAK MARKET DATA SOURCE
# ============================================================

class KotakMarketDataSource:

    def __init__(
        self,
        hub: MarketDataHub,
        credentials: Optional[KotakCredentials] = None,
        environment: str = KOTAK_ENVIRONMENT,
    ):
        self.hub = hub

        self.credentials = (
            credentials
            if credentials is not None
            else load_kotak_credentials()
        )

        self.environment = environment

        self.client = None

        self.authenticated = False

        self.stream_state = "DISCONNECTED"

        self.last_error = ""

        self.last_rejection_reason = ""

        self.last_message_type = ""

        self.last_raw_keys: List[str] = []

        self.last_source_timestamp: Optional[datetime] = None

        self.last_receive_timestamp: Optional[datetime] = None

        self.last_symbol = ""

        self.last_ltp: Optional[float] = None

        # IMPORTANT:
        # ticks_received counts actual market-data ticks only.
        self.messages_received = 0

        self.ticks_received = 0
        self.ticks_accepted = 0
        self.ticks_rejected = 0

        self.subscription_count = 0

        self._stop_event = threading.Event()
        self._lock = threading.RLock()

    # --------------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------------

    def on_open(
        self,
        message: Any = None,
    ) -> None:
        with self._lock:
            self.stream_state = "OPEN"

    def on_error(
        self,
        error: Any = None,
    ) -> None:
        with self._lock:
            self.last_error = str(
                error if error is not None else ""
            )
            self.stream_state = "ERROR"

    def on_close(
        self,
        message: Any = None,
    ) -> None:
        with self._lock:
            self.stream_state = "CLOSED"

    # --------------------------------------------------------
    # RAW MESSAGE CALLBACK
    # --------------------------------------------------------

    def on_message(
        self,
        message: Any,
    ) -> None:
        """
        Receive raw Kotak websocket payload.

        ACK/control messages are NOT treated as market ticks.
        """

        receive_time = utc_now()

        with self._lock:
            self.messages_received += 1
            self.last_receive_timestamp = receive_time
            self.last_message_type = type(message).__name__

        try:
            records = normalize_message_records(message)

            if not records:
                with self._lock:
                    self.last_rejection_reason = (
                        "websocket message was not a supported "
                        "dict/list/JSON payload"
                    )
                return

            for record in records:
                if not isinstance(record, dict):
                    continue

                self._process_raw_tick(record)

        except Exception as exc:
            with self._lock:
                self.last_error = (
                    f"on_message: {type(exc).__name__}: {exc}"
                )

    # --------------------------------------------------------
    # RAW TICK PROCESSING
    # --------------------------------------------------------

    def _process_raw_tick(
        self,
        record: Dict[str, Any],
    ) -> None:
        with self._lock:
            self.last_raw_keys = [
                str(key)
                for key in list(record.keys())[:40]
            ]

        # ----------------------------------------------------
        # FIRST GATE:
        # A control/ACK message normally has no LTP.
        # Therefore it is NOT a tick.
        # ----------------------------------------------------

        ltp = extract_ltp(record)

        if ltp is None:
            with self._lock:
                self.last_rejection_reason = (
                    "non-market websocket message / ACK "
                    "(no valid LTP)"
                )
            return

        # This is an actual market-data candidate.
        with self._lock:
            self.ticks_received += 1

        token = extract_token(record)
        symbol = extract_symbol(record)

        if not symbol:
            if token:
                symbol = f"TOKEN_{token}"
            else:
                # For index feed variants the instrument name can
                # be absent. Keep a safe logical symbol rather than
                # throwing away an otherwise valid LTP tick.
                symbol = "NIFTY_50"

        try:
            source_timestamp = parse_timestamp(record)

        except Exception as exc:
            with self._lock:
                self.ticks_rejected += 1
                self.last_rejection_reason = (
                    f"timestamp: {exc}"
                )
                self.last_error = (
                    f"timestamp: {exc}"
                )
            return

        exchange_segment = (
            extract_exchange_segment(record)
            or "nse_cm"
        )

        mapping = dict(record)

        # Canonical fields for MarketDataHub.
        mapping["ltp"] = ltp

        open_price = extract_open(record)
        high_price = extract_high(record)
        low_price = extract_low(record)
        close_price = extract_close(record)
        volume = extract_volume(record)
        oi = extract_oi(record)

        if open_price is not None:
            mapping["open"] = open_price

        if high_price is not None:
            mapping["high"] = high_price

        if low_price is not None:
            mapping["low"] = low_price

        if close_price is not None:
            mapping["close"] = close_price

        if volume is not None:
            mapping["volume"] = volume

        if oi is not None:
            mapping["oi"] = oi

        try:
            accepted = self.hub.ingest_mapping(
                mapping=mapping,
                source="KOTAK_NEO",
                symbol=symbol,
                timestamp=source_timestamp,
                instrument_token=token,
                exchange_segment=exchange_segment,
            )

        except Exception as exc:
            with self._lock:
                self.ticks_rejected += 1
                self.last_error = (
                    f"hub_ingest: {type(exc).__name__}: {exc}"
                )
                self.last_rejection_reason = (
                    f"hub_ingest: {type(exc).__name__}: {exc}"
                )
            return

        with self._lock:
            if accepted:
                self.ticks_accepted += 1

                self.last_source_timestamp = (
                    source_timestamp
                )

                self.last_symbol = symbol
                self.last_ltp = ltp

                self.last_rejection_reason = ""
                self.last_error = ""

            else:
                self.ticks_rejected += 1
                self.last_rejection_reason = (
                    "MarketDataHub rejected the raw observation"
                )

    # --------------------------------------------------------
    # AUTHENTICATION
    # --------------------------------------------------------

    def authenticate(
        self,
        totp_override: Optional[str] = None,
    ) -> bool:
        """
        Authenticate with Kotak Neo.

        The current 6-digit TOTP is supplied at runtime.
        """

        if NeoAPI is None:
            raise RuntimeError(
                "neo_api_client is not installed. "
                "Install the Kotak Neo API package."
            )

        required = {
            "KOTAK_CONSUMER_KEY": getattr(
                self.credentials,
                "consumer_key",
                "",
            ),
            "KOTAK_MOBILE": getattr(
                self.credentials,
                "mobile",
                "",
            ),
            "KOTAK_UCC": getattr(
                self.credentials,
                "ucc",
                "",
            ),
            "KOTAK_MPIN": getattr(
                self.credentials,
                "mpin",
                "",
            ),
        }

        missing = [
            name
            for name, value in required.items()
            if not str(value or "").strip()
        ]

        if missing:
            raise RuntimeError(
                "Missing Kotak credentials: "
                + ", ".join(missing)
            )

        totp = str(
            totp_override
            or getattr(
                self.credentials,
                "totp",
                "",
            )
            or ""
        ).strip()

        if not totp:
            raise RuntimeError(
                "Current 6-digit KOTAK TOTP is required."
            )

        if not totp.isdigit() or len(totp) != 6:
            raise RuntimeError(
                "KOTAK TOTP must be the current 6-digit code."
            )

        self.client = NeoAPI(
            environment=self.environment,
            access_token=None,
            neo_fin_key=None,
            consumer_key=self.credentials.consumer_key,
        )

        self.client.on_message = self.on_message
        self.client.on_error = self.on_error
        self.client.on_close = self.on_close
        self.client.on_open = self.on_open

        # Keep the legacy callback API because this deployed
        # environment has already proven authentication works with it.
        step1 = self.client.totp_login(
            mobile_number=self.credentials.mobile,
            ucc=self.credentials.ucc,
            totp=totp,
        )

        if (
            isinstance(step1, dict)
            and step1.get("error")
        ):
            safe_response = dict(step1)

            for key in (
                "token",
                "access_token",
                "refresh_token",
                "session_token",
                "authorization",
                "auth_token",
            ):
                if key in safe_response:
                    safe_response[key] = "***REDACTED***"

            raise RuntimeError(
                "Kotak TOTP login failed | "
                f"response={safe_response}"
            )

        step2 = self.client.totp_validate(
            mpin=self.credentials.mpin
        )

        if (
            isinstance(step2, dict)
            and step2.get("error")
        ):
            raise RuntimeError(
                "Kotak MPIN validation failed."
            )

        with self._lock:
            self.authenticated = True
            self.stream_state = "AUTHENTICATED"
            self.last_error = ""

        return True

    # --------------------------------------------------------
    # SUBSCRIPTION
    # --------------------------------------------------------

    def subscribe(
        self,
        instruments: List[Dict[str, str]],
        is_index: bool = False,
    ) -> int:
        if not self.authenticated:
            raise RuntimeError(
                "Kotak Neo is not authenticated."
            )

        if self.client is None:
            raise RuntimeError(
                "Kotak Neo client unavailable."
            )

        if not instruments:
            raise ValueError(
                "No instruments supplied."
            )

        # IMPORTANT:
        # NIFTY 50 index requires isIndex=True with the legacy
        # callback-based Kotak API.
        result = self.client.subscribe(
            instrument_tokens=instruments,
            isIndex=is_index,
            isDepth=False,
        )

        if (
            isinstance(result, dict)
            and result.get("error")
        ):
            raise RuntimeError(
                "Kotak subscription failed | "
                "error response received"
            )

        with self._lock:
            self.subscription_count += len(instruments)
            self.stream_state = "STREAMING"

        return len(instruments)

    # --------------------------------------------------------
    # CONNECTION
    # --------------------------------------------------------

    def connect_and_subscribe(
        self,
        instruments: List[Dict[str, str]],
        is_index: bool = False,
    ) -> int:
        self.authenticate()

        return self.subscribe(
            instruments=instruments,
            is_index=is_index,
        )

    # --------------------------------------------------------
    # HEALTH
    # --------------------------------------------------------

    def data_age_seconds(
        self,
    ) -> Optional[float]:
        with self._lock:
            if self.last_source_timestamp is None:
                return None

            age = (
                utc_now()
                - self.last_source_timestamp
            ).total_seconds()

            return max(0.0, age)

    def health(
        self,
        max_age_seconds: float = (
            DEFAULT_MAX_TICK_AGE_SECONDS
        ),
    ) -> Dict[str, Any]:
        age = self.data_age_seconds()

        if not self.authenticated:
            status = "NOT_AUTHENTICATED"

        elif age is None:
            status = "NO_TICK"

        elif age <= max_age_seconds:
            status = "HEALTHY"

        else:
            status = "STALE"

        with self._lock:
            return {
                "version": VERSION,
                "source": "KOTAK_NEO",
                "environment": self.environment,
                "authenticated": self.authenticated,
                "stream_state": self.stream_state,
                "status": status,

                # Actual market ticks only.
                "ticks_received": self.ticks_received,
                "ticks_accepted": self.ticks_accepted,
                "ticks_rejected": self.ticks_rejected,

                # All websocket callbacks/messages.
                "messages_received": self.messages_received,

                "subscription_count": (
                    self.subscription_count
                ),

                "last_symbol": self.last_symbol,
                "last_ltp": self.last_ltp,
                "data_age_seconds": age,

                "last_error": self.last_error,
                "last_rejection_reason": (
                    self.last_rejection_reason
                ),
                "last_message_type": (
                    self.last_message_type
                ),
                "last_raw_keys": (
                    self.last_raw_keys
                ),
            }

    # --------------------------------------------------------
    # STOP
    # --------------------------------------------------------

    def stop(self) -> None:
        self._stop_event.set()

        with self._lock:
            self.stream_state = "STOPPED"


# ============================================================
# DEMONSTRATION / LIVE TEST
# ============================================================

def build_default_instruments() -> List[Dict[str, str]]:
    """
    Minimal live subscription.

    NIFTY 50 is an INDEX, therefore the live test explicitly
    calls subscribe(..., isIndex=True).
    """

    return [
        {
            "instrument_token": "Nifty 50",
            "exchange_segment": "nse_cm",
        }
    ]


def live_test() -> int:
    """
    REAL LIVE TEST.

    PASS requires:
        1. Kotak authentication.
        2. Index subscription success.
        3. At least one real market-data tick.
        4. Valid source timestamp.
        5. Successful MarketDataHub ingestion.
        6. Successful Hub persistence.
    """

    print()
    print("KOTAK -> MARKET DATA HUB LIVE TEST")
    print("==================================")
    print()

    hub = MarketDataHub()

    source = KotakMarketDataSource(
        hub=hub
    )

    try:
        print(
            "AUTHENTICATION : connecting..."
        )

        source.authenticate()

        print(
            "AUTHENTICATION : PASS"
        )

        instruments = (
            build_default_instruments()
        )

        print(
            "SUBSCRIPTION    : connecting..."
        )

        # CRITICAL FIX:
        # NIFTY 50 is an INDEX.
        count = source.subscribe(
            instruments=instruments,
            is_index=True,
        )

        print(
            f"SUBSCRIPTION    : PASS ({count})"
        )

        print()
        print(
            "Waiting for REAL Kotak tick..."
        )

        deadline = (
            time.monotonic()
            + 30.0
        )

        while time.monotonic() < deadline:
            health = source.health(
                max_age_seconds=30.0
            )

            if health["ticks_accepted"] > 0:
                break

            time.sleep(1.0)

        health = source.health(
            max_age_seconds=30.0
        )

        print()
        print(
            f"KOTAK STREAM    : "
            f"{health['stream_state']}"
        )

        print(
            f"MESSAGES RX     : "
            f"{health['messages_received']}"
        )

        print(
            f"TICKS RECEIVED  : "
            f"{health['ticks_received']}"
        )

        print(
            f"TICKS ACCEPTED  : "
            f"{health['ticks_accepted']}"
        )

        print(
            f"TICKS REJECTED  : "
            f"{health['ticks_rejected']}"
        )

        print(
            f"LAST SYMBOL     : "
            f"{health['last_symbol'] or '-'}"
        )

        print(
            f"LAST LTP        : "
            f"{health['last_ltp']}"
        )

        print(
            f"DATA AGE        : "
            f"{health['data_age_seconds']}"
        )

        if health["last_rejection_reason"]:
            print(
                f"LAST REJECT     : "
                f"{health['last_rejection_reason']}"
            )

        if health["last_raw_keys"]:
            print(
                f"LAST RAW KEYS   : "
                f"{', '.join(health['last_raw_keys'])}"
            )

        hub_health = hub.health(
            max_age_seconds=30.0
        )

        print(
            f"HUB RECEIVED    : "
            f"{hub_health['received_count']}"
        )

        print(
            f"HUB PERSISTED   : "
            f"{hub_health['persisted_count']}"
        )

        print(
            f"HUB STATUS      : "
            f"{hub_health['status']}"
        )

        # ----------------------------------------------------
        # FINAL GATES
        # ----------------------------------------------------

        if health["ticks_received"] <= 0:
            print()
            print(
                "LIVE DATA TEST : FAIL"
            )
            print(
                "No actual market-data tick was "
                "received within 30 seconds."
            )
            return 1

        if health["ticks_accepted"] <= 0:
            print()
            print(
                "LIVE DATA TEST : FAIL"
            )
            print(
                "A market-data message arrived, "
                "but MarketDataHub did not accept it."
            )
            return 1

        if (
            health["data_age_seconds"] is None
            or health["data_age_seconds"] > 30.0
        ):
            print()
            print(
                "LIVE DATA TEST : FAIL"
            )
            print(
                "Received market data is stale."
            )
            return 1

        if hub_health["persisted_count"] <= 0:
            print()
            print(
                "LIVE DATA TEST : FAIL"
            )
            print(
                "Kotak tick reached the source layer "
                "but did not reach persistent Hub storage."
            )
            return 1

        print()
        print(
            "LIVE DATA TEST : PASS"
        )

        print(
            "Kotak -> WebSocket -> Parser -> "
            "MarketDataHub -> Persistence : PASS"
        )

        return 0

    except Exception as exc:
        print()
        print(
            "LIVE DATA TEST : FAIL"
        )

        print(
            f"REASON         : "
            f"{type(exc).__name__}: {exc}"
        )

        return 1

    finally:
        source.stop()


# ============================================================
# LOCAL STRUCTURAL TEST
# ============================================================

def local_test() -> None:
    """
    Offline structural test.

    This does NOT contact Kotak.
    """

    from tempfile import TemporaryDirectory

    from market_data_hub import RawObservationStore

    with TemporaryDirectory() as temp_dir:
        store = RawObservationStore(
            path=f"{temp_dir}/test.jsonl"
        )

        hub = MarketDataHub(
            store=store
        )

        source = KotakMarketDataSource(
            hub=hub
        )

        now = utc_now_iso()

        raw = {
            "tk": "TESTTOKEN",
            "display_symbol": "NIFTY",
            "exchange_segment": "nse_cm",
            "lstup_time": now,
            "ltp": 25000.0,
            "open": 24950.0,
            "high": 25050.0,
            "low": 24900.0,
            "close": 25000.0,
            "volume": 100000,
        }

        source.on_message(raw)

        assert source.messages_received == 1

        assert source.ticks_received == 1

        assert source.ticks_accepted == 1

        assert source.ticks_rejected == 0

        assert hub.received_count == 1

        assert hub.persisted_count == 1

        assert source.last_symbol == "NIFTY"

        assert source.last_ltp == 25000.0

        print(
            "KOTAK MARKET DATA SOURCE TEST: PASS"
        )

        print(
            "  message normalization : PASS"
        )

        print(
            "  raw parsing            : PASS"
        )

        print(
            "  timestamp              : PASS"
        )

        print(
            "  LTP extraction         : PASS"
        )

        print(
            "  hub ingestion          : PASS"
        )

        print(
            "  persistence            : PASS"
        )

        print(
            "  network call           : NONE"
        )


# ============================================================
# ACK / CONTROL MESSAGE TEST
# ============================================================

def control_message_test() -> None:
    """
    Verifies that a subscription ACK/control payload is not
    incorrectly counted as a market tick.
    """

    from tempfile import TemporaryDirectory

    from market_data_hub import RawObservationStore

    with TemporaryDirectory() as temp_dir:
        store = RawObservationStore(
            path=f"{temp_dir}/ack_test.jsonl"
        )

        hub = MarketDataHub(
            store=store
        )

        source = KotakMarketDataSource(
            hub=hub
        )

        ack = {
            "type": "ack",
            "stat": "Ok",
            "event": "subscribe",
            "instrument_token": "Nifty 50",
        }

        source.on_message(ack)

        assert source.messages_received == 1

        assert source.ticks_received == 0

        assert source.ticks_accepted == 0

        assert hub.received_count == 0

        assert hub.persisted_count == 0

        print(
            "KOTAK ACK FILTER TEST: PASS"
        )

        print(
            "  ACK not counted as tick : PASS"
        )

        print(
            "  Hub not polluted        : PASS"
        )


# ============================================================
# CLI
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Kotak Neo raw market-data producer "
            "for Market Data Hub."
        )
    )

    parser.add_argument(
        "--live",
        action="store_true",
        help=(
            "Connect to Kotak Neo and "
            "wait for a real NIFTY 50 tick."
        ),
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Run offline structural and "
            "ACK-filter tests."
        ),
    )

    args = parser.parse_args()

    if args.live:
        return live_test()

    if args.test:
        local_test()
        control_message_test()

        print()
        print(
            "ALL LOCAL TESTS: PASS"
        )

        return 0

    local_test()
    control_message_test()

    print()
    print(
        "NOTE: local tests used synthetic "
        "data only."
    )

    print(
        "Use --live for the real Kotak "
        "connectivity test."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
