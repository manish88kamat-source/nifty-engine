"""
Kotak Neo -> Market Data Hub
Version: MDH_KOTAK_1.0.0

Independent raw market-data producer.
No strategy, alpha, regime, or BUY/SELL logic.
"""

from __future__ import annotations

import argparse
import inspect
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None

from kotak_credentials import KotakCredentials, load_kotak_credentials
from market_data_hub import MarketDataHub


VERSION = "1.0.0"
KOTAK_ENVIRONMENT = "prod"
DEFAULT_MAX_TICK_AGE_SECONDS = 30.0
DEFAULT_HEARTBEAT_SECONDS = 5.0
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


def first_value(record: Dict[str, Any], keys: Iterable[str]) -> Any:
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


# ============================================================
# TIMESTAMP
# ============================================================

def parse_timestamp(record: Dict[str, Any]) -> datetime:
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
    )

    value = first_value(record, keys)

    if value is None:
        raise ValueError("Kotak tick has no source timestamp")

    if isinstance(value, datetime):
        dt = value

    elif isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        dt = datetime.fromtimestamp(numeric, tz=timezone.utc)

    else:
        text = str(value).strip()

        if not text:
            raise ValueError("Empty Kotak source timestamp")

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
                    "Unsupported Kotak timestamp: " + text
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

def extract_token(record: Dict[str, Any]) -> str:
    value = first_value(
        record,
        (
            "instrument_token",
            "instrumentToken",
            "token",
            "tk",
            "exchange_token",
            "exchangeToken",
        ),
    )
    return "" if value is None else str(value).strip()


def extract_symbol(record: Dict[str, Any]) -> str:
    value = first_value(
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
        ),
    )
    return "" if value is None else str(value).strip()


def extract_exchange_segment(record: Dict[str, Any]) -> str:
    value = first_value(
        record,
        (
            "exchange_segment",
            "exchangeSegment",
            "exchange",
            "exch_seg",
            "es",
        ),
    )
    return "" if value is None else str(value).strip()


# ============================================================
# MARKET FIELDS
# ============================================================

def extract_ltp(record: Dict[str, Any]) -> Optional[float]:
    return first_float(
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
        ),
    )


def extract_open(record: Dict[str, Any]) -> Optional[float]:
    return first_float(record, ("open", "o", "Open"))


def extract_high(record: Dict[str, Any]) -> Optional[float]:
    return first_float(record, ("high", "h", "High"))


def extract_low(record: Dict[str, Any]) -> Optional[float]:
    return first_float(record, ("low", "l", "Low"))


def extract_close(record: Dict[str, Any]) -> Optional[float]:
    return first_float(record, ("close", "c", "Close"))


def extract_volume(record: Dict[str, Any]) -> Optional[float]:
    return first_float(
        record,
        ("volume", "vol", "v", "Volume"),
    )


def extract_oi(record: Dict[str, Any]) -> Optional[float]:
    return first_float(
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

        self.last_source_timestamp: Optional[datetime] = None
        self.last_receive_timestamp: Optional[datetime] = None
        self.last_symbol = ""
        self.last_ltp: Optional[float] = None

        self.ticks_received = 0
        self.ticks_accepted = 0
        self.ticks_rejected = 0
        self.subscription_count = 0

        self._stop_event = threading.Event()
        self._lock = threading.RLock()

    # --------------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------------

    def on_open(self, message: Any = None) -> None:
        with self._lock:
            self.stream_state = "OPEN"

    def on_error(self, error: Any = None) -> None:
        with self._lock:
            self.last_error = str(
                error if error is not None else ""
            )
            self.stream_state = "ERROR"

    def on_close(self, message: Any = None) -> None:
        with self._lock:
            self.stream_state = "CLOSED"

    # --------------------------------------------------------
    # RAW MESSAGE CALLBACK
    # --------------------------------------------------------

    def on_message(self, message: Any) -> None:
        try:
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except Exception:
                    return

            records = (
                message
                if isinstance(message, list)
                else [message]
            )

            for record in records:
                if isinstance(record, dict):
                    self._process_raw_tick(record)

        except Exception as exc:
            with self._lock:
                self.last_error = f"on_message: {exc}"

    # --------------------------------------------------------
    # RAW TICK PROCESSING
    # --------------------------------------------------------

    def _process_raw_tick(
        self,
        record: Dict[str, Any],
    ) -> None:
        receive_time = utc_now()

        with self._lock:
            self.ticks_received += 1
            self.last_receive_timestamp = receive_time

        token = extract_token(record)
        symbol = extract_symbol(record)

        if not symbol:
            if token:
                symbol = f"TOKEN_{token}"
            else:
                with self._lock:
                    self.ticks_rejected += 1
                return

        try:
            source_timestamp = parse_timestamp(record)
        except Exception as exc:
            with self._lock:
                self.ticks_rejected += 1
                self.last_error = f"timestamp: {exc}"
            return

        ltp = extract_ltp(record)

        if ltp is None:
            with self._lock:
                self.ticks_rejected += 1
                self.last_error = "tick has no valid LTP"
            return

        exchange_segment = extract_exchange_segment(record)
        mapping = dict(record)
        mapping["ltp"] = ltp

        values = {
            "open": extract_open(record),
            "high": extract_high(record),
            "low": extract_low(record),
            "close": extract_close(record),
            "volume": extract_volume(record),
            "oi": extract_oi(record),
        }

        for key, value in values.items():
            if value is not None:
                mapping[key] = value

        accepted = self.hub.ingest_mapping(
            mapping=mapping,
            source="KOTAK_NEO",
            symbol=symbol,
            timestamp=source_timestamp,
            instrument_token=token,
            exchange_segment=exchange_segment,
        )

        with self._lock:
            if accepted:
                self.ticks_accepted += 1
                self.last_source_timestamp = source_timestamp
                self.last_symbol = symbol
                self.last_ltp = ltp
            else:
                self.ticks_rejected += 1

    # --------------------------------------------------------
    # MOBILE NORMALIZATION
    # --------------------------------------------------------

    @staticmethod
    def _normalize_mobile(value: Any) -> str:
        """
        Accepts:
            9876543210
            919876543210
            +919876543210
            +91 9876543210
            09876543210

        Sends to Kotak:
            +919876543210
        """
        raw = str(value or "").strip()

        digits = "".join(
            ch for ch in raw
            if ch.isdigit()
        )

        # Remove accidental leading zero.
        if len(digits) == 11 and digits.startswith("0"):
            digits = digits[1:]

        # Remove India country code if already present.
        if len(digits) == 12 and digits.startswith("91"):
            digits = digits[2:]

        if (
            len(digits) != 10
            or not digits.isdigit()
            or digits[0] not in "6789"
        ):
            raise RuntimeError(
                "Kotak mobile number must be a valid "
                "10-digit Indian registered mobile number."
            )

        return "+91" + digits

    # --------------------------------------------------------
    # RESPONSE HELPERS
    # --------------------------------------------------------

    @staticmethod
    def _response_has_error(response: Any) -> bool:
        if not isinstance(response, dict):
            return False

        if response.get("error"):
            return True

        status = str(
            response.get("status", "")
        ).strip().lower()

        if status == "error":
            return True

        data = response.get("data")

        if isinstance(data, dict):
            if data.get("error"):
                return True

            data_status = str(
                data.get("status", "")
            ).strip().lower()

            if data_status == "error":
                return True

        return False

    @staticmethod
    def _safe_response(response: Any) -> Any:
        if not isinstance(response, dict):
            return response

        safe = dict(response)

        sensitive_keys = (
            "token",
            "access_token",
            "refresh_token",
            "session_token",
            "authorization",
            "auth_token",
            "sid",
            "rid",
            "Auth",
        )

        for key in sensitive_keys:
            if key in safe:
                safe[key] = "***REDACTED***"

        data = safe.get("data")

        if isinstance(data, dict):
            safe_data = dict(data)

            for key in sensitive_keys:
                if key in safe_data:
                    safe_data[key] = "***REDACTED***"

            safe["data"] = safe_data

        return safe

    # --------------------------------------------------------
    # AUTHENTICATION
    # --------------------------------------------------------

    def authenticate(
        self,
        totp_override: Optional[str] = None,
    ) -> bool:
        if NeoAPI is None:
            raise RuntimeError(
                "neo_api_client is not installed. "
                "Install the official Kotak Neo API v2 package."
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

        # IMPORTANT:
        # Secret may contain 10 digits, +91, 91, spaces, etc.
        # Kotak receives +91XXXXXXXXXX.
        mobile = self._normalize_mobile(
            getattr(
                self.credentials,
                "mobile",
                "",
            )
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

        # ----------------------------------------------------
        # SDK COMPATIBILITY
        # ----------------------------------------------------
        # Kotak SDK releases have exposed both spellings:
        #   mobile_number
        #   mobilenumber
        #
        # Inspect the installed method and use the spelling
        # supported by that exact installed SDK.
        # ----------------------------------------------------

        try:
            signature = inspect.signature(
                self.client.totp_login
            )
            parameter_names = set(
                signature.parameters.keys()
            )
        except Exception:
            parameter_names = set()

        if "mobilenumber" in parameter_names:

            step1 = self.client.totp_login(
                mobilenumber=mobile,
                ucc=self.credentials.ucc,
                totp=totp,
            )

        elif "mobile_number" in parameter_names:

            step1 = self.client.totp_login(
                mobile_number=mobile,
                ucc=self.credentials.ucc,
                totp=totp,
            )

        else:
            # Fallback for SDK versions exposing **kwargs.
            try:
                step1 = self.client.totp_login(
                    mobilenumber=mobile,
                    ucc=self.credentials.ucc,
                    totp=totp,
                )
            except TypeError:
                step1 = self.client.totp_login(
                    mobile_number=mobile,
                    ucc=self.credentials.ucc,
                    totp=totp,
                )

        if self._response_has_error(step1):
            raise RuntimeError(
                "Kotak TOTP login failed | "
                f"response={self._safe_response(step1)}"
            )

        # ----------------------------------------------------
        # MPIN VALIDATION
        # ----------------------------------------------------

        step2 = self.client.totp_validate(
            mpin=self.credentials.mpin
        )

        if self._response_has_error(step2):
            raise RuntimeError(
                "Kotak MPIN validation failed | "
                f"response={self._safe_response(step2)}"
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

        result = self.client.subscribe(
            instrument_tokens=instruments,
            isIndex=is_index,
        )

        if (
            isinstance(result, dict)
            and result.get("error")
        ):
            raise RuntimeError(
                "Kotak subscription failed"
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

    def data_age_seconds(self) -> Optional[float]:
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
        max_age_seconds: float = DEFAULT_MAX_TICK_AGE_SECONDS,
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
                "ticks_received": self.ticks_received,
                "ticks_accepted": self.ticks_accepted,
                "ticks_rejected": self.ticks_rejected,
                "subscription_count": self.subscription_count,
                "last_symbol": self.last_symbol,
                "last_ltp": self.last_ltp,
                "data_age_seconds": age,
                "last_error": self.last_error,
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
    return [
        {
            "instrument_token": "Nifty 50",
            "exchange_segment": "nse_cm",
        }
    ]


def live_test() -> int:

    print()
    print("KOTAK -> MARKET DATA HUB LIVE TEST")
    print("==================================")
    print()

    hub = MarketDataHub()

    source = KotakMarketDataSource(
        hub=hub
    )

    try:

        print("AUTHENTICATION : connecting...")

        source.authenticate()

        print("AUTHENTICATION : PASS")

        instruments = build_default_instruments()

        print("SUBSCRIPTION    : connecting...")

        count = source.subscribe(
            instruments
        )

        print(
            f"SUBSCRIPTION    : PASS ({count})"
        )

        print()
        print("Waiting for REAL Kotak tick...")

        deadline = time.monotonic() + 30.0

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

        if health["ticks_accepted"] <= 0:
            print()
            print("LIVE DATA TEST : FAIL")
            print(
                "No real Kotak tick was "
                "received within 30 seconds."
            )
            return 1

        if (
            health["data_age_seconds"] is None
            or health["data_age_seconds"] > 30.0
        ):
            print()
            print("LIVE DATA TEST : FAIL")
            print("Received data is stale.")
            return 1

        if hub_health["persisted_count"] <= 0:
            print()
            print("LIVE DATA TEST : FAIL")
            print(
                "Kotak tick did not reach "
                "persistent Hub storage."
            )
            return 1

        print()
        print("LIVE DATA TEST : PASS")

        return 0

    except Exception as exc:

        print()
        print("LIVE DATA TEST : FAIL")
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

        raw = {
            "tk": "TESTTOKEN",
            "display_symbol": "NIFTY",
            "lstup_time": utc_now_iso(),
            "ltp": 25000.0,
            "open": 24950.0,
            "high": 25050.0,
            "low": 24900.0,
            "close": 25000.0,
            "volume": 100000,
        }

        source.on_message(raw)

        assert source.ticks_received == 1
        assert source.ticks_accepted == 1
        assert source.ticks_rejected == 0
        assert hub.received_count == 1
        assert hub.persisted_count == 1
        assert source.last_symbol == "NIFTY"
        assert source.last_ltp == 25000.0

        # Mobile normalization tests.
        assert (
            source._normalize_mobile("9876543210")
            == "+919876543210"
        )

        assert (
            source._normalize_mobile("919876543210")
            == "+919876543210"
        )

        assert (
            source._normalize_mobile("+919876543210")
            == "+919876543210"
        )

        assert (
            source._normalize_mobile("+91 9876543210")
            == "+919876543210"
        )

        assert (
            source._normalize_mobile("09876543210")
            == "+919876543210"
        )

        print(
            "KOTAK MARKET DATA SOURCE TEST: PASS"
        )
        print("  raw parsing      : PASS")
        print("  timestamp        : PASS")
        print("  LTP extraction    : PASS")
        print("  hub ingestion     : PASS")
        print("  persistence       : PASS")
        print("  mobile normalize  : PASS")
        print("  network call      : NONE")


# ============================================================
# CLI
# ============================================================

def main() -> int:

    parser = argparse.ArgumentParser(
        description=(
            "Kotak Neo raw market-data "
            "producer for Market Data Hub."
        )
    )

    parser.add_argument(
        "--live",
        action="store_true",
        help=(
            "Connect to Kotak Neo and "
            "wait for a real tick."
        ),
    )

    args = parser.parse_args()

    if args.live:
        return live_test()

    local_test()

    print()
    print(
        "NOTE: local test used synthetic "
        "data only."
    )
    print(
        "Use --live for the real Kotak "
        "connectivity test."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
