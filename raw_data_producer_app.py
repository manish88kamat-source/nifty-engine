#!/usr/bin/env python3
"""
Leak-Proof Raw Data Producer | Institutional Research Bus

PURPOSE
-------
Pure raw-market-data acquisition layer.

STRICT RULES
------------
- Zero indicators
- Zero technical analysis
- Zero ML
- Zero alpha calculations
- Zero trading decisions
- Zero regime calculations
- Zero opinions from other engines

DATA FLOW
---------
Kotak Neo
    |
    +--> NIFTY Index raw quote
    |
    +--> Active NIFTY Future raw quote
    |
    +--> NIFTY Option raw quotes
    |
    +--> NIFTY heavyweight raw quotes
    |
    +--> Supabase raw_observations
    |
Yahoo
    |
    +--> Raw macro observations
    |
    +--> Supabase raw_observations

IMPORTANT KOTAK FIELD SEMANTICS
--------------------------------
pSymbol       = instrument token
pTrdSymbol    = trading symbol
pExchSeg      = exchange segment
pInstType     = instrument type
pOptionType   = CE / PE
dStrikePrice; = strike price in some Scrip Master payloads
pExpiryDate   = expiry field

This implementation intentionally supports both:
- current Kotak Neo Python SDK response shapes
- legacy/v2-style response shapes

No blind NIFTY future token fallback is used.
"""

from __future__ import annotations

import os
import re
import json
import time
import base64
import hmac
import hashlib
import struct
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
import requests

try:
    import streamlit as st
except ImportError:
    st = None

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None


# ============================================================================
# CONSTANTS
# ============================================================================

IST = ZoneInfo("Asia/Kolkata")

NIFTY_INDEX_TOKEN = "26000"
NIFTY_INDEX_SEGMENT = "nse_cm"
NIFTY_FO_SEGMENT = "nse_fo"

CONFIG = {
    "neo_environment": "prod",

    # Number of strikes on each side of ATM.
    # 5 means:
    # ATM-250 ... ATM-50, ATM, ATM+50 ... ATM+250
    "pcr_strike_count": 5,

    # NIFTY option strike spacing.
    "pcr_strike_step": 50.0,

    # Supabase.
    "supabase_url": os.getenv("SUPABASE_URL", "").strip(),
    "supabase_key": os.getenv("SUPABASE_KEY", "").strip(),

    # Producer polling.
    "poll_interval_sec": 3.0,

    # Yahoo macro refresh interval.
    "macro_every_n_cycles": 10,

    # Maximum tolerated age for a discovered contract.
    # Only used as a defensive validation.
    "max_contract_search_days_forward": 400,
}


# ============================================================================
# TIME HELPERS
# ============================================================================

def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


# ============================================================================
# GENERAL HELPERS
# ============================================================================

def safe_str(value: Any) -> str:
    """
    Convert arbitrary API values to a clean string.
    """
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    return str(value).strip()


def first_non_empty(record: Dict[str, Any], keys: List[str]) -> str:
    """
    Return first non-empty value from a list of possible API field names.
    """
    for key in keys:
        if key in record:
            value = safe_str(record.get(key))
            if value:
                return value
    return ""


def normalize_number(value: Any) -> Optional[float]:
    """
    Convert numeric-looking values to float.

    Handles:
    - commas
    - whitespace
    - trailing semicolons
    - integer/float values
    """
    if value is None:
        return None

    text = safe_str(value)
    if not text:
        return None

    text = text.replace(",", "").replace(";", "").strip()

    try:
        return float(text)
    except Exception:
        return None


def normalize_token(record: Dict[str, Any]) -> str:
    """
    Kotak Scrip Master authoritative token field is pSymbol.

    Older payloads may expose alternate names, so we retain defensive
    compatibility.
    """
    return first_non_empty(
        record,
        [
            "pSymbol",
            "pSymbolToken",
            "instrument_token",
            "token",
            "symbolToken",
        ],
    )


def normalize_trading_symbol(record: Dict[str, Any]) -> str:
    """
    Normalize trading-symbol field.
    """
    return first_non_empty(
        record,
        [
            "pTrdSymbol",
            "tradingSymbol",
            "display_symbol",
            "ts",
            "symbol",
        ],
    ).upper().strip()


def normalize_exchange_segment(record: Dict[str, Any]) -> str:
    """
    Normalize exchange segment.
    """
    return first_non_empty(
        record,
        [
            "pExchSeg",
            "exchange_segment",
            "exchange",
            "segment",
        ],
    ).lower().strip()


def normalize_instrument_type(record: Dict[str, Any]) -> str:
    """
    Normalize instrument type.
    """
    return first_non_empty(
        record,
        [
            "pInstType",
            "instrument_type",
            "inst_type",
        ],
    ).upper().strip()


def normalize_option_type(record: Dict[str, Any]) -> str:
    """
    Normalize CE / PE.
    """
    value = first_non_empty(
        record,
        [
            "pOptionType",
            "option_type",
            "optionType",
        ],
    ).upper().strip()

    if value in ("CE", "CALL"):
        return "CE"

    if value in ("PE", "PUT"):
        return "PE"

    return value


# ============================================================================
# TOTP / AUTH HELPERS
# ============================================================================

def generate_live_totp(secret_or_otp: str) -> str:
    """
    Accept either:
    - current 6-digit OTP
    - Base32 TOTP secret
    """
    raw = str(secret_or_otp or "").strip().replace(" ", "").upper()

    if raw.isdigit() and len(raw) == 6:
        return raw

    try:
        if len(raw) % 8:
            raw += "=" * (8 - len(raw) % 8)

        key = base64.b32decode(raw, casefold=True)

        counter = int(time.time() // 30)
        msg = struct.pack(">Q", counter)

        digest = hmac.new(
            key,
            msg,
            hashlib.sha1
        ).digest()

        offset = digest[19] & 15

        token = (
            struct.unpack(
                ">I",
                digest[offset:offset + 4]
            )[0]
            & 0x7fffffff
        ) % 1000000

        return f"{token:06d}"

    except Exception:
        return raw


def normalize_kotak_mobile(value: str) -> str:
    """
    Normalize Indian mobile number to +91XXXXXXXXXX.
    """
    raw = str(value or "").strip()

    if not raw:
        return ""

    digits = "".join(
        ch for ch in raw
        if ch.isdigit()
    )

    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]

    if len(digits) != 10:
        return raw

    return "+91" + digits


def env_or_secret(name: str, default: str = "") -> str:
    """
    Environment variable first, Streamlit secrets second.
    """
    val = os.getenv(name, "")

    if val:
        return val

    if st is not None:
        try:
            val = st.secrets.get(name, "")

            if val:
                return str(val)

        except Exception:
            pass

    return default


# ============================================================================
# EXPIRY PARSING
# ============================================================================

def parse_date_string(value: Any) -> Optional[date]:
    """
    Parse common date formats returned by broker APIs.
    """
    text = safe_str(value)

    if not text:
        return None

    text = text.strip()

    # Remove trailing punctuation.
    text = text.replace(";", "").strip()

    # Direct ISO/date parsing.
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d/%b/%Y",
        "%d/%b/%y",
        "%d %b %Y",
        "%d %b %y",
        "%d-%B-%Y",
        "%d-%B-%y",
        "%d %B %Y",
        "%d %B %y",
        "%Y%m%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            pass

    # ISO timestamp.
    try:
        iso_text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(iso_text).date()
    except Exception:
        pass

    # Numeric epoch-like value.
    numeric = normalize_number(text)

    if numeric is not None:

        # Normal Unix epoch seconds.
        try:
            if 1_000_000_000 <= numeric <= 3_000_000_000:
                return datetime.fromtimestamp(
                    numeric,
                    tz=timezone.utc
                ).date()
        except Exception:
            pass

        # Kotak Scrip Master has historically exposed expiry fields
        # with non-standard/legacy epoch offsets in some payload versions.
        #
        # Rather than blindly assuming a single epoch, test several
        # plausible interpretations and accept only realistic dates.
        candidates: List[date] = []

        offsets = [
            0,
            315532800,       # 1980 -> Unix conversion family
            315619200,
            946684800,
        ]

        for offset in offsets:
            try:
                seconds = numeric + offset

                if 0 < seconds < 5_000_000_000:
                    d = datetime.fromtimestamp(
                        seconds,
                        tz=timezone.utc
                    ).date()

                    if (
                        date(2010, 1, 1)
                        <= d
                        <= date(2100, 12, 31)
                    ):
                        candidates.append(d)

            except Exception:
                pass

        if candidates:
            # Prefer a date nearest to today.
            today = today_ist()

            return min(
                candidates,
                key=lambda d: abs(
                    (d - today).days
                )
            )

    return None


def extract_expiry(record: Dict[str, Any]) -> Optional[date]:
    """
    Extract expiry from every commonly observed Kotak field.

    Priority:
    pExpiryDate
    lExpiryDate
    pMaturityDate
    pLastTradingDate
    lExpiryDate variants
    """
    fields = [
        "pExpiryDate",
        "lExpiryDate",
        "pMaturityDate",
        "pLastTradingDate",
        "expiry",
        "expiry_date",
        "expiryDate",
    ]

    for field in fields:

        if field not in record:
            continue

        parsed = parse_date_string(
            record.get(field)
        )

        if parsed:
            return parsed

    # Last-resort attempt:
    # infer expiry from common NIFTY trading symbol formats.
    symbol = normalize_trading_symbol(record)

    if symbol:

        # Example family:
        # NIFTY28AUG26FUT
        # NIFTY28AUG2625000CE
        match = re.search(
            r"NIFTY(\d{1,2})([A-Z]{3})(\d{2,4})",
            symbol
        )

        if match:
            day_text = match.group(1)
            month_text = match.group(2)
            year_text = match.group(3)

            try:
                day_num = int(day_text)

                if len(year_text) == 2:
                    year_num = 2000 + int(year_text)
                else:
                    year_num = int(year_text)

                month_num = datetime.strptime(
                    month_text,
                    "%b"
                ).month

                return date(
                    year_num,
                    month_num,
                    day_num
                )

            except Exception:
                pass

    return None


# ============================================================================
# STRIKE PARSING
# ============================================================================

def extract_strike(record: Dict[str, Any]) -> Optional[float]:
    """
    Extract option strike.

    Kotak payloads may expose:
        dStrikePrice;
        dStrikePrice
        strike_price
        strikePrice

    Some payloads may encode the strike in a scaled form.
    """
    possible_fields = [
        "dStrikePrice;",
        "dStrikePrice",
        "strike_price",
        "strikePrice",
        "strike",
    ]

    for field in possible_fields:

        if field not in record:
            continue

        value = normalize_number(
            record.get(field)
        )

        if value is None:
            continue

        if value <= 0:
            continue

        # Raw Scrip Master payloads have historically appeared
        # with scaled strike representations.
        #
        # NIFTY strikes are normally around tens of thousands.
        if value > 1_000_000:
            value = value / 100.0

        elif value > 100_000:
            value = value / 100.0

        return float(value)

    # Symbol fallback.
    symbol = normalize_trading_symbol(record)

    if not symbol:
        return None

    option_type = normalize_option_type(record)

    if option_type not in ("CE", "PE"):
        if symbol.endswith("CE"):
            option_type = "CE"
        elif symbol.endswith("PE"):
            option_type = "PE"

    if option_type in ("CE", "PE"):

        stripped = symbol[:-2]

        # Find trailing numeric portion.
        match = re.search(
            r"(\d+(?:\.\d+)?)$",
            stripped
        )

        if match:
            try:
                value = float(
                    match.group(1)
                )

                if value > 0:
                    return value

            except Exception:
                pass

    return None


# ============================================================================
# RECORD EXTRACTION
# ============================================================================

def extract_records(response: Any) -> List[Dict[str, Any]]:
    """
    Normalize all common Kotak API response shapes.

    Handles:
    - list[dict]
    - {"result": [...]}
    - {"data": [...]}
    - {"values": [...]}
    - {"result": {"data": [...]}}
    """
    if response is None:
        return []

    if isinstance(response, list):
        return [
            x for x in response
            if isinstance(x, dict)
        ]

    if isinstance(response, dict):

        for key in (
            "result",
            "data",
            "values",
            "records",
            "scrips",
        ):

            value = response.get(key)

            if isinstance(value, list):
                return [
                    x for x in value
                    if isinstance(x, dict)
                ]

            if isinstance(value, dict):

                nested = extract_records(
                    value
                )

                if nested:
                    return nested

    return []


# ============================================================================
# KOTAK CONNECTOR
# ============================================================================

class KotakConnector:

    def __init__(self):

        self.consumer_key = env_or_secret(
            "KOTAK_CONSUMER_KEY"
        )

        self.mobile = normalize_kotak_mobile(
            env_or_secret("KOTAK_MOBILE")
        )

        self.ucc = env_or_secret(
            "KOTAK_UCC"
        )

        self.totp_secret = env_or_secret(
            "KOTAK_TOTP"
        )

        self.mpin = env_or_secret(
            "KOTAK_MPIN"
        )

        self.client = None
        self.connected = False

        # IMPORTANT:
        # This must be an actual NIFTY FUT token.
        # Never use 26000 here.
        self.future_token: Optional[str] = None
        self.future_symbol: Optional[str] = None
        self.future_expiry: Optional[date] = None

        # NIFTY spot/index.
        self.spot_token = NIFTY_INDEX_TOKEN

        # Option tokens.
        self.pcr_tokens: List[str] = []

        # Keep metadata for publishing.
        self.option_contracts: Dict[str, Dict[str, Any]] = {}

        # Discovery state.
        self.nfo_records: List[Dict[str, Any]] = []

        self.logs: List[str] = []

        self.heavy_tokens = {
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

    # ------------------------------------------------------------------------
    # LOGGING
    # ------------------------------------------------------------------------

    def log(self, message: str):
        timestamp = now_ist().strftime(
            "%H:%M:%S"
        )

        self.logs.append(
            f"[{timestamp}] {message}"
        )

        # Keep memory bounded.
        self.logs = self.logs[-100:]

    # ------------------------------------------------------------------------
    # LOGIN
    # ------------------------------------------------------------------------

    def login(
        self,
        totp_override: str = ""
    ) -> bool:

        if NeoAPI is None:
            raise RuntimeError(
                "neo_api_client library is not installed."
            )

        totp = (
            totp_override.strip()
            or self.totp_secret
        )

        missing = []

        if not self.consumer_key:
            missing.append(
                "KOTAK_CONSUMER_KEY"
            )

        if not self.mobile:
            missing.append(
                "KOTAK_MOBILE"
            )

        if not self.ucc:
            missing.append(
                "KOTAK_UCC"
            )

        if not totp:
            missing.append(
                "KOTAK_TOTP"
            )

        if not self.mpin:
            missing.append(
                "KOTAK_MPIN"
            )

        if missing:
            raise RuntimeError(
                "Missing Kotak Neo authentication credentials: "
                + ", ".join(missing)
            )

        self.client = NeoAPI(
            environment=CONFIG[
                "neo_environment"
            ],
            consumer_key=self.consumer_key,
        )

        step1 = self.client.totp_login(
            mobile_number=self.mobile,
            ucc=self.ucc,
            totp=generate_live_totp(totp),
        )

        if isinstance(step1, dict):

            if step1.get("error"):
                raise RuntimeError(
                    f"Login Step 1 Error: "
                    f"{step1['error']}"
                )

            if step1.get("Error"):
                raise RuntimeError(
                    f"Login Step 1 Error: "
                    f"{step1['Error']}"
                )

        step2 = self.client.totp_validate(
            mpin=self.mpin
        )

        if isinstance(step2, dict):

            if step2.get("error"):
                raise RuntimeError(
                    f"Login Step 2 Error: "
                    f"{step2['error']}"
                )

            if step2.get("Error"):
                raise RuntimeError(
                    f"Login Step 2 Error: "
                    f"{step2['Error']}"
                )

        self.connected = True

        self.log(
            "Kotak authentication successful."
        )

        return True

    # ------------------------------------------------------------------------
    # GET NFO MASTER
    # ------------------------------------------------------------------------

    def load_nfo_scrip_master(
        self
    ) -> List[Dict[str, Any]]:

        if not self.connected:
            raise RuntimeError(
                "Kotak connector is not authenticated."
            )

        # Primary method:
        # search_scrip() returns NFO records in the current SDK family.
        records: List[Dict[str, Any]] = []

        try:

            response = self.client.search_scrip(
                exchange_segment=NIFTY_FO_SEGMENT,
                symbol="NIFTY",
            )

            records = extract_records(
                response
            )

            self.log(
                "search_scrip(NIFTY) returned "
                f"{len(records)} structured records."
            )

        except Exception as exc:

            self.log(
                "Primary NIFTY search_scrip failed: "
                f"{exc}"
            )

        # Secondary capitalization attempt.
        if not records:

            try:

                response = self.client.search_scrip(
                    exchange_segment=NIFTY_FO_SEGMENT,
                    symbol="Nifty",
                )

                records = extract_records(
                    response
                )

                self.log(
                    "Secondary Nifty search returned "
                    f"{len(records)} structured records."
                )

            except Exception as exc:

                self.log(
                    "Secondary search_scrip failed: "
                    f"{exc}"
                )

        if not records:
            raise RuntimeError(
                "Kotak returned no structured NFO "
                "scrip records."
            )

        self.nfo_records = records

        self.log(
            f"Total raw scrip records retrieved: "
            f"{len(records)}"
        )

        return records

    # ------------------------------------------------------------------------
    # FUTURE DETECTION
    # ------------------------------------------------------------------------

    def find_active_nifty_future(
        self,
        records: List[Dict[str, Any]]
    ) -> Tuple[str, str, Optional[date]]:

        today = today_ist()

        candidates = []

        for record in records:

            if not isinstance(record, dict):
                continue

            segment = normalize_exchange_segment(
                record
            )

            if segment and segment != NIFTY_FO_SEGMENT:
                continue

            symbol = normalize_trading_symbol(
                record
            )

            token = normalize_token(
                record
            )

            inst_type = normalize_instrument_type(
                record
            )

            if not token or not symbol:
                continue

            # Absolutely do not accept the NIFTY index token.
            if token == NIFTY_INDEX_TOKEN:
                continue

            # Exclude options.
            option_type = normalize_option_type(
                record
            )

            if option_type in ("CE", "PE"):
                continue

            if symbol.endswith("CE") or symbol.endswith("PE"):
                continue

            # Must be NIFTY.
            if "NIFTY" not in symbol:
                continue

            # Exclude other NIFTY-family indices.
            excluded = (
                "BANKNIFTY",
                "FINNIFTY",
                "MIDCPNIFTY",
                "NIFTYNXT",
                "SENSEX",
            )

            if any(
                x in symbol
                for x in excluded
            ):
                continue

            # Strong instrument-type test.
            is_future = (
                "FUT" in inst_type
                or "FUTIDX" in inst_type
                or symbol.endswith("FUT")
                or "FUT" in symbol
            )

            if not is_future:
                continue

            expiry = extract_expiry(
                record
            )

            # Expiry should be in future.
            if expiry is not None:

                if expiry < today:
                    continue

            candidates.append(
                (
                    expiry,
                    symbol,
                    token,
                    record,
                )
            )

        if not candidates:
            raise RuntimeError(
                "Active NIFTY future contract could "
                "not be discovered from NFO scrip "
                "master. No valid NIFTY FUTIDX/FUT "
                "record matched the parser."
            )

        # Prefer nearest future expiry.
        candidates.sort(
            key=lambda x: (
                x[0] is None,
                x[0] if x[0] else date.max,
                x[1],
            )
        )

        expiry, symbol, token, record = (
            candidates[0]
        )

        return (
            token,
            symbol,
            expiry,
        )

    # ------------------------------------------------------------------------
    # SPOT PRICE
    # ------------------------------------------------------------------------

    def get_nifty_spot(
        self
    ) -> float:

        fallback = 24300.0

        if not self.client:
            return fallback

        try:

            response = self.client.quotes(
                instrument_tokens=[
                    {
                        "instrument_token":
                            self.spot_token,
                        "exchange_segment":
                            NIFTY_INDEX_SEGMENT,
                    }
                ],
                quote_type="ltp",
            )

            records = extract_records(
                response
            )

            for record in records:

                if not isinstance(record, dict):
                    continue

                for key in (
                    "ltp",
                    "last_price",
                    "lp",
                    "LTP",
                ):

                    value = normalize_number(
                        record.get(key)
                    )

                    if (
                        value is not None
                        and value > 0
                    ):
                        return float(value)

        except Exception as exc:

            self.log(
                "NIFTY spot quote unavailable; "
                f"using defensive reference only: {exc}"
            )

        return fallback

    # ------------------------------------------------------------------------
    # TARGET STRIKES
    # ------------------------------------------------------------------------

    def build_target_strikes(
        self,
        spot_price: float
    ) -> List[float]:

        step = float(
            CONFIG["pcr_strike_step"]
        )

        count = int(
            CONFIG["pcr_strike_count"]
        )

        atm = round(
            spot_price / step
        ) * step

        strikes = []

        for i in range(
            -count,
            count + 1
        ):

            strike = atm + (
                i * step
            )

            strikes.append(
                round(strike, 2)
            )

        return strikes

    # ------------------------------------------------------------------------
    # OPTION DISCOVERY FROM BROAD MASTER
    # ------------------------------------------------------------------------

    def discover_options_from_master(
        self,
        records: List[Dict[str, Any]],
        target_strikes: List[float],
        target_expiry: Optional[date],
    ) -> Dict[str, Dict[str, Any]]:

        discovered: Dict[
            str,
            Dict[str, Any]
        ] = {}

        target_set = {
            round(float(x), 2)
            for x in target_strikes
        }

        for record in records:

            if not isinstance(record, dict):
                continue

            segment = normalize_exchange_segment(
                record
            )

            if (
                segment
                and segment != NIFTY_FO_SEGMENT
            ):
                continue

            symbol = normalize_trading_symbol(
                record
            )

            token = normalize_token(
                record
            )

            if not symbol or not token:
                continue

            if token == NIFTY_INDEX_TOKEN:
                continue

            if "NIFTY" not in symbol:
                continue

            # Exclude other index families.
            excluded = (
                "BANKNIFTY",
                "FINNIFTY",
                "MIDCPNIFTY",
                "NIFTYNXT",
                "SENSEX",
            )

            if any(
                x in symbol
                for x in excluded
            ):
                continue

            option_type = normalize_option_type(
                record
            )

            if option_type not in (
                "CE",
                "PE",
            ):

                if symbol.endswith("CE"):
                    option_type = "CE"

                elif symbol.endswith("PE"):
                    option_type = "PE"

            if option_type not in (
                "CE",
                "PE",
            ):
                continue

            expiry = extract_expiry(
                record
            )

            # If target expiry is known, enforce it.
            if (
                target_expiry is not None
                and expiry is not None
                and expiry != target_expiry
            ):
                continue

            strike = extract_strike(
                record
            )

            if strike is None:
                continue

            # Normalize strike.
            strike = round(
                float(strike),
                2
            )

            # Exact match first.
            if strike not in target_set:

                # Defensive tolerance for floating-point/scaled
                # representations.
                matched = None

                for target in target_set:

                    if abs(
                        strike - target
                    ) < 0.01:

                        matched = target
                        break

                if matched is None:
                    continue

                strike = matched

            key = (
                f"{option_type}:"
                f"{strike:.2f}"
            )

            # If duplicate records exist, retain the first
            # valid token.
            if key not in discovered:

                discovered[key] = {
                    "token": token,
                    "symbol": symbol,
                    "option_type":
                        option_type,
                    "strike": strike,
                    "expiry": (
                        expiry.isoformat()
                        if expiry
                        else None
                    ),
                    "exchange_segment":
                        NIFTY_FO_SEGMENT,
                    "raw_record": record,
                }

        return discovered

    # ------------------------------------------------------------------------
    # TARGETED OPTION FALLBACK
    # ------------------------------------------------------------------------

    def targeted_option_search(
        self,
        target_strikes: List[float],
        target_expiry: Optional[date],
    ) -> Dict[str, Dict[str, Any]]:

        discovered: Dict[
            str,
            Dict[str, Any]
        ] = {}

        if not self.client:
            return discovered

        # search_scrip() officially accepts:
        # exchange_segment, symbol, expiry,
        # option_type, strike_price.
        #
        # Different SDK builds can expect slightly different
        # expiry string representations. We therefore try several
        # safe formats.

        expiry_formats = []

        if target_expiry:

            expiry_formats = [
                target_expiry.strftime(
                    "%d%b%Y"
                ).upper(),

                target_expiry.strftime(
                    "%d-%b-%Y"
                ).upper(),

                target_expiry.strftime(
                    "%d/%b/%Y"
                ).upper(),

                target_expiry.strftime(
                    "%Y-%m-%d"
                ),
            ]

        else:
            expiry_formats = [""]

        for strike in target_strikes:

            for option_type in (
                "CE",
                "PE",
            ):

                found = False

                for expiry_text in expiry_formats:

                    try:

                        response = (
                            self.client.search_scrip(
                                exchange_segment=
                                    NIFTY_FO_SEGMENT,
                                symbol="NIFTY",
                                expiry=expiry_text,
                                option_type=
                                    option_type,
                                strike_price=str(
                                    int(strike)
                                ),
                            )
                        )

                        records = extract_records(
                            response
                        )

                        for record in records:

                            if not isinstance(
                                record,
                                dict
                            ):
                                continue

                            token = normalize_token(
                                record
                            )

                            symbol = (
                                normalize_trading_symbol(
                                    record
                                )
                            )

                            if not token or not symbol:
                                continue

                            if token == NIFTY_INDEX_TOKEN:
                                continue

                            actual_type = (
                                normalize_option_type(
                                    record
                                )
                            )

                            if (
                                actual_type
                                not in (
                                    "CE",
                                    "PE",
                                )
                            ):

                                if symbol.endswith(
                                    "CE"
                                ):
                                    actual_type = "CE"

                                elif symbol.endswith(
                                    "PE"
                                ):
                                    actual_type = "PE"

                            if actual_type != option_type:
                                continue

                            actual_strike = (
                                extract_strike(
                                    record
                                )
                            )

                            if (
                                actual_strike is not None
                                and abs(
                                    actual_strike
                                    - strike
                                ) > 0.01
                            ):
                                continue

                            actual_expiry = (
                                extract_expiry(
                                    record
                                )
                            )

                            if (
                                target_expiry
                                and actual_expiry
                                and actual_expiry
                                != target_expiry
                            ):
                                continue

                            key = (
                                f"{option_type}:"
                                f"{strike:.2f}"
                            )

                            discovered[key] = {
                                "token": token,
                                "symbol": symbol,
                                "option_type":
                                    option_type,
                                "strike": strike,
                                "expiry": (
                                    actual_expiry.isoformat()
                                    if actual_expiry
                                    else (
                                        target_expiry.isoformat()
                                        if target_expiry
                                        else None
                                    )
                                ),
                                "exchange_segment":
                                    NIFTY_FO_SEGMENT,
                                "raw_record": record,
                            }

                            found = True
                            break

                        if found:
                            break

                    except Exception as exc:

                        self.log(
                            "Targeted option search "
                            f"{option_type} {strike} "
                            f"expiry={expiry_text or 'AUTO'} "
                            f"warning: {exc}"
                        )

        return discovered

    # ------------------------------------------------------------------------
    # FULL DISCOVERY
    # ------------------------------------------------------------------------

    def discover_instruments(self) -> bool:

        if not self.connected or not self.client:
            raise RuntimeError(
                "Kotak connector is not authenticated."
            )

        self.logs.clear()

        # Reset old state.
        self.future_token = None
        self.future_symbol = None
        self.future_expiry = None
        self.pcr_tokens = []
        self.option_contracts = {}

        # ------------------------------------------------------------
        # STEP 1: NFO MASTER
        # ------------------------------------------------------------

        records = self.load_nfo_scrip_master()

        # ------------------------------------------------------------
        # STEP 2: FUTURE
        # ------------------------------------------------------------

        (
            future_token,
            future_symbol,
            future_expiry,
        ) = self.find_active_nifty_future(
            records
        )

        # IMPORTANT:
        # We explicitly reject 26000.
        if future_token == NIFTY_INDEX_TOKEN:
            raise RuntimeError(
                "Parser attempted to bind NIFTY index "
                "token 26000 as a Future. Discovery "
                "was rejected."
            )

        self.future_token = future_token
        self.future_symbol = future_symbol
        self.future_expiry = future_expiry

        expiry_text = (
            future_expiry.isoformat()
            if future_expiry
            else "UNKNOWN"
        )

        self.log(
            "Bound Active Nifty Future: "
            f"{future_symbol} "
            f"(Token: {future_token}, "
            f"Expiry: {expiry_text})"
        )

        # ------------------------------------------------------------
        # STEP 3: NIFTY SPOT
        # ------------------------------------------------------------

        spot_price = self.get_nifty_spot()

        self.log(
            f"NIFTY spot reference: "
            f"{spot_price:.2f}"
        )

        # ------------------------------------------------------------
        # STEP 4: TARGET STRIKES
        # ------------------------------------------------------------

        target_strikes = (
            self.build_target_strikes(
                spot_price
            )
        )

        self.log(
            "Target option strikes: "
            + ", ".join(
                f"{x:.0f}"
                for x in target_strikes
            )
        )

        # ------------------------------------------------------------
        # STEP 5: FIND NEAREST VALID OPTION EXPIRY
        # ------------------------------------------------------------

        today = today_ist()

        option_expiries = set()

        for record in records:

            if not isinstance(record, dict):
                continue

            symbol = normalize_trading_symbol(
                record
            )

            if "NIFTY" not in symbol:
                continue

            if (
                "BANKNIFTY" in symbol
                or "FINNIFTY" in symbol
                or "MIDCPNIFTY" in symbol
                or "NIFTYNXT" in symbol
            ):
                continue

            option_type = normalize_option_type(
                record
            )

            if option_type not in (
                "CE",
                "PE",
            ):

                if symbol.endswith("CE"):
                    option_type = "CE"

                elif symbol.endswith("PE"):
                    option_type = "PE"

            if option_type not in (
                "CE",
                "PE",
            ):
                continue

            expiry = extract_expiry(
                record
            )

            if expiry and expiry >= today:
                option_expiries.add(
                    expiry
                )

        target_option_expiry = None

        if option_expiries:
            target_option_expiry = min(
                option_expiries
            )

        self.log(
            "Selected NIFTY option expiry: "
            + (
                target_option_expiry.isoformat()
                if target_option_expiry
                else "NOT PARSED"
            )
        )

        # ------------------------------------------------------------
        # STEP 6: DISCOVER OPTIONS DIRECTLY FROM MASTER
        # ------------------------------------------------------------

        discovered = (
            self.discover_options_from_master(
                records=records,
                target_strikes=target_strikes,
                target_expiry=
                    target_option_expiry,
            )
        )

        self.log(
            "Master-scan option matches: "
            f"{len(discovered)}"
        )

        # ------------------------------------------------------------
        # STEP 7: TARGETED SEARCH IF MASTER SCAN IS INCOMPLETE
        # ------------------------------------------------------------

        expected_count = (
            len(target_strikes) * 2
        )

        if len(discovered) < expected_count:

            self.log(
                "Option master scan incomplete "
                f"({len(discovered)}/{expected_count}). "
                "Starting targeted search_scrip fallback."
            )

            targeted = (
                self.targeted_option_search(
                    target_strikes=
                        target_strikes,
                    target_expiry=
                        target_option_expiry,
                )
            )

            for key, value in targeted.items():

                if key not in discovered:
                    discovered[key] = value

            self.log(
                "After targeted option discovery: "
                f"{len(discovered)}/{expected_count}"
            )

        # ------------------------------------------------------------
        # STEP 8: FINAL TOKEN LIST
        # ------------------------------------------------------------

        self.option_contracts = discovered

        self.pcr_tokens = sorted(
            {
                str(
                    item["token"]
                )
                for item in discovered.values()
                if item.get("token")
            }
        )

        # ------------------------------------------------------------
        # STEP 9: LOG CONTRACTS
        # ------------------------------------------------------------

        if discovered:

            ordered = sorted(
                discovered.items(),
                key=lambda x: (
                    float(
                        x[1].get(
                            "strike",
                            0
                        )
                    ),
                    x[1].get(
                        "option_type",
                        ""
                    ),
                ),
            )

            for key, item in ordered:

                self.log(
                    "Mapped option: "
                    f"{item.get('symbol')} "
                    f"| Token={item.get('token')} "
                    f"| Strike={item.get('strike')} "
                    f"| Type={item.get('option_type')}"
                )

        # ------------------------------------------------------------
        # STEP 10: FINAL VALIDATION
        # ------------------------------------------------------------

        if not self.future_token:
            raise RuntimeError(
                "Discovery failed: no active "
                "NIFTY Future token."
            )

        if self.future_token == NIFTY_INDEX_TOKEN:
            raise RuntimeError(
                "Discovery safety gate failed: "
                "NIFTY index token 26000 was selected "
                "as Future."
            )

        if not self.pcr_tokens:
            self.log(
                "WARNING: zero NIFTY option tokens "
                "were mapped."
            )

        self.log(
            "Discovery complete: "
            f"Future={self.future_token}, "
            f"Options={len(self.pcr_tokens)}"
        )

        return True

    # ------------------------------------------------------------------------
    # RAW QUOTES
    # ------------------------------------------------------------------------

    def fetch_raw_quotes(
        self
    ) -> List[Dict[str, Any]]:

        if not self.connected or not self.client:
            return []

        tokens_to_poll = [
            {
                "instrument_token":
                    self.spot_token,
                "exchange_segment":
                    NIFTY_INDEX_SEGMENT,
            }
        ]

        # Active Future.
        if self.future_token:

            tokens_to_poll.append(
                {
                    "instrument_token":
                        str(self.future_token),
                    "exchange_segment":
                        NIFTY_FO_SEGMENT,
                }
            )

        # Heavyweights.
        for symbol, token in (
            self.heavy_tokens.items()
        ):

            tokens_to_poll.append(
                {
                    "instrument_token":
                        str(token),
                    "exchange_segment":
                        "nse_cm",
                }
            )

        # Options.
        for token in self.pcr_tokens:

            tokens_to_poll.append(
                {
                    "instrument_token":
                        str(token),
                    "exchange_segment":
                        NIFTY_FO_SEGMENT,
                }
            )

        if not tokens_to_poll:
            return []

        try:

            response = self.client.quotes(
                instrument_tokens=
                    tokens_to_poll,
                quote_type="all",
            )

            records = extract_records(
                response
            )

            return records

        except Exception as exc:

            self.log(
                f"Quote fetch error: {exc}"
            )

            return []


# ============================================================================
# YAHOO CONNECTOR
# ============================================================================

class YahooConnector:

    @staticmethod
    def fetch_macro_data(
        tickers: List[str] = None
    ) -> Dict[str, pd.DataFrame]:

        if tickers is None:
            tickers = [
                "GC=F",
                "SI=F",
                "DX-Y.NYB",
                "^GSPC",
            ]

        data_map: Dict[
            str,
            pd.DataFrame
        ] = {}

        try:

            raw_data = yf.download(
                tickers,
                period="5d",
                interval="1d",
                progress=False,
                group_by="ticker",
                auto_adjust=False,
            )

            for ticker in tickers:

                try:

                    if len(tickers) == 1:

                        df = raw_data

                    else:

                        if (
                            isinstance(
                                raw_data.columns,
                                pd.MultiIndex
                            )
                            and ticker
                            in raw_data.columns
                        ):
                            df = raw_data[ticker]

                        else:
                            df = pd.DataFrame()

                    if (
                        isinstance(df, pd.DataFrame)
                        and not df.empty
                    ):
                        data_map[ticker] = df

                except Exception:
                    continue

        except Exception as exc:

            print(
                f"Yahoo fetch error: {exc}"
            )

        return data_map


# ============================================================================
# SUPABASE PUBLISHER
# ============================================================================

class SupabasePublisher:

    def __init__(self):

        self.url = env_or_secret(
            "SUPABASE_URL",
            CONFIG["supabase_url"],
        )

        self.key = env_or_secret(
            "SUPABASE_KEY",
            CONFIG["supabase_key"],
        )

    def publish_observation(
        self,
        source: str,
        symbol: str,
        token: str,
        raw_payload: dict,
    ) -> bool:

        if not self.url or not self.key:
            return False

        try:

            endpoint = (
                f"{self.url.rstrip('/')}"
                "/rest/v1/raw_observations"
            )

            headers = {
                "apikey": self.key,
                "Authorization":
                    f"Bearer {self.key}",
                "Content-Type":
                    "application/json",
                "Prefer":
                    "return=minimal",
            }

            record = {
                "source": source,
                "symbol": symbol,
                "instrument_token":
                    str(token),
                "observation_timestamp":
                    now_ist().isoformat(),
                "raw": raw_payload,
            }

            response = requests.post(
                endpoint,
                headers=headers,
                json=record,
                timeout=5,
            )

            if response.status_code in (
                200,
                201,
                204,
            ):
                return True

            print(
                "Supabase publish failed: "
                f"{response.status_code} "
                f"{response.text[:500]}"
            )

            return False

        except Exception as exc:

            print(
                f"Supabase publish error: {exc}"
            )

            return False


# ============================================================================
# STREAMLIT UI
# ============================================================================

def render_discovery_summary(
    kotak: KotakConnector
):

    st.subheader(
        "Discovery Status"
    )

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "NIFTY Future Token",
        kotak.future_token
        if kotak.future_token
        else "NOT DISCOVERED",
    )

    c2.metric(
        "Mapped Options",
        len(kotak.pcr_tokens),
    )

    c3.metric(
        "NFO Records",
        len(kotak.nfo_records),
    )

    if kotak.future_symbol:

        expiry_text = (
            kotak.future_expiry.isoformat()
            if kotak.future_expiry
            else "UNKNOWN"
        )

        st.caption(
            f"Active Future: "
            f"{kotak.future_symbol} | "
            f"Expiry: {expiry_text}"
        )

    if kotak.option_contracts:

        rows = []

        for item in sorted(
            kotak.option_contracts.values(),
            key=lambda x: (
                float(
                    x.get(
                        "strike",
                        0
                    )
                ),
                x.get(
                    "option_type",
                    "",
                ),
            ),
        ):

            rows.append(
                {
                    "Symbol":
                        item.get(
                            "symbol",
                            "",
                        ),
                    "Token":
                        item.get(
                            "token",
                            "",
                        ),
                    "Strike":
                        item.get(
                            "strike",
                            "",
                        ),
                    "Type":
                        item.get(
                            "option_type",
                            "",
                        ),
                    "Expiry":
                        item.get(
                            "expiry",
                            "",
                        ),
                }
            )

        if rows:

            st.dataframe(
                pd.DataFrame(rows),
                use_container_width=True,
                hide_index=True,
            )


# ============================================================================
# MAIN
# ============================================================================

def main():

    if st is None:

        print(
            "Streamlit not available."
        )

        return

    st.set_page_config(
        page_title=
            "Institutional Raw Data Producer Bus",
        layout="wide",
    )

    st.title(
        "📡 Institutional Raw Data Producer Bus"
    )

    # ------------------------------------------------------------------------
    # SESSION STATE
    # ------------------------------------------------------------------------

    if "kotak" not in st.session_state:
        st.session_state.kotak = (
            KotakConnector()
        )

    if "producer_running" not in st.session_state:
        st.session_state.producer_running = False

    kotak: KotakConnector = (
        st.session_state.kotak
    )

    supabase = SupabasePublisher()

    # ------------------------------------------------------------------------
    # SIDEBAR
    # ------------------------------------------------------------------------

    with st.sidebar:

        st.header(
            "🔑 Authentication"
        )

        totp_input = st.text_input(
            "Live TOTP Code",
            type="password",
        )

        col1, col2 = st.columns(2)

        with col1:

            if st.button(
                "Connect Kotak"
            ):

                try:

                    with st.spinner(
                        "Authenticating..."
                    ):

                        kotak.login(
                            totp_override=
                                totp_input
                        )

                    st.success(
                        "Authenticated Successfully!"
                    )

                except Exception as exc:

                    st.error(
                        str(exc)
                    )

        with col2:

            if st.button(
                "Discover Instruments",
                disabled=
                    not kotak.connected,
            ):

                try:

                    with st.spinner(
                        "Downloading NFO "
                        "scrip master and "
                        "discovering contracts..."
                    ):

                        kotak.discover_instruments()

                    st.success(
                        "Discovery Complete!"
                    )

                except Exception as exc:

                    st.error(
                        str(exc)
                    )

        st.markdown("---")

        st.header(
            "🚀 Producer Control"
        )

        can_start = (
            kotak.connected
            and kotak.future_token
        )

        if not st.session_state.producer_running:

            if st.button(
                "Start Raw Producer Loop",
                type="primary",
                disabled=not can_start,
            ):

                st.session_state.producer_running = True

                st.rerun()

        else:

            if st.button(
                "Stop Producer Loop",
                type="secondary",
            ):

                st.session_state.producer_running = False

                st.rerun()

        st.markdown("---")

        st.caption(
            "Raw-only architecture. "
            "No indicators, alpha, ML or "
            "trading decisions."
        )

    # ------------------------------------------------------------------------
    # TOP METRICS
    # ------------------------------------------------------------------------

    col_s1, col_s2, col_s3, col_s4 = (
        st.columns(4)
    )

    col_s1.metric(
        "Kotak Connection",
        (
            "CONNECTED"
            if kotak.connected
            else "DISCONNECTED"
        ),
    )

    col_s2.metric(
        "Active Future Token",
        (
            kotak.future_token
            if kotak.future_token
            else "NOT DISCOVERED"
        ),
    )

    col_s3.metric(
        "Mapped Options Count",
        len(kotak.pcr_tokens),
    )

    col_s4.metric(
        "NFO Records",
        len(kotak.nfo_records),
    )

    # ------------------------------------------------------------------------
    # DISCOVERY DETAILS
    # ------------------------------------------------------------------------

    if kotak.future_symbol:

        expiry_text = (
            kotak.future_expiry.isoformat()
            if kotak.future_expiry
            else "UNKNOWN"
        )

        st.info(
            f"Active NIFTY Future: "
            f"{kotak.future_symbol} | "
            f"Token: {kotak.future_token} | "
            f"Expiry: {expiry_text}"
        )

    if kotak.logs:

        with st.expander(
            "Discovery & Execution Logs",
            expanded=True,
        ):

            for log in kotak.logs[-30:]:

                st.text(log)

    # ------------------------------------------------------------------------
    # DISCOVERY TABLE
    # ------------------------------------------------------------------------

    if kotak.option_contracts:

        with st.expander(
            "Mapped NIFTY Option Contracts",
            expanded=False,
        ):

            render_discovery_summary(
                kotak
            )

    # ------------------------------------------------------------------------
    # PRODUCER LOOP
    # ------------------------------------------------------------------------

    if st.session_state.producer_running:

        st.success(
            "🟢 Raw Producer is active. "
            "Polling broker quotes and "
            "publishing to Supabase "
            "`raw_observations`..."
        )

        status_container = st.empty()

        log_container = st.empty()

        poll_cycle = 0

        while (
            st.session_state.producer_running
        ):

            try:

                raw_quotes = (
                    kotak.fetch_raw_quotes()
                )

                published_count = 0

                for quote in raw_quotes:

                    if not isinstance(
                        quote,
                        dict,
                    ):
                        continue

                    token = first_non_empty(
                        quote,
                        [
                            "exchange_token",
                            "instrument_token",
                            "pSymbol",
                            "pSymbolToken",
                            "token",
                        ],
                    )

                    symbol = first_non_empty(
                        quote,
                        [
                            "display_symbol",
                            "pTrdSymbol",
                            "tradingSymbol",
                            "symbol",
                        ],
                    )

                    if not token:
                        token = "UNKNOWN"

                    if not symbol:
                        symbol = "UNKNOWN"

                    success = (
                        supabase.publish_observation(
                            source="kotak_live",
                            symbol=symbol,
                            token=token,
                            raw_payload=quote,
                        )
                    )

                    if success:
                        published_count += 1

                # ------------------------------------------------------------
                # YAHOO MACRO RAW DATA
                # ------------------------------------------------------------

                if (
                    poll_cycle
                    % CONFIG[
                        "macro_every_n_cycles"
                    ]
                    == 0
                ):

                    macro_data = (
                        YahooConnector
                        .fetch_macro_data()
                    )

                    for ticker, df in (
                        macro_data.items()
                    ):

                        if df.empty:
                            continue

                        latest_row = (
                            df.iloc[-1]
                            .to_dict()
                        )

                        clean_row = {}

                        for key, value in (
                            latest_row.items()
                        ):

                            if pd.isna(value):
                                clean_row[
                                    str(key)
                                ] = None

                            elif hasattr(
                                value,
                                "item",
                            ):

                                try:
                                    clean_row[
                                        str(key)
                                    ] = value.item()

                                except Exception:
                                    clean_row[
                                        str(key)
                                    ] = str(value)

                            else:

                                clean_row[
                                    str(key)
                                ] = value

                        supabase.publish_observation(
                            source="yahoo_macro",
                            symbol=ticker,
                            token=ticker,
                            raw_payload=clean_row,
                        )

                status_container.info(
                    f"Last Poll: "
                    f"{now_ist().strftime('%H:%M:%S')} "
                    f"| Published "
                    f"{published_count} raw quotes "
                    f"| Options mapped: "
                    f"{len(kotak.pcr_tokens)} "
                    f"| Future: "
                    f"{kotak.future_token}"
                )

                poll_cycle += 1

            except Exception as exc:

                log_container.error(
                    f"Producer loop exception: "
                    f"{exc}"
                )

            time.sleep(
                float(
                    CONFIG[
                        "poll_interval_sec"
                    ]
                )
            )


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
