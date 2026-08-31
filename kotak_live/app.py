#!/usr/bin/env python3
"""
Leak-Proof Raw Data Producer | Institutional Research Bus

- Zero local calculations, zero indicators, zero ML.
- Robust nearest-expiry Nifty Future token discovery & option mapping.
- Publishes raw normalized payloads directly to Supabase `raw_observations` via REST.
- Throttling-safe background loop (no st.rerun abuse).
- Kotak LIVE only.
- Historical/yfinance ingestion is intentionally NOT executed here.
"""

from __future__ import annotations

import os
import re
import json
import time
import base64
import csv
import io
import hmac
import hashlib
import struct
import sys
import signal
import subprocess

from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
import uuid

from concurrent.futures import ThreadPoolExecutor, as_completed


try:
    import streamlit as st
except ImportError:
    st = None


try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None


IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


# ---------------------------------------------------------------------------
# REQUIRED DATA CONTRACT
# ---------------------------------------------------------------------------

REQUIRED_DATA_MATRIX = {
    "NIFTY_3MIN": {
        "realtime": [
            "NIFTY spot OHLC/LTP",
            "nearest valid NIFTY future OHLC/LTP/volume/OI",
            "10 heavyweight quotes",
            "22 current-expiry PCR option contracts",
            "future best bid/ask + bid/ask quantities when supplied by Kotak",
        ],
        "historical": [
            "NIFTY spot daily OHLCV for optional warm-up/context",
        ],
    },
    "NEXT_DAY_ALPHA": {
        "realtime": [
            "shortlisted stock live OHLC/LTP/volume where available",
            "NIFTY/sector reference live raw observations used by existing confirmation",
        ],
        "historical": [
            "NIFTY-500 stocks: 320d daily OHLCV",
            "NIFTY benchmark: 320d daily OHLCV",
            "V7 MTF basket: 320d daily, 180d 1h requested, 55d 15m",
            "India VIX: 320d daily",
        ],
    },
    "GSR": {
        "realtime": [
            "raw OHLC/LTP/volume/OI/bid/ask observations",
            "futures_close/spot_close where the source supplies them",
            "option raw fields where available",
        ],
        "historical": [
            "raw historical OHLCV observations sufficient for replay/warm-up",
        ],
    },
}


# ---------------------------------------------------------------------------
# HISTORICAL LIMIT DOCUMENTATION
# ---------------------------------------------------------------------------
# These values remain part of the data contract/documentation.
# They are NOT executed by this Kotak LIVE producer.

YFINANCE_LIMITS = {
    "intraday_max_days_documented": 60,
    "1h_requested_days_by_v7": 180,
    "1d_requested_days_by_v7": 320,
    "15m_requested_days_by_v7": 55,
}


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

CONFIG = {
    "neo_environment": "prod",

    "pcr_strike_count": 5,
    "pcr_strike_step": 50.0,

    "supabase_url": os.getenv(
        "SUPABASE_URL",
        ""
    ).strip(),

    "supabase_key": os.getenv(
        "SUPABASE_KEY",
        ""
    ).strip(),

    "poll_interval_sec": 3.0,

    # Kept as configuration/documentation only.
    # Historical/macro ingestion is NOT executed by this file.
    "macro_every_n_cycles": 10,

    "next_day_daily_days": 320,
    "next_day_mtf_hourly_days": 180,
    "next_day_mtf_15m_days": 55,
    "next_day_vix_days": 320,
    "nifty_history_days": 320,

    "history_batch_size": 250,
    "history_workers": 6,

    "supabase_timeout_sec": 15,
}


# ---------------------------------------------------------------------------
# TOTP
# ---------------------------------------------------------------------------

def generate_live_totp(secret_or_otp: str) -> str:
    raw = str(
        secret_or_otp or ""
    ).strip().replace(
        " ",
        ""
    ).upper()

    if raw.isdigit() and len(raw) == 6:
        return raw

    try:
        if len(raw) % 8:
            raw += "=" * (
                8 - len(raw) % 8
            )

        key = base64.b32decode(
            raw,
            casefold=True
        )

        counter = int(
            time.time() // 30
        )

        msg = struct.pack(
            ">Q",
            counter
        )

        digest = hmac.new(
            key,
            msg,
            hashlib.sha1
        ).digest()

        offset = digest[19] & 15

        token = (
            struct.unpack(
                ">I",
                digest[
                    offset:
                    offset + 4
                ]
            )[0]
            & 0x7fffffff
        ) % 1000000

        return f"{token:06d}"

    except Exception:
        return raw


# ---------------------------------------------------------------------------
# KOTAK MOBILE NORMALIZATION
# ---------------------------------------------------------------------------

def normalize_kotak_mobile(value: str) -> str:
    raw = str(
        value or ""
    ).strip()

    if not raw:
        return ""

    digits = "".join(
        ch
        for ch in raw
        if ch.isdigit()
    )

    if (
        digits.startswith("91")
        and len(digits) == 12
    ):
        digits = digits[2:]

    if len(digits) != 10:
        return raw

    return "+91" + digits


# ---------------------------------------------------------------------------
# ENV / STREAMLIT SECRETS
# ---------------------------------------------------------------------------

def env_or_secret(
    name,
    default=""
):
    val = os.getenv(
        name,
        ""
    )

    if val:
        return val

    if st is not None:
        try:
            val = st.secrets.get(
                name,
                ""
            )

            if val:
                return str(val)

        except Exception:
            pass

    return default


# ---------------------------------------------------------------------------
# SCRIP MASTER NORMALIZATION
# ---------------------------------------------------------------------------

def _normalize_scrip_record(
    record: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Normalize only scrip-master column spelling/whitespace.
    """

    out: Dict[str, Any] = {}

    for key, value in record.items():

        clean_key = str(
            key
        ).strip().lstrip(
            "\ufeff"
        ).strip()

        if clean_key.endswith(";"):
            clean_key = clean_key[:-1]

        out[clean_key] = (
            value.strip()
            if isinstance(value, str)
            else value
        )

    return out


def _normalize_kotak_nfo_expiry(
    record: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Mirror Kotak Neo v2 SDK's NFO expiry normalization
    for fallback rows.
    """

    key = "pExpiryDate"

    value = record.get(
        key
    )

    if value is None:
        return record

    raw = str(
        value
    ).strip().replace(
        ";",
        ""
    )

    if not raw:
        return record

    for fmt in (
        "%d%b%Y",
        "%d-%b-%Y",
        "%d/%b/%Y",
        "%Y-%m-%d"
    ):

        try:

            parsed = datetime.strptime(
                raw,
                fmt
            )

            record[key] = parsed.strftime(
                "%d%b%Y"
            ).upper()

            return record

        except Exception:
            pass

    try:

        epoch = float(
            raw
        )

        if epoch > 0:

            epoch_seconds = (
                epoch
                + 315511200
            )

            parsed = datetime.fromtimestamp(
                epoch_seconds
            )

            record[key] = parsed.strftime(
                "%d%b%Y"
            ).upper()

    except Exception:
        pass

    return record


# ---------------------------------------------------------------------------
# CSV PARSER
# ---------------------------------------------------------------------------

def _csv_text_to_records(
    csv_text: str
) -> List[Dict[str, Any]]:
    """
    Parse Kotak scrip-master CSV,
    including its JSON-envelope variant.
    """

    if not isinstance(
        csv_text,
        str
    ):
        return []

    text_value = csv_text.lstrip(
        "\ufeff\r\n\t "
    )

    if not text_value:
        return []

    if text_value.startswith("{"):

        try:

            envelope = json.loads(
                text_value
            )

            if isinstance(
                envelope,
                dict
            ):

                for key in (
                    "nse",
                    "NSE",
                    "nse_fo",
                    "NSE_FO"
                ):

                    payload = envelope.get(
                        key
                    )

                    if (
                        isinstance(
                            payload,
                            str
                        )
                        and payload.strip()
                    ):
                        csv_text = payload
                        break

        except Exception:
            pass

    try:

        reader = csv.DictReader(
            io.StringIO(
                str(csv_text).lstrip(
                    "\ufeff"
                )
            )
        )

        records: List[
            Dict[str, Any]
        ] = []

        for row in reader:

            if not row:
                continue

            normalized = _normalize_scrip_record(
                dict(row)
            )

            if (
                str(
                    normalized.get(
                        "pSymbol",
                        ""
                    )
                ).strip()
                and
                str(
                    normalized.get(
                        "pTrdSymbol",
                        ""
                    )
                ).strip()
            ):

                records.append(
                    _normalize_kotak_nfo_expiry(
                        normalized
                    )
                )

        return records

    except Exception:
        return []


# ---------------------------------------------------------------------------
# SCRIP MASTER URL EXTRACTION
# ---------------------------------------------------------------------------

def _scrip_master_urls(
    response: Any
) -> List[str]:

    urls: List[str] = []

    def add(
        value: Any
    ) -> None:

        if isinstance(
            value,
            str
        ):

            value = value.strip()

            if (
                value.startswith(
                    (
                        "http://",
                        "https://"
                    )
                )
                and value not in urls
            ):
                urls.append(value)

        elif isinstance(
            value,
            (list, tuple)
        ):

            for item in value:
                add(item)

    if isinstance(
        response,
        str
    ):
        add(response)

    elif isinstance(
        response,
        dict
    ):

        add(
            response.get(
                "filesPaths"
            )
        )

        add(
            response.get(
                "filePath"
            )
        )

        add(
            response.get(
                "url"
            )
        )

        add(
            response.get(
                "nse_fo"
            )
        )

        add(
            response.get(
                "NSE_FO"
            )
        )

    return urls


# ---------------------------------------------------------------------------
# NFO CSV PAYLOAD
# ---------------------------------------------------------------------------

def _extract_nfo_csv_payload(
    response: Any
) -> List[Dict[str, Any]]:

    if isinstance(
        response,
        str
    ):
        return _csv_text_to_records(
            response
        )

    if isinstance(
        response,
        dict
    ):

        for key in (
            "nse",
            "NSE",
            "nse_fo",
            "NSE_FO"
        ):

            value = response.get(
                key
            )

            if isinstance(
                value,
                str
            ):

                parsed = _csv_text_to_records(
                    value
                )

                if parsed:
                    return parsed

    return []


# ---------------------------------------------------------------------------
# GENERIC RESPONSE RECORD EXTRACTION
# ---------------------------------------------------------------------------

def extract_records(
    response: Any
) -> List[Dict[str, Any]]:

    if response is None:
        return []

    if isinstance(
        response,
        list
    ):

        return [
            x
            for x in response
            if isinstance(
                x,
                dict
            )
        ]

    if isinstance(
        response,
        dict
    ):

        for key in (
            "result",
            "data",
            "values",
            "records",
            "scrips"
        ):

            value = response.get(
                key
            )

            if isinstance(
                value,
                list
            ):

                return [
                    x
                    for x in value
                    if isinstance(
                        x,
                        dict
                    )
                ]

            if isinstance(
                value,
                dict
            ):

                nested = extract_records(
                    value
                )

                if nested:
                    return nested

        return _extract_nfo_csv_payload(
            response
        )

    if isinstance(
        response,
        str
    ):
        return _csv_text_to_records(
            response
        )

    return []


# ===========================================================================
# KOTAK CONNECTOR
# ===========================================================================

class KotakConnector:

    def __init__(self):

        self.consumer_key = env_or_secret(
            "KOTAK_CONSUMER_KEY"
        )

        self.mobile = normalize_kotak_mobile(
            env_or_secret(
                "KOTAK_MOBILE"
            )
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

        self.future_token = None
        self.future_symbol = None
        self.future_expiry: Optional[date] = None

        self.spot_token = "Nifty 50"

        self.atm_reference_price = None

        self.pcr_tokens = []
        self.option_contracts = {}

        self.nfo_records = []

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

        self.logs = []


    def log(
        self,
        message: str
    ):

        timestamp = now_ist().strftime(
            "%H:%M:%S"
        )

        self.logs.append(
            f"[{timestamp}] {message}"
        )

        self.logs = self.logs[
            -100:
        ]


    # -----------------------------------------------------------------------
    # LOGIN
    # -----------------------------------------------------------------------

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

        if not all(
            [
                self.consumer_key,
                self.mobile,
                self.ucc,
                totp,
                self.mpin,
            ]
        ):

            raise RuntimeError(
                "Missing Kotak Neo authentication credentials."
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
            totp=generate_live_totp(
                totp
            ),
        )

        if (
            isinstance(
                step1,
                dict
            )
            and (
                step1.get("error")
                or step1.get("Error")
            )
        ):

            raise RuntimeError(
                "Login Step 1 Error: "
                f"{step1.get('error') or step1.get('Error')}"
            )

        step2 = self.client.totp_validate(
            mpin=self.mpin
        )

        if (
            isinstance(
                step2,
                dict
            )
            and (
                step2.get("error")
                or step2.get("Error")
            )
        ):

            raise RuntimeError(
                "Login Step 2 Error: "
                f"{step2.get('error') or step2.get('Error')}"
            )

        self.connected = True

        self.log(
            "Kotak authentication successful."
        )

        return True


    # -----------------------------------------------------------------------
    # NFO SCRIP MASTER
    # -----------------------------------------------------------------------

    def load_nfo_scrip_master(
        self
    ) -> List[Dict[str, Any]]:

        if not self.connected:
            raise RuntimeError(
                "Kotak connector is not authenticated."
            )

        records: List[
            Dict[str, Any]
        ] = []

        try:

            response = self.client.search_scrip(
                exchange_segment="nse_fo",
                symbol="NIFTY",
            )

            records = extract_records(
                response
            )

        except Exception as exc:

            self.log(
                f"Primary search_scrip failed: {exc}"
            )


        if not records:

            try:

                response = self.client.search_scrip(
                    exchange_segment="nse_fo",
                    symbol="Nifty",
                )

                records = extract_records(
                    response
                )

            except Exception as exc:

                self.log(
                    f"Secondary search_scrip failed: {exc}"
                )


        if records:

            self.nfo_records = records

            self.log(
                "Total raw NFO scrip records retrieved: "
                f"{len(records)}"
            )

            return records


        try:

            master_response = self.client.scrip_master(
                exchange_segment="nse_fo"
            )

            records = _extract_nfo_csv_payload(
                master_response
            )

            if records:

                self.nfo_records = records

                self.log(
                    "NFO fallback payload parsed directly: "
                    f"{len(records)} records"
                )

                return records


            urls = _scrip_master_urls(
                master_response
            )

            self.log(
                "NFO scrip_master fallback URLs discovered: "
                f"{len(urls)}"
            )


            nfo_urls = [
                u
                for u in urls
                if "nse_fo" in u.lower()
            ]

            urls = nfo_urls or urls


            for url in urls:

                try:

                    response = requests.get(
                        url,
                        headers={
                            "Accept":
                                "text/csv,application/json,*/*",
                        },
                        timeout=25,
                    )

                    self.log(
                        "NFO scrip-master download: "
                        f"HTTP {response.status_code}, "
                        f"bytes={len(response.content)}"
                    )

                    if response.status_code >= 400:
                        continue


                    parsed = _extract_nfo_csv_payload(
                        response.text
                    )


                    if not parsed:

                        try:

                            parsed = _extract_nfo_csv_payload(
                                response.json()
                            )

                        except Exception:
                            pass


                    if parsed:

                        self.nfo_records = parsed

                        self.log(
                            "NFO scrip-master fallback parsed: "
                            f"{len(parsed)} raw records"
                        )

                        return parsed


                except Exception as exc:

                    self.log(
                        "NFO scrip-master URL fallback failed: "
                        f"{exc}"
                    )


        except Exception as exc:

            self.log(
                f"NFO scrip_master fallback failed: {exc}"
            )


        raise RuntimeError(
            "Kotak NFO discovery returned no usable records "
            "after the tested search_scrip path and official "
            "scrip_master fallback."
        )


    # -----------------------------------------------------------------------
    # INSTRUMENT DISCOVERY
    # -----------------------------------------------------------------------

    def discover_instruments(
        self
    ) -> bool:

        if (
            not self.connected
            or not self.client
        ):

            raise RuntimeError(
                "Kotak connector is not authenticated."
            )


        self.logs.clear()

        self.future_token = None
        self.future_symbol = None
        self.future_expiry = None

        self.pcr_tokens = []
        self.option_contracts = {}


        records = self.load_nfo_scrip_master()


        candidates = []


        for r in records:

            if not isinstance(
                r,
                dict
            ):
                continue


            sym = str(
                r.get(
                    "pTrdSymbol",
                    r.get(
                        "tradingSymbol",
                        r.get(
                            "ts",
                            r.get(
                                "symbol",
                                ""
                            )
                        )
                    )
                )
            ).upper().strip()


            token = str(
                r.get(
                    "pSymbol",
                    r.get(
                        "pSymbolToken",
                        r.get(
                            "instrument_token",
                            r.get(
                                "token",
                                ""
                            )
                        )
                    )
                )
            )


            inst_type = str(
                r.get(
                    "pInstType",
                    ""
                )
            ).upper()


            option_type = str(
                r.get(
                    "pOptionType",
                    ""
                )
            ).upper()


            if not token or not sym:
                continue


            if token == "26000":
                continue


            if (
                option_type in (
                    "CE",
                    "PE"
                )
                or sym.endswith("CE")
                or sym.endswith("PE")
            ):
                continue


            if (
                not sym.startswith("NIFTY")
                or sym.startswith("NIFTYNXT")
            ):
                continue


            if any(
                x in sym
                for x in [
                    "BANKNIFTY",
                    "FINNIFTY",
                    "MIDCPNIFTY",
                    "SENSEX",
                ]
            ):
                continue


            is_fut = (
                "FUT" in inst_type
                or "FUT" in sym
                or sym.endswith("FUT")
            )


            if is_fut:

                candidates.append(
                    (
                        sym,
                        token,
                        r
                    )
                )


        if not candidates:

            raise RuntimeError(
                "Active NIFTY future contract could not be "
                "discovered from scrip master."
            )


        today = today_ist()

        parsed_candidates = []


        for (
            sym,
            token,
            record
        ) in candidates:

            expiry_text = str(
                record.get(
                    "pExpiryDate",
                    ""
                )
            ).replace(
                ";",
                ""
            ).strip()


            expiry = None


            if expiry_text:

                for fmt in (
                    "%d%b%Y",
                    "%d-%b-%Y",
                    "%d/%b/%Y",
                    "%Y-%m-%d",
                ):

                    try:

                        expiry = datetime.strptime(
                            expiry_text,
                            fmt
                        ).date()

                        break

                    except Exception:
                        pass


            if (
                expiry is not None
                and expiry < today
            ):
                continue


            parsed_candidates.append(
                (
                    expiry
                    if expiry
                    else date.max,
                    sym,
                    token,
                    record,
                )
            )


        if not parsed_candidates:

            raise RuntimeError(
                "NIFTY futures were found, but all discovered "
                "contracts appear to be expired or have invalid expiry."
            )


        parsed_candidates.sort(
            key=lambda x: x[0]
        )


        (
            selected_expiry,
            selected_symbol,
            selected_token,
            _
        ) = parsed_candidates[0]


        self.future_symbol = selected_symbol
        self.future_token = selected_token


        self.future_expiry = (
            None
            if selected_expiry == date.max
            else selected_expiry
        )


        self.log(
            "Bound Active Nifty Future: "
            f"{self.future_symbol} "
            f"(Token: {self.future_token}, "
            f"Expiry: "
            f"{self.future_expiry.isoformat() if self.future_expiry else 'UNKNOWN'})"
        )


        # -------------------------------------------------------------------
        # SPOT REFERENCE
        # -------------------------------------------------------------------

        spot_price = None


        try:

            spot_res = self.client.quotes(
                instrument_tokens=[
                    {
                        "instrument_token":
                            self.spot_token,

                        "exchange_segment":
                            "nse_cm",
                    }
                ],
                quote_type="ltp",
            )


            spot_recs = extract_records(
                spot_res
            )


            for sr in spot_recs:

                for k in (
                    "lp",
                    "last_price",
                    "ltp",
                    "c",
                    "close",
                ):

                    try:

                        val = float(
                            sr.get(
                                k,
                                0
                            )
                        )

                    except Exception:

                        val = 0.0


                    if val > 0:

                        spot_price = val
                        break


                if spot_price:
                    break


        except Exception as exc:

            self.log(
                "Native NIFTY spot quote unavailable: "
                f"{exc}"
            )


        # -------------------------------------------------------------------
        # FUTURE LTP FALLBACK
        # -------------------------------------------------------------------

        if (
            spot_price is None
            or spot_price <= 0
        ):

            try:

                fut_res = self.client.quotes(
                    instrument_tokens=[
                        {
                            "instrument_token":
                                str(
                                    self.future_token
                                ),

                            "exchange_segment":
                                "nse_fo",
                        }
                    ],
                    quote_type="ltp",
                )


                fut_recs = extract_records(
                    fut_res
                )


                for fr in fut_recs:

                    for k in (
                        "lp",
                        "last_price",
                        "ltp",
                        "c",
                        "close",
                    ):

                        try:

                            val = float(
                                fr.get(
                                    k,
                                    0
                                )
                            )

                        except Exception:

                            val = 0.0


                        if val > 0:

                            spot_price = val
                            break


                    if spot_price:
                        break


                if (
                    spot_price
                    and spot_price > 0
                ):

                    self.log(
                        "NIFTY spot quote unavailable; "
                        "using active future LTP "
                        f"{spot_price:.2f} as ATM reference "
                        f"(Token: {self.future_token})."
                    )


            except Exception as exc:

                self.log(
                    "Active NIFTY future LTP fallback unavailable: "
                    f"{exc}"
                )


        # -------------------------------------------------------------------
        # OPTION MAPPING
        # -------------------------------------------------------------------

        if (
            spot_price is None
            or spot_price <= 0
        ):

            self.log(
                "No valid NIFTY price reference; "
                "PCR option mapping skipped."
            )

            self.option_contracts = {}
            self.pcr_tokens = []

            return True


        self.atm_reference_price = float(
            spot_price
        )


        step = CONFIG[
            "pcr_strike_step"
        ]

        atm = round(
            spot_price / step
        ) * step

        count = CONFIG[
            "pcr_strike_count"
        ]


        target_strikes = {
            atm + (
                i * step
            )
            for i in range(
                -count,
                count + 1
            )
        }


        discovered = {}


        for r in records:

            if not isinstance(
                r,
                dict
            ):
                continue


            sym = str(
                r.get(
                    "pTrdSymbol",
                    r.get(
                        "tradingSymbol",
                        ""
                    )
                )
            ).upper().strip()


            token = str(
                r.get(
                    "pSymbol",
                    r.get(
                        "pSymbolToken",
                        ""
                    )
                )
            )


            if not sym or not token:
                continue


            if (
                not sym.startswith("NIFTY")
                or sym.startswith("NIFTYNXT")
            ):
                continue


            if any(
                x in sym
                for x in [
                    "BANKNIFTY",
                    "FINNIFTY",
                    "MIDCPNIFTY",
                    "SENSEX",
                ]
            ):
                continue


            opt_type = str(
                r.get(
                    "pOptionType",
                    ""
                )
            ).upper()


            if opt_type not in (
                "CE",
                "PE"
            ):

                if sym.endswith("CE"):
                    opt_type = "CE"

                elif sym.endswith("PE"):
                    opt_type = "PE"


            if opt_type not in (
                "CE",
                "PE"
            ):
                continue


            option_expiry_text = str(
                r.get(
                    "pExpiryDate",
                    ""
                )
            ).replace(
                ";",
                ""
            ).strip()


            option_expiry = None


            if option_expiry_text:

                for fmt in (
                    "%d%b%Y",
                    "%d-%b-%Y",
                    "%d/%b/%Y",
                    "%Y-%m-%d",
                ):

                    try:

                        option_expiry = datetime.strptime(
                            option_expiry_text,
                            fmt
                        ).date()

                        break

                    except Exception:
                        pass


            if (
                self.future_expiry is not None
                and option_expiry is not None
            ):

                if (
                    option_expiry
                    != self.future_expiry
                ):
                    continue


            strike_val = None


            for sk in (
                "dStrikePrice;",
                "dStrikePrice",
                "strike_price",
                "strikePrice",
            ):

                if sk in r:

                    try:

                        v = float(
                            str(
                                r.get(sk)
                            )
                            .replace(
                                ";",
                                ""
                            )
                            .replace(
                                ",",
                                ""
                            )
                        )


                        if v > 1000000:
                            v /= 100.0


                        strike_val = v
                        break


                    except Exception:
                        pass


            if strike_val is None:

                match = re.search(
                    r"(\d+(?:\.\d+)?)$",
                    sym[:-2]
                )


                if match:

                    try:

                        strike_val = float(
                            match.group(1)
                        )

                    except Exception:
                        pass


            if strike_val is not None:

                strike_val = round(
                    strike_val,
                    2
                )


                for target in target_strikes:

                    if abs(
                        strike_val - target
                    ) < 0.5:

                        key = (
                            f"{opt_type}:"
                            f"{target:.2f}"
                        )


                        discovered[key] = {

                            "token":
                                token,

                            "symbol":
                                sym,

                            "option_type":
                                opt_type,

                            "strike":
                                target,

                        }


                        break


        self.option_contracts = discovered


        self.pcr_tokens = sorted(
            list(
                {
                    str(
                        item["token"]
                    )

                    for item
                    in discovered.values()

                    if item.get(
                        "token"
                    )
                }
            )
        )


        self.log(
            "Discovery complete: "
            f"Future={self.future_token}, "
            f"Options mapped={len(self.pcr_tokens)}"
        )


        return True


    # -----------------------------------------------------------------------
    # LIVE RAW QUOTES
    # -----------------------------------------------------------------------

    def _ensure_market_data_client(self) -> bool:
        """Ensure a fresh Kotak client exists for the quotes-only data path.

        Kotak Neo documents the quotes API as usable with the consumer key
        without a completed TOTP/MPIN session.  The raw producer only needs
        market data, so feed recovery must not force a fresh 2FA login.
        """
        if NeoAPI is None:
            return False
        try:
            self.client = NeoAPI(
                environment=CONFIG["neo_environment"],
                consumer_key=self.consumer_key,
            )
            self.connected = True
            return True
        except Exception as exc:
            self.client = None
            self.connected = False
            self.log(f"Market-data client rebuild failed: {exc}")
            return False

    def fetch_raw_quotes(
        self
    ) -> List[Dict[str, Any]]:

        if not self.client:
            self._ensure_market_data_client()

        # Quotes are a market-data endpoint and do not require a completed
        # TOTP/MPIN session.  Do not turn a feed reconnect into an auth prompt.
        if not self.client:
            return []


        tokens_to_poll = [

            {
                "instrument_token":
                    self.spot_token,

                "exchange_segment":
                    "nse_cm",
            },

        ]


        if self.future_token:

            tokens_to_poll.append(

                {
                    "instrument_token":
                        str(
                            self.future_token
                        ),

                    "exchange_segment":
                        "nse_fo",
                }

            )


        for (
            sym,
            tok
        ) in self.heavy_tokens.items():

            tokens_to_poll.append(

                {
                    "instrument_token":
                        str(tok),

                    "exchange_segment":
                        "nse_cm",
                }

            )


        for tok in self.pcr_tokens:

            tokens_to_poll.append(

                {
                    "instrument_token":
                        str(tok),

                    "exchange_segment":
                        "nse_fo",
                }

            )


        try:
            response = self.client.quotes(
                instrument_tokens=tokens_to_poll,
                quote_type="all"
            )
            records = extract_records(response)
            if records:
                self.connected = True
                return records

            raise RuntimeError("Kotak returned no quote records.")

        except Exception as exc:
            self.log(f"Quote fetch error: {exc}")

            # Surgical recovery: rebuild only the market-data client and retry
            # once.  No TOTP is generated or requested on this path.
            self.client = None
            self.connected = False
            if not self._ensure_market_data_client():
                return []
            try:
                response = self.client.quotes(
                    instrument_tokens=tokens_to_poll,
                    quote_type="all"
                )
                records = extract_records(response)
                if records:
                    self.connected = True
                    self.log(f"Market-data recovery successful: {len(records)} quotes.")
                    return records
                self.log("Market-data recovery returned no quote records.")
            except Exception as retry_exc:
                self.client = None
                self.connected = False
                self.log(f"Market-data recovery failed: {retry_exc}")
            return []


# ===========================================================================
# SUPABASE PUBLISHER
# ===========================================================================

class SupabasePublisher:
    """
    Append-only raw bus publisher.

    Calculations never happen here.
    """

    def __init__(
        self,
        url_override: str = "",
        key_override: str = ""
    ):

        self.url = str(

            url_override
            or env_or_secret(
                "SUPABASE_URL",
                CONFIG[
                    "supabase_url"
                ]
            )

        ).strip()


        self.key = str(

            key_override
            or env_or_secret(
                "SUPABASE_KEY",
                CONFIG[
                    "supabase_key"
                ]
            )

        ).strip()


    def _headers(self):

        return {

            "apikey":
                self.key,

            "Authorization":
                f"Bearer {self.key}",

            "Content-Type":
                "application/json",

            "Prefer":
                "return=minimal",

        }


    def health(
        self
    ) -> Dict[str, Any]:

        if (
            not self.url
            or not self.key
        ):

            return {

                "configured":
                    False,

                "reachable":
                    False,

                "error":
                    "Supabase URL/Key missing",

            }


        endpoint = (
            f"{self.url.rstrip('/')}"
            "/rest/v1/raw_observations"
        )


        try:

            response = requests.get(

                endpoint,

                headers={

                    "apikey":
                        self.key,

                    "Authorization":
                        f"Bearer {self.key}",

                    "Accept":
                        "application/json",

                },

                params={
                    "select": "id",
                    "limit": "1",
                },

                timeout=float(
                    CONFIG[
                        "supabase_timeout_sec"
                    ]
                ),

            )


            return {

                "configured":
                    True,

                "reachable":
                    response.status_code
                    in (
                        200,
                        206
                    ),

                "http_status":
                    response.status_code,

                "error": (

                    ""

                    if response.status_code
                    in (
                        200,
                        206
                    )

                    else response.text[
                        :250
                    ]

                ),

            }


        except Exception as exc:

            return {

                "configured":
                    True,

                "reachable":
                    False,

                "error":
                    str(exc),

            }


    def publish_observations_batch(
        self,
        source: str,
        symbol: str,
        token: str,
        raw_payloads: List[dict],
    ) -> int:

        if (
            not self.url
            or not self.key
            or not raw_payloads
        ):
            return 0


        total = 0


        endpoint = (
            f"{self.url.rstrip('/')}"
            "/rest/v1/raw_observations"
        )


        batch_size = max(
            1,
            int(
                CONFIG[
                    "history_batch_size"
                ]
            )
        )


        for i in range(
            0,
            len(raw_payloads),
            batch_size
        ):

            batch = raw_payloads[
                i:i + batch_size
            ]


            records = []


            for payload in batch:

                records.append({

                    "source":
                        source,

                    "symbol":
                        symbol,

                    "instrument_token":
                        str(token),

                    "observation_timestamp":
                        now_ist().isoformat(),

                    "raw":
                        payload,

                })


            try:

                response = requests.post(

                    endpoint,

                    headers=
                        self._headers(),

                    json=
                        records,

                    timeout=float(
                        CONFIG[
                            "supabase_timeout_sec"
                        ]
                    )

                )


                if response.status_code in (
                    200,
                    201,
                    204
                ):

                    total += len(
                        records
                    )

                else:

                    print(
                        "Supabase batch publish failed "
                        f"[{response.status_code}]: "
                        f"{response.text[:300]}"
                    )


            except Exception as exc:

                print(
                    "Supabase batch publish error: "
                    f"{exc}"
                )


        return total


    def publish_observation(
        self,
        source: str,
        symbol: str,
        token: str,
        raw_payload: dict,
    ) -> bool:

        return (

            self.publish_observations_batch(

                source,
                symbol,
                token,
                [raw_payload]

            )

            == 1

        )


# ===========================================================================
# ===========================================================================
# PERSISTENT KOTAK WORKER + MONITOR BRIDGE
# ===========================================================================

WORKER_STATE_PATH = os.path.join(
    os.getenv("KOTAK_WORKER_STATE_DIR", "/tmp"),
    "kotak_live_worker_state.json",
)


def _worker_now() -> str:
    return now_ist().strftime("%Y-%m-%d %H:%M:%S IST")


def _worker_write_state(**updates):
    state = {}
    try:
        if os.path.exists(WORKER_STATE_PATH):
            with open(WORKER_STATE_PATH, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
                if isinstance(loaded, dict):
                    state.update(loaded)
    except Exception:
        pass
    state.update(updates)
    state["updated_at"] = _worker_now()
    tmp_path = WORKER_STATE_PATH + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=True, indent=2)
        os.replace(tmp_path, WORKER_STATE_PATH)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def _worker_read_state() -> dict:
    try:
        with open(WORKER_STATE_PATH, "r", encoding="utf-8") as fh:
            value = json.load(fh)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _worker_alive(pid) -> bool:
    try:
        pid = int(pid)
        if pid <= 0:
            return False
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _worker_publish_quotes(publisher, quotes) -> int:
    published = 0
    for quote in quotes:
        if not isinstance(quote, dict):
            continue
        token = str(quote.get("exchange_token", quote.get("instrument_token", quote.get("pSymbol", quote.get("pSymbolToken", "UNKNOWN")))))
        symbol = str(quote.get("display_symbol", quote.get("pTrdSymbol", quote.get("tradingSymbol", "UNKNOWN"))))
        if publisher.publish_observation("kotak_live", symbol, token, quote):
            published += 1
    return published


def _worker_login_and_discover(kotak, reason: str):
    _worker_write_state(
        status="RECONNECTING",
        connection="RECONNECTING",
        recovery_reason=reason,
        recovery_started_at=_worker_now(),
    )
    kotak.client = None
    kotak.connected = False
    kotak.login()
    kotak.discover_instruments()


def _worker_market_data_recover(kotak, reason: str) -> bool:
    """Recover the raw market-data path without re-running TOTP authentication."""
    _worker_write_state(
        status="RECONNECTING",
        connection="RECONNECTING",
        recovery_reason=reason,
        recovery_started_at=_worker_now(),
    )
    kotak.client = None
    kotak.connected = False
    return bool(kotak._ensure_market_data_client())


def run_kotak_worker(startup_totp: str = "") -> None:
    """Persistent worker executed by this same app.py with --kotak-worker.

    The manual TOTP is used only for the initial authenticated bootstrap.
    Subsequent raw-market-data recovery never asks for another TOTP.
    """
    pid = os.getpid()
    poll_interval = float(CONFIG.get("poll_interval_sec", 3.0))
    consecutive_failures = 0
    total_failures = 0
    recovery_attempts = 0
    successful_recoveries = 0
    last_error = ""
    last_kotak_fetch = ""
    last_supabase_write = ""
    last_recovery = ""
    last_quotes = 0
    last_published = 0
    auth_count = 0

    _worker_write_state(
        status="STARTING", connection="STARTING", pid=pid,
        consecutive_failures=0, total_failures=0,
        recovery_attempts=0, successful_recoveries=0,
        last_error="", last_kotak_fetch="", last_supabase_write="",
        last_quotes=0, last_published=0, active_future="",
        future_symbol="", future_expiry="", pcr_contracts=0,
        nfo_records=0, auto_reauth=False,
    )

    kotak = KotakConnector()
    publisher = SupabasePublisher()
    totp_secret = str(os.getenv("KOTAK_TOTP", "")).strip()
    auto_reauth = bool(totp_secret and not (totp_secret.isdigit() and len(totp_secret) == 6))

    try:
        if not publisher.url or not publisher.key:
            raise RuntimeError("Supabase configuration is missing.")

        _worker_write_state(status="AUTHENTICATING", connection="AUTHENTICATING", auto_reauth=auto_reauth)
        kotak.login(totp_override=startup_totp)
        auth_count += 1
        _worker_write_state(status="DISCOVERING", connection="AUTHENTICATED", auth_count=auth_count)
        kotak.discover_instruments()
        _worker_write_state(
            status="LIVE", connection="AUTHENTICATED",
            active_future=str(kotak.future_token or ""),
            future_symbol=str(kotak.future_symbol or ""),
            future_expiry=(kotak.future_expiry.isoformat() if kotak.future_expiry else ""),
            pcr_contracts=len(kotak.pcr_tokens), nfo_records=len(kotak.nfo_records),
            auth_count=auth_count, auto_reauth=auto_reauth,
        )

        while True:
            cycle_started = time.time()
            try:
                raw_quotes = kotak.fetch_raw_quotes()
                last_kotak_fetch = _worker_now()
                last_quotes = len(raw_quotes)
                if not raw_quotes:
                    raise RuntimeError("Kotak returned no raw quotes.")
                last_published = _worker_publish_quotes(publisher, raw_quotes)
                if last_published > 0:
                    last_supabase_write = _worker_now()
                consecutive_failures = 0
                last_error = ""
                _worker_write_state(
                    status="LIVE", connection="AUTHENTICATED", feed_age_sec=0,
                    last_kotak_fetch=last_kotak_fetch, last_supabase_write=last_supabase_write,
                    last_quotes=last_quotes, last_published=last_published,
                    consecutive_failures=0, total_failures=total_failures,
                    recovery_attempts=recovery_attempts, successful_recoveries=successful_recoveries,
                    last_recovery=last_recovery, last_error="",
                    active_future=str(kotak.future_token or ""),
                    future_symbol=str(kotak.future_symbol or ""),
                    future_expiry=(kotak.future_expiry.isoformat() if kotak.future_expiry else ""),
                    pcr_contracts=len(kotak.pcr_tokens), nfo_records=len(kotak.nfo_records),
                    auth_count=auth_count, auto_reauth=auto_reauth,
                )
            except Exception as exc:
                consecutive_failures += 1
                total_failures += 1
                last_error = str(exc)
                _worker_write_state(
                    status=("FEED_LOST" if consecutive_failures < 3 else "RECONNECTING"),
                    connection=("AUTHENTICATED" if kotak.connected else "DISCONNECTED"),
                    last_quotes=last_quotes, last_published=last_published,
                    consecutive_failures=consecutive_failures, total_failures=total_failures,
                    recovery_attempts=recovery_attempts, successful_recoveries=successful_recoveries,
                    last_error=last_error, last_kotak_fetch=last_kotak_fetch,
                    last_supabase_write=last_supabase_write,
                    active_future=str(kotak.future_token or ""),
                    future_symbol=str(kotak.future_symbol or ""),
                    pcr_contracts=len(kotak.pcr_tokens), nfo_records=len(kotak.nfo_records),
                    auto_reauth=auto_reauth,
                )
                if consecutive_failures >= 3:
                    recovery_attempts += 1
                    if not auto_reauth:
                        try:
                            recovered = _worker_market_data_recover(
                                kotak,
                                f"{consecutive_failures} consecutive feed failures; market-data-only recovery",
                            )
                            if recovered:
                                successful_recoveries += 1
                                consecutive_failures = 0
                                last_recovery = _worker_now()
                                last_error = ""
                                _worker_write_state(
                                    status="LIVE", connection="CONNECTED",
                                    recovery_attempts=recovery_attempts,
                                    successful_recoveries=successful_recoveries,
                                    last_recovery=last_recovery, last_error="",
                                    auth_count=auth_count, auto_reauth=False,
                                )
                            else:
                                _worker_write_state(
                                    status="FEED_LOST", connection="DISCONNECTED",
                                    recovery_attempts=recovery_attempts,
                                    successful_recoveries=successful_recoveries,
                                    last_error=last_error, auth_count=auth_count,
                                    auto_reauth=False,
                                )
                                time.sleep(min(15.0, 2.0 * recovery_attempts))
                        except Exception as recover_exc:
                            kotak.connected = False
                            kotak.client = None
                            last_error = str(recover_exc)
                            _worker_write_state(
                                status="FEED_LOST", connection="DISCONNECTED",
                                recovery_attempts=recovery_attempts,
                                successful_recoveries=successful_recoveries,
                                last_error=last_error, auth_count=auth_count,
                                auto_reauth=False,
                            )
                            time.sleep(min(15.0, 2.0 * recovery_attempts))
                    else:
                        try:
                            _worker_login_and_discover(kotak, f"{consecutive_failures} consecutive feed failures")
                            auth_count += 1
                            successful_recoveries += 1
                            consecutive_failures = 0
                            last_recovery = _worker_now()
                            last_error = ""
                            _worker_write_state(
                                status="LIVE", connection="AUTHENTICATED",
                                recovery_attempts=recovery_attempts, successful_recoveries=successful_recoveries,
                                last_recovery=last_recovery, last_error="", auth_count=auth_count,
                                active_future=str(kotak.future_token or ""),
                                future_symbol=str(kotak.future_symbol or ""),
                                future_expiry=(kotak.future_expiry.isoformat() if kotak.future_expiry else ""),
                                pcr_contracts=len(kotak.pcr_tokens), nfo_records=len(kotak.nfo_records),
                                auto_reauth=True,
                            )
                        except Exception as recover_exc:
                            kotak.connected = False
                            kotak.client = None
                            last_error = str(recover_exc)
                            _worker_write_state(
                                status="AUTH_REQUIRED", connection="DISCONNECTED",
                                recovery_attempts=recovery_attempts, successful_recoveries=successful_recoveries,
                                last_recovery=last_recovery, last_error=last_error, auth_count=auth_count,
                                auto_reauth=auto_reauth,
                            )
                            time.sleep(min(30.0, 5.0 * recovery_attempts))
            elapsed = time.time() - cycle_started
            time.sleep(max(0.25, poll_interval - elapsed))
    except Exception as exc:
        _worker_write_state(
            status="AUTH_REQUIRED", connection="DISCONNECTED", pid=pid,
            last_error=str(exc), auth_count=auth_count, auto_reauth=auto_reauth,
        )


def _prepare_worker_environment(kotak, supabase_url: str, supabase_key: str, startup_totp: str = ""):
    env = os.environ.copy()
    for name in ("KOTAK_CONSUMER_KEY", "KOTAK_MOBILE", "KOTAK_UCC", "KOTAK_MPIN"):
        value = env_or_secret(name, "")
        if value:
            env[name] = str(value)
    configured_totp = env_or_secret("KOTAK_TOTP", "")
    if configured_totp:
        env["KOTAK_TOTP"] = str(configured_totp)
    elif getattr(kotak, "totp_secret", ""):
        env["KOTAK_TOTP"] = str(kotak.totp_secret)
    if startup_totp:
        env["KOTAK_STARTUP_TOTP"] = str(startup_totp)
    if supabase_url:
        env["SUPABASE_URL"] = str(supabase_url)
    if supabase_key:
        env["SUPABASE_KEY"] = str(supabase_key)
    return env


def _start_kotak_worker(kotak, supabase_url: str, supabase_key: str, startup_totp: str = ""):
    existing_pid = _worker_read_state().get("pid")
    if _worker_alive(existing_pid):
        return int(existing_pid), "already_running"
    env = _prepare_worker_environment(kotak, supabase_url, supabase_key, startup_totp)
    process = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--kotak-worker"],
        env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True,
    )
    return process.pid, "started"


def _stop_kotak_worker() -> bool:
    pid = _worker_read_state().get("pid")
    if not _worker_alive(pid):
        _worker_write_state(status="STOPPED", connection="DISCONNECTED", pid=None)
        return False
    try:
        os.kill(int(pid), signal.SIGTERM)
    except Exception:
        try:
            os.kill(int(pid), signal.SIGKILL)
        except Exception:
            pass
    _worker_write_state(status="STOPPED", connection="DISCONNECTED", pid=None)
    return True


def _worker_monitor_view():
    state = _worker_read_state()
    pid = state.get("pid")
    if pid and not _worker_alive(pid):
        state["status"] = "STOPPED"
        state["connection"] = "DISCONNECTED"
        state["pid"] = None
    return state


# MAIN STREAMLIT APP
# ===========================================================================

def main():

    if st is None:

        print(
            "Streamlit not available."
        )

        return


    st.set_page_config(

        page_title=
            "Institutional Raw Data Producer Bus",

        layout=
            "wide",

    )


    st.title(
        " Institutional Raw Data Producer Bus"
    )


    if "kotak" not in st.session_state:

        st.session_state.kotak = (
            KotakConnector()
        )


    if "producer_running" not in st.session_state:

        st.session_state.producer_running = False


    if "worker_running" not in st.session_state:

        st.session_state.worker_running = False


    kotak: KotakConnector = (
        st.session_state.kotak
    )


    with st.sidebar:

        # -------------------------------------------------------------------
        # AUTHENTICATION
        # -------------------------------------------------------------------

        st.header(
            " Authentication"
        )


        totp_input = st.text_input(

            "Live TOTP Code",

            type=
                "password",

            key=
                "live_totp_input",

            placeholder=
                "Enter current 6-digit TOTP",

        )


        st.markdown("---")


        # -------------------------------------------------------------------
        # SUPABASE
        # -------------------------------------------------------------------

        st.header(
            " Supabase RAW BUS"
        )


        supabase_url_input = st.text_input(

            "Supabase URL",

            value=
                st.session_state.get(

                    "supabase_url_input",

                    env_or_secret(
                        "SUPABASE_URL",
                        ""
                    ),

                ),

            key=
                "supabase_url_input",

            placeholder=
                "https://your-project.supabase.co",

        )


        supabase_key_input = st.text_input(

            "Supabase Key",

            value=
                st.session_state.get(

                    "supabase_key_input",

                    env_or_secret(
                        "SUPABASE_KEY",
                        ""
                    ),

                ),

            type=
                "password",

            key=
                "supabase_key_input",

            placeholder=
                "Supabase anon/service key",

        )


        if st.button(

            "[OK] Confirm / Apply Configuration",

            type=
                "primary",

            use_container_width=
                True,

        ):

            st.session_state[
                "config_confirmed"
            ] = True


            st.session_state[
                "config_confirmed_at"
            ] = now_ist().strftime(

                "%Y-%m-%d %H:%M:%S IST"

            )


            st.success(
                "Configuration applied for this session."
            )


        config_confirmed = bool(

            st.session_state.get(
                "config_confirmed",
                False
            )

        )


        if config_confirmed:

            st.caption(

                "OK Configuration active * "
                f"{st.session_state.get('config_confirmed_at', '')}"

            )


        supabase = SupabasePublisher(

            url_override=
                supabase_url_input,

            key_override=
                supabase_key_input,

        )


        # -------------------------------------------------------------------
        # SUPABASE TEST
        # -------------------------------------------------------------------

        if st.button(

            " Test Supabase RAW BUS",

            disabled=
                not config_confirmed,

            use_container_width=
                True,

        ):

            health = supabase.health()


            if health.get(
                "reachable"
            ):

                st.success(
                    "Supabase RAW BUS reachable."
                )

            else:

                st.error(

                    health.get(

                        "error",

                        "Supabase connection failed."

                    )

                )


        st.markdown("---")


        # -------------------------------------------------------------------
        # KOTAK LIVE PRODUCER
        # -------------------------------------------------------------------

        st.header(
            " Live Raw Producer"
        )


        c1, c2 = st.columns(2)


        with c1:

            if st.button(

                " Connect Kotak",

                disabled=
                    not config_confirmed,

                use_container_width=
                    True,

            ):

                try:

                    if not totp_input.strip():

                        st.error(
                            "Enter the current Live TOTP Code first."
                        )

                    else:

                        with st.spinner(
                            "Authenticating with Kotak Neo..."
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


        with c2:

            if st.button(

                " Discover Instruments",

                disabled=
                    not (
                        config_confirmed
                        and kotak.connected
                    ),

                use_container_width=
                    True,

            ):

                try:

                    kotak.discover_instruments()

                    st.success(
                        "Discovery Complete!"
                    )

                except Exception as exc:

                    st.error(
                        str(exc)
                    )


        # -------------------------------------------------------------------
        # START/STOP PERSISTENT WORKER
        # -------------------------------------------------------------------

        can_start = bool(
            config_confirmed
            and kotak.connected
            and kotak.future_token
            and supabase.url
            and supabase.key
        )

        worker_state = _worker_monitor_view()
        worker_pid = worker_state.get("pid")
        worker_is_alive = _worker_alive(worker_pid)

        if not worker_is_alive:
            if st.button(
                "Start Persistent Raw Worker",
                type="primary",
                disabled=not can_start,
            ):
                try:
                    pid, mode = _start_kotak_worker(
                        kotak, supabase.url, supabase.key, totp_input.strip()
                    )
                    st.session_state.worker_running = True
                    st.session_state.producer_running = True
                    st.success(f"Worker {mode}. PID={pid}.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Worker start failed: {exc}")
        else:
            if st.button("Stop Persistent Raw Worker"):
                _stop_kotak_worker()
                st.session_state.worker_running = False
                st.session_state.producer_running = False
                st.rerun()


        # -------------------------------------------------------------------
        # LIVE RAW TEST
        # -------------------------------------------------------------------

        if st.button(

            "Test Live Raw -> Supabase",

            disabled=
                not can_start,

        ):

            try:

                raw_quotes = (
                    kotak.fetch_raw_quotes()
                )


                published = 0


                for quote in raw_quotes:

                    if not isinstance(
                        quote,
                        dict
                    ):
                        continue


                    token = str(

                        quote.get(

                            "exchange_token",

                            quote.get(

                                "instrument_token",

                                quote.get(

                                    "pSymbol",

                                    quote.get(

                                        "pSymbolToken",

                                        "UNKNOWN"

                                    )

                                )

                            )

                        )

                    )


                    symbol = str(

                        quote.get(

                            "display_symbol",

                            quote.get(

                                "pTrdSymbol",

                                quote.get(

                                    "tradingSymbol",

                                    "UNKNOWN"

                                )

                            )

                        )

                    )


                    if supabase.publish_observation(

                        "kotak_live",

                        symbol,

                        token,

                        quote

                    ):

                        published += 1


                st.session_state[
                    "last_live_test"
                ] = {

                    "kotak_quotes_received":
                        len(raw_quotes),

                    "supabase_rows_published":
                        published,

                    "active_future":
                        kotak.future_token,

                    "pcr_contracts":
                        len(kotak.pcr_tokens),

                    "status": (

                        "PASS"

                        if raw_quotes
                        and published

                        else "PARTIAL/NO_DATA"

                    ),

                }


            except Exception as exc:

                st.session_state[
                    "last_live_test"
                ] = {

                    "status":
                        "ERROR",

                    "error":
                        str(exc),

                }


        if st.session_state.get(
            "last_live_test"
        ):

            st.json(
                st.session_state[
                    "last_live_test"
                ]
            )


        st.markdown("---")


        # -------------------------------------------------------------------
        # HISTORICAL NOTICE
        # -------------------------------------------------------------------

        st.header(
            " Historical Raw Producer"
        )


        st.info(

            "Historical/yfinance ingestion is intentionally "
            "isolated from this Kotak LIVE environment. "
            "This app publishes Kotak LIVE raw data only."

        )


    # =========================================================================
    # MAIN METRICS
    # =========================================================================

    col1, col2, col3, col4 = st.columns(4)


    col1.metric(

        "Kotak",

        (
            "CONNECTED"
            if kotak.connected
            else "DISCONNECTED"
        )

    )


    col2.metric(

        "Active Future",

        kotak.future_token
        or "NOT DISCOVERED"

    )


    col3.metric(

        "PCR Contracts",

        len(
            kotak.pcr_tokens
        )

    )


    col4.metric(

        "Supabase",

        (

            "READY"

            if (
                supabase.url
                and supabase.key
            )

            else
                "NOT CONFIGURED"

        )

    )


    # =========================================================================
    # LIVE RAW BUS HEALTH
    # =========================================================================

    st.markdown(
        "### Live Raw Bus Health"
    )


    live_status = (

        "READY"

        if (
            kotak.connected
            and kotak.future_token
        )

        else
            "NOT READY"

    )


    sup_health = (
        supabase.health()
    )


    st.write({

        "Kotak": (

            "CONNECTED"

            if kotak.connected

            else
                "DISCONNECTED"

        ),

        "Active Future": (

            kotak.future_token

            or "NOT DISCOVERED"

        ),

        "NFO Master Records":
            len(
                kotak.nfo_records
            ),

        "PCR Contracts":
            len(
                kotak.pcr_tokens
            ),

        "Supabase": (

            "REACHABLE"

            if sup_health.get(
                "reachable"
            )

            else
                "NOT READY"

        ),

        "Live Raw Producer":
            live_status,

    })


    # =========================================================================
    # DATA COVERAGE AUDIT
    # =========================================================================

    st.markdown(
        "### Required Data Coverage Audit"
    )


    st.markdown(
        "### Raw Data Contract"
    )


    st.code(

        "Kotak Neo -> LIVE RAW -> Supabase -> all 3 engines\n"
        "yfinance -> HISTORICAL RAW -> Supabase -> all 3 engines\n"
        "No features / scores / labels / regime / decisions cross the bus.",

        language=
            "text",

    )


    # =========================================================================
    # LOGS
    # =========================================================================

    if kotak.logs:

        with st.expander(

            "Discovery & Execution Logs",

            expanded=
                True

        ):

            for log in kotak.logs[-30:]:

                st.text(
                    log
                )


    # =========================================================================
    # LIVE PRODUCER MONITOR
    # =========================================================================

    worker_state = _worker_monitor_view()
    worker_status = str(worker_state.get("status", "STOPPED"))
    worker_connection = str(worker_state.get("connection", "DISCONNECTED"))

    if worker_status == "LIVE":
        st.success(
            "[LIVE] Persistent Kotak worker is active. "
            "Raw quotes are being published to Supabase `raw_observations`."
        )
    elif worker_status == "RECONNECTING":
        st.warning("RECONNECTING: worker is recovering the Kotak session.")
    elif worker_status == "FEED_LOST":
        st.warning("FEED LOST: waiting for automatic recovery.")
    elif worker_status == "AUTH_REQUIRED":
        st.error("AUTH REQUIRED: automatic recovery could not restore the session.")
    elif worker_status in {"STARTING", "AUTHENTICATING", "DISCOVERING"}:
        st.info("Worker is starting.")
    else:
        st.info("Persistent worker is not running.")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Worker", worker_status)
    m2.metric("Connection", worker_connection)
    m3.metric("Last Quotes", worker_state.get("last_quotes", 0))
    m4.metric(
        "Feed Age",
        (f"{worker_state.get('feed_age_sec')}s"
         if worker_state.get("feed_age_sec") is not None else "-"),
    )

    st.write({
        "Last Kotak Fetch": worker_state.get("last_kotak_fetch", "-"),
        "Last Supabase Write": worker_state.get("last_supabase_write", "-"),
        "Consecutive Failures": worker_state.get("consecutive_failures", 0),
        "Total Failures": worker_state.get("total_failures", 0),
        "Recovery Attempts": worker_state.get("recovery_attempts", 0),
        "Successful Recoveries": worker_state.get("successful_recoveries", 0),
        "Last Recovery": worker_state.get("last_recovery", "-"),
        "Active Future": worker_state.get("active_future", "-"),
        "Future Symbol": worker_state.get("future_symbol", "-"),
        "Future Expiry": worker_state.get("future_expiry", "-"),
        "PCR Contracts": worker_state.get("pcr_contracts", 0),
        "NFO Records": worker_state.get("nfo_records", 0),
        "Worker PID": worker_state.get("pid", "-"),
        "Auto Re-auth": ("ENABLED" if worker_state.get("auto_reauth", False) else "DISABLED"),
        "Last Error": worker_state.get("last_error", "-"),
    })

    if worker_state.get("updated_at"):
        st.caption("Worker state updated: " + str(worker_state["updated_at"]))

    if worker_status in {"STARTING", "AUTHENTICATING", "DISCOVERING", "LIVE", "FEED_LOST", "RECONNECTING"}:
        time.sleep(2.0)
        st.rerun()


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    if "--kotak-worker" in sys.argv:
        run_kotak_worker(os.getenv("KOTAK_STARTUP_TOTP", "").strip())
    else:
        main()
