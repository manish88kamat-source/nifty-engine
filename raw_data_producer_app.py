#!/usr/bin/env python3
"""
Leak-Proof Raw Data Producer | Institutional Research Bus
- Zero local calculations, zero indicators, zero ML.
- Robust nearest-expiry Nifty Future token discovery & option mapping.
- Publishes raw normalized payloads directly to Supabase `raw_observations` via REST.
- Throttling-safe background loop (no st.rerun abuse).
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
import csv
import io
import zipfile
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
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


REQUIRED_DATA_MATRIX = {
    "NIFTY_3MIN": {
        "realtime": [
            "NIFTY spot OHLC/LTP", "nearest valid NIFTY future OHLC/LTP/volume/OI",
            "10 heavyweight quotes", "22 current-expiry PCR option contracts",
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

YFINANCE_LIMITS = {
    "intraday_max_days_documented": 60,
    "1h_requested_days_by_v7": 180,
    "1d_requested_days_by_v7": 320,
    "15m_requested_days_by_v7": 55,
}

CONFIG = {
    "neo_environment": "prod",
    "pcr_strike_count": 5,
    "pcr_strike_step": 50.0,
    "supabase_url": os.getenv("SUPABASE_URL", "").strip(),
    "supabase_key": os.getenv("SUPABASE_KEY", "").strip(),
    "poll_interval_sec": 3.0,
    "macro_every_n_cycles": 10,
    # Raw-history coverage required by the three current engines.
    "next_day_daily_days": 320,
    "next_day_mtf_hourly_days": 180,
    "next_day_mtf_15m_days": 55,
    "next_day_vix_days": 320,
    "nifty_history_days": 320,
    "history_batch_size": 250,
    "history_workers": 6,
    "supabase_timeout_sec": 15,
}


def generate_live_totp(secret_or_otp: str) -> str:
    raw = str(secret_or_otp or "").strip().replace(" ", "").upper()
    if raw.isdigit() and len(raw) == 6:
        return raw
    try:
        if len(raw) % 8:
            raw += "=" * (8 - len(raw) % 8)
        key = base64.b32decode(raw, casefold=True)
        counter = int(time.time() // 30)
        msg = struct.pack(">Q", counter)
        digest = hmac.new(key, msg, hashlib.sha1).digest()
        offset = digest[19] & 15
        token = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000
        return f"{token:06d}"
    except Exception:
        return raw


def normalize_kotak_mobile(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    if len(digits) != 10:
        return raw
    return "+91" + digits


def env_or_secret(name, default=""):
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


def _normalize_scrip_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize only scrip-master column spelling/whitespace."""
    out: Dict[str, Any] = {}
    for key, value in record.items():
        clean_key = str(key).strip().lstrip("\ufeff").strip()
        if clean_key.endswith(";"):
            clean_key = clean_key[:-1]
        out[clean_key] = value.strip() if isinstance(value, str) else value
    return out


def _csv_text_to_records(csv_text: str) -> List[Dict[str, Any]]:
    """Parse Kotak scrip-master CSV, including its JSON-envelope variant."""
    if not isinstance(csv_text, str):
        return []
    text_value = csv_text.lstrip("\ufeff\r\n\t ")
    if not text_value:
        return []

    if text_value.startswith("{"):
        try:
            envelope = json.loads(text_value)
            if isinstance(envelope, dict):
                for key in ("nse", "NSE", "nse_fo", "NSE_FO", "data"):
                    payload = envelope.get(key)
                    if isinstance(payload, str) and payload.strip():
                        csv_text = payload
                        break
        except Exception:
            pass

    try:
        reader = csv.DictReader(io.StringIO(str(csv_text).lstrip("\ufeff")))
        records: List[Dict[str, Any]] = []
        for row in reader:
            if not row:
                continue
            normalized = _normalize_scrip_record(dict(row))
            if (
                str(normalized.get("pSymbol", "")).strip()
                and str(normalized.get("pTrdSymbol", "")).strip()
            ):
                records.append(normalized)
        return records
    except Exception:
        return []


def _scrip_master_urls(response: Any) -> List[str]:
    """Accept both current documented URL and filesPaths response shapes."""
    urls: List[str] = []

    def add(value: Any) -> None:
        if isinstance(value, str):
            value = value.strip()
            if value.startswith(("http://", "https://")) and value not in urls:
                urls.append(value)
        elif isinstance(value, (list, tuple)):
            for item in value:
                add(item)

    if isinstance(response, str):
        add(response)
    elif isinstance(response, dict):
        add(response.get("filesPaths"))
        add(response.get("filePath"))
        add(response.get("url"))
        add(response.get("nse_fo"))
        add(response.get("NSE_FO"))
        add(response.get("data"))
        for v in response.values():
            if isinstance(v, (list, str)):
                add(v)
    return urls


def _extract_nfo_csv_payload(response: Any) -> List[Dict[str, Any]]:
    """Parse an already-returned CSV, ZIP archive bytes, or JSON-envelope payload."""
    if response is None:
        return []

    raw_bytes = None
    text_value = ""
    if hasattr(response, "content") and hasattr(response, "text"):
        raw_bytes = response.content
        text_value = response.text
    elif isinstance(response, bytes):
        raw_bytes = response
        try:
            text_value = response.decode("utf-8", errors="ignore")
        except Exception:
            text_value = ""
    elif isinstance(response, str):
        text_value = response
        raw_bytes = response.encode("utf-8", errors="ignore")
    elif isinstance(response, dict):
        for key in ("nse", "NSE", "nse_fo", "NSE_FO", "data", "file"):
            value = response.get(key)
            if value is not None:
                parsed = _extract_nfo_csv_payload(value)
                if parsed:
                    return parsed
        return []
    else:
        return []

    # Handle ZIP archives if returned by broker scrip master endpoints
    if raw_bytes and raw_bytes.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as z:
                for filename in z.namelist():
                    if filename.endswith(".csv"):
                        with z.open(filename) as f:
                            csv_text = f.read().decode("utf-8", errors="ignore")
                            parsed = _csv_text_to_records(csv_text)
                            if parsed:
                                return parsed
        except Exception:
            pass

    return _csv_text_to_records(text_value)


def extract_records(response: Any) -> List[Dict[str, Any]]:
    if response is None:
        return []
    if isinstance(response, list):
        return [x for x in response if isinstance(x, dict)]
    if isinstance(response, dict):
        for key in ("result", "data", "values", "records", "scrips"):
            value = response.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                nested = extract_records(value)
                if nested:
                    return nested
        return _extract_nfo_csv_payload(response)
    if isinstance(response, str):
        return _csv_text_to_records(response)
    return []


class KotakConnector:
    def __init__(self):
        self.consumer_key = env_or_secret("KOTAK_CONSUMER_KEY")
        self.mobile = normalize_kotak_mobile(env_or_secret("KOTAK_MOBILE"))
        self.ucc = env_or_secret("KOTAK_UCC")
        self.totp_secret = env_or_secret("KOTAK_TOTP")
        self.mpin = env_or_secret("KOTAK_MPIN")

        self.client = None
        self.connected = False
        self.future_token = None
        self.future_symbol = None
        self.future_expiry: Optional[date] = None
        self.spot_token = "26000"
        self.pcr_tokens = []
        self.option_contracts = {}
        self.nfo_records = []
        self.heavy_tokens = {
            "HDFCBANK": "1333", "RELIANCE": "2885", "ICICIBANK": "4963",
            "INFY": "1594", "ITC": "1660", "TCS": "11536",
            "LT": "11483", "AXISBANK": "5900", "KOTAKBANK": "1922", "SBIN": "3045"
        }
        self.logs = []

    def log(self, message: str):
        timestamp = now_ist().strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")
        self.logs = self.logs[-100:]

    def login(self, totp_override: str = "") -> bool:
        if NeoAPI is None:
            raise RuntimeError("neo_api_client library is not installed.")
        
        totp = totp_override.strip() or self.totp_secret
        if not all([self.consumer_key, self.mobile, self.ucc, totp, self.mpin]):
            raise RuntimeError("Missing Kotak Neo authentication credentials.")

        self.client = NeoAPI(environment=CONFIG["neo_environment"], consumer_key=self.consumer_key)
        
        step1 = self.client.totp_login(mobile_number=self.mobile, ucc=self.ucc, totp=generate_live_totp(totp))
        if isinstance(step1, dict) and (step1.get("error") or step1.get("Error")):
            raise RuntimeError(f"Login Step 1 Error: {step1.get('error') or step1.get('Error')}")
            
        step2 = self.client.totp_validate(mpin=self.mpin)
        if isinstance(step2, dict) and (step2.get("error") or step2.get("Error")):
            raise RuntimeError(f"Login Step 2 Error: {step2.get('error') or step2.get('Error')}")

        self.connected = True
        self.log("Kotak authentication successful.")
        return True

    def load_nfo_scrip_master(self) -> List[Dict[str, Any]]:
        if not self.connected:
            raise RuntimeError("Kotak connector is not authenticated.")

        records: List[Dict[str, Any]] = []

        # Strategy 1: SDK scrip_master() with no arguments (Standard neo_api_client v2 spec)
        try:
            resp = self.client.scrip_master()
            parsed = _extract_nfo_csv_payload(resp)
            if parsed:
                self.nfo_records = parsed
                self.log(f"NFO scrip_master() parsed successfully: {len(parsed)} records")
                return parsed
            urls = _scrip_master_urls(resp)
            for url in urls:
                if "nse_fo" in url.lower() or not urls:
                    try:
                        res = requests.get(url, timeout=20)
                        parsed = _extract_nfo_csv_payload(res)
                        if parsed:
                            self.nfo_records = parsed
                            self.log(f"NFO scrip_master URL downloaded: {len(parsed)} records")
                            return parsed
                    except Exception:
                        pass
        except Exception as exc:
            self.log(f"scrip_master() attempt failed: {exc}")

        # Strategy 2: SDK scrip_master with exchange segment arguments
        for seg in ("nse_fo", "NFO", "NSE_FO"):
            try:
                resp = self.client.scrip_master(exchange_segment=seg)
                parsed = _extract_nfo_csv_payload(resp)
                if parsed:
                    self.nfo_records = parsed
                    self.log(f"NFO scrip_master({seg}) parsed: {len(parsed)} records")
                    return parsed
                urls = _scrip_master_urls(resp)
                for url in urls:
                    try:
                        res = requests.get(url, timeout=20)
                        parsed = _extract_nfo_csv_payload(res)
                        if parsed:
                            self.nfo_records = parsed
                            self.log(f"NFO URL downloaded for {seg}: {len(parsed)} records")
                            return parsed
                    except Exception:
                        pass
            except Exception as exc:
                self.log(f"scrip_master({seg}) attempt failed: {exc}")

        # Strategy 3: search_scrip or scrip_search across different keyword signatures
        search_methods = ["search_scrip", "scrip_search"]
        search_args = [
            {"exchange_segment": "nse_fo", "symbol": "NIFTY"},
            {"exchange_segment": "nse_fo", "scrip": "NIFTY"},
            {"exchange_segment": "nse_fo"},
            {"exchange": "nse_fo", "symbol": "NIFTY"},
        ]
        for meth_name in search_methods:
            meth = getattr(self.client, meth_name, None)
            if callable(meth):
                for args in search_args:
                    try:
                        resp = meth(**args)
                        parsed = extract_records(resp)
                        if parsed:
                            self.nfo_records = parsed
                            self.log(f"NFO found via {meth_name}({args}): {len(parsed)} records")
                            return parsed
                    except Exception:
                        pass

        # Strategy 4: Direct public CDN fallback trying recent rolling dates
        today_date = today_ist()
        for delta_days in range(5):
            d = today_date - pd.Timedelta(days=delta_days)
            date_str = d.strftime("%Y-%m-%d")
            url = f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{date_str}/transformed/nse_fo.csv"
            try:
                res = requests.get(url, timeout=20)
                if res.status_code == 200:
                    parsed = _extract_nfo_csv_payload(res)
                    if parsed:
                        self.nfo_records = parsed
                        self.log(f"NFO fallback CDN downloaded for {date_str}: {len(parsed)} records")
                        return parsed
            except Exception:
                pass

        raise RuntimeError(
            "Kotak NFO discovery returned no usable records after trying all API methods and direct CDN fallbacks."
        )

    def discover_instruments(self) -> bool:
        if not self.connected or not self.client:
            raise RuntimeError("Kotak connector is not authenticated.")
        
        self.logs.clear()
        self.future_token = None
        self.future_symbol = None
        self.future_expiry = None
        self.pcr_tokens = []
        self.option_contracts = {}

        records = self.load_nfo_scrip_master()

        # --------------------------------------------------------
        # COLLECT ALL NIFTY FUTURE CANDIDATES
        # --------------------------------------------------------
        candidates = []
        for r in records:
            if not isinstance(r, dict):
                continue
            
            sym = str(r.get("pTrdSymbol", r.get("tradingSymbol", r.get("ts", r.get("symbol", ""))) )).upper().strip()
            token = str(r.get("pSymbol", r.get("pSymbolToken", r.get("instrument_token", r.get("token", "")))))
            inst_type = str(r.get("pInstType", "")).upper()
            option_type = str(r.get("pOptionType", "")).upper()

            if not token or not sym:
                continue
            if token == "26000":
                continue
            if option_type in ("CE", "PE") or sym.endswith("CE") or sym.endswith("PE"):
                continue
            
            # Must strictly be NIFTY and NOT variants like NIFTYNXT, BANKNIFTY, FINNIFTY, MIDCPNIFTY
            if not sym.startswith("NIFTY") or sym.startswith("NIFTYNXT"):
                continue
            if any(x in sym for x in ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]):
                continue

            is_fut = ("FUT" in inst_type or "FUT" in sym or sym.endswith("FUT"))
            if is_fut:
                candidates.append((sym, token, r))

        if not candidates:
            raise RuntimeError("Active NIFTY future contract could not be discovered from scrip master.")

        # --------------------------------------------------------
        # SELECT NEAREST NON-EXPIRED NIFTY FUTURE
        # --------------------------------------------------------
        today = today_ist()
        parsed_candidates = []

        for sym, token, record in candidates:
            expiry_text = str(record.get("pExpiryDate", "")).replace(";", "").strip()
            expiry = None

            if expiry_text:
                for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%b/%Y", "%Y-%m-%d"):
                    try:
                        expiry = datetime.strptime(expiry_text, fmt).date()
                        break
                    except Exception:
                        pass

            # Ignore expired contracts
            if expiry is not None and expiry < today:
                continue

            parsed_candidates.append(
                (
                    expiry if expiry else date.max,
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

        # Nearest valid expiry first
        parsed_candidates.sort(key=lambda x: x[0])

        selected_expiry, selected_symbol, selected_token, _ = parsed_candidates[0]

        self.future_symbol = selected_symbol
        self.future_token = selected_token
        self.future_expiry = None if selected_expiry == date.max else selected_expiry

        self.log(
            f"Bound Active Nifty Future: {self.future_symbol} "
            f"(Token: {self.future_token}, Expiry: "
            f"{self.future_expiry.isoformat() if self.future_expiry else 'UNKNOWN'})"
        )

        # --------------------------------------------------------
        # OPTIONS DISCOVERY (Around ATM)
        # --------------------------------------------------------
        spot_price = None
        try:
            spot_res = self.client.quotes(instrument_tokens=[{"instrument_token": self.spot_token, "exchange_segment": "nse_cm"}], quote_type="ltp")
            spot_recs = extract_records(spot_res)
            for sr in spot_recs:
                for k in ("lp", "last_price", "ltp"):
                    val = float(sr.get(k, 0))
                    if val > 0:
                        spot_price = val
                        break
        except Exception:
            pass

        if spot_price is None or spot_price <= 0:
            self.log("Spot unavailable; PCR option mapping skipped rather than using a fabricated ATM.")
            self.option_contracts = {}
            self.pcr_tokens = []
            return True

        step = CONFIG["pcr_strike_step"]
        atm = round(spot_price / step) * step
        count = CONFIG["pcr_strike_count"]
        target_strikes = {atm + (i * step) for i in range(-count, count + 1)}

        discovered = {}
        for r in records:
            if not isinstance(r, dict):
                continue
            sym = str(r.get("pTrdSymbol", r.get("tradingSymbol", ""))).upper().strip()
            token = str(r.get("pSymbol", r.get("pSymbolToken", "")))
            if not sym or not token:
                continue
            
            if not sym.startswith("NIFTY") or sym.startswith("NIFTYNXT"):
                continue
            if any(x in sym for x in ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]):
                continue

            opt_type = str(r.get("pOptionType", "")).upper()
            if opt_type not in ("CE", "PE"):
                if sym.endswith("CE"):
                    opt_type = "CE"
                elif sym.endswith("PE"):
                    opt_type = "PE"
            
            if opt_type not in ("CE", "PE"):
                continue

            strike_val = None
            for sk in ("dStrikePrice;", "dStrikePrice", "strike_price", "strikePrice"):
                if sk in r:
                    try:
                        v = float(str(r.get(sk)).replace(";", "").replace(",", ""))
                        if v > 1000000:
                            v /= 100.0
                        strike_val = v
                        break
                    except Exception:
                        pass
            
            if strike_val is None:
                match = re.search(r"(\d+(?:\.\d+)?)$", sym[:-2])
                if match:
                    try:
                        strike_val = float(match.group(1))
                    except Exception:
                        pass

            if strike_val is not None:
                strike_val = round(strike_val, 2)
                for target in target_strikes:
                    if abs(strike_val - target) < 0.5:
                        key = f"{opt_type}:{target:.2f}"
                        discovered[key] = {
                            "token": token,
                            "symbol": sym,
                            "option_type": opt_type,
                            "strike": target,
                        }
                        break

        self.option_contracts = discovered
        self.pcr_tokens = sorted(list({str(item["token"]) for item in discovered.values() if item.get("token")}))
        self.log(f"Discovery complete: Future={self.future_token}, Options mapped={len(self.pcr_tokens)}")
        return True

    def fetch_raw_quotes(self) -> List[Dict[str, Any]]:
        if not self.connected or not self.client:
            return []
        
        tokens_to_poll = [
            {"instrument_token": self.spot_token, "exchange_segment": "nse_cm"},
        ]
        if self.future_token:
            tokens_to_poll.append({"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"})
        
        for sym, tok in self.heavy_tokens.items():
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        
        for tok in self.pcr_tokens:
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})

        try:
            response = self.client.quotes(instrument_tokens=tokens_to_poll, quote_type="all")
            return extract_records(response)
        except Exception as exc:
            self.log(f"Quote fetch error: {exc}")
            return []


class YahooConnector:
    """Historical/raw Yahoo producer. No indicators, scores, resampling, or engine calculations."""

    @staticmethod
    def _download(
        ticker: str,
        period: Optional[str] = None,
        days: Optional[int] = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        try:
            kwargs = {
                "interval": interval,
                "progress": False,
                "auto_adjust": False,
                "threads": False,
            }

            if period:
                kwargs["period"] = period
            elif days is not None:
                end = now_ist()
                start = end - pd.Timedelta(days=int(days))
                kwargs["start"] = start.to_pydatetime()
                kwargs["end"] = end.to_pydatetime()

            df = yf.download(ticker, **kwargs)

            if df is None or df.empty:
                return pd.DataFrame()

            if isinstance(df.columns, pd.MultiIndex):
                if len(set(df.columns.get_level_values(-1))) == 1:
                    df.columns = [c[0] for c in df.columns]
                else:
                    df.columns = [
                        c[-1] if isinstance(c, tuple) else c
                        for c in df.columns
                    ]

            df = df.reset_index()

            time_col = (
                "Datetime"
                if "Datetime" in df.columns
                else "Date"
                if "Date" in df.columns
                else df.columns[0]
            )

            rename = {time_col: "event_timestamp"}
            for c in ("Open", "High", "Low", "Close", "Volume"):
                if c in df.columns:
                    rename[c] = c.lower()

            df = df.rename(columns=rename)

            keep = [
                c for c in
                ["event_timestamp", "open", "high", "low", "close", "volume"]
                if c in df.columns
            ]
            df = df[keep].copy()

            if "event_timestamp" not in df.columns or "close" not in df.columns:
                return pd.DataFrame()

            ts = pd.to_datetime(
                df["event_timestamp"],
                errors="coerce",
                utc=True,
            )
            df["event_timestamp"] = ts.dt.tz_convert(IST)

            for c in ("open", "high", "low", "close", "volume"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            df = (
                df.dropna(subset=["event_timestamp", "close"])
                .drop_duplicates("event_timestamp")
                .sort_values("event_timestamp")
                .reset_index(drop=True)
            )

            return df

        except Exception as exc:
            print(
                f"Yahoo history error for {ticker} "
                f"[{interval}, requested_days={days}]: {exc}"
            )
            return pd.DataFrame()

    @classmethod
    def fetch_symbol_history(
        cls,
        symbol: str,
        days: int,
        interval: str,
    ) -> pd.DataFrame:
        ticker = symbol if any(ch in str(symbol) for ch in ("^", "=", ".")) else f"{symbol}.NS"
        return cls._download(ticker, days=days, interval=interval)

    @classmethod
    def fetch_macro_data(
        cls,
        tickers: Optional[List[str]] = None,
    ) -> Dict[str, pd.DataFrame]:
        if tickers is None:
            tickers = ["GC=F", "SI=F", "DX-Y.NYB", "^GSPC"]
        return {
            ticker: cls._download(ticker, days=5, interval="1d")
            for ticker in tickers
        }

    @classmethod
    def fetch_vix(cls) -> pd.DataFrame:
        return cls._download(
            "^INDIAVIX",
            days=CONFIG["next_day_vix_days"],
            interval="1d",
        )


class HistoricalRawProducer:
    """Fetch and publish raw historical observations required by the engines."""

    def __init__(self, publisher):
        self.publisher = publisher
        self.last_stats: Dict[str, Any] = {}

    @staticmethod
    def _raw_rows(
        symbol: str,
        df: pd.DataFrame,
        timeframe: str,
        dataset: str,
        source: str,
    ) -> List[dict]:
        rows: List[dict] = []

        if df is None or df.empty:
            return rows

        for _, r in df.iterrows():
            event_ts = r.get("event_timestamp")
            if pd.isna(event_ts):
                continue

            def numeric_or_none(value):
                if value is None or pd.isna(value):
                    return None
                try:
                    return float(value)
                except Exception:
                    return None

            payload = {
                "dataset": dataset,
                "timeframe": timeframe,
                "event_timestamp": str(event_ts),
                "open": numeric_or_none(r.get("open")),
                "high": numeric_or_none(r.get("high")),
                "low": numeric_or_none(r.get("low")),
                "close": numeric_or_none(r.get("close")),
                "volume": numeric_or_none(r.get("volume")),
                "raw_source": source,
            }

            payload["observation_id"] = hashlib.sha256(
                (
                    f"{source}|{dataset}|{symbol}|"
                    f"{timeframe}|{payload['event_timestamp']}"
                ).encode("utf-8")
            ).hexdigest()

            rows.append(payload)

        return rows

    @staticmethod
    def coverage_report() -> Dict[str, Any]:
        return {
            "contracts": {
                "next_day_daily": {
                    "requested_days": CONFIG["next_day_daily_days"],
                    "interval": "1d",
                    "source": "yfinance",
                    "policy": "store actual returned raw history",
                },
                "next_day_mtf_hourly": {
                    "requested_days": CONFIG["next_day_mtf_hourly_days"],
                    "interval": "1h",
                    "source": "yfinance",
                    "status": "REQUEST_PRESERVED_SOURCE_COVERAGE_MAY_BE_SHORTER",
                    "policy": (
                        "Never fabricate, resample, duplicate, or relabel "
                        "shorter returned history as 180d."
                    ),
                },
                "next_day_mtf_15m": {
                    "requested_days": CONFIG["next_day_mtf_15m_days"],
                    "interval": "15m",
                    "source": "yfinance",
                    "policy": "store actual returned raw history",
                },
                "india_vix_daily": {
                    "requested_days": CONFIG["next_day_vix_days"],
                    "interval": "1d",
                    "source": "yfinance",
                    "policy": "store actual returned raw history",
                },
                "nifty_daily": {
                    "requested_days": CONFIG["nifty_history_days"],
                    "interval": "1d",
                    "source": "yfinance",
                    "policy": "store actual returned raw history",
                },
            },
            "intraday_source_rule": (
                "The producer requests the engine-required window exactly. "
                "If yfinance returns less, only the returned raw observations "
                "are published and the coverage gap is exposed."
            ),
        }

    def publish_history(
        self,
        symbol: str,
        df: pd.DataFrame,
        timeframe: str,
        dataset: str,
    ) -> int:
        rows = self._raw_rows(
            symbol,
            df,
            timeframe,
            dataset,
            "yfinance",
        )

        count = self.publisher.publish_observations_batch(
            source="yahoo_historical",
            symbol=symbol,
            token=f"{symbol}.NS",
            raw_payloads=rows,
        )

        self.last_stats[f"{dataset}:{symbol}"] = {
            "requested_timeframe": timeframe,
            "requested_days": None,
            "returned_rows": len(rows),
            "published_rows": count,
            "first_event_timestamp": rows[0]["event_timestamp"] if rows else None,
            "last_event_timestamp": rows[-1]["event_timestamp"] if rows else None,
            "source": "yfinance",
        }
        return count

    def publish_nifty_history(self) -> int:
        df = YahooConnector._download(
            "^NSEI",
            days=CONFIG["nifty_history_days"],
            interval="1d",
        )
        return self.publish_history(
            "NIFTY_SPOT",
            df,
            "1d",
            "nifty_spot_daily",
        )

    def publish_next_day_universe_history(
        self,
        symbols: List[str],
    ) -> Dict[str, int]:
        out: Dict[str, int] = {}

        def worker(sym: str):
            df = YahooConnector.fetch_symbol_history(
                sym,
                CONFIG["next_day_daily_days"],
                "1d",
            )
            return sym, self.publish_history(
                sym,
                df,
                "1d",
                "next_day_stock_daily",
            )

        with ThreadPoolExecutor(
            max_workers=CONFIG["history_workers"]
        ) as pool:
            futures = [pool.submit(worker, s) for s in symbols]
            for fut in as_completed(futures):
                try:
                    sym, count = fut.result()
                    out[sym] = count
                except Exception as exc:
                    out[f"ERROR:{len(out)}"] = 0
                    print(f"Next-Day history worker error: {exc}")

        return out

    def publish_mtf_history(
        self,
        symbols: List[str],
    ) -> Dict[str, Dict[str, int]]:
        out: Dict[str, Dict[str, int]] = {}

        def worker(sym: str):
            result: Dict[str, int] = {}

            requests_to_make = (
                ("1d", CONFIG["next_day_daily_days"], "next_day_mtf_daily"),
                ("1h", CONFIG["next_day_mtf_hourly_days"], "next_day_mtf_hourly"),
                ("15m", CONFIG["next_day_mtf_15m_days"], "next_day_mtf_15m"),
            )

            for interval, days, dataset in requests_to_make:
                df = YahooConnector.fetch_symbol_history(
                    sym,
                    days,
                    interval,
                )
                result[interval] = self.publish_history(
                    sym,
                    df,
                    interval,
                    dataset,
                )

            return sym, result

        with ThreadPoolExecutor(
            max_workers=CONFIG["history_workers"]
        ) as pool:
            futures = [pool.submit(worker, s) for s in symbols]
            for fut in as_completed(futures):
                try:
                    sym, result = fut.result()
                    out[sym] = result
                except Exception as exc:
                    print(f"MTF history worker error: {exc}")

        return out

    def publish_vix(self) -> int:
        df = YahooConnector.fetch_vix()
        return self.publish_history(
            "INDIAVIX",
            df,
            "1d",
            "india_vix_daily",
        )


class SupabasePublisher:
    """Append-only raw bus publisher."""
    def __init__(self):
        self.url = env_or_secret("SUPABASE_URL", CONFIG["supabase_url"])
        self.key = env_or_secret("SUPABASE_KEY", CONFIG["supabase_key"])

    def _headers(self):
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def publish_observations_batch(self, source: str, symbol: str, token: str, raw_payloads: List[dict]) -> int:
        if not self.url or not self.key or not raw_payloads:
            return 0
        total = 0
        endpoint = f"{self.url.rstrip('/')}/rest/v1/raw_observations"
        batch_size = max(1, int(CONFIG["history_batch_size"]))
        for i in range(0, len(raw_payloads), batch_size):
            batch = raw_payloads[i:i + batch_size]
            records = []
            for payload in batch:
                records.append({
                    "source": source,
                    "symbol": symbol,
                    "instrument_token": str(token),
                    "observation_timestamp": now_ist().isoformat(),
                    "raw": payload,
                })
            try:
                response = requests.post(
                    endpoint, headers=self._headers(), json=records,
                    timeout=float(CONFIG["supabase_timeout_sec"])
                )
                if response.status_code in (200, 201, 204):
                    total += len(records)
                else:
                    print(f"Supabase batch publish failed [{response.status_code}]: {response.text[:300]}")
            except Exception as exc:
                print(f"Supabase batch publish error: {exc}")
        return total

    def publish_observation(self, source: str, symbol: str, token: str, raw_payload: dict) -> bool:
        return self.publish_observations_batch(source, symbol, token, [raw_payload]) == 1


def main():
    if st is None:
        print("Streamlit not available.")
        return

    st.set_page_config(page_title="Institutional Raw Data Producer Bus", layout="wide")
    st.title("📡 Institutional Raw Data Producer Bus")

    if "kotak" not in st.session_state:
        st.session_state.kotak = KotakConnector()
    if "producer_running" not in st.session_state:
        st.session_state.producer_running = False
    if "historical_running" not in st.session_state:
        st.session_state.historical_running = False

    kotak: KotakConnector = st.session_state.kotak
    supabase = SupabasePublisher()
    historical = HistoricalRawProducer(supabase)

    with st.sidebar:
        st.header("🔑 Authentication")
        totp_input = st.text_input("Live TOTP Code", type="password")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Connect Kotak"):
                try:
                    kotak.login(totp_override=totp_input)
                    st.success("Authenticated Successfully!")
                except Exception as exc:
                    st.error(str(exc))
        with c2:
            if st.button("Discover Instruments", disabled=not kotak.connected):
                try:
                    kotak.discover_instruments()
                    st.success("Discovery Complete!")
                except Exception as exc:
                    st.error(str(exc))

        st.markdown("---")
        st.header("📡 Live Raw Producer")
        can_start = bool(kotak.connected and kotak.future_token and supabase.url and supabase.key)
        if not st.session_state.producer_running:
            if st.button("Start Raw Producer Loop", type="primary", disabled=not can_start):
                st.session_state.producer_running = True
                st.rerun()
        else:
            if st.button("Stop Producer Loop"):
                st.session_state.producer_running = False
                st.rerun()

        st.markdown("---")
        st.header("📚 Historical Raw Producer")
        st.caption("yfinance → Supabase only.")
        hist_symbols_text = st.text_area("NIFTY-500 symbols (one per line)", height=120, key="hist_symbols")
        mtf_symbols_text = st.text_area("MTF basket symbols (one per line)", height=100, key="mtf_symbols")
        if st.button("Publish NIFTY History", disabled=not (supabase.url and supabase.key)):
            with st.spinner("Publishing NIFTY historical raw data..."):
                count = historical.publish_nifty_history()
            st.success(f"NIFTY historical rows published: {count}")

        if st.button("Publish Next-Day 500 History", disabled=not (supabase.url and supabase.key)):
            try:
                symbols = [x.strip().upper() for x in hist_symbols_text.replace(",", "\n").splitlines() if x.strip()]
                if not symbols:
                    st.warning("Provide the NIFTY-500 symbol list first.")
                else:
                    with st.spinner(f"Publishing {len(symbols)} symbols × 320 daily bars..."):
                        stats = historical.publish_next_day_universe_history(symbols)
                    st.success(f"Completed: {len(stats)} symbols processed.")
            except Exception as exc:
                st.error(str(exc))

        if st.button("Publish V7 MTF + VIX", disabled=not (supabase.url and supabase.key)):
            symbols = [x.strip().upper() for x in mtf_symbols_text.replace(",", "\n").splitlines() if x.strip()]
            if not symbols:
                st.warning("Provide the shortlisted MTF symbols first.")
            else:
                with st.spinner(f"Publishing MTF history for {len(symbols)} symbols..."):
                    stats = historical.publish_mtf_history(symbols)
                    vix_count = historical.publish_vix()
                st.success(f"MTF completed for {len(stats)} symbols; VIX rows: {vix_count}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Kotak", "CONNECTED" if kotak.connected else "DISCONNECTED")
    col2.metric("Active Future", kotak.future_token or "NOT DISCOVERED")
    col3.metric("PCR Contracts", len(kotak.pcr_tokens))
    col4.metric("Supabase", "READY" if supabase.url and supabase.key else "NOT CONFIGURED")

    st.markdown("### Required Data Coverage Audit")
    coverage = HistoricalRawProducer.coverage_report()
    st.json(coverage)

    st.markdown("### Raw Data Contract")
    st.code(
        "Kotak Neo → LIVE RAW → Supabase → all 3 engines\n"
        "yfinance → HISTORICAL RAW → Supabase → all 3 engines\n"
        "No features / scores / labels / regime / decisions cross the bus.",
        language="text",
    )

    if kotak.logs:
        with st.expander("Discovery & Execution Logs", expanded=True):
            for log in kotak.logs[-30:]:
                st.text(log)

    if st.session_state.producer_running:
        st.success("🟢 Raw Producer is active. Kotak raw quotes are being published to Supabase `raw_observations`.")
        status_container = st.empty()
        log_container = st.empty()
        poll_cycle = 0
        while st.session_state.producer_running:
            try:
                raw_quotes = kotak.fetch_raw_quotes()
                published_count = 0
                for quote in raw_quotes:
                    if not isinstance(quote, dict):
                        continue
                    token = str(quote.get("exchange_token", quote.get("instrument_token", quote.get("pSymbol", quote.get("pSymbolToken", "UNKNOWN")))))
                    symbol = str(quote.get("display_symbol", quote.get("pTrdSymbol", quote.get("tradingSymbol", "UNKNOWN"))))
                    if supabase.publish_observation("kotak_live", symbol, token, quote):
                        published_count += 1

                if poll_cycle % CONFIG["macro_every_n_cycles"] == 0:
                    for ticker, df in YahooConnector.fetch_macro_data().items():
                        if df.empty:
                            continue
                        latest_row = df.iloc[-1].to_dict()
                        clean_row = {k: (None if pd.isna(v) else v.item() if hasattr(v, "item") else v) for k, v in latest_row.items()}
                        supabase.publish_observation("yahoo_macro", ticker, ticker, {
                            "dataset": "macro_daily", "timeframe": "1d", "event_timestamp": str(df.iloc[-1]["event_timestamp"]),
                            "raw": clean_row, "raw_source": "yfinance"
                        })

                status_container.info(f"Last Poll: {now_ist().strftime('%H:%M:%S')} | Published {published_count} raw quotes | Options mapped: {len(kotak.pcr_tokens)}")
                poll_cycle += 1
            except Exception as exc:
                log_container.error(f"Producer loop exception: {exc}")
            time.sleep(float(CONFIG["poll_interval_sec"]))


if __name__ == "__main__":
    main()
