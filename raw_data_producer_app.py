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
import csv
import io
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
import importlib.util
import platform
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

NSE_NIFTY500_CSV_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"


def fetch_nifty500_symbols_from_nse() -> List[str]:
    """Fetch the current NIFTY-500 constituent symbols from NSE's official CSV.

    This is universe metadata only; no market calculation or signal logic is performed.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36",
        "Accept": "text/csv,text/plain,*/*",
        "Referer": "https://www.nseindia.com/",
    }
    response = requests.get(NSE_NIFTY500_CSV_URL, headers=headers, timeout=20)
    response.raise_for_status()
    text = response.text.lstrip("\ufeff")
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        raise RuntimeError("NSE NIFTY-500 CSV returned no rows.")

    symbol_key = next((k for k in rows[0].keys() if str(k).strip().lower() in {"symbol", "symbols"}), None)
    if not symbol_key:
        raise RuntimeError(f"NSE NIFTY-500 CSV has no Symbol column. Columns: {list(rows[0].keys())}")

    symbols = []
    for row in rows:
        sym = str(row.get(symbol_key, "")).strip().upper()
        if sym and sym not in symbols:
            symbols.append(sym)
    if len(symbols) < 400:
        raise RuntimeError(f"NSE NIFTY-500 universe looks incomplete: {len(symbols)} symbols returned.")
    return symbols


YFINANCE_LIMITS = {
    "intraday_adaptive": True,
    "1h_requested_days_by_v7": 180,
    "1d_requested_days_by_v7": 320,
    "15m_requested_days_by_v7": 55,
    "policy": "request desired window, then use actual source-available window only",
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


def _normalize_kotak_nfo_expiry(record: Dict[str, Any]) -> Dict[str, Any]:
    """Mirror Kotak Neo v2 SDK's NFO expiry normalization for fallback rows."""
    key = "pExpiryDate"
    value = record.get(key)
    if value is None:
        return record

    raw = str(value).strip().replace(";", "")
    if not raw:
        return record

    # Already normalized by a server/CSV variant.
    for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%b/%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
            record[key] = parsed.strftime("%d%b%Y").upper()
            return record
        except Exception:
            pass

    # Kotak Neo v2 converts NSE-FO epoch seconds using this offset before
    # formatting as DDMMMYYYY. Keep the exact SDK convention.
    try:
        epoch = float(raw)
        if epoch > 0:
            epoch_seconds = epoch + 315511200
            parsed = datetime.fromtimestamp(epoch_seconds)
            record[key] = parsed.strftime("%d%b%Y").upper()
    except Exception:
        pass

    return record


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
                for key in ("nse", "NSE", "nse_fo", "NSE_FO"):
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
                records.append(_normalize_kotak_nfo_expiry(normalized))
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
    return urls


def _extract_nfo_csv_payload(response: Any) -> List[Dict[str, Any]]:
    """Parse an already-returned CSV or JSON-envelope payload."""
    if isinstance(response, str):
        return _csv_text_to_records(response)
    if isinstance(response, dict):
        for key in ("nse", "NSE", "nse_fo", "NSE_FO"):
            value = response.get(key)
            if isinstance(value, str):
                parsed = _csv_text_to_records(value)
                if parsed:
                    return parsed
    return []


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
        # Kotak Neo does not use Zerodha's numeric NIFTY spot token (26000).
        # Keep the real Kotak spot identifier separate from the ATM reference fallback.
        self.spot_token = "Nifty 50"
        self.atm_reference_price = None
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

        # Primary: existing tested search_scrip path.
        records: List[Dict[str, Any]] = []
        try:
            response = self.client.search_scrip(
                exchange_segment="nse_fo",
                symbol="NIFTY",
            )
            records = extract_records(response)
        except Exception as exc:
            self.log(f"Primary search_scrip failed: {exc}")

        # Secondary: same tested path with alternate casing.
        if not records:
            try:
                response = self.client.search_scrip(
                    exchange_segment="nse_fo",
                    symbol="Nifty",
                )
                records = extract_records(response)
            except Exception as exc:
                self.log(f"Secondary search_scrip failed: {exc}")

        if records:
            self.nfo_records = records
            self.log(f"Total raw NFO scrip records retrieved: {len(records)}")
            return records

        # Surgical fallback only: Kotak's official scrip_master contract can
        # return either a direct CSV URL or a filesPaths list. Some deployments
        # have also returned the CSV inside an {"nse": "..."} JSON envelope.
        try:
            master_response = self.client.scrip_master(
                exchange_segment="nse_fo"
            )

            records = _extract_nfo_csv_payload(master_response)
            if records:
                self.nfo_records = records
                self.log(
                    f"NFO fallback payload parsed directly: {len(records)} records"
                )
                return records

            urls = _scrip_master_urls(master_response)
            self.log(f"NFO scrip_master fallback URLs discovered: {len(urls)}")

            nfo_urls = [u for u in urls if "nse_fo" in u.lower()]
            urls = nfo_urls or urls

            for url in urls:
                try:
                    # Match the official Kotak Neo v2 SDK behavior:
                    # scrip_master() authenticates the API call that resolves
                    # the URL; the returned CDN/file URL is downloaded directly.
                    response = requests.get(
                        url,
                        headers={
                            "Accept": "text/csv,application/json,*/*",
                        },
                        timeout=25,
                    )
                    self.log(
                        f"NFO scrip-master download: HTTP "
                        f"{response.status_code}, bytes={len(response.content)}"
                    )
                    if response.status_code >= 400:
                        continue

                    parsed = _extract_nfo_csv_payload(response.text)

                    if not parsed:
                        try:
                            parsed = _extract_nfo_csv_payload(response.json())
                        except Exception:
                            pass

                    if parsed:
                        self.nfo_records = parsed
                        self.log(
                            f"NFO scrip-master fallback parsed: "
                            f"{len(parsed)} raw records"
                        )
                        return parsed
                except Exception as exc:
                    self.log(f"NFO scrip-master URL fallback failed: {exc}")

        except Exception as exc:
            self.log(f"NFO scrip_master fallback failed: {exc}")

        raise RuntimeError(
            "Kotak NFO discovery returned no usable records after the "
            "tested search_scrip path and official scrip_master fallback."
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
        # Primary reference: Kotak's native NIFTY spot identifier.
        # If that quote is unavailable, use the already discovered active
        # NIFTY future LTP ONLY as the ATM reference. This does not create a
        # synthetic spot observation and does not alter the engine calculations.
        spot_price = None
        try:
            spot_res = self.client.quotes(
                instrument_tokens=[
                    {"instrument_token": self.spot_token, "exchange_segment": "nse_cm"}
                ],
                quote_type="ltp",
            )
            spot_recs = extract_records(spot_res)
            for sr in spot_recs:
                for k in ("lp", "last_price", "ltp", "c", "close"):
                    try:
                        val = float(sr.get(k, 0))
                    except Exception:
                        val = 0.0
                    if val > 0:
                        spot_price = val
                        break
                if spot_price:
                    break
        except Exception as exc:
            self.log(f"Native NIFTY spot quote unavailable: {exc}")

        if spot_price is None or spot_price <= 0:
            # No hardcoded token and no fabricated price. Reuse the LTP of the
            # future contract that was just resolved from Kotak's live master.
            try:
                fut_res = self.client.quotes(
                    instrument_tokens=[
                        {"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"}
                    ],
                    quote_type="ltp",
                )
                fut_recs = extract_records(fut_res)
                for fr in fut_recs:
                    for k in ("lp", "last_price", "ltp", "c", "close"):
                        try:
                            val = float(fr.get(k, 0))
                        except Exception:
                            val = 0.0
                        if val > 0:
                            spot_price = val
                            break
                    if spot_price:
                        break
                if spot_price and spot_price > 0:
                    self.log(
                        f"NIFTY spot quote unavailable; using active future LTP "
                        f"{spot_price:.2f} as ATM reference (Token: {self.future_token})."
                    )
            except Exception as exc:
                self.log(f"Active NIFTY future LTP fallback unavailable: {exc}")

        if spot_price is None or spot_price <= 0:
            self.log("No valid NIFTY price reference; PCR option mapping skipped.")
            self.option_contracts = {}
            self.pcr_tokens = []
            return True

        self.atm_reference_price = float(spot_price)
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

            # PCR must stay on the same current/nearest expiry as the bound
            # NIFTY future. Never mix strikes from another expiry.
            option_expiry_text = str(r.get("pExpiryDate", "")).replace(";", "").strip()
            option_expiry = None
            if option_expiry_text:
                for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%b/%Y", "%Y-%m-%d"):
                    try:
                        option_expiry = datetime.strptime(option_expiry_text, fmt).date()
                        break
                    except Exception:
                        pass
            if self.future_expiry is not None and option_expiry is not None:
                if option_expiry != self.future_expiry:
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

    # yfinance/Yahoo intraday coverage is source-limited and can vary by ticker.
    # We therefore never force an oversized intraday window.  The connector
    # tries the requested window first, then progressively smaller source-safe
    # windows and keeps ONLY the rows Yahoo actually returns.
    INTRADAY_FALLBACK_DAYS = {
        "1h": (180, 120, 90, 60, 30, 14, 7, 3, 1),
        "60m": (180, 120, 90, 60, 30, 14, 7, 3, 1),
        "15m": (55, 50, 45, 30, 14, 7, 3, 1),
        "30m": (60, 45, 30, 14, 7, 3, 1),
        "5m": (30, 14, 7, 3, 1),
        "2m": (30, 14, 7, 3, 1),
        "1m": (7, 3, 1),
    }
    last_diagnostics: Dict[str, Any] = {}

    @staticmethod
    def _clean_downloaded_frame(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize a real Yahoo response without creating observations."""
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
            "Datetime" if "Datetime" in df.columns
            else "Date" if "Date" in df.columns
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

        ts = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True)
        df["event_timestamp"] = ts.dt.tz_convert(IST)
        for c in ("open", "high", "low", "close", "volume"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        return (
            df.dropna(subset=["event_timestamp", "close"])
            .drop_duplicates("event_timestamp")
            .sort_values("event_timestamp")
            .reset_index(drop=True)
        )

    @classmethod
    def _request_days(cls, ticker: str, days: int, interval: str) -> pd.DataFrame:
        """Request one real source window."""
        end = now_ist()
        start = end - pd.Timedelta(days=int(days))
        kwargs = {
            "interval": interval,
            "progress": False,
            "auto_adjust": False,
            "threads": False,
            "start": start.to_pydatetime(),
            "end": end.to_pydatetime(),
        }
        return cls._clean_downloaded_frame(yf.download(ticker, **kwargs))

    @classmethod
    def _raw_yahoo_chart_probe(
        cls,
        ticker: str,
        days: int,
        interval: str,
    ) -> Dict[str, Any]:
        """Diagnostic-only direct Yahoo Chart API probe.

        This does NOT publish data and does NOT replace yfinance.  It exists to
        separate a Yahoo/network failure from a yfinance transport/adapter
        failure. Yahoo's v8 Chart endpoint is the OHLCV endpoint used by many
        independent clients and does not require a crumb for chart data.
        """
        end = now_ist()
        start = end - pd.Timedelta(days=int(days))
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": interval,
            "events": "history",
            "includePrePost": "false",
        }
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
        }
        started = time.perf_counter()
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=15,
            )
            elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
            body_preview = response.text[:500]
            payload = None
            try:
                payload = response.json()
            except Exception:
                payload = None

            chart = payload.get("chart", {}) if isinstance(payload, dict) else {}
            api_error = chart.get("error") if isinstance(chart, dict) else None
            results = chart.get("result") if isinstance(chart, dict) else None
            result = results[0] if results else None
            timestamps = (result or {}).get("timestamp") or []

            return {
                "transport": "direct_yahoo_chart",
                "url_host": "query1.finance.yahoo.com",
                "ticker": ticker,
                "interval": interval,
                "requested_days": int(days),
                "http_status": int(response.status_code),
                "elapsed_ms": elapsed_ms,
                "rows": int(len(timestamps)),
                "api_error": api_error,
                "response_preview": body_preview if response.status_code != 200 else "",
                "status": (
                    "DATA"
                    if response.ok and result and timestamps
                    else "YAHOO_API_ERROR"
                    if api_error
                    else "HTTP_ERROR"
                    if not response.ok
                    else "EMPTY_RESULT"
                ),
            }
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
            return {
                "transport": "direct_yahoo_chart",
                "url_host": "query1.finance.yahoo.com",
                "ticker": ticker,
                "interval": interval,
                "requested_days": int(days),
                "http_status": None,
                "elapsed_ms": elapsed_ms,
                "rows": 0,
                "api_error": None,
                "response_preview": "",
                "status": "TRANSPORT_EXCEPTION",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }

    @classmethod
    def deep_diagnostic(cls) -> Dict[str, Any]:
        """Deep, non-publishing source diagnosis.

        Four representative contracts are tested through both yfinance and
        Yahoo's direct Chart API. The result is diagnostic only; no fallback
        source is silently introduced into the publisher.
        """
        probes = [
            ("NIFTY daily", "^NSEI", 320, "1d"),
            ("Representative 1h", "RELIANCE.NS", 180, "1h"),
            ("Representative 15m", "RELIANCE.NS", 55, "15m"),
            ("India VIX daily", "^INDIAVIX", 320, "1d"),
        ]
        out: Dict[str, Any] = {
            "diagnostic_only": True,
            "publishes_data": False,
            "yfinance_version": getattr(yf, "__version__", "unknown"),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "curl_cffi_installed": importlib.util.find_spec("curl_cffi") is not None,
            "probes": {},
        }

        try:
            import curl_cffi  # type: ignore
            out["curl_cffi_version"] = getattr(curl_cffi, "__version__", "unknown")
        except Exception:
            out["curl_cffi_version"] = None

        for label, ticker, days, interval in probes:
            yf_started = time.perf_counter()
            try:
                df = cls._download(ticker, days=days, interval=interval)
                yf_diag = dict(cls.last_diagnostics)
                yf_result = {
                    "status": yf_diag.get("status", "AVAILABLE" if not df.empty else "NO_DATA"),
                    "rows": int(len(df)),
                    "actual_returned_days": cls._actual_days(df),
                    "source_window_used_days": yf_diag.get("source_window_used_days"),
                    "attempted_windows_days": yf_diag.get("attempted_windows_days", []),
                    "last_error": yf_diag.get("last_error"),
                    "error": yf_diag.get("error"),
                    "elapsed_ms": round((time.perf_counter() - yf_started) * 1000, 1),
                }
            except Exception as exc:
                yf_result = {
                    "status": "EXCEPTION",
                    "rows": 0,
                    "actual_returned_days": 0.0,
                    "source_window_used_days": None,
                    "attempted_windows_days": [],
                    "last_error": None,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "elapsed_ms": round((time.perf_counter() - yf_started) * 1000, 1),
                }

            direct = cls._raw_yahoo_chart_probe(ticker, days, interval)
            if yf_result.get("rows", 0) > 0 and direct.get("rows", 0) > 0:
                diagnosis = "YFINANCE_AND_YAHOO_OK"
            elif yf_result.get("rows", 0) == 0 and direct.get("rows", 0) > 0:
                diagnosis = "YAHOO_OK_YFINANCE_PATH_FAIL"
            elif yf_result.get("rows", 0) == 0 and direct.get("rows", 0) == 0:
                diagnosis = "BOTH_PATHS_FAIL_OR_WINDOW_UNAVAILABLE"
            else:
                diagnosis = "YFINANCE_OK_DIRECT_PATH_EMPTY_OR_REJECTED"

            out["probes"][label] = {
                "ticker": ticker,
                "requested_days": days,
                "interval": interval,
                "yfinance": yf_result,
                "direct_yahoo_chart": direct,
                "diagnosis": diagnosis,
            }

        return out

    @classmethod
    def _download(
        cls,
        ticker: str,
        period: Optional[str] = None,
        days: Optional[int] = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """
        Fetch ONLY real Yahoo/yfinance observations.

        For intraday data, Yahoo may reject a requested window that is longer
        than the source currently permits for that interval/ticker.  Instead of
        treating that as NO_DATA, we retry with smaller windows until Yahoo
        returns real rows.  The returned frame is never padded, resampled,
        duplicated, or relabelled as the original requested coverage.
        """
        requested_days = int(days) if days is not None else None
        attempts = []

        try:
            if period:
                attempts = [("period", period)]
                df = cls._clean_downloaded_frame(yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    progress=False,
                    auto_adjust=False,
                    threads=False,
                ))
                cls.last_diagnostics = {
                    "ticker": ticker, "interval": interval,
                    "requested_days": requested_days, "requested_period": period,
                    "attempted_windows_days": [],
                    "actual_returned_days": cls._actual_days(df),
                    "returned_rows": int(len(df)),
                    "status": "AVAILABLE" if not df.empty else "NO_DATA",
                    "coverage_policy": "actual returned rows only",
                }
                return df

            if days is None:
                days = 1

            if interval in cls.INTRADAY_FALLBACK_DAYS:
                candidates = [int(days)] + [
                    int(x) for x in cls.INTRADAY_FALLBACK_DAYS[interval]
                    if int(x) < int(days)
                ]
                # De-duplicate while preserving order.
                windows = list(dict.fromkeys(candidates))
            else:
                # Daily/other non-intraday requests can safely use the exact
                # requested window; if the source has less, Yahoo returns less.
                windows = [int(days)]

            df = pd.DataFrame()
            used_days = None
            last_error = None

            for window_days in windows:
                attempts.append(window_days)
                try:
                    candidate = cls._request_days(ticker, window_days, interval)
                    if not candidate.empty:
                        df = candidate
                        used_days = window_days
                        break
                except Exception as exc:
                    last_error = str(exc)
                    continue

            actual_days = cls._actual_days(df)
            status = "AVAILABLE" if not df.empty else "NO_DATA"
            if used_days is not None and requested_days is not None and used_days < requested_days:
                status = "AVAILABLE_SHORTER_SOURCE_WINDOW"

            cls.last_diagnostics = {
                "ticker": ticker,
                "interval": interval,
                "requested_days": requested_days,
                "attempted_windows_days": attempts,
                "source_window_used_days": used_days,
                "actual_returned_days": actual_days,
                "returned_rows": int(len(df)),
                "status": status,
                "coverage_policy": "actual returned rows only",
            }
            if last_error:
                cls.last_diagnostics["last_error"] = last_error
            return df

        except Exception as exc:
            cls.last_diagnostics = {
                "ticker": ticker, "interval": interval,
                "requested_days": requested_days,
                "attempted_windows_days": attempts,
                "source_window_used_days": None,
                "actual_returned_days": 0,
                "returned_rows": 0,
                "status": "NO_DATA",
                "coverage_policy": "actual returned rows only",
                "error": str(exc),
            }
            print(
                f"Yahoo history error for {ticker} "
                f"[{interval}, requested_days={days}]: {exc}"
            )
            return pd.DataFrame()

    @staticmethod
    def _actual_days(df: pd.DataFrame) -> float:
        if df is None or df.empty or "event_timestamp" not in df.columns:
            return 0.0
        try:
            delta = df["event_timestamp"].iloc[-1] - df["event_timestamp"].iloc[0]
            return round(max(0.0, delta.total_seconds() / 86400.0), 2)
        except Exception:
            return 0.0

    @classmethod
    def health_probe(cls) -> Dict[str, Any]:
        """Probe real source coverage and report actual returned history."""
        probes = [
            ("NIFTY daily", "^NSEI", 320, "1d"),
            ("Representative 1h", "RELIANCE.NS", 180, "1h"),
            ("Representative 15m", "RELIANCE.NS", 55, "15m"),
            ("India VIX daily", "^INDIAVIX", 320, "1d"),
        ]
        out: Dict[str, Any] = {}
        for label, ticker, days, interval in probes:
            df = cls._download(ticker, days=days, interval=interval)
            diag = dict(cls.last_diagnostics)
            out[label] = {
                "ticker": ticker,
                "requested_days": days,
                "interval": interval,
                "returned_rows": int(len(df)),
                "actual_returned_days": cls._actual_days(df),
                "source_window_used_days": diag.get("source_window_used_days"),
                "attempted_windows_days": diag.get("attempted_windows_days", []),
                "first_timestamp": str(df.iloc[0]["event_timestamp"]) if not df.empty else None,
                "last_timestamp": str(df.iloc[-1]["event_timestamp"]) if not df.empty else None,
                "status": diag.get("status", "AVAILABLE" if not df.empty else "NO_DATA"),
                "coverage_policy": "actual returned rows only",
            }
        return out

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
    """
    Fetch and publish raw historical observations required by the engines.

    This class deliberately performs NO:
      - indicators
      - feature engineering
      - scoring
      - regime detection
      - labels
      - signals
      - strategy logic
      - timeframe resampling

    A requested history window is never silently represented as complete when
    the source returned less data. The actual returned timestamps/row count
    are preserved in the raw payload and coverage audit.
    """

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

            # Deterministic identity for the raw observation.
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
        """
        Source/request audit only.

        V7 requested windows remain visible, but the producer now uses an
        adaptive source-window policy for intraday intervals. If Yahoo cannot
        serve the requested window, smaller windows are tried and only actual
        returned rows are published. No missing portion is fabricated.
        """
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
                "The producer requests the engine-required window first. "
                "If Yahoo rejects or cannot supply that window, the producer "
                "tries smaller source-safe windows and publishes only actual "
                "returned raw observations; no resampling, duplication, or "
                "relabeling is allowed."
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
    """Append-only raw bus publisher. Calculations never happen here."""
    def __init__(self, url_override: str = "", key_override: str = ""):
        # UI overrides are session-scoped only. Environment/Streamlit secrets
        # remain the non-UI fallback. No credentials are written into source.
        self.url = str(url_override or env_or_secret("SUPABASE_URL", CONFIG["supabase_url"])).strip()
        self.key = str(key_override or env_or_secret("SUPABASE_KEY", CONFIG["supabase_key"])).strip()

    def _headers(self):
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def health(self) -> Dict[str, Any]:
        if not self.url or not self.key:
            return {"configured": False, "reachable": False, "error": "Supabase URL/Key missing"}
        endpoint = f"{self.url.rstrip('/')}/rest/v1/raw_observations"
        try:
            response = requests.get(
                endpoint,
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Accept": "application/json",
                },
                params={"select": "id", "limit": "1"},
                timeout=float(CONFIG["supabase_timeout_sec"]),
            )
            return {
                "configured": True,
                "reachable": response.status_code in (200, 206),
                "http_status": response.status_code,
                "error": "" if response.status_code in (200, 206) else response.text[:250],
            }
        except Exception as exc:
            return {"configured": True, "reachable": False, "error": str(exc)}

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

    with st.sidebar:
        st.header("🔑 Authentication")
        totp_input = st.text_input("Live TOTP Code", type="password")

        st.markdown("---")
        st.header("🗄️ Supabase RAW BUS")
        supabase_url_input = st.text_input(
            "Supabase URL",
            value=st.session_state.get("supabase_url_input", env_or_secret("SUPABASE_URL", "")),
            key="supabase_url_input",
            placeholder="https://your-project.supabase.co",
        )
        supabase_key_input = st.text_input(
            "Supabase Key",
            value=st.session_state.get("supabase_key_input", env_or_secret("SUPABASE_KEY", "")),
            type="password",
            key="supabase_key_input",
            placeholder="Supabase anon/service key",
        )
        supabase = SupabasePublisher(
            url_override=supabase_url_input,
            key_override=supabase_key_input,
        )
        historical = HistoricalRawProducer(supabase)

        if st.button("Test Supabase RAW BUS"):
            health = supabase.health()
            if health.get("reachable"):
                st.success("Supabase RAW BUS reachable.")
            else:
                st.error(health.get("error", "Supabase connection failed."))

        st.markdown("---")
        st.header("📡 Live Raw Producer")
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

        can_start = bool(kotak.connected and kotak.future_token and supabase.url and supabase.key)
        if not st.session_state.producer_running:
            if st.button("Start Raw Producer Loop", type="primary", disabled=not can_start):
                st.session_state.producer_running = True
                st.rerun()
        else:
            if st.button("Stop Producer Loop"):
                st.session_state.producer_running = False
                st.rerun()

        if st.button(
            "Test Live Raw → Supabase",
            disabled=not can_start,
        ):
            try:
                raw_quotes = kotak.fetch_raw_quotes()
                published = 0
                for quote in raw_quotes:
                    if not isinstance(quote, dict):
                        continue
                    token = str(
                        quote.get(
                            "exchange_token",
                            quote.get("instrument_token", quote.get("pSymbol", quote.get("pSymbolToken", "UNKNOWN")))
                        )
                    )
                    symbol = str(
                        quote.get(
                            "display_symbol",
                            quote.get("pTrdSymbol", quote.get("tradingSymbol", "UNKNOWN"))
                        )
                    )
                    if supabase.publish_observation("kotak_live", symbol, token, quote):
                        published += 1
                st.session_state["last_live_test"] = {
                    "kotak_quotes_received": len(raw_quotes),
                    "supabase_rows_published": published,
                    "active_future": kotak.future_token,
                    "pcr_contracts": len(kotak.pcr_tokens),
                    "status": "PASS" if raw_quotes and published else "PARTIAL/NO_DATA",
                }
            except Exception as exc:
                st.session_state["last_live_test"] = {"status": "ERROR", "error": str(exc)}
        if st.session_state.get("last_live_test"):
            st.json(st.session_state["last_live_test"])

        st.markdown("---")
        st.header("📚 Historical Raw Producer")
        st.caption("yfinance → Supabase only. Requested windows are preserved; only actual returned source history is published.")
        if st.button("Test yfinance Data Source"):
            with st.spinner("Probing yfinance source coverage..."):
                yf_health = YahooConnector.health_probe()
            st.session_state["yf_health"] = yf_health
        if st.session_state.get("yf_health"):
            st.json(st.session_state["yf_health"])

        if st.button("🔬 Deep-Diagnose Yahoo / yfinance"):
            with st.spinner("Running yfinance + direct Yahoo Chart comparison..."):
                st.session_state["yf_deep_diagnostic"] = YahooConnector.deep_diagnostic()

        if st.session_state.get("yf_deep_diagnostic"):
            st.markdown("#### Yahoo / yfinance Deep Diagnostic")
            st.caption(
                "Diagnostic only: no data is published and no fallback source is silently introduced. "
                "This compares the yfinance path against Yahoo's direct v8 Chart OHLCV endpoint."
            )
            st.json(st.session_state["yf_deep_diagnostic"])
        st.caption("Universe metadata is loaded from NSE; historical prices remain yfinance → Supabase only.")

        if "hist_symbols" not in st.session_state:
            st.session_state.hist_symbols = ""
        if "mtf_symbols" not in st.session_state:
            st.session_state.mtf_symbols = ""

        if st.button("Load NIFTY-500 from NSE"):
            try:
                with st.spinner("Loading current NIFTY-500 constituent list from NSE..."):
                    nse_symbols = fetch_nifty500_symbols_from_nse()
                st.session_state.hist_symbols = "\n".join(nse_symbols)
                st.success(f"Loaded {len(nse_symbols)} NIFTY-500 symbols from NSE.")
                st.rerun()
            except Exception as exc:
                st.error(f"NIFTY-500 universe load failed: {exc}")

        hist_symbols_text = st.text_area(
            "NIFTY-500 symbols (one per line)",
            height=120,
            key="hist_symbols",
        )
        mtf_symbols_text = st.text_area(
            "MTF basket symbols (one per line)",
            height=100,
            key="mtf_symbols",
        )

        if st.button("Publish NIFTY History", type="primary"):
            if not supabase.url or not supabase.key:
                st.error("Supabase URL/Key missing. Enter them in the sidebar first.")
            else:
                try:
                    with st.spinner("Publishing NIFTY historical raw data..."):
                        count = historical.publish_nifty_history()
                    st.success(f"NIFTY historical rows published: {count}")
                except Exception as exc:
                    st.error(f"NIFTY history publish failed: {exc}")

        if st.button("Publish Next-Day 500 History"):
            if not supabase.url or not supabase.key:
                st.error("Supabase URL/Key missing. Enter them in the sidebar first.")
            else:
                try:
                    symbols = [x.strip().upper() for x in hist_symbols_text.replace(",", "\n").splitlines() if x.strip()]
                    if not symbols:
                        with st.spinner("No list supplied — loading current NIFTY-500 list from NSE..."):
                            symbols = fetch_nifty500_symbols_from_nse()
                        st.session_state.hist_symbols = "\n".join(symbols)
                        st.info(f"Using {len(symbols)} symbols loaded from NSE.")
                    with st.spinner(f"Publishing {len(symbols)} symbols × 320 daily bars..."):
                        stats = historical.publish_next_day_universe_history(symbols)
                    st.success(f"Completed: {len(stats)} symbols processed.")
                except Exception as exc:
                    st.error(f"Next-Day 500 history publish failed: {exc}")

        if st.button("Publish V7 MTF + VIX"):
            if not supabase.url or not supabase.key:
                st.error("Supabase URL/Key missing. Enter them in the sidebar first.")
            else:
                symbols = [x.strip().upper() for x in mtf_symbols_text.replace(",", "\n").splitlines() if x.strip()]
                if not symbols:
                    st.warning("Provide the shortlisted MTF symbols first. The producer will not invent a basket.")
                else:
                    try:
                        with st.spinner(f"Publishing MTF history for {len(symbols)} symbols..."):
                            stats = historical.publish_mtf_history(symbols)
                            vix_count = historical.publish_vix()
                        st.success(f"MTF completed for {len(stats)} symbols; VIX rows: {vix_count}")
                    except Exception as exc:
                        st.error(f"MTF + VIX publish failed: {exc}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Kotak", "CONNECTED" if kotak.connected else "DISCONNECTED")
    col2.metric("Active Future", kotak.future_token or "NOT DISCOVERED")
    col3.metric("PCR Contracts", len(kotak.pcr_tokens))
    col4.metric("Supabase", "READY" if supabase.url and supabase.key else "NOT CONFIGURED")

    st.markdown("### Live Raw Bus Health")
    live_status = "READY" if (kotak.connected and kotak.future_token) else "NOT READY"
    sup_health = supabase.health()
    st.write({
        "Kotak": "CONNECTED" if kotak.connected else "DISCONNECTED",
        "Active Future": kotak.future_token or "NOT DISCOVERED",
        "NFO Master Records": len(kotak.nfo_records),
        "PCR Contracts": len(kotak.pcr_tokens),
        "Supabase": "REACHABLE" if sup_health.get("reachable") else "NOT READY",
        "Live Raw Producer": live_status,
    })

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

                # Macro raw data is supplementary market context; it remains raw and uncalculated.
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
