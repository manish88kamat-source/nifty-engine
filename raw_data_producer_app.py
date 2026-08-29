#!/usr/init/env python3
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


IST = ZoneInfo("Asia/Kolkata")

def now_ist() -> datetime:
    return datetime.now(IST)

def today_ist() -> date:
    return now_ist().date()

CONFIG = {
    "neo_environment": "prod",
    "pcr_strike_count": 5,
    "pcr_strike_step": 50.0,
    "supabase_url": os.getenv("SUPABASE_URL", "").strip(),
    "supabase_key": os.getenv("SUPABASE_KEY", "").strip(),
    "poll_interval_sec": 3.0,
    "macro_every_n_cycles": 10,
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
        
        records = []
        try:
            response = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
            records = extract_records(response)
        except Exception as exc:
            self.log(f"Primary search_scrip failed: {exc}")

        if not records:
            try:
                response = self.client.search_scrip(exchange_segment="nse_fo", symbol="Nifty")
                records = extract_records(response)
            except Exception as exc:
                self.log(f"Secondary search_scrip failed: {exc}")

        if not records:
            raise RuntimeError("Kotak returned no structured NFO scrip records.")

        self.nfo_records = records
        self.log(f"Total raw scrip records retrieved: {len(records)}")
        return records

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
        spot_price = 24300.0
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
    @staticmethod
    def fetch_macro_data(tickers: List[str] = None) -> Dict[str, pd.DataFrame]:
        if tickers is None:
            tickers = ["GC=F", "SI=F", "DX-Y.NYB", "^GSPC"]
        data_map = {}
        try:
            raw_data = yf.download(tickers, period="5d", interval="1d", progress=False, group_by="ticker", auto_adjust=False)
            for ticker in tickers:
                try:
                    if len(tickers) == 1:
                        df = raw_data
                    else:
                        df = raw_data[ticker] if isinstance(raw_data, pd.DataFrame) and ticker in raw_data.columns else pd.DataFrame()
                    if not df.empty:
                        data_map[ticker] = df
                except Exception:
                    continue
        except Exception as exc:
            print(f"Yahoo fetch error: {exc}")
        return data_map


class SupabasePublisher:
    def __init__(self):
        self.url = env_or_secret("SUPABASE_URL", CONFIG["supabase_url"])
        self.key = env_or_secret("SUPABASE_KEY", CONFIG["supabase_key"])

    def publish_observation(self, source: str, symbol: str, token: str, raw_payload: dict) -> bool:
        if not self.url or not self.key:
            return False
        try:
            endpoint = f"{self.url.rstrip('/')}/rest/v1/raw_observations"
            headers = {
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal"
            }
            record = {
                "source": source,
                "symbol": symbol,
                "instrument_token": str(token),
                "observation_timestamp": now_ist().isoformat(),
                "raw": raw_payload
            }
            response = requests.post(endpoint, headers=headers, json=record, timeout=5)
            return response.status_code in (200, 201, 204)
        except Exception as exc:
            print(f"Supabase publish error: {exc}")
            return False


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

    kotak: KotakConnector = st.session_state.kotak
    supabase = SupabasePublisher()

    with st.sidebar:
        st.header("🔑 Authentication")
        totp_input = st.text_input("Live TOTP Code", type="password")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Connect Kotak"):
                try:
                    with st.spinner("Authenticating..."):
                        kotak.login(totp_override=totp_input)
                        st.success("Authenticated Successfully!")
                except Exception as exc:
                    st.error(str(exc))
        with col2:
            if st.button("Discover Instruments", disabled=not kotak.connected):
                try:
                    with st.spinner("Downloading NFO scrip master & discovering contracts..."):
                        kotak.discover_instruments()
                        st.success("Discovery Complete!")
                except Exception as exc:
                    st.error(str(exc))

        st.markdown("---")
        st.header("🚀 Producer Control")
        can_start = kotak.connected and kotak.future_token
        if not st.session_state.producer_running:
            if st.button("Start Raw Producer Loop", type="primary", disabled=not can_start):
                st.session_state.producer_running = True
                st.rerun()
        else:
            if st.button("Stop Producer Loop", type="secondary"):
                st.session_state.producer_running = False
                st.rerun()

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    col_s1.metric("Kotak Connection", "CONNECTED" if kotak.connected else "DISCONNECTED")
    col_s2.metric("Active Future Token", kotak.future_token if kotak.future_token else "NOT DISCOVERED")
    col_s3.metric("Mapped Options Count", len(kotak.pcr_tokens))
    col_s4.metric("NFO Records", len(kotak.nfo_records))

    if kotak.logs:
        with st.expander("Discovery & Execution Logs", expanded=True):
            for log in kotak.logs[-30:]:
                st.text(log)

    if st.session_state.producer_running:
        st.success("🟢 Raw Producer is active. Polling broker quotes and publishing to Supabase `raw_observations`...")
        
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
                    
                    success = supabase.publish_observation(
                        source="kotak_live",
                        symbol=symbol,
                        token=token,
                        raw_payload=quote
                    )
                    if success:
                        published_count += 1

                # Safe Yahoo Macro fetch isolation
                if poll_cycle % CONFIG["macro_every_n_cycles"] == 0:
                    try:
                        macro_data = YahooConnector.fetch_macro_data()
                        for ticker, df in macro_data.items():
                            if df.empty:
                                continue
                            latest_row = df.iloc[-1].to_dict()
                            clean_row = {}
                            for key, value in latest_row.items():
                                if pd.isna(value):
                                    clean_row[str(key)] = None
                                elif hasattr(value, "item"):
                                    try:
                                        clean_row[str(key)] = value.item()
                                    except Exception:
                                        clean_row[str(key)] = str(value)
                                else:
                                    clean_row[str(key)] = value

                            supabase.publish_observation(
                                source="yahoo_macro",
                                symbol=ticker,
                                token=ticker,
                                raw_payload=clean_row
                            )
                    except Exception:
                        pass # Ignore transient Yahoo formatting or network hiccups gracefully

                status_container.info(f"Last Poll: {now_ist().strftime('%H:%M:%S')} | Published {published_count} raw quotes | Options mapped: {len(kotak.pcr_tokens)}")
                poll_cycle += 1
            except Exception as exc:
                log_container.error(f"Producer loop exception: {exc}")
            
            time.sleep(float(CONFIG["poll_interval_sec"]))


if __name__ == "__main__":
    main()
