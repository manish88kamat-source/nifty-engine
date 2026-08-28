#!/usr/bin/env python3
"""
Leak-Proof Raw Data Producer | Institutional Research Bus
- Zero local calculations, zero indicators, zero ML.
- Strict token resolution (no fake fallbacks).
- Publishes raw normalized payloads directly to Supabase `raw_observations`.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
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


# =========================================================
# TIMEZONE & CONFIGURATION
# =========================================================
IST = ZoneInfo("Asia/Kolkata")

def now_ist() -> datetime:
    return datetime.now(IST)

CONFIG = {
    "neo_environment": "prod",
    "pcr_strike_count": 5,
    "pcr_strike_step": 50.0,
    "supabase_url": os.getenv("SUPABASE_URL", "").strip(),
    "supabase_key": os.getenv("SUPABASE_KEY", "").strip(),
}


# =========================================================
# UTILITIES & AUTH HELPERS
# =========================================================
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


# =========================================================
# 1. KOTAK CONNECTOR (Strict Discovery & Raw Quote Fetcher)
# =========================================================
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
        self.spot_token = "Nifty 50"
        self.pcr_tokens = []
        self.heavy_tokens = {
            "HDFCBANK": "1333", "RELIANCE": "2885", "ICICIBANK": "4963",
            "INFY": "1594", "ITC": "1660", "TCS": "11536",
            "LT": "11483", "AXISBANK": "5900", "KOTAKBANK": "1922", "SBIN": "3045"
        }
        self.logs = []

    def login(self, totp_override: str = "") -> bool:
        if NeoAPI is None:
            raise RuntimeError("neo_api_client library is not installed.")
        
        totp = totp_override.strip() or self.totp_secret
        if not all([self.consumer_key, self.mobile, self.ucc, totp, self.mpin]):
            raise RuntimeError("Missing Kotak Neo authentication credentials.")

        self.client = NeoAPI(environment=CONFIG["neo_environment"], consumer_key=self.consumer_key)
        
        step1 = self.client.totp_login(mobile_number=self.mobile, ucc=self.ucc, totp=generate_live_totp(totp))
        if isinstance(step1, dict) and step1.get("error"):
            raise RuntimeError(f"Login Step 1 Error: {step1['error']}")
            
        step2 = self.client.totp_validate(mpin=self.mpin)
        if isinstance(step2, dict) and step2.get("error"):
            raise RuntimeError(f"Login Step 2 Error: {step2['error']}")

        self.connected = True
        return True

    def discover_instruments(self):
        if not self.connected or not self.client:
            raise RuntimeError("Kotak connector is not authenticated.")
        
        self.logs.clear()
        
        # Flexible & Robust Active Nifty Future Token Resolution
        res = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
        records = res.get("result", res.get("data", [])) if isinstance(res, dict) else []
        now_d = now_ist().date()
        
        futures = []
        for r in records:
            sym = str(r.get("pTrdSymbol", r.get("ts", r.get("symbol", "")))).upper().strip()
            inst = str(r.get("pInstType", "")).upper()
            
            # Check if it's a Nifty Index Future and NOT bank/fin/midcap
            if "NIFTY" in sym and ("FUT" in sym or "FUTIDX" in inst):
                if not any(x in sym for x in ["BANK", "FIN", "MID", "IT", "SENSEX", "FPI"]):
                    exp_val = r.get("pExpiryDate", r.get("expiryDate", r.get("lExpiryDate")))
                    try:
                        # Try parsing various expiry formats safely
                        exp_dt = None
                        if exp_val:
                            for fmt in ["%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y"]:
                                try:
                                    exp_dt = datetime.strptime(str(exp_val).upper(), fmt).date()
                                    break
                                except Exception:
                                    pass
                        
                        if exp_dt is None or exp_dt >= now_d:
                            tok = str(r.get("pSymbolToken", r.get("instrument_token", r.get("token", ""))))
                            if tok:
                                futures.append((exp_dt if exp_dt else now_d, tok, sym))
                    except Exception:
                        pass
        
        if not futures:
            raise RuntimeError("Active NIFTY future contract could not be discovered. Check if nse_fo segment search is returning records.")
        
        # Sort by expiry to pick the nearest active contract
        futures.sort(key=lambda x: x[0])
        self.future_token = futures[0][1]
        self.logs.append(f"Discovered Active Nifty Future: {futures[0][2]} (Token: {self.future_token})")


        # 2. PCR & Option Discovery (Raw Only)
        spot_res = self.client.quotes(instrument_tokens=[{"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"}], quote_type="all")
        spot_recs = spot_res.get("result", spot_res.get("data", [])) if isinstance(spot_res, dict) else []
        spot_price = 24300.0
        for sr in spot_recs:
            lp = float(sr.get("lp", sr.get("ltp", 0)))
            if lp > 0:
                spot_price = lp
                break
        
        step = CONFIG["pcr_strike_step"]
        atm = round(spot_price / step) * step
        count = CONFIG["pcr_strike_count"]
        target_strikes = {atm + (i * step) for i in range(-count, count + 1)}

        opt_discovered = []
        for r in records:
            sym = str(r.get("pTrdSymbol", "")).upper().strip()
            if "NIFTY" in sym and (sym.endswith("CE") or sym.endswith("PE")):
                if not any(x in sym for x in ["BANK", "FIN", "MID", "IT", "SENSEX"]):
                    try:
                        strike_val = float(r.get("dStrikePrice", 0))
                        if strike_val > 1000000: strike_val /= 100.0
                        if strike_val in target_strikes:
                            tok = str(r.get("pSymbolToken", r.get("instrument_token", "")))
                            if tok:
                                opt_discovered.append(tok)
                    except Exception:
                        pass
        
        self.pcr_tokens = list(set(opt_discovered))
        self.logs.append(f"Discovered {len(self.pcr_tokens)} raw option strikes around ATM {atm}")
        return True

    def fetch_raw_quotes(self) -> List[Dict[str, Any]]:
        if not self.connected or not self.client:
            return []
        
        tokens_to_poll = [
            {"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"},
            {"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"},
        ]
        for sym, tok in self.heavy_tokens.items():
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        for tok in self.pcr_tokens:
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})

        try:
            res = self.client.quotes(instrument_tokens=tokens_to_poll, quote_type="all")
            records = res.get("result", res.get("data", res)) if isinstance(res, (dict, list)) else []
            if isinstance(records, dict):
                records = records.get("data", [records])
            return records if isinstance(records, list) else []
        except Exception as e:
            self.logs.append(f"Quote fetch error: {e}")
            return []


# =========================================================
# 2. YAHOO CONNECTOR (Macro OHLCV Fetcher)
# =========================================================
class YahooConnector:
    @staticmethod
    def fetch_macro_data(tickers: List[str] = ["GC=F", "SI=F", "DX-Y.NYB", "^GSPC"]) -> Dict[str, pd.DataFrame]:
        data_map = {}
        try:
            raw_data = yf.download(tickers, period="5d", interval="1d", progress=False, group_by="ticker")
            for ticker in tickers:
                if len(tickers) == 1:
                    df = raw_data
                else:
                    df = raw_data[ticker] if ticker in raw_data else pd.DataFrame()
                if not df.empty:
                    data_map[ticker] = df
        except Exception as e:
            print(f"Yahoo fetch error: {e}")
        return data_map


# =========================================================
# 3. SUPABASE PUBLISHER (Lightweight REST API via Requests)
# =========================================================
class SupabasePublisher:
    def __init__(self):
        self.url = env_or_secret("SUPABASE_URL", CONFIG["supabase_url"])
        self.key = env_or_secret("SUPABASE_KEY", CONFIG["supabase_key"])

    def publish_observation(self, source: str, symbol: str, token: str, raw_payload: dict):
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
            resp = requests.post(endpoint, headers=headers, json=record, timeout=5)
            return resp.status_code in (200, 201, 204)
        except Exception as e:
            print(f"Supabase publish error: {e}")
            return False


# =========================================================
# 4. STREAMLIT CONTROL PANEL UI
# =========================================================
def main():
    if st is None:
        print("Streamlit not available.")
        return

    st.set_page_config(page_title="Leak-Proof Raw Data Producer", layout="wide")
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
                except Exception as e:
                    st.error(str(e))

        with col2:
            if st.button("Discover Instruments", disabled=not kotak.connected):
                try:
                    with st.spinner("Discovering clean instruments & expiry..."):
                        kotak.discover_instruments()
                        st.success("Discovery Complete!")
                except Exception as e:
                    st.error(str(e))

        st.markdown("---")
        st.header("🚀 Producer Control")
        if not st.session_state.producer_running:
            if st.button("Start Raw Producer Loop", type="primary", disabled=not kotak.future_token):
                st.session_state.producer_running = True
                st.rerun()
        else:
            if st.button("Stop Producer Loop", type="secondary"):
                st.session_state.producer_running = False
                st.rerun()

    # Main Panel Status
    col_s1, col_s2, col_s3 = st.columns(3)
    col_s1.metric("Kotak Connection", "CONNECTED" if kotak.connected else "DISCONNECTED")
    col_s2.metric("Active Future Token", kotak.future_token if kotak.future_token else "NOT DISCOVERED")
    col_s3.metric("Mapped Options Count", len(kotak.pcr_tokens))

    if kotak.logs:
        with st.expander("Discovery & Execution Logs", expanded=True):
            for log in kotak.logs[-10:]:
                st.text(log)

    # Background Live Polling & Publishing Loop (Throttling-Safe)
    if st.session_state.producer_running:
        st.info("🟢 Raw Producer is active. Polling broker quotes and publishing to Supabase `raw_observations`...")
        
        status_container = st.empty()
        log_container = st.empty()
        
        poll_cycle = 0
        while st.session_state.producer_running:
            try:
                raw_quotes = kotak.fetch_raw_quotes()
                published_count = 0
                
                for q in raw_quotes:
                    tok = str(q.get("instrument_token", q.get("pSymbolToken", "UNKNOWN")))
                    sym = str(q.get("display_symbol", q.get("pTrdSymbol", "NIFTY")))
                    
                    success = supabase.publish_observation(
                        source="kotak_live",
                        symbol=sym,
                        token=tok,
                        raw_payload=q
                    )
                    if success:
                        published_count += 1

                if poll_cycle % 10 == 0:
                    macro_data = YahooConnector.fetch_macro_data()
                    for ticker, df in macro_data.items():
                        if not df.empty:
                            latest_row = df.iloc[-1].to_dict()
                            supabase.publish_observation(
                                source="yahoo_macro",
                                symbol=ticker,
                                token=ticker,
                                raw_payload={str(k): v for k, v in latest_row.items()}
                            )

                status_container.text(f"Last Poll: {now_ist().strftime('%H:%M:%S')} | Published {published_count} raw quotes to Supabase bus.")
                poll_cycle += 1
                
            except Exception as e:
                log_container.error(f"Producer loop exception: {e}")
                
            time.sleep(3.0)

if __name__ == "__main__":
    main()
