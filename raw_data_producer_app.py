#!/usr/init/env python3
"""
Leak-Proof Raw Data Producer | Institutional Research Bus
- Zero local calculations, zero indicators, zero ML.
- Strict token resolution with robust fallback.
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
        
        records = []
        try:
            res = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
            if isinstance(res, dict):
                records = res.get("result", res.get("data", res.get("values", [])))
            elif isinstance(res, list):
                records = res
        except Exception as e:
            self.logs.append(f"Primary search_scrip warning: {e}")

        if not records:
            try:
                res2 = self.client.search_scrip(exchange_segment="nse_fo", symbol="Nifty")
                if isinstance(res2, dict):
                    records = res2.get("result", res2.get("data", res2.get("values", [])))
                elif isinstance(res2, list):
                    records = res2
            except Exception:
                pass

        found_token = None
        found_symbol = None
        for r in records:
            if not isinstance(r, dict):
                continue
            sym = str(r.get("pTrdSymbol", r.get("ts", r.get("symbol", r.get("tradingSymbol", ""))))).upper().strip()
            tok = str(r.get("pSymbolToken", r.get("instrument_token", r.get("token", r.get("symbolToken", "")))))
            if "NIFTY" in sym and tok and "FUT" in sym:
                if not any(x in sym for x in ["BANK", "FIN", "MID", "IT", "SENSEX"]):
                    found_token = tok
                    found_symbol = sym
                    break

        if not found_token and records:
            for r in records:
                if not isinstance(r, dict):
                    continue
                sym = str(r.get("pTrdSymbol", r.get("ts", ""))).upper().strip()
                tok = str(r.get("pSymbolToken", r.get("instrument_token", "")))
                if tok and "NIFTY" in sym:
                    found_token = tok
                    found_symbol = sym
                    break

        if not found_token:
            found_token = "26000"
            found_symbol = "NIFTY_FUT_FALLBACK"
            self.logs.append("Using fallback Nifty Future token mapping.")

        self.future_token = found_token
        self.logs.append(f"Bound Active Nifty Future: {found_symbol} (Token: {self.future_token})")

        try:
            spot_price = 24300.0
            step = CONFIG["pcr_strike_step"]
            atm = round(spot_price / step) * step
            count = CONFIG["pcr_strike_count"]
            target_strikes = {atm + (i * step) for i in range(-count, count + 1)}

            opt_discovered = []
            for r in records:
                if not isinstance(r, dict):
                    continue
                sym = str(r.get("pTrdSymbol", "")).upper().strip()
                if "NIFTY" in sym and (sym.endswith("CE") or sym.endswith("PE")):
                    try:
                        strike_val = float(r.get("dStrikePrice", 0))
                        if strike_val > 1000000:
                            strike_val /= 100.0
                        if strike_val in target_strikes:
                            tok = str(r.get("pSymbolToken", r.get("instrument_token", "")))
                            if tok:
                                opt_discovered.append(tok)
                    except Exception:
                        pass
            self.pcr_tokens = list(set(opt_discovered))
            self.logs.append(f"Discovered {len(self.pcr_tokens)} raw option strikes.")
        except Exception as e:
            self.logs.append(f"Option discovery warning: {e}")

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

    col_s1, col_s2, col_s3 = st.columns(3)
    col_s1.metric("Kotak Connection", "CONNECTED" if kotak.connected else "DISCONNECTED")
    col_s2.metric("Active Future Token", kotak.future_token if kotak.future_token else "NOT DISCOVERED")
    col_s3.metric("Mapped Options Count", len(kotak.pcr_tokens))

    if kotak.logs:
        with st.expander("Discovery & Execution Logs", expanded=True):
            for log in kotak.logs[-10:]:
                st.text(log)

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
