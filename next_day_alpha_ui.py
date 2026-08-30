#!/usr/bin/env python3
"""
NEXT-DAY ALPHA UI — RAW BUS LOCKED

Architecture lock:
    Kotak Neo -> raw_data_producer_kotak_live.py -> Supabase raw_observations
    yfinance  -> raw_data_producer_yfinance_history.py -> Supabase raw_observations
    Supabase RAW BUS -> Next-Day Alpha Engine/UI

This UI NEVER logs into Kotak Neo and NEVER asks for TOTP.
It is a read-only consumer of the common Supabase RAW BUS.
The existing NIFTY 3-Min and GSR paths are untouched.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

from next_day_alpha_engine import NextDayAlphaEngine

IST_OFFSET = timedelta(hours=5, minutes=30)
ROOT = Path(__file__).resolve().parent
RAW_TABLE = os.getenv("SUPABASE_RAW_TABLE", "raw_observations")

st.set_page_config(page_title="Next-Day Alpha", page_icon="📈", layout="wide")
st.title("NEXT-DAY INTRADAY STOCK ALPHA")
st.caption("Standalone • Supabase RAW BUS consumer • No direct Kotak login • No TOTP")

def secret(name: str, default: str = "") -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    try:
        return str(st.secrets.get(name, default)).strip()
    except Exception:
        return default

SUPABASE_URL = secret("SUPABASE_URL").rstrip("/")
SUPABASE_KEY = secret("SUPABASE_KEY")

def sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }

def supabase_health() -> Dict[str, Any]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {"configured": False, "reachable": False, "error": "SUPABASE_URL/SUPABASE_KEY missing"}
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{RAW_TABLE}",
            headers=sb_headers(),
            params={"select": "id", "limit": "1"},
            timeout=10,
        )
        return {
            "configured": True,
            "reachable": r.status_code in (200, 206),
            "http_status": r.status_code,
            "error": "" if r.status_code in (200, 206) else r.text[:250],
        }
    except Exception as exc:
        return {"configured": True, "reachable": False, "error": str(exc)}

def raw_rows(symbol: Optional[str] = None, since_minutes: int = 15, limit: int = 1000) -> List[Dict[str, Any]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    params: Dict[str, str] = {
        "select": "*",
        "order": "observation_timestamp.desc",
        "limit": str(limit),
    }
    if symbol:
        params["symbol"] = f"eq.{symbol.replace('.NS','').upper()}"
    start = datetime.utcnow() - timedelta(minutes=since_minutes)
    params["observation_timestamp"] = f"gte.{start.isoformat()}Z"
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{RAW_TABLE}",
            headers=sb_headers(),
            params=params,
            timeout=15,
        )
        if r.status_code >= 400:
            return []
        payload = r.json()
        return payload if isinstance(payload, list) else []
    except Exception:
        return []

def payload_of(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("raw")
    return raw if isinstance(raw, dict) else row

def raw_value(raw: Dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        try:
            v = float(raw.get(k))
            if pd.notna(v):
                return v
        except Exception:
            pass
    return None

def live_bus_health() -> Dict[str, Any]:
    rows = raw_rows(since_minutes=10, limit=200)
    kotak = []
    for row in rows:
        source = str(row.get("source", "")).lower()
        if "kotak" in source or "neo" in source:
            kotak.append(row)
    if not kotak:
        return {
            "connected": False,
            "quote_received": False,
            "last_quote_at": None,
            "symbol": None,
            "ltp": None,
            "rows": 0,
        }
    latest = kotak[0]
    raw = payload_of(latest)
    return {
        "connected": True,
        "quote_received": True,
        "last_quote_at": latest.get("observation_timestamp") or raw.get("timestamp") or raw.get("received_at"),
        "symbol": latest.get("symbol") or raw.get("symbol"),
        "ltp": raw_value(raw, "ltp", "lp", "last_price", "lastPrice", "c", "close"),
        "rows": len(kotak),
    }

@st.cache_resource
def get_engine() -> NextDayAlphaEngine:
    return NextDayAlphaEngine()

engine = get_engine()

if st.button("🔄 Refresh Dashboard", use_container_width=True):
    st.rerun()

# IMPORTANT: UI does not call engine.kotak_health(); it checks the common RAW BUS.
sb = supabase_health()
live = live_bus_health()

h1, h2, h3, h4 = st.columns(4)
h1.metric("RAW BUS", "READY" if sb.get("reachable") else "OFFLINE")
h2.metric("LIVE RAW", "RECEIVED" if live.get("quote_received") else "NOT RECEIVED")
h3.metric("LIVE SYMBOL", live.get("symbol") or "—")
ltp = live.get("ltp")
h4.metric("LTP", f"{ltp:.2f}" if isinstance(ltp, (int,float)) else "—")

st.subheader("DATA SOURCE HEALTH")
c1, c2 = st.columns(2)
with c1:
    if sb.get("reachable"):
        st.success("SUPABASE RAW BUS: CONNECTED")
        st.caption(f"Table: {RAW_TABLE}")
    elif sb.get("configured"):
        st.error("SUPABASE RAW BUS: UNREACHABLE")
        st.caption(str(sb.get("error", "")))
    else:
        st.error("SUPABASE RAW BUS: NOT CONFIGURED")
        st.caption("Add SUPABASE_URL and SUPABASE_KEY to Streamlit Secrets.")
with c2:
    if live.get("quote_received"):
        st.success("KOTAK LIVE → RAW BUS: RECEIVED")
        st.caption(f"Last raw quote: {live.get('last_quote_at')}")
    else:
        st.warning("KOTAK LIVE → RAW BUS: NO RECENT QUOTE")
        st.caption("The separate Kotak Raw Producer must be running/connected.")

with st.expander("RAW BUS LIVE HEALTH", expanded=False):
    st.write(live)

try:
    result = engine.latest()
except Exception as exc:
    result = {}
    st.error(f"Engine result read failed: {exc}")

if not result:
    st.info("No saved day-ahead snapshot yet. The scheduled engine is armed for the next run.")
    st.caption("Historical source: Supabase RAW BUS • Live/opening source: Supabase RAW BUS fed by Kotak Producer")
    st.stop()

day = result.get("day_ahead", {})
morning = result.get("morning_confirmation", {})
architecture = result.get("architecture", {})

top5 = day.get("top5", [])
final = morning.get("final", [])
confirmations = morning.get("confirmations", [])

c1, c2, c3, c4 = st.columns(4)
c1.metric("TOP 5", len(top5))
c2.metric("Morning", morning.get("status", "PENDING"))
c3.metric("Final", len(final))
c4.metric("Engine", result.get("version", "UNKNOWN"))

st.subheader("TOP 5 OVERNIGHT SHORTLIST")
if top5:
    rows = []
    for x in top5:
        rows.append({
            "Rank": x.get("rank"),
            "Stock": x.get("symbol"),
            "Sector": x.get("industry", "UNKNOWN"),
            "Thesis": x.get("direction"),
            "Score": x.get("day_ahead_score"),
            "Selection": x.get("selection_score"),
            "Trend": x.get("trend_score"),
            "Momentum": x.get("momentum_score"),
            "RS": x.get("relative_strength_score"),
            "Sector Score": x.get("sector_score"),
            "Volume": x.get("volume_score"),
            "Volatility": x.get("volatility_score"),
            "Catalyst": x.get("catalyst_score"),
            "Anti-FP": x.get("anti_false_positive_score"),
            "LTP": x.get("ltp"),
            "ATR %": x.get("atr_pct"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.info("NO QUALIFIED CANDIDATE")

st.subheader("THESIS → RISK / SETUP")
if top5:
    detail = []
    for x in top5:
        detail.append({
            "Stock": x.get("symbol"),
            "Thesis": str(x.get("direction", "UNKNOWN")).upper(),
            "Setup": x.get("setup_type", "UNKNOWN"),
            "Score": x.get("day_ahead_score"),
            "LTP": x.get("ltp"),
            "ATR %": x.get("atr_pct"),
            "1D %": x.get("ret_1d"),
            "5D %": x.get("ret_5d"),
            "20D %": x.get("ret_20d"),
            "RS 5D": x.get("rs_5d"),
            "RS 20D": x.get("rs_20d"),
        })
    st.dataframe(pd.DataFrame(detail), use_container_width=True, hide_index=True)

st.subheader("09:15–09:20 MORNING CONFIRMATION")
if confirmations:
    st.dataframe(pd.DataFrame(confirmations), use_container_width=True, hide_index=True)
else:
    st.info(f"Morning confirmation: {morning.get('status', 'PENDING')}")

if final:
    st.success("FINAL TRADE CANDIDATES")
    st.dataframe(pd.DataFrame(final), use_container_width=True, hide_index=True)
else:
    st.info("NO TRADE — engine never forces two trades.")

with st.expander("ENGINE / DATA CONTRACT", expanded=False):
    st.write({
        "engine": result.get("engine"),
        "version": result.get("version"),
        "generated_at": result.get("generated_at"),
        "data_as_of": result.get("data_as_of"),
        "NIFTY_3MIN_MODIFIED": architecture.get("nifty_3min_engine_modified"),
        "shared_raw_data_allowed": architecture.get("shared_raw_data_allowed"),
        "shared_calculated_features": architecture.get("shared_calculated_features"),
        "shared_scores": architecture.get("shared_scores"),
        "shared_decisions": architecture.get("shared_decisions"),
        "shared_labels": architecture.get("shared_labels"),
        "shared_predictions": architecture.get("shared_predictions"),
        "historical_source": "SUPABASE_RAW_BUS",
        "live_source": "KOTAK_NEO_VIA_SUPABASE_RAW_BUS",
        "kotak_credentials_in_this_app": False,
        "totp_in_this_app": False,
    })

st.caption("Quality scores are ranking scores, not win probabilities. Raw BUS contains observations only.")
