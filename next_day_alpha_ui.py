#!/usr/bin/env python3
"""Standalone Streamlit UI for the isolated Next-Day Alpha Engine.
Does not import or modify app.py/NIFTY/GSR decision paths.
"""
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import streamlit as st

from next_day_alpha_engine import NextDayAlphaEngine

ROOT = Path(__file__).resolve().parent

st.set_page_config(page_title="Next-Day Alpha | TOP 15", layout="wide")
st.title("NEXT-DAY INTRADAY STOCK ALPHA")
st.caption("Standalone UI â€¢ Raw-data sharing only â€¢ app.py / NIFTY / GSR untouched")

# Keep the engine scheduler alive inside the Streamlit process so the
# 15:31 day-ahead scan and 09:15-09:20 raw opening capture can run
# automatically without requiring a terminal command.
engine = NextDayAlphaEngine()
engine.start_if_due_background()

# Non-sensitive live broker health. Credentials themselves are never shown.
try:
    kotak = engine.kotak_health("NIFTY 50")
except Exception as exc:
    kotak = {"connected": False, "quote_received": False, "source": "ERROR", "login_error": str(exc)}

h1, h2, h3, h4 = st.columns(4)
h1.metric("Neo SDK", "READY" if kotak.get("sdk_available") else "MISSING")
h2.metric("Neo Login", "CONNECTED" if kotak.get("connected") else "OFFLINE")
h3.metric("Raw Quote", "RECEIVED" if kotak.get("quote_received") else "NOT RECEIVED")
probe_ltp = kotak.get("probe_ltp")
h4.metric("NIFTY LTP", f"{probe_ltp:.2f}" if isinstance(probe_ltp, (int, float)) and probe_ltp == probe_ltp else "â€”")

with st.expander("KOTAK NEO LIVE HEALTH", expanded=False):
    st.write({
        "source": kotak.get("source"),
        "credentials_present": kotak.get("credentials_present"),
        "connected": kotak.get("connected"),
        "quote_received": kotak.get("quote_received"),
        "last_quote_at": kotak.get("last_quote_at"),
        "last_quote_symbol": kotak.get("last_quote_symbol"),
        "login_error": kotak.get("login_error"),
        "last_quote_error": kotak.get("last_quote_error"),
    })

try:
    engine.run_if_due()
except Exception as exc:
    st.error(f"Engine startup/auto-run failed: {exc}")
result = engine.latest()
source_health = engine.data_source_health()
day = result.get("day_ahead", {}) if isinstance(result, dict) else {}
morning = result.get("morning_confirmation", {}) if isinstance(result, dict) else {}
candidates = day.get("top15", day.get("top5", []))

st.subheader("DATA SOURCE HEALTH")

def _source_status(label, data):
    status = str(data.get("status", "NOT_TESTED"))
    if status == "CONNECTED":
        st.success(f"{label}: CONNECTED")
    elif status in ("FETCHING", "LOGIN_TESTING"):
        st.warning(f"{label}: {status}")
    elif status == "LOGIN_OK_NO_QUOTE":
        st.warning(f"{label}: LOGIN OK â€¢ QUOTE NOT RECEIVED")
    elif status == "DISABLED":
        st.info(f"{label}: DISABLED")
    elif status == "NOT_TESTED":
        st.info(f"{label}: NOT TESTED YET")
    else:
        st.error(f"{label}: {status}")
    details = []
    if label.startswith("YFINANCE"):
        details.append(f"Historical fetch OK: {data.get('symbols_ok', 0)} symbols")
        if data.get("last_success_ist"): details.append(f"Last success: {data['last_success_ist']}")
    else:
        if data.get("probe_symbol"): details.append(f"Probe: {data.get('probe_symbol')}")
        if data.get("probe_ltp") is not None: details.append(f"LTP: {data.get('probe_ltp')}")
        if data.get("last_success_ist"): details.append(f"Last quote: {data['last_success_ist']}")
    if data.get("error"): details.append(f"Error: {data['error']}")
    st.caption(" â€¢ ".join(details))

s1, s2 = st.columns(2)
with s1:
    _source_status("YFINANCE / YAHOO â€” HISTORICAL", source_health.get("YFINANCE", {}))
with s2:
    _source_status("KOTAK NEO â€” LIVE", source_health.get("KOTAK_NEO", {}))

if not result:
    st.info("No saved day-ahead snapshot yet. Background engine is armed for the 15:31 IST scan; no terminal command is required.")
    st.caption("Historical scan: Yahoo. Live/intraday: Kotak Neo/shared raw only.")
    st.stop()

macro = result.get("macro_regime", {})
c1, c2, c3, c4 = st.columns(4)
c1.metric("TOP 15", len(candidates))
c2.metric("Morning", morning.get("status", "PENDING"))
c3.metric("Final", len(morning.get("final", [])))
c4.metric("Market/VIX", macro.get("regime", "UNKNOWN"))

st.subheader("15 STOCK OVERNIGHT SHORTLIST")
if candidates:
    rows=[]
    for x in candidates:
        rows.append({
            "Rank": x.get("rank"),
            "Stock": x.get("symbol"),
            "Sector": x.get("sector_bucket"),
            "Thesis": x.get("direction"),
            "Night Score": x.get("night_deep_dive_score", x.get("v7_score")),
            "MTF": x.get("mtf_score"),
            "Volume Shock": x.get("volume_shock_score", x.get("volume_score")),
            "7D Vol": x.get("volume_ratio_7"),
            "Catalyst": x.get("event_direction", x.get("catalyst_score")),
            "NSE/BSE Events": f"{x.get('nse_event_count',0)}/{x.get('bse_event_count',0)}",
            "R:R": x.get("rr"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.info("NO QUALIFIED CANDIDATE")

# Detailed thesis/reality view for the shortlist.
st.subheader("THESIS â†’ MORNING REALITY")
if candidates:
    detail=[]
    for x in candidates:
        detail.append({
            "Stock": x.get("symbol"),
            "Thesis": str(x.get("direction", "UNKNOWN")).upper(),
            "Sector": x.get("sector_bucket", "UNKNOWN"),
            "Night Score": x.get("day_ahead_score"),
            "Setup": x.get("setup_type", "UNKNOWN"),
            "Invalidation": x.get("invalidation"),
            "Target": x.get("target"),
            "R:R": x.get("rr"),
            "Catalyst": x.get("event_direction", x.get("scanner_family", "NO_EDGE")),
        })
    st.dataframe(pd.DataFrame(detail), use_container_width=True, hide_index=True)

st.subheader("09:15â€“09:20 CONFIRMATION")
confirmations = morning.get("confirmations", [])
if confirmations:
    st.dataframe(pd.DataFrame(confirmations), use_container_width=True, hide_index=True)
else:
    st.info("Morning confirmation pending.")

final = morning.get("final", [])
if final:
    st.success("FINAL TRADE CANDIDATES")
    st.dataframe(pd.DataFrame(final), use_container_width=True, hide_index=True)
else:
    st.info("NO TRADE â€” engine never forces two trades.")

st.caption("Quality scores are ranking scores, not win probabilities. Historical calibration is required before any probability claim.")
