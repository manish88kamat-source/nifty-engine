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
st.caption("Standalone UI • Raw-data sharing only • app.py / NIFTY / GSR untouched")

engine = NextDayAlphaEngine()
try:
    engine.run_if_due()
except Exception as exc:
    st.error(f"Engine startup/auto-run failed: {exc}")
result = engine.latest()
day = result.get("day_ahead", {}) if isinstance(result, dict) else {}
morning = result.get("morning_confirmation", {}) if isinstance(result, dict) else {}
candidates = day.get("top15", day.get("top5", []))

if not result:
    st.info("No saved day-ahead snapshot yet. The dashboard will automatically start the day-ahead scan after 15:31 IST; no manual terminal command is required.")
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
st.subheader("THESIS → MORNING REALITY")
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

st.subheader("09:15–09:20 CONFIRMATION")
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
    st.info("NO TRADE — engine never forces two trades.")

st.caption("Quality scores are ranking scores, not win probabilities. Historical calibration is required before any probability claim.")
