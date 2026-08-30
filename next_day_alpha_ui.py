#!/usr/bin/env python3
"""
Standalone Streamlit UI for the isolated Next-Day Alpha Engine.

This UI is intentionally aligned with the current engine contract:
- Historical data: Supabase RAW BUS
- Live/opening raw data: Kotak Neo
- Day-ahead output: TOP 5
- Morning confirmation: 09:15-09:20
- Final outcome: FINAL 2 / FINAL 1 / NO TRADE

It does not import or modify app.py, the NIFTY 3-Min engine,
or GSR decision paths.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from next_day_alpha_engine import NextDayAlphaEngine


ROOT = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# PAGE
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Next-Day Alpha",
    page_icon="ðŸ“ˆ",
    layout="wide",
)

st.title("NEXT-DAY INTRADAY STOCK ALPHA")
st.caption(
    "Standalone â€¢ Raw-data sharing only â€¢ "
    "NIFTY / GSR decision paths untouched"
)


# ---------------------------------------------------------------------------
# SINGLE ENGINE INSTANCE
# ---------------------------------------------------------------------------
# Streamlit reruns the script on every interaction. cache_resource prevents
# every rerun from creating another background scheduler thread.

@st.cache_resource
def get_engine() -> NextDayAlphaEngine:
    engine = NextDayAlphaEngine()
    engine.start_if_due_background()
    return engine


engine = get_engine()


# ---------------------------------------------------------------------------
# ACTIONS
# ---------------------------------------------------------------------------

refresh = st.button("ðŸ”„ Refresh Dashboard", use_container_width=True)

if refresh:
    st.rerun()


# Run any operation that is due. The background worker remains armed.
try:
    engine.run_if_due()
except Exception as exc:
    st.error(f"Engine auto-run failed: {exc}")


# ---------------------------------------------------------------------------
# KOTAK NEO HEALTH
# ---------------------------------------------------------------------------

try:
    kotak = engine.kotak_health("NIFTY 50")
except Exception as exc:
    kotak = {
        "sdk_available": False,
        "live_enabled": False,
        "credentials_present": False,
        "connected": False,
        "quote_received": False,
        "source": "ERROR",
        "login_error": f"{type(exc).__name__}: {exc}",
    }


h1, h2, h3, h4 = st.columns(4)

h1.metric(
    "Neo SDK",
    "READY" if kotak.get("sdk_available") else "MISSING",
)

h2.metric(
    "Neo Login",
    "CONNECTED" if kotak.get("connected") else "OFFLINE",
)

h3.metric(
    "Raw Quote",
    "RECEIVED" if kotak.get("quote_received") else "NOT RECEIVED",
)

probe_ltp = kotak.get("probe_ltp")
if isinstance(probe_ltp, (int, float)) and probe_ltp == probe_ltp:
    ltp_text = f"{probe_ltp:.2f}"
else:
    ltp_text = "â€”"

h4.metric("NIFTY LTP", ltp_text)


with st.expander("KOTAK NEO LIVE HEALTH", expanded=False):
    st.write(
        {
            "source": kotak.get("source"),
            "live_enabled": kotak.get("live_enabled"),
            "credentials_present": kotak.get("credentials_present"),
            "connected": kotak.get("connected"),
            "quote_received": kotak.get("quote_received"),
            "last_quote_at": kotak.get("last_quote_at"),
            "last_quote_symbol": kotak.get("last_quote_symbol"),
            "login_error": kotak.get("login_error"),
            "last_quote_error": kotak.get("last_quote_error"),
        }
    )


# ---------------------------------------------------------------------------
# SUPABASE RAW BUS HEALTH
# ---------------------------------------------------------------------------
# The current engine does not expose a data_source_health() method.
# Therefore this UI does not call a nonexistent engine API.
# We show configuration state and the authoritative source contract instead.

st.subheader("DATA SOURCE HEALTH")

supabase_url = os.getenv("SUPABASE_URL", "").strip()
supabase_key = os.getenv("SUPABASE_KEY", "").strip()

# Streamlit Cloud secrets fallback.
if not supabase_url or not supabase_key:
    try:
        supabase_url = str(st.secrets.get("SUPABASE_URL", "")).strip()
        supabase_key = str(st.secrets.get("SUPABASE_KEY", "")).strip()
    except Exception:
        pass

s1, s2 = st.columns(2)

with s1:
    if supabase_url and supabase_key:
        st.success("SUPABASE RAW BUS: CONFIGURED")
        st.caption("Historical market observations: Supabase RAW BUS")
    else:
        st.error("SUPABASE RAW BUS: NOT CONFIGURED")
        st.caption("Add SUPABASE_URL and SUPABASE_KEY to Streamlit Secrets.")

with s2:
    if kotak.get("connected") and kotak.get("quote_received"):
        st.success("KOTAK NEO LIVE: CONNECTED + QUOTE RECEIVED")
    elif kotak.get("connected"):
        st.warning("KOTAK NEO LIVE: LOGIN OK â€¢ QUOTE NOT RECEIVED")
    else:
        st.warning("KOTAK NEO LIVE: OFFLINE")

    st.caption("Live/opening raw observations: Kotak Neo")


# ---------------------------------------------------------------------------
# LATEST RESULT
# ---------------------------------------------------------------------------

result = engine.latest()

if not result:
    st.info(
        "No saved day-ahead snapshot yet. "
        "Background engine is armed for the 15:31 IST scan."
    )
    st.caption(
        "Historical: Supabase RAW BUS â€¢ "
        "Live/opening: Kotak Neo"
    )
    st.stop()


day = result.get("day_ahead", {})
morning = result.get("morning_confirmation", {})
architecture = result.get("architecture", {})

top5 = day.get("top5", [])
final = morning.get("final", [])
confirmations = morning.get("confirmations", [])


# ---------------------------------------------------------------------------
# ENGINE STATUS
# ---------------------------------------------------------------------------

c1, c2, c3, c4 = st.columns(4)

c1.metric("TOP 5", len(top5))
c2.metric("Morning", morning.get("status", "PENDING"))
c3.metric("Final", len(final))
c4.metric("Engine", result.get("version", "UNKNOWN"))


# ---------------------------------------------------------------------------
# DAY-AHEAD TOP 5
# ---------------------------------------------------------------------------

st.subheader("TOP 5 OVERNIGHT SHORTLIST")

if top5:
    rows = []

    for x in top5:
        rows.append(
            {
                "Rank": x.get("rank"),
                "Stock": x.get("symbol"),
                "Sector": x.get("industry", "UNKNOWN"),
                "Thesis": x.get("direction"),
                "Score": x.get("day_ahead_score"),
                "Selection": x.get("selection_score"),
                "Trend": x.get("trend_score"),
                "Momentum": x.get("momentum_score"),
                "RS": x.get("relative_strength_score"),
                "Sector": x.get("sector_score"),
                "Volume": x.get("volume_score"),
                "Volatility": x.get("volatility_score"),
                "Catalyst": x.get("catalyst_score"),
                "Anti-FP": x.get("anti_false_positive_score"),
                "LTP": x.get("ltp"),
                "ATR %": x.get("atr_pct"),
            }
        )

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("NO QUALIFIED CANDIDATE")


# ---------------------------------------------------------------------------
# THESIS / RISK VIEW
# ---------------------------------------------------------------------------

st.subheader("THESIS â†’ RISK / SETUP")

if top5:
    detail = []

    for x in top5:
        detail.append(
            {
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
            }
        )

    st.dataframe(
        pd.DataFrame(detail),
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# MORNING CONFIRMATION
# ---------------------------------------------------------------------------

st.subheader("09:15â€“09:20 MORNING CONFIRMATION")

if confirmations:
    st.dataframe(
        pd.DataFrame(confirmations),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info(
        f"Morning confirmation: "
        f"{morning.get('status', 'PENDING')}"
    )


# ---------------------------------------------------------------------------
# FINAL TRADE CANDIDATES
# ---------------------------------------------------------------------------

if final:
    st.success("FINAL TRADE CANDIDATES")

    st.dataframe(
        pd.DataFrame(final),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("NO TRADE â€” engine never forces two trades.")


# ---------------------------------------------------------------------------
# ARCHITECTURE / DATA CONTRACT
# ---------------------------------------------------------------------------

with st.expander("ENGINE / DATA CONTRACT", expanded=False):
    st.write(
        {
            "engine": result.get("engine"),
            "version": result.get("version"),
            "generated_at": result.get("generated_at"),
            "data_as_of": result.get("data_as_of"),
            "NIFTY_3MIN_MODIFIED": architecture.get(
                "nifty_3min_engine_modified"
            ),
            "shared_raw_data_allowed": architecture.get(
                "shared_raw_data_allowed"
            ),
            "shared_calculated_features": architecture.get(
                "shared_calculated_features"
            ),
            "shared_scores": architecture.get("shared_scores"),
            "shared_decisions": architecture.get("shared_decisions"),
            "shared_labels": architecture.get("shared_labels"),
            "shared_predictions": architecture.get(
                "shared_predictions"
            ),
            "next_day_can_read_nifty_calculations": architecture.get(
                "next_day_can_read_nifty_calculations"
            ),
            "historical_source": "SUPABASE_RAW_BUS",
            "live_source": architecture.get(
                "live_intraday_primary",
                "KOTAK_NEO",
            ),
            "catalyst_source": architecture.get(
                "catalyst_primary",
                "NSE_CORPORATE_FILINGS",
            ),
        }
    )


st.caption(
    "Quality scores are ranking scores, not win probabilities. "
    "Historical calibration is required before making probability claims."
)
