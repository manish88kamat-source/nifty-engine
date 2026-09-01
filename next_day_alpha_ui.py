# PATCHED next_day_alpha_ui.py
# Drop-in replacement for the current UI file.
# Core engine/math/indicator/learning logic is NOT changed.

#!/usr/bin/env python3
from __future__ import annotations

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

st.set_page_config(page_title="Next-Day Alpha", page_icon="", layout="wide")
st.title("NEXT-DAY INTRADAY STOCK ALPHA")
st.caption("Standalone - Supabase RAW BUS consumer - No direct Kotak login - No TOTP")


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


def _canonical_equity_symbol(value: Any) -> str:
    s = str(value or "").upper().strip()
    for suffix in (".NS", "-EQ", "_EQ"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    return s


def _is_equity_symbol(value: Any) -> bool:
    s = _canonical_equity_symbol(value)
    if not s or s in {"NIFTY_SPOT", "NIFTY 50", "NIFTY50", "^NSEI", "INDIAVIX", "^INDIAVIX"}:
        return False
    if s.endswith(("CE", "PE", "FUT")) or "FUT" in s:
        return False
    if any(x in s for x in ("BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")):
        return False
    if __import__("re").search(r"\d{2}[A-Z]{3}\d+", s) or __import__("re").search(r"\d{4,}", s):
        return False
    return bool(__import__("re").fullmatch(r"[A-Z][A-Z0-9&._-]*", s))


def raw_rows(symbol: Optional[str] = None, since_minutes: int = 15, limit: int = 1000, source: Optional[str] = None) -> List[Dict[str, Any]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    params: Dict[str, str] = {
        "select": "*",
        "order": "observation_timestamp.desc",
        "limit": str(limit),
    }
    if source:
        params["source"] = f"eq.{source}"
    if symbol:
        clean = _canonical_equity_symbol(symbol)
        if not _is_equity_symbol(clean):
            return []
        params["symbol"] = f"eq.{clean}"
    from datetime import timezone
    start = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
    params["observation_timestamp"] = f"gte.{start.isoformat()}"
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


def canonical_display_symbol(row: Dict[str, Any]) -> str:
    raw = payload_of(row)
    value = (
        row.get("symbol")
        or raw.get("display_symbol")
        or raw.get("pTrdSymbol")
        or raw.get("tradingSymbol")
        or raw.get("symbol")
    )
    return str(value).strip() if value not in (None, "") else "-"


def live_bus_health() -> Dict[str, Any]:
    """Show only the latest Kotak LIVE equity quote; derivatives are ignored."""
    rows = raw_rows(since_minutes=10, limit=500, source="kotak_live")
    equity_rows = []
    for row in rows:
        raw = payload_of(row)
        symbol = row.get("symbol") or raw.get("display_symbol") or raw.get("pTrdSymbol") or raw.get("tradingSymbol")
        if not _is_equity_symbol(symbol):
            continue
        equity_rows.append(row)

    if not equity_rows:
        return {
            "connected": False, "quote_received": False, "last_quote_at": None,
            "latest_raw_instrument": None, "latest_raw_ltp": None,
            "latest_raw_source": None, "rows": 0, "derivatives_ignored": len(rows),
        }

    latest = equity_rows[0]
    raw = payload_of(latest)
    return {
        "connected": True, "quote_received": True,
        "last_quote_at": latest.get("observation_timestamp") or raw.get("timestamp") or raw.get("received_at"),
        "latest_raw_instrument": _canonical_equity_symbol(latest.get("symbol") or raw.get("display_symbol") or raw.get("pTrdSymbol") or raw.get("tradingSymbol")),
        "latest_raw_ltp": raw_value(raw, "ltp", "lp", "last_price", "lastPrice", "c", "close"),
        "latest_raw_source": latest.get("source") or raw.get("raw_source"),
        "rows": len(equity_rows), "derivatives_ignored": len(rows) - len(equity_rows),
    }


@st.cache_resource
def get_engine() -> NextDayAlphaEngine:
    return NextDayAlphaEngine()


engine = get_engine()

if st.button("Refresh Dashboard", use_container_width=True):
    st.rerun()

# UI checks the common RAW BUS only.
sb = supabase_health()
live = live_bus_health()

h1, h2, h3, h4 = st.columns(4)
h1.metric("RAW BUS", "READY" if sb.get("reachable") else "OFFLINE")
h2.metric("LIVE RAW", "RECEIVED" if live.get("quote_received") else "NOT RECEIVED")
h3.metric("LATEST RAW INSTRUMENT", live.get("latest_raw_instrument") or "-")
ltp = live.get("latest_raw_ltp")
h4.metric("RAW LTP", f"{ltp:.2f}" if isinstance(ltp, (int, float)) else "-")

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
        st.success("KOTAK LIVE -> RAW BUS: RECEIVED")
        st.caption(f"Last raw quote: {live.get('last_quote_at')}")
        st.caption(f"Latest RAW instrument: {live.get('latest_raw_instrument') or '-'}")
    else:
        st.warning("KOTAK LIVE -> RAW BUS: NO RECENT QUOTE")
        st.caption("The separate Kotak Raw Producer must be running/connected.")

with st.expander("RAW BUS LIVE HEALTH", expanded=False):
    st.write(live)

try:
    result = engine.today_snapshot()
except Exception as exc:
    result = {}
    st.error(f"Engine result read failed: {exc}")

if not result:
    status = {}
    try:
        from next_day_alpha_engine import day_ahead_status
        status = day_ahead_status()
    except Exception:
        pass
    if status.get("status") == "FAILED":
        st.error("DAY-AHEAD SNAPSHOT FAILED FOR TODAY")
        if status.get("error"):
            st.code(str(status["error"]))
        st.caption("The scan must complete successfully before a frozen TOP 15 is published.")
    else:
        st.warning("NO FROZEN DAY-AHEAD SNAPSHOT FOR TODAY")
        st.caption("The scheduled day-ahead scan has not completed successfully yet.")
    st.caption("Historical source: Supabase RAW BUS | Live/opening source: Supabase RAW BUS fed by Kotak Producer")
    if st.button("Run Day-Ahead Scan Now", type="primary", use_container_width=True):
        try:
            with st.spinner("Running day-ahead scan from Supabase RAW BUS..."):
                engine.run_day_ahead()
            st.success("Day-ahead scan completed and snapshot frozen.")
            st.rerun()
        except Exception as exc:
            st.error(f"Day-ahead scan failed: {exc}")
    st.stop()

day = result.get("day_ahead", {})
morning = result.get("morning_confirmation", {})
architecture = result.get("architecture", {})

# Current engine uses the legacy JSON key "top5" as a compatibility name,
# while DAY_AHEAD_TOP_N/TOP15_COUNT is already 15. Prefer a future "top15"
# key if present, otherwise read the saved legacy key. No regeneration occurs.
top15 = day.get("top15", day.get("top5", []))
final = morning.get("final", [])
confirmations = morning.get("confirmations", [])

c1, c2, c3, c4 = st.columns(4)
c1.metric("FROZEN TOP 15", len(top15))
c2.metric("Morning", morning.get("status", "PENDING"))
c3.metric("Final", len(final))
c4.metric("Engine", result.get("version", "UNKNOWN"))

st.subheader("FROZEN TOP 15 OVERNIGHT SHORTLIST")
st.caption("Today's saved engine snapshot only. Refreshing this dashboard does not regenerate or replace the shortlist.")

if top15:
    rows = []
    for x in top15:
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

st.subheader("THESIS -> RISK / SETUP")
if top15:
    detail = []
    for x in top15:
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

st.subheader("09:15-09:20 MORNING CONFIRMATION")
if confirmations:
    st.dataframe(pd.DataFrame(confirmations), use_container_width=True, hide_index=True)
else:
    st.info(f"Morning confirmation: {morning.get('status', 'PENDING')}")

if final:
    st.success("FINAL TRADE CANDIDATES")
    st.dataframe(pd.DataFrame(final), use_container_width=True, hide_index=True)
else:
    st.info("NO TRADE - engine never forces two trades.")

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
