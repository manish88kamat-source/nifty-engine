# NEXT-DAY ALPHA UI - V3 ROOT-CAUSE DEBUG BOUND
# Based directly on the uploaded current UI.
# Engine/math/indicator/learning logic is NOT changed here.
# Older engine fallbacks are intentionally disabled.

#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

# LOCKED ENGINE BINDING
# This UI intentionally refuses to fall back to older engine generations.
# The locked base for the current root-cause investigation is:
# next_day_alpha_engine_FINAL_BUGFIXED_V3_ROOT_CAUSE_FIXED.py
ENGINE_MODULE_NAME = "next_day_alpha_engine_FINAL_BUGFIXED_V3_ROOT_CAUSE_FIXED"

try:
    engine_module = __import__(
        ENGINE_MODULE_NAME,
        fromlist=[
            "NextDayAlphaEngine",
            "day_ahead_status",
            "raw_bus_contract_diagnostics",
        ],
    )
    NextDayAlphaEngine = engine_module.NextDayAlphaEngine
    day_ahead_status_fn = getattr(engine_module, "day_ahead_status", None)
    raw_bus_contract_diagnostics_fn = getattr(
        engine_module, "raw_bus_contract_diagnostics", None
    )
except Exception as exc:
    raise RuntimeError(
        "LOCKED ENGINE IMPORT FAILED: "
        f"{ENGINE_MODULE_NAME}.py must be deployed beside this UI. "
        f"Older engine fallbacks are intentionally disabled. "
        f"Original error: {exc}"
    ) from exc

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
    if __import__("re").search(r"\d{2}[A-Z]{3}\d+(?:CE|PE)$", s):
        return False
    if __import__("re").search(r"\d{2}[A-Z]{3}.*FUT$", s) or __import__("re").search(r"(?:FUT|FUTURES)$", s):
        return False
    if __import__("re").search(r"\d{4,}", s):
        return False
    if any(x in s for x in ("BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")):
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


def render_raw_bus_contract_diagnostics() -> None:
    """Render the authoritative V3 RAW-BUS contract diagnostics."""
    st.subheader("RAW BUS CONTRACT DIAGNOSTICS")

    if not callable(raw_bus_contract_diagnostics_fn):
        st.error(
            "V3 diagnostic API missing: raw_bus_contract_diagnostics(). "
            "This indicates an incorrect/stale engine deployment."
        )
        return

    try:
        diag = raw_bus_contract_diagnostics_fn()
    except Exception as exc:
        st.error(f"RAW BUS diagnostic query failed: {exc}")
        return

    st.json(diag)

    datasets = diag.get("datasets", {}) if isinstance(diag, dict) else {}
    if not datasets:
        return

    rows = []
    for name, item in datasets.items():
        rows.append(
            {
                "Check": name,
                "Dataset": item.get("dataset"),
                "Rows": item.get("rows"),
                "Symbols": ", ".join(item.get("symbols", [])) or "-",
            }
        )

    if rows:
        st.dataframe(
            pd.DataFrame(rows),
            use_container_width=True,
            hide_index=True,
        )


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
    snapshot_method = getattr(engine, "today_snapshot", None)
    if callable(snapshot_method):
        result = snapshot_method()
    else:
        result = {}
        st.error("Canonical engine API missing: today_snapshot()")
except Exception as exc:
    result = {}
    st.error(f"Engine result read failed: {exc}")

if not result:
    status = {}
    if callable(day_ahead_status_fn):
        try:
            status = day_ahead_status_fn()
        except Exception as exc:
            st.error(f"Engine status read failed: {exc}")

    if status.get("status") == "FAILED":
        st.error("DAY-AHEAD SNAPSHOT FAILED FOR TODAY")
        if status.get("error"):
            st.code(str(status["error"]))
    else:
        st.warning("NO FROZEN DAY-AHEAD SNAPSHOT FOR TODAY")
        st.caption("The scheduled day-ahead scan has not completed successfully yet.")

    st.caption(
        "Historical source: Supabase RAW BUS | "
        "Live/opening source: Supabase RAW BUS fed by Kotak Producer"
    )

    with st.expander("WHY DID THE DAY-AHEAD SCAN FAIL?", expanded=True):
        st.caption(
            "The diagnostic below reads the same Supabase RAW BUS contract "
            "used by the locked V3 engine. It does not manufacture or alter data."
        )
        render_raw_bus_contract_diagnostics()

    if st.button(
        "Run Day-Ahead Scan Now",
        type="primary",
        use_container_width=True,
    ):
        try:
            with st.spinner("Running day-ahead scan from Supabase RAW BUS..."):
                engine.run_day_ahead()
            st.success("Day-ahead scan completed and snapshot frozen.")
            st.rerun()
        except Exception as exc:
            st.error(f"Day-ahead scan failed: {exc}")
            with st.expander("FAILURE DIAGNOSTICS", expanded=True):
                render_raw_bus_contract_diagnostics()

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
        "locked_engine_module": ENGINE_MODULE_NAME,
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
