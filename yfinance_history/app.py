#!/usr/bin/env python3
"""
Leak-Proof Historical Raw Producer | yfinance -> Supabase RAW BUS

LOCKED ARCHITECTURE
-------------------
yfinance -> HISTORICAL RAW -> Supabase -> all 3 engines

This app is ONLY a historical raw-data producer.

Rules
-----
- No indicators.
- No features.
- No scores.
- No labels.
- No regime.
- No strategy decisions.
- No synthetic rows.
- No resampling.
- No fabricated coverage.
- Do not modify the Kotak LIVE producer.
- NIFTY-500 constituents are resolved dynamically from NSE Indices.
- Requested windows are requests only.
- Actual rows returned by yfinance are authoritative.
"""

from __future__ import annotations

import json
import os
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import streamlit as st
import yfinance as yf


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    "nifty500_daily_days": 320,
    "nifty_benchmark": "^NSEI",
    "nifty_benchmark_days": 320,
    "v7_hourly_days_requested": 180,
    "v7_15m_days_requested": 55,
    "india_vix_ticker": "^INDIAVIX",
    "india_vix_days": 320,
    "batch_size": int(os.getenv("HISTORY_BATCH_SIZE", "250")),
    "workers": int(os.getenv("HISTORY_WORKERS", "6")),
    "timeout_sec": float(os.getenv("SUPABASE_TIMEOUT_SEC", "15")),
    "nifty500_constituent_url": os.getenv(
        "NIFTY500_CONSTITUENT_URL",
        "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    ),
    "nifty500_cache_ttl_sec": int(os.getenv("NIFTY500_CACHE_TTL_SEC", "21600")),
}


# ============================================================================
# HELPERS
# ============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def env_or_secret(name: str, default: str = "") -> str:
    value = env(name, "")
    if value:
        return value

    try:
        secret_value = st.secrets.get(name, "")
        if secret_value:
            return str(secret_value).strip()
    except Exception:
        pass

    return default


def clean_scalar(value: Any) -> Any:
    try:
        if value is None:
            return None

        if hasattr(value, "item"):
            value = value.item()

        if isinstance(value, float) and value != value:
            return None

        return value
    except Exception:
        return None


# ============================================================================
# SUPABASE RAW PUBLISHER
# ============================================================================

class SupabaseRawPublisher:
    """
    Append-only publisher.

    No calculations are performed here.
    """

    def __init__(self, url: str, key: str) -> None:
        self.url = str(url or "").strip().rstrip("/")
        self.key = str(key or "").strip()

        if not self.url or not self.key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY are required."
            )

        self.endpoint = (
            f"{self.url}/rest/v1/raw_observations"
        )

    def headers(self) -> Dict[str, str]:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def health(self) -> Dict[str, Any]:
        try:
            response = requests.get(
                self.endpoint,
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Accept": "application/json",
                },
                params={
                    "select": "id",
                    "limit": "1",
                },
                timeout=CONFIG["timeout_sec"],
            )

            return {
                "reachable": response.status_code in (200, 206),
                "http_status": response.status_code,
                "error": (
                    ""
                    if response.status_code in (200, 206)
                    else response.text[:300]
                ),
            }

        except Exception as exc:
            return {
                "reachable": False,
                "http_status": None,
                "error": str(exc),
            }

    def publish(
        self,
        source: str,
        symbol: str,
        token: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        total = 0
        batch_size = max(1, int(CONFIG["batch_size"]))

        for start in range(0, len(rows), batch_size):
            batch = rows[start:start + batch_size]

            records = [
                {
                    "source": source,
                    "symbol": symbol,
                    "instrument_token": str(token),
                    "observation_timestamp": utc_now_iso(),
                    "raw": row,
                }
                for row in batch
            ]

            response = requests.post(
                self.endpoint,
                headers=self.headers(),
                json=records,
                timeout=CONFIG["timeout_sec"],
            )

            if response.status_code not in (200, 201, 204):
                raise RuntimeError(
                    "Supabase publish failed "
                    f"[{response.status_code}]: "
                    f"{response.text[:500]}"
                )

            total += len(records)

        return total


# ============================================================================
# DYNAMIC NIFTY-500 UNIVERSE
# ============================================================================

YAHOO_SUFFIX = ".NS"


def _symbol_to_yahoo(symbol: str) -> str:
    """Convert an NSE equity symbol to its Yahoo Finance NSE ticker."""
    value = str(symbol or "").strip().upper()
    if not value:
        return ""
    if value.endswith(YAHOO_SUFFIX):
        return value
    return f"{value}{YAHOO_SUFFIX}"


def fetch_nifty500_constituents() -> Tuple[List[str], str]:
    """
    Fetch the current NIFTY-500 constituent list directly from NSE Indices.

    The constituent list is NOT stored in the repository. This prevents a
    stale 500-symbol file from silently becoming the research universe.
    """
    url = CONFIG["nifty500_constituent_url"]
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,application/octet-stream,*/*",
            "Referer": "https://www.niftyindices.com/",
        },
        timeout=30,
    )
    response.raise_for_status()

    text = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    field_map = {
        str(name).strip().lower(): name
        for name in (reader.fieldnames or [])
        if name
    }

    symbol_column = field_map.get("symbol")
    if not symbol_column:
        raise RuntimeError(
            "NIFTY-500 constituent CSV did not contain a Symbol column."
        )

    symbols: List[str] = []
    for row in reader:
        raw_symbol = str(row.get(symbol_column, "") or "").strip()
        yahoo_symbol = _symbol_to_yahoo(raw_symbol)
        if yahoo_symbol:
            symbols.append(yahoo_symbol)

    symbols = list(dict.fromkeys(symbols))

    # Guard against an HTML/error page or truncated response being accepted
    # as a valid universe. Do not silently run an incomplete NIFTY-500 scan.
    if len(symbols) < 450:
        raise RuntimeError(
            f"Dynamic NIFTY-500 constituent fetch returned only "
            f"{len(symbols)} symbols; refusing incomplete universe."
        )

    return symbols, url


@st.cache_data(ttl=CONFIG["nifty500_cache_ttl_sec"], show_spinner=False)
def get_dynamic_nifty500() -> Tuple[List[str], str]:
    return fetch_nifty500_constituents()


# ============================================================================
# YFINANCE RAW FETCH

# ============================================================================

def dataframe_rows(
    ticker: str,
    interval: str,
    frame: Any,
) -> List[Dict[str, Any]]:
    if frame is None or frame.empty:
        return []

    rows: List[Dict[str, Any]] = []

    for index, values in frame.iterrows():
        row: Dict[str, Any] = {
            "event_timestamp": str(index),
            "ticker": ticker,
            "interval": interval,
        }

        for column, value in values.items():
            row[str(column)] = clean_scalar(value)

        rows.append(row)

    return rows


def fetch_history(
    ticker: str,
    period: str,
    interval: str,
) -> Tuple[str, str, List[Dict[str, Any]], str]:
    """
    Return ONLY rows actually returned by yfinance.

    No interpolation.
    No resampling.
    No padding.
    No synthetic rows.
    """

    try:
        frame = yf.Ticker(ticker).history(
            period=period,
            interval=interval,
            auto_adjust=False,
            actions=False,
            prepost=False,
            repair=False,
            keepna=False,
            raise_errors=False,
        )

        rows = dataframe_rows(
            ticker=ticker,
            interval=interval,
            frame=frame,
        )

        return ticker, interval, rows, ""

    except Exception as exc:
        return ticker, interval, [], str(exc)


def publish_one(
    publisher: SupabaseRawPublisher,
    ticker: str,
    period: str,
    interval: str,
    dataset: str,
) -> Dict[str, Any]:

    symbol, actual_interval, rows, error = fetch_history(
        ticker,
        period,
        interval,
    )

    if error:
        return {
            "ticker": ticker,
            "interval": interval,
            "dataset": dataset,
            "rows_returned": 0,
            "rows_published": 0,
            "status": "ERROR",
            "error": error,
        }

    if not rows:
        return {
            "ticker": ticker,
            "interval": actual_interval,
            "dataset": dataset,
            "rows_returned": 0,
            "rows_published": 0,
            "status": "NO_DATA",
            "error": "",
        }

    # Metadata only. No calculations.
    for row in rows:
        row["dataset"] = dataset
        row["requested_period"] = period
        row["source"] = "yfinance"

    try:
        published = publisher.publish(
            source="yfinance_history",
            symbol=symbol,
            token=symbol,
            rows=rows,
        )

        return {
            "ticker": ticker,
            "interval": actual_interval,
            "dataset": dataset,
            "rows_returned": len(rows),
            "rows_published": published,
            "status": "PASS",
            "error": "",
        }

    except Exception as exc:
        return {
            "ticker": ticker,
            "interval": actual_interval,
            "dataset": dataset,
            "rows_returned": len(rows),
            "rows_published": 0,
            "status": "PUBLISH_ERROR",
            "error": str(exc),
        }


def run_jobs(
    publisher: SupabaseRawPublisher,
    jobs: Iterable[Tuple[str, str, str, str]],
    progress_callback=None,
) -> List[Dict[str, Any]]:

    job_list = list(jobs)
    results: List[Dict[str, Any]] = []

    if not job_list:
        return results

    max_workers = max(1, int(CONFIG["workers"]))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                publish_one,
                publisher,
                ticker,
                period,
                interval,
                dataset,
            ): (
                ticker,
                interval,
                dataset,
            )
            for ticker, period, interval, dataset in job_list
        }

        completed = 0

        for future in as_completed(future_map):
            result = future.result()
            results.append(result)

            completed += 1

            if progress_callback is not None:
                progress_callback(
                    completed,
                    len(job_list),
                    result,
                )

    return results


# ============================================================================
# JOB BUILDER
# ============================================================================

def build_jobs(
    nifty500_tickers: List[str],
    include_nifty500: bool,
    include_benchmark: bool,
    include_vix: bool,
    include_v7_mtf: bool,
    mtf_tickers: List[str],
) -> List[Tuple[str, str, str, str]]:

    jobs: List[Tuple[str, str, str, str]] = []

    if include_nifty500:
        for ticker in nifty500_tickers:
            jobs.append(
                (
                    ticker,
                    f"{CONFIG['nifty500_daily_days']}d",
                    "1d",
                    "nifty500_daily",
                )
            )

    if include_benchmark:
        jobs.append(
            (
                CONFIG["nifty_benchmark"],
                f"{CONFIG['nifty_benchmark_days']}d",
                "1d",
                "nifty_benchmark_daily",
            )
        )

    if include_vix:
        jobs.append(
            (
                CONFIG["india_vix_ticker"],
                f"{CONFIG['india_vix_days']}d",
                "1d",
                "india_vix_daily",
            )
        )

    if include_v7_mtf:
        for ticker in mtf_tickers:
            jobs.append(
                (
                    ticker,
                    f"{CONFIG['v7_hourly_days_requested']}d",
                    "1h",
                    "v7_mtf_hourly_requested",
                )
            )

            jobs.append(
                (
                    ticker,
                    f"{CONFIG['v7_15m_days_requested']}d",
                    "15m",
                    "v7_mtf_15m_requested",
                )
            )

    return jobs


# ============================================================================
# STREAMLIT APP
# ============================================================================

st.set_page_config(
    page_title="yfinance Historical Raw Producer",
    page_icon="ðŸ“š",
    layout="wide",
)

st.title("ðŸ“š Historical Raw Data Producer")
st.caption("yfinance â†’ HISTORICAL RAW â†’ Supabase â†’ all 3 engines")

st.info(
    "LOCKED CONTRACT: this worker publishes raw historical observations only. "
    "No indicators, features, scores, labels, regime or strategy decisions "
    "are calculated here."
)


# ----------------------------------------------------------------------------
# SESSION STATE
# ----------------------------------------------------------------------------

if "last_run_summary" not in st.session_state:
    st.session_state.last_run_summary = None

if "last_results" not in st.session_state:
    st.session_state.last_results = []


# ----------------------------------------------------------------------------
# SIDEBAR
# ----------------------------------------------------------------------------

with st.sidebar:
    st.header("ðŸ—„ï¸ Supabase RAW BUS")

    supabase_url = st.text_input(
        "Supabase URL",
        value=env_or_secret("SUPABASE_URL", ""),
        type="default",
        placeholder="https://your-project.supabase.co",
    )

    supabase_key = st.text_input(
        "Supabase Key",
        value=env_or_secret("SUPABASE_KEY", ""),
        type="password",
        placeholder="Supabase anon/service key",
    )

    st.markdown("---")

    st.header("ðŸ“ NIFTY-500 Universe")

    ticker_upload = st.file_uploader(
        "Optional: upload nifty500_yahoo_tickers.txt",
        type=["txt", "csv"],
        help=(
            "One Yahoo ticker per line. "
            "If omitted, the repository file is used."
        ),
    )

    st.markdown("---")

    st.header("ðŸŽ¯ Historical Jobs")

    run_nifty500 = st.checkbox(
        "NIFTY-500 daily â€¢ 320d",
        value=True,
    )

    run_benchmark = st.checkbox(
        "NIFTY benchmark â€¢ 320d",
        value=True,
    )

    run_vix = st.checkbox(
        "India VIX â€¢ 320d",
        value=True,
    )

    run_v7 = st.checkbox(
        "V7 MTF basket â€¢ 1h + 15m",
        value=False,
        help=(
            "1h request = 180d; 15m request = 55d. "
            "yfinance may return less because intraday history "
            "is source-limited."
        ),
    )

    mtf_upload = st.file_uploader(
        "Optional: upload V7 MTF ticker file",
        type=["txt", "csv"],
        key="mtf_upload",
    )

    st.markdown("---")

    st.header("âš™ï¸ Runtime")

    st.write(
        {
            "Batch size": CONFIG["batch_size"],
            "Workers": CONFIG["workers"],
            "Supabase timeout": CONFIG["timeout_sec"],
            "NIFTY-500 daily": "320d",
            "V7 hourly requested": "180d",
            "V7 15m requested": "55d",
        }
    )


# ----------------------------------------------------------------------------
# DYNAMIC UNIVERSE RESOLUTION
# ----------------------------------------------------------------------------

nifty500_tickers: List[str] = []
ticker_source = ""

try:
    nifty500_tickers, ticker_source = get_dynamic_nifty500()
except Exception as exc:
    ticker_source = "not available"
    st.error(
        "Unable to obtain the current NIFTY-500 constituent universe. "
        "The NIFTY-500 job is disabled for this run rather than using a "
        "stale/incomplete symbol list."
    )
    st.caption(str(exc))


mtf_tickers: List[str] = []

if mtf_upload is not None:
    mtf_text = mtf_upload.getvalue().decode(
        "utf-8",
        errors="replace",
    )
    mtf_tickers = parse_ticker_text(mtf_text)


# ----------------------------------------------------------------------------
# TOP STATUS
# ----------------------------------------------------------------------------

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "NIFTY-500 tickers",
    len(nifty500_tickers),
)

c2.metric(
    "Supabase",
    "CONFIGURED"
    if supabase_url and supabase_key
    else "NOT CONFIGURED",
)

c3.metric(
    "V7 MTF tickers",
    len(mtf_tickers),
)

c4.metric(
    "Mode",
    "RAW ONLY",
)


# ----------------------------------------------------------------------------
# SUPABASE TEST
# ----------------------------------------------------------------------------

st.subheader("ðŸ”Œ Supabase RAW BUS")

test_col, run_col = st.columns(2)

with test_col:
    if st.button(
        "Test Supabase Connection",
        use_container_width=True,
    ):
        if not supabase_url or not supabase_key:
            st.error(
                "Enter Supabase URL and Supabase Key in the sidebar first."
            )
        else:
            try:
                publisher = SupabaseRawPublisher(
                    supabase_url,
                    supabase_key,
                )
                health = publisher.health()

                if health["reachable"]:
                    st.success(
                        "Supabase RAW BUS reachable."
                    )
                else:
                    st.error(
                        "Supabase RAW BUS not reachable: "
                        f"{health['error']}"
                    )

            except Exception as exc:
                st.error(str(exc))


# ----------------------------------------------------------------------------
# RUN
# ----------------------------------------------------------------------------

with run_col:
    run_clicked = st.button(
        "ðŸš€ Run Historical Raw Producer",
        type="primary",
        use_container_width=True,
    )


if run_clicked:

    if not supabase_url or not supabase_key:
        st.error(
            "Supabase URL and Supabase Key are required. "
            "Enter them in the sidebar and run again."
        )
        st.stop()

    if run_nifty500 and not nifty500_tickers:
        st.error(
            "NIFTY-500 job is selected but the current constituent universe "
            "could not be fetched from NSE Indices. Fix the source/network "
            "issue and run again."
        )
        st.stop()

    if run_v7 and not mtf_tickers:
        st.error(
            "V7 MTF is selected but no V7 MTF ticker file was uploaded. "
            "Upload the basket file or turn V7 MTF off."
        )
        st.stop()

    jobs = build_jobs(
        nifty500_tickers=nifty500_tickers,
        include_nifty500=run_nifty500,
        include_benchmark=run_benchmark,
        include_vix=run_vix,
        include_v7_mtf=run_v7,
        mtf_tickers=mtf_tickers,
    )

    if not jobs:
        st.error(
            "No historical jobs selected."
        )
        st.stop()

    st.subheader("ðŸ“¡ Producer Execution")

    progress = st.progress(0)
    status_box = st.empty()
    log_box = st.empty()

    try:
        publisher = SupabaseRawPublisher(
            supabase_url,
            supabase_key,
        )

        health = publisher.health()

        if not health["reachable"]:
            st.error(
                "Supabase health check failed: "
                f"{health['error']}"
            )
            st.stop()

        status_box.info(
            f"Supabase reachable. Starting {len(jobs)} raw jobs..."
        )

        live_log: List[str] = []

        def on_progress(
            completed: int,
            total: int,
            result: Dict[str, Any],
        ) -> None:

            pct = int(
                completed * 100 / max(1, total)
            )

            progress.progress(
                completed / max(1, total)
            )

            message = (
                f"{completed}/{total} | "
                f"{result['status']} | "
                f"{result['dataset']} | "
                f"{result['ticker']} | "
                f"returned={result['rows_returned']} | "
                f"published={result['rows_published']}"
            )

            live_log.append(message)

            if result.get("error"):
                live_log.append(
                    f"ERROR: {result['error']}"
                )

            log_box.code(
                "\n".join(live_log[-25:]),
                language="text",
            )

            status_box.info(
                f"Progress {pct}% â€¢ {message}"
            )

        results = run_jobs(
            publisher,
            jobs,
            progress_callback=on_progress,
        )

        summary = {
            "run_timestamp_utc": utc_now_iso(),
            "jobs": len(results),
            "rows_returned": sum(
                int(r["rows_returned"])
                for r in results
            ),
            "rows_published": sum(
                int(r["rows_published"])
                for r in results
            ),
            "pass": sum(
                1
                for r in results
                if r["status"] == "PASS"
            ),
            "no_data": sum(
                1
                for r in results
                if r["status"] == "NO_DATA"
            ),
            "errors": sum(
                1
                for r in results
                if r["status"]
                in (
                    "ERROR",
                    "PUBLISH_ERROR",
                )
            ),
        }

        st.session_state.last_results = results
        st.session_state.last_run_summary = summary

        progress.progress(1.0)

        if summary["errors"] == 0:
            st.success(
                "Historical raw producer completed successfully."
            )
        else:
            st.warning(
                "Run completed with one or more fetch/publish errors."
            )

    except Exception as exc:
        st.error(
            f"Historical producer failed: {exc}"
        )


# ----------------------------------------------------------------------------
# LAST RUN SUMMARY
# ----------------------------------------------------------------------------

if st.session_state.last_run_summary:

    st.subheader("ðŸ“Š Last Run Summary")

    st.json(
        st.session_state.last_run_summary
    )


# ----------------------------------------------------------------------------
# RESULTS
# ----------------------------------------------------------------------------

if st.session_state.last_results:

    st.subheader("ðŸ“‹ Job Results")

    # Keep UI simple and dependency-light.
    display_rows = []

    for result in st.session_state.last_results:
        display_rows.append(
            {
                "status": result["status"],
                "dataset": result["dataset"],
                "ticker": result["ticker"],
                "interval": result["interval"],
                "returned": result["rows_returned"],
                "published": result["rows_published"],
                "error": result["error"],
            }
        )

    st.dataframe(
        display_rows,
        use_container_width=True,
        hide_index=True,
    )


# ----------------------------------------------------------------------------
# ARCHITECTURE AUDIT
# ----------------------------------------------------------------------------

st.subheader("ðŸ”’ Architecture Lock")

st.code(
    "Kotak Neo  â†’ LIVE RAW       â†’ Supabase raw_observations â†’ all 3 engines\n"
    "yfinance   â†’ HISTORICAL RAW â†’ Supabase raw_observations â†’ all 3 engines\n"
    "                 â†‘\n"
    "        THIS APP ONLY\n\n"
    "No features / scores / labels / regime / decisions cross the bus.",
    language="text",
)

st.caption(
    f"Ticker source: {ticker_source}. "
    "Actual yfinance coverage is authoritative; requested periods "
    "are never treated as guaranteed coverage."
)
