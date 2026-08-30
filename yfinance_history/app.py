#!/usr/bin/env python3
"""
NIFTY Raw Bus — yfinance HISTORY environment

Scope:
    yfinance historical raw observations -> Supabase raw_observations only.

Isolation:
    - NO Kotak Neo dependency.
    - NO live broker connection.
    - NO indicators, features, scores, labels, regime, signals or strategy logic.
    - NO timeframe resampling.
    - Only observations actually returned by Yahoo/yfinance are published.
"""

from __future__ import annotations

import os
import csv
import io
import time
import hashlib
import platform
import importlib.util
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
import requests

try:
    import streamlit as st
except ImportError:
    st = None


IST = ZoneInfo("Asia/Kolkata")
NSE_NIFTY500_CSV_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"

CONFIG = {
    "next_day_daily_days": 320,
    "next_day_mtf_hourly_days": 180,
    "next_day_mtf_15m_days": 55,
    "next_day_vix_days": 320,
    "nifty_history_days": 320,
    "history_batch_size": 250,
    "history_workers": 6,
    "supabase_timeout_sec": 15,
}

YFINANCE_LIMITS = {
    "intraday_adaptive": True,
    "1h_requested_days_by_v7": 180,
    "1d_requested_days_by_v7": 320,
    "15m_requested_days_by_v7": 55,
    "policy": "request desired window, then use actual source-available window only",
}


def now_ist() -> datetime:
    return datetime.now(IST)


def env_or_secret(name: str, default: str = "") -> str:
    value = os.getenv(name, "")
    if value:
        return value
    if st is not None:
        try:
            value = st.secrets.get(name, "")
            if value:
                return str(value)
        except Exception:
            pass
    return default


def fetch_nifty500_symbols_from_nse() -> List[str]:
    """Universe metadata only; no market calculations."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/126 Safari/537.36"
        ),
        "Accept": "text/csv,text/plain,*/*",
        "Referer": "https://www.nseindia.com/",
    }
    response = requests.get(NSE_NIFTY500_CSV_URL, headers=headers, timeout=20)
    response.raise_for_status()

    text = response.text.lstrip("\ufeff")
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        raise RuntimeError("NSE NIFTY-500 CSV returned no rows.")

    symbol_key = next(
        (
            k for k in rows[0].keys()
            if str(k).strip().lower() in {"symbol", "symbols"}
        ),
        None,
    )
    if not symbol_key:
        raise RuntimeError(
            f"NSE NIFTY-500 CSV has no Symbol column. "
            f"Columns: {list(rows[0].keys())}"
        )

    symbols: List[str] = []
    for row in rows:
        sym = str(row.get(symbol_key, "")).strip().upper()
        if sym and sym not in symbols:
            symbols.append(sym)

    if len(symbols) < 400:
        raise RuntimeError(
            f"NIFTY-500 universe looks incomplete: {len(symbols)} symbols returned."
        )

    return symbols


class YahooConnector:
    """
    Historical/raw Yahoo producer.

    IMPORTANT:
        Requested windows are never fabricated. For intraday intervals,
        smaller source-safe windows are tried only when necessary.
    """

    INTRADAY_FALLBACK_DAYS = {
        "1h": (180, 120, 90, 60, 30, 14, 7, 3, 1),
        "60m": (180, 120, 90, 60, 30, 14, 7, 3, 1),
        "15m": (55, 50, 45, 30, 14, 7, 3, 1),
        "30m": (60, 45, 30, 14, 7, 3, 1),
        "5m": (30, 14, 7, 3, 1),
        "2m": (30, 14, 7, 3, 1),
        "1m": (7, 3, 1),
    }

    last_diagnostics: Dict[str, Any] = {}

    @staticmethod
    def _clean_downloaded_frame(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize a real Yahoo response without creating observations."""
        if df is None or df.empty:
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            if len(set(df.columns.get_level_values(-1))) == 1:
                df.columns = [c[0] for c in df.columns]
            else:
                df.columns = [
                    c[-1] if isinstance(c, tuple) else c
                    for c in df.columns
                ]

        df = df.reset_index()

        time_col = (
            "Datetime"
            if "Datetime" in df.columns
            else "Date"
            if "Date" in df.columns
            else df.columns[0]
        )

        rename = {time_col: "event_timestamp"}
        for column in ("Open", "High", "Low", "Close", "Volume"):
            if column in df.columns:
                rename[column] = column.lower()

        df = df.rename(columns=rename)

        keep = [
            c for c in
            ["event_timestamp", "open", "high", "low", "close", "volume"]
            if c in df.columns
        ]
        df = df[keep].copy()

        if "event_timestamp" not in df.columns or "close" not in df.columns:
            return pd.DataFrame()

        ts = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True)
        df["event_timestamp"] = ts.dt.tz_convert(IST)

        for column in ("open", "high", "low", "close", "volume"):
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        return (
            df.dropna(subset=["event_timestamp", "close"])
            .drop_duplicates("event_timestamp")
            .sort_values("event_timestamp")
            .reset_index(drop=True)
        )

    @classmethod
    def _request_days(cls, ticker: str, days: int, interval: str) -> pd.DataFrame:
        end = now_ist()
        start = end - pd.Timedelta(days=int(days))

        return cls._clean_downloaded_frame(
            yf.download(
                ticker,
                interval=interval,
                progress=False,
                auto_adjust=False,
                threads=False,
                start=start,
                end=end,
            )
        )

    @staticmethod
    def _actual_days(df: pd.DataFrame) -> float:
        if df is None or df.empty or "event_timestamp" not in df.columns:
            return 0.0
        try:
            delta = df["event_timestamp"].iloc[-1] - df["event_timestamp"].iloc[0]
            return round(max(0.0, delta.total_seconds() / 86400.0), 2)
        except Exception:
            return 0.0

    @classmethod
    def _download(
        cls,
        ticker: str,
        period: Optional[str] = None,
        days: Optional[int] = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        requested_days = int(days) if days is not None else None
        attempts: List[int] = []

        try:
            if period:
                df = cls._clean_downloaded_frame(
                    yf.download(
                        ticker,
                        period=period,
                        interval=interval,
                        progress=False,
                        auto_adjust=False,
                        threads=False,
                    )
                )
                cls.last_diagnostics = {
                    "ticker": ticker,
                    "interval": interval,
                    "requested_days": requested_days,
                    "requested_period": period,
                    "attempted_windows_days": [],
                    "actual_returned_days": cls._actual_days(df),
                    "returned_rows": int(len(df)),
                    "status": "AVAILABLE" if not df.empty else "NO_DATA",
                    "coverage_policy": "actual returned rows only",
                }
                return df

            if days is None:
                days = 1

            if interval in cls.INTRADAY_FALLBACK_DAYS:
                candidates = [int(days)] + [
                    int(x)
                    for x in cls.INTRADAY_FALLBACK_DAYS[interval]
                    if int(x) < int(days)
                ]
                windows = list(dict.fromkeys(candidates))
            else:
                windows = [int(days)]

            df = pd.DataFrame()
            used_days = None
            last_error = None

            for window_days in windows:
                attempts.append(window_days)
                try:
                    candidate = cls._request_days(
                        ticker, window_days, interval
                    )
                    if not candidate.empty:
                        df = candidate
                        used_days = window_days
                        break
                except Exception as exc:
                    last_error = str(exc)

            actual_days = cls._actual_days(df)
            status = "AVAILABLE" if not df.empty else "NO_DATA"

            if (
                used_days is not None
                and requested_days is not None
                and used_days < requested_days
            ):
                status = "AVAILABLE_SHORTER_SOURCE_WINDOW"

            cls.last_diagnostics = {
                "ticker": ticker,
                "interval": interval,
                "requested_days": requested_days,
                "attempted_windows_days": attempts,
                "source_window_used_days": used_days,
                "actual_returned_days": actual_days,
                "returned_rows": int(len(df)),
                "status": status,
                "coverage_policy": "actual returned rows only",
            }

            if last_error:
                cls.last_diagnostics["last_error"] = last_error

            return df

        except Exception as exc:
            cls.last_diagnostics = {
                "ticker": ticker,
                "interval": interval,
                "requested_days": requested_days,
                "attempted_windows_days": attempts,
                "source_window_used_days": None,
                "actual_returned_days": 0.0,
                "returned_rows": 0,
                "status": "NO_DATA",
                "coverage_policy": "actual returned rows only",
                "error": str(exc),
            }
            return pd.DataFrame()

    @classmethod
    def fetch_symbol_history(
        cls,
        symbol: str,
        days: int,
        interval: str,
    ) -> pd.DataFrame:
        ticker = (
            symbol
            if any(ch in str(symbol) for ch in ("^", "=", "."))
            else f"{symbol}.NS"
        )
        return cls._download(ticker, days=days, interval=interval)

    @classmethod
    def fetch_vix(cls) -> pd.DataFrame:
        return cls._download(
            "^INDIAVIX",
            days=CONFIG["next_day_vix_days"],
            interval="1d",
        )

    @classmethod
    def health_probe(cls) -> Dict[str, Any]:
        probes = [
            ("NIFTY daily", "^NSEI", 320, "1d"),
            ("Representative 1h", "RELIANCE.NS", 180, "1h"),
            ("Representative 15m", "RELIANCE.NS", 55, "15m"),
            ("India VIX daily", "^INDIAVIX", 320, "1d"),
        ]

        out: Dict[str, Any] = {}

        for label, ticker, days, interval in probes:
            df = cls._download(ticker, days=days, interval=interval)
            diag = dict(cls.last_diagnostics)

            out[label] = {
                "ticker": ticker,
                "requested_days": days,
                "interval": interval,
                "returned_rows": int(len(df)),
                "actual_returned_days": cls._actual_days(df),
                "source_window_used_days": diag.get("source_window_used_days"),
                "attempted_windows_days": diag.get("attempted_windows_days", []),
                "first_timestamp": (
                    str(df.iloc[0]["event_timestamp"])
                    if not df.empty else None
                ),
                "last_timestamp": (
                    str(df.iloc[-1]["event_timestamp"])
                    if not df.empty else None
                ),
                "status": diag.get(
                    "status",
                    "AVAILABLE" if not df.empty else "NO_DATA",
                ),
                "coverage_policy": "actual returned rows only",
            }

        return out


class SupabasePublisher:
    """Append-only raw bus publisher. No calculations occur here."""

    def __init__(self, url_override: str = "", key_override: str = ""):
        self.url = str(
            url_override or env_or_secret("SUPABASE_URL", "")
        ).strip()
        self.key = str(
            key_override or env_or_secret("SUPABASE_KEY", "")
        ).strip()

    def _headers(self) -> Dict[str, str]:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def health(self) -> Dict[str, Any]:
        if not self.url or not self.key:
            return {
                "configured": False,
                "reachable": False,
                "error": "Supabase URL/Key missing",
            }

        endpoint = f"{self.url.rstrip('/')}/rest/v1/raw_observations"

        try:
            response = requests.get(
                endpoint,
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Accept": "application/json",
                },
                params={"select": "id", "limit": "1"},
                timeout=float(CONFIG["supabase_timeout_sec"]),
            )

            return {
                "configured": True,
                "reachable": response.status_code in (200, 206),
                "http_status": response.status_code,
                "error": (
                    ""
                    if response.status_code in (200, 206)
                    else response.text[:250]
                ),
            }

        except Exception as exc:
            return {
                "configured": True,
                "reachable": False,
                "error": str(exc),
            }

    def publish_observations_batch(
        self,
        source: str,
        symbol: str,
        token: str,
        raw_payloads: List[dict],
    ) -> int:
        if not self.url or not self.key or not raw_payloads:
            return 0

        total = 0
        endpoint = f"{self.url.rstrip('/')}/rest/v1/raw_observations"
        batch_size = max(1, int(CONFIG["history_batch_size"]))

        for i in range(0, len(raw_payloads), batch_size):
            batch = raw_payloads[i:i + batch_size]
            records = []

            for payload in batch:
                records.append({
                    "source": source,
                    "symbol": symbol,
                    "instrument_token": str(token),
                    "observation_timestamp": now_ist().isoformat(),
                    "raw": payload,
                })

            try:
                response = requests.post(
                    endpoint,
                    headers=self._headers(),
                    json=records,
                    timeout=float(CONFIG["supabase_timeout_sec"]),
                )

                if response.status_code in (200, 201, 204):
                    total += len(records)
                else:
                    print(
                        "Supabase batch publish failed "
                        f"[{response.status_code}]: {response.text[:300]}"
                    )

            except Exception as exc:
                print(f"Supabase batch publish error: {exc}")

        return total


class HistoricalRawProducer:
    """
    Fetch and publish raw historical observations.

    No:
        - indicators
        - feature engineering
        - scoring
        - regime detection
        - labels
        - signals
        - strategy logic
        - timeframe resampling
    """

    def __init__(self, publisher: SupabasePublisher):
        self.publisher = publisher
        self.last_stats: Dict[str, Any] = {}

    @staticmethod
    def _raw_rows(
        symbol: str,
        df: pd.DataFrame,
        timeframe: str,
        dataset: str,
        source: str,
    ) -> List[dict]:
        rows: List[dict] = []

        if df is None or df.empty:
            return rows

        for _, row in df.iterrows():
            event_ts = row.get("event_timestamp")

            if pd.isna(event_ts):
                continue

            def numeric_or_none(value):
                if value is None or pd.isna(value):
                    return None
                try:
                    return float(value)
                except Exception:
                    return None

            payload = {
                "dataset": dataset,
                "timeframe": timeframe,
                "event_timestamp": str(event_ts),
                "open": numeric_or_none(row.get("open")),
                "high": numeric_or_none(row.get("high")),
                "low": numeric_or_none(row.get("low")),
                "close": numeric_or_none(row.get("close")),
                "volume": numeric_or_none(row.get("volume")),
                "raw_source": source,
            }

            payload["observation_id"] = hashlib.sha256(
                (
                    f"{source}|{dataset}|{symbol}|"
                    f"{timeframe}|{payload['event_timestamp']}"
                ).encode("utf-8")
            ).hexdigest()

            rows.append(payload)

        return rows

    def publish_history(
        self,
        symbol: str,
        df: pd.DataFrame,
        timeframe: str,
        dataset: str,
    ) -> int:
        rows = self._raw_rows(
            symbol,
            df,
            timeframe,
            dataset,
            "yfinance",
        )

        count = self.publisher.publish_observations_batch(
            source="yahoo_historical",
            symbol=symbol,
            token=f"{symbol}.NS",
            raw_payloads=rows,
        )

        self.last_stats[f"{dataset}:{symbol}"] = {
            "requested_timeframe": timeframe,
            "returned_rows": len(rows),
            "published_rows": count,
            "first_event_timestamp": (
                rows[0]["event_timestamp"] if rows else None
            ),
            "last_event_timestamp": (
                rows[-1]["event_timestamp"] if rows else None
            ),
            "source": "yfinance",
        }

        return count

    def publish_nifty_history(self) -> int:
        df = YahooConnector._download(
            "^NSEI",
            days=CONFIG["nifty_history_days"],
            interval="1d",
        )

        return self.publish_history(
            "NIFTY_SPOT",
            df,
            "1d",
            "nifty_spot_daily",
        )

    def publish_next_day_universe_history(
        self,
        symbols: List[str],
    ) -> Dict[str, int]:
        out: Dict[str, int] = {}

        def worker(symbol: str):
            df = YahooConnector.fetch_symbol_history(
                symbol,
                CONFIG["next_day_daily_days"],
                "1d",
            )
            return symbol, self.publish_history(
                symbol,
                df,
                "1d",
                "next_day_stock_daily",
            )

        with ThreadPoolExecutor(
            max_workers=CONFIG["history_workers"]
        ) as pool:
            futures = [pool.submit(worker, symbol) for symbol in symbols]

            for future in as_completed(futures):
                try:
                    symbol, count = future.result()
                    out[symbol] = count
                except Exception as exc:
                    out[f"ERROR:{len(out)}"] = 0
                    print(f"Next-Day history worker error: {exc}")

        return out

    def publish_mtf_history(
        self,
        symbols: List[str],
    ) -> Dict[str, Dict[str, int]]:
        out: Dict[str, Dict[str, int]] = {}

        def worker(symbol: str):
            result: Dict[str, int] = {}

            requests_to_make = (
                (
                    "1d",
                    CONFIG["next_day_daily_days"],
                    "next_day_mtf_daily",
                ),
                (
                    "1h",
                    CONFIG["next_day_mtf_hourly_days"],
                    "next_day_mtf_hourly",
                ),
                (
                    "15m",
                    CONFIG["next_day_mtf_15m_days"],
                    "next_day_mtf_15m",
                ),
            )

            for interval, days, dataset in requests_to_make:
                df = YahooConnector.fetch_symbol_history(
                    symbol,
                    days,
                    interval,
                )
                result[interval] = self.publish_history(
                    symbol,
                    df,
                    interval,
                    dataset,
                )

            return symbol, result

        with ThreadPoolExecutor(
            max_workers=CONFIG["history_workers"]
        ) as pool:
            futures = [pool.submit(worker, symbol) for symbol in symbols]

            for future in as_completed(futures):
                try:
                    symbol, result = future.result()
                    out[symbol] = result
                except Exception as exc:
                    print(f"MTF history worker error: {exc}")

        return out

    def publish_vix(self) -> int:
        df = YahooConnector.fetch_vix()

        return self.publish_history(
            "INDIAVIX",
            df,
            "1d",
            "india_vix_daily",
        )


def coverage_report() -> Dict[str, Any]:
    return {
        "environment": "yfinance_history_only",
        "kotak_dependency": False,
        "contracts": {
            "next_day_daily": {
                "requested_days": CONFIG["next_day_daily_days"],
                "interval": "1d",
                "source": "yfinance",
                "policy": "store actual returned raw history",
            },
            "next_day_mtf_hourly": {
                "requested_days": CONFIG["next_day_mtf_hourly_days"],
                "interval": "1h",
                "source": "yfinance",
                "policy": (
                    "Never fabricate, resample, duplicate, or relabel "
                    "shorter returned history as 180d."
                ),
            },
            "next_day_mtf_15m": {
                "requested_days": CONFIG["next_day_mtf_15m_days"],
                "interval": "15m",
                "source": "yfinance",
                "policy": "store actual returned raw history",
            },
            "india_vix_daily": {
                "requested_days": CONFIG["next_day_vix_days"],
                "interval": "1d",
                "source": "yfinance",
                "policy": "store actual returned raw history",
            },
            "nifty_daily": {
                "requested_days": CONFIG["nifty_history_days"],
                "interval": "1d",
                "source": "yfinance",
                "policy": "store actual returned raw history",
            },
        },
        "intraday_source_rule": (
            "Request the engine-required window first. If Yahoo rejects "
            "or cannot supply it, try smaller source-safe windows and "
            "publish only actual returned raw observations."
        ),
    }


def main():
    if st is None:
        print("Streamlit is not installed.")
        return

    st.set_page_config(
        page_title="NIFTY yfinance Historical Raw Producer",
        layout="wide",
    )

    st.title("📚 NIFTY yfinance Historical Raw Producer")
    st.caption(
        "yfinance → HISTORICAL RAW → Supabase only. "
        "No Kotak dependency. No calculations."
    )

    with st.sidebar:
        st.header("🗄️ Supabase RAW BUS")

        supabase_url = st.text_input(
            "Supabase URL",
            value=env_or_secret("SUPABASE_URL", ""),
        )
        supabase_key = st.text_input(
            "Supabase Key",
            value=env_or_secret("SUPABASE_KEY", ""),
            type="password",
        )

        supabase = SupabasePublisher(
            url_override=supabase_url,
            key_override=supabase_key,
        )
        historical = HistoricalRawProducer(supabase)

        if st.button("Test Supabase RAW BUS"):
            health = supabase.health()
            if health.get("reachable"):
                st.success("Supabase RAW BUS reachable.")
            else:
                st.error(health.get("error", "Supabase connection failed."))

        st.markdown("---")
        st.header("📡 yfinance Source")

        if st.button("Test yfinance Data Source"):
            with st.spinner("Checking Yahoo/yfinance source coverage..."):
                st.session_state["yf_health"] = YahooConnector.health_probe()

        if st.session_state.get("yf_health"):
            st.json(st.session_state["yf_health"])

        st.markdown("---")
        st.header("📋 NIFTY-500 Universe")

        if "hist_symbols" not in st.session_state:
            st.session_state["hist_symbols"] = ""

        if "mtf_symbols" not in st.session_state:
            st.session_state["mtf_symbols"] = ""

        if st.button("Load NIFTY-500 from NSE"):
            try:
                with st.spinner("Loading current NIFTY-500 list from NSE..."):
                    symbols = fetch_nifty500_symbols_from_nse()

                st.session_state["hist_symbols"] = "\n".join(symbols)
                st.success(f"Loaded {len(symbols)} NIFTY-500 symbols.")
            except Exception as exc:
                st.error(f"NIFTY-500 load failed: {exc}")

        hist_symbols_text = st.text_area(
            "NIFTY-500 symbols (one per line)",
            height=160,
            key="hist_symbols",
        )

        mtf_symbols_text = st.text_area(
            "MTF basket symbols (one per line)",
            height=120,
            key="mtf_symbols",
        )

        st.markdown("---")
        st.header("🚀 Publish Historical RAW")

        if st.button("Publish NIFTY History", type="primary"):
            if not supabase.url or not supabase.key:
                st.error("Supabase URL/Key missing.")
            else:
                try:
                    with st.spinner("Publishing NIFTY historical raw data..."):
                        count = historical.publish_nifty_history()
                    st.success(f"NIFTY historical rows published: {count}")
                except Exception as exc:
                    st.error(f"NIFTY history publish failed: {exc}")

        if st.button("Publish Next-Day 500 History"):
            if not supabase.url or not supabase.key:
                st.error("Supabase URL/Key missing.")
            else:
                try:
                    symbols = [
                        x.strip().upper()
                        for x in hist_symbols_text.replace(",", "\n").splitlines()
                        if x.strip()
                    ]

                    if not symbols:
                        with st.spinner("Loading NIFTY-500 from NSE..."):
                            symbols = fetch_nifty500_symbols_from_nse()

                        st.session_state["hist_symbols"] = "\n".join(symbols)

                    with st.spinner(
                        f"Publishing {len(symbols)} symbols × "
                        f"{CONFIG['next_day_daily_days']} daily bars..."
                    ):
                        stats = historical.publish_next_day_universe_history(
                            symbols
                        )

                    st.success(
                        f"Completed: {len(stats)} symbols processed."
                    )
                except Exception as exc:
                    st.error(
                        f"Next-Day 500 history publish failed: {exc}"
                    )

        if st.button("Publish MTF + VIX"):
            if not supabase.url or not supabase.key:
                st.error("Supabase URL/Key missing.")
            else:
                symbols = [
                    x.strip().upper()
                    for x in mtf_symbols_text.replace(",", "\n").splitlines()
                    if x.strip()
                ]

                if not symbols:
                    st.warning(
                        "Provide the shortlisted MTF basket first."
                    )
                else:
                    try:
                        with st.spinner(
                            f"Publishing MTF history for "
                            f"{len(symbols)} symbols..."
                        ):
                            stats = historical.publish_mtf_history(symbols)
                            vix_count = historical.publish_vix()

                        st.success(
                            f"MTF completed for {len(stats)} symbols; "
                            f"VIX rows: {vix_count}"
                        )
                    except Exception as exc:
                        st.error(f"MTF + VIX publish failed: {exc}")

    col1, col2, col3 = st.columns(3)
    col1.metric("yfinance", "READY")
    col2.metric(
        "Supabase",
        "READY" if supabase.url and supabase.key else "NOT CONFIGURED",
    )
    col3.metric("Kotak dependency", "NONE")

    st.markdown("### Historical Raw Bus Contract")
    st.code(
        "yfinance → HISTORICAL RAW → Supabase → all 3 engines\n"
        "Kotak Neo is NOT imported or used in this environment.\n"
        "No features / scores / labels / regime / decisions cross the bus.",
        language="text",
    )

    st.markdown("### Required Data Coverage Audit")
    st.json(coverage_report())


if __name__ == "__main__":
    main()
