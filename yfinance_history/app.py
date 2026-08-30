#!/usr/bin/env python3
"""
Leak-Proof Historical Raw Producer | yfinance -> Supabase RAW BUS

Purpose
-------
Fetch historical raw OHLCV from yfinance and publish ONLY the rows
actually returned by Yahoo/yfinance into Supabase `raw_observations`.

Architecture
------------
yfinance -> HISTORICAL RAW -> Supabase -> all 3 engines

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
- Requested windows are requests only; actual returned coverage is authoritative.

Coverage requests
-----------------
- NIFTY-500 stocks: 320d daily
- NIFTY benchmark: 320d daily
- V7 MTF basket: 320d daily, 180d 1h requested, 55d 15m requested
- India VIX: 320d daily

Important yfinance limitation
-----------------------------
yfinance documents that intraday data cannot extend beyond the last 60 days.
Therefore the 180d 1h and 55d 15m requests are made honestly, but only rows
actually returned by yfinance are published. Nothing is backfilled or invented.

Universe
--------
The producer reads Yahoo tickers from `nifty500_yahoo_tickers.txt`, one ticker
per line. Keep this file as the authoritative NIFTY-500 universe for this worker.
Blank lines and lines beginning with # are ignored.

Environment
-----------
SUPABASE_URL
SUPABASE_KEY

Optional:
NIFTY500_TICKERS_FILE
HISTORY_BATCH_SIZE
HISTORY_WORKERS
SUPABASE_TIMEOUT_SEC
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yfinance as yf


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
    "tickers_file": os.getenv(
        "NIFTY500_TICKERS_FILE",
        "nifty500_yahoo_tickers.txt",
    ),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


class SupabaseRawPublisher:
    def __init__(self) -> None:
        self.url = env("SUPABASE_URL")
        self.key = env("SUPABASE_KEY")

        if not self.url or not self.key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY are required."
            )

        self.endpoint = (
            f"{self.url.rstrip('/')}/rest/v1/raw_observations"
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
                params={"select": "id", "limit": "1"},
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
        batch_size = max(1, CONFIG["batch_size"])

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
                    f"Supabase publish failed "
                    f"[{response.status_code}]: {response.text[:500]}"
                )

            total += len(records)

        return total


def load_tickers(path: str) -> List[str]:
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"NIFTY-500 ticker file not found: {file_path}. "
            "Create it with one Yahoo ticker per line."
        )

    tickers: List[str] = []

    for line in file_path.read_text(
        encoding="utf-8"
    ).splitlines():
        ticker = line.strip()

        if not ticker or ticker.startswith("#"):
            continue

        tickers.append(ticker)

    # Preserve file order while removing duplicates.
    return list(dict.fromkeys(tickers))


def clean_scalar(value: Any) -> Any:
    try:
        if value is None:
            return None

        if hasattr(value, "item"):
            value = value.item()

        if isinstance(value, float):
            if value != value:
                return None

        return value
    except Exception:
        return None


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
    Return only data actually returned by yfinance.
    No resampling, interpolation, padding, or synthetic rows.
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

        return (
            ticker,
            interval,
            rows,
            "",
        )

    except Exception as exc:
        return (
            ticker,
            interval,
            [],
            str(exc),
        )


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

    # Keep dataset metadata inside the raw payload. No calculations.
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
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(
        max_workers=max(1, CONFIG["workers"])
    ) as executor:
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
            for ticker, period, interval, dataset in jobs
        }

        for future in as_completed(future_map):
            result = future.result()
            results.append(result)

            print(
                f"[{result['status']}] "
                f"{result['dataset']} "
                f"{result['ticker']} "
                f"{result['interval']} "
                f"returned={result['rows_returned']} "
                f"published={result['rows_published']}"
            )

            if result.get("error"):
                print(
                    f"  ERROR: {result['error']}"
                )

    return results


def main() -> None:
    print("=" * 72)
    print("HISTORICAL RAW PRODUCER")
    print("yfinance -> Supabase raw_observations")
    print("=" * 72)

    publisher = SupabaseRawPublisher()

    health = publisher.health()

    print(
        f"Supabase: "
        f"{'REACHABLE' if health['reachable'] else 'NOT READY'}"
    )

    if not health["reachable"]:
        raise RuntimeError(
            f"Supabase health check failed: {health['error']}"
        )

    tickers = load_tickers(
        CONFIG["tickers_file"]
    )

    print(
        f"NIFTY-500 universe rows loaded: "
        f"{len(tickers)}"
    )

    jobs: List[Tuple[str, str, str, str]] = []

    # 1) NIFTY-500 daily historical raw.
    for ticker in tickers:
        jobs.append(
            (
                ticker,
                f"{CONFIG['nifty500_daily_days']}d",
                "1d",
                "nifty500_daily",
            )
        )

    # 2) NIFTY benchmark daily.
    jobs.append(
        (
            CONFIG["nifty_benchmark"],
            f"{CONFIG['nifty_benchmark_days']}d",
            "1d",
            "nifty_benchmark_daily",
        )
    )

    # 3) India VIX daily.
    jobs.append(
        (
            CONFIG["india_vix_ticker"],
            f"{CONFIG['india_vix_days']}d",
            "1d",
            "india_vix_daily",
        )
    )

    # 4) V7 MTF basket.
    #
    # This is intentionally separate from the full 500-stock daily basket.
    # The actual basket can be supplied through V7_MTF_TICKERS_FILE.
    mtf_file = os.getenv(
        "V7_MTF_TICKERS_FILE",
        "",
    ).strip()

    if mtf_file:
        mtf_tickers = load_tickers(mtf_file)

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
    else:
        print(
            "V7_MTF_TICKERS_FILE not set: "
            "V7 intraday basket jobs skipped for this run."
        )

    print(
        f"Total historical jobs queued: {len(jobs)}"
    )

    results = run_jobs(
        publisher,
        jobs,
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
            1 for r in results
            if r["status"] == "PASS"
        ),
        "no_data": sum(
            1 for r in results
            if r["status"] == "NO_DATA"
        ),
        "errors": sum(
            1 for r in results
            if r["status"] in (
                "ERROR",
                "PUBLISH_ERROR",
            )
        ),
    }

    print("=" * 72)
    print(
        json.dumps(
            summary,
            indent=2,
        )
    )
    print("=" * 72)

    # A successful run can still contain NO_DATA rows because the source
    # is authoritative. Only actual fetch/publish failures fail the process.
    if summary["errors"] > 0:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
