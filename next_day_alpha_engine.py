#!/usr/bin/env python3
"""
NIFTY NEXT-DAY STOCK ALPHA ENGINE
=================================

FINAL STANDALONE VERSION

Purpose
-------
A completely isolated stock-selection engine for:

1. Market-close / day-ahead scan
2. Selecting exactly up to TOP 15 high-quality overnight stock candidates
3. Assigning directional bias (LONG / SHORT)
4. Next morning 09:15-09:20 confirmation
5. Producing FINAL 2 / FINAL 1 / NO TRADE

ARCHITECTURE
------------
This engine is intentionally independent from the existing NIFTY 3-Min Micro
Engine.

Allowed:
    Shared RAW DATA / RAW CACHE

Not allowed:
    Sharing calculated features
    Sharing scores
    Sharing regime decisions
    Sharing labels
    Sharing predictions
    Sharing trade decisions

The existing NIFTY 3-Min Engine remains untouched.

Common raw data may be acquired once and cached, then consumed independently by
both engines.

DAY-AHEAD LOGIC
---------------
Trend structure       20%
Momentum              15%
Relative Strength     20%
Sector Strength       10%
Volume / Participation10%
Volatility             8%
Catalyst               7%
Setup Quality         10%

The final score is a QUALITY SCORE, NOT a mathematical probability.

A score of 90 does NOT mean a 90% win probability.
A real 90% probability can only be established after historical calibration.

MORNING CONFIRMATION
--------------------
The previous-night thesis is tested against:

- Gap versus NIFTY
- Gap versus sector
- Opening range
- VWAP
- Opening volume
- Relative strength
- Sector confirmation
- Acceptance / rejection
- Breakout / breakdown
- Directional consistency
- Excessive-gap / exhaustion filter

Only confirmed candidates can enter FINAL 2.

If no candidate passes the threshold:
    NO TRADE

The engine never forces two trades.

OPTION ARCHITECTURE
-------------------
This engine identifies the underlying stock opportunity.

It does NOT decide the option contract.

Later architecture can consume FINAL 2 and independently choose:
    equity vs option
    strike
    delta
    expiry
    IV
    OI
    liquidity
    spread
    slippage

This prevents stock-selection logic from becoming mixed with option-selection
logic.

DEPENDENCIES
------------
numpy
pandas
requests
Supabase Raw Bus (historical raw input)
Supabase Raw Bus (live raw input)

PUBLIC API
----------
engine = NextDayAlphaEngine()

engine.run_if_due()
engine.latest()
engine.live_top5()
engine.start_if_due_background()

CLI:
    python next_day_alpha_engine_final.py
"""

from __future__ import annotations

import json
import math
import os
import base64
import hmac
import hashlib
import struct
import threading
import time
import logging
import random
from functools import wraps
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

try:
    from neo_api_client import NeoAPI
except Exception:
    NeoAPI = None


# ============================================================================
# CONFIGURATION
# ============================================================================

IST = ZoneInfo("Asia/Kolkata")

ROOT = Path(os.getenv("NEXT_DAY_ALPHA_ROOT", "./next_day_alpha"))
ROOT.mkdir(parents=True, exist_ok=True)

CACHE_JSON = ROOT / "latest.json"
OUTCOME_JSONL = ROOT / "outcomes.jsonl"
UNIVERSE_CACHE = ROOT / "nifty500_universe.csv"
RAW_CACHE_DIR = ROOT / "raw_cache"
RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

NSE_500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
YF_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
NIFTY_TICKER = "^NSEI"

# Optional shared RAW-DATA bridge. Only whitelisted raw fields are accepted.
# This bridge is intentionally read-only from the Next-Day Engine side.
SHARED_RAW_CACHE_DIR = Path(os.getenv("SHARED_RAW_CACHE_DIR", "./shared_raw_cache"))
SHARED_RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Kotak Neo is the preferred live/intraday source. Yahoo is retained only as a
# historical/fallback source for fields that are not available in the raw cache.
KOTAK_ENVIRONMENT = os.getenv("KOTAK_ENVIRONMENT", "prod")
KOTAK_USE_LIVE = os.getenv("NEXT_DAY_KOTAK_LIVE", "1") != "0"
KOTAK_CAPTURE_SECONDS = int(os.getenv("NEXT_DAY_KOTAK_CAPTURE_SECONDS", "3"))
KOTAK_MAX_QUOTE_BATCH = int(os.getenv("NEXT_DAY_KOTAK_MAX_QUOTE_BATCH", "100"))

# Trusted catalyst sources. Exchange/regulator/company-origin information is
# preferred; generic keyword news is deliberately not used as a primary score.
NSE_CORPORATE_API = "https://www.nseindia.com/api/corporate-announcements"
SEBI_FILINGS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=3"

# ----------------------------- Universe gates ------------------------------

MIN_PRICE = 40.0
MIN_HISTORY_DAYS = 210
MIN_AVG_TURNOVER_CR = 20.0
MIN_AVG_VOLUME = 100_000

# ----------------------------- Ranking -------------------------------------

DAY_AHEAD_MIN_SCORE = 68.0
# No forced trade rule: fewer than 5 may be returned if quality gates fail.
# The overnight research basket is now TOP 15. TOP5_COUNT remains a
# compatibility alias for older callers; it no longer controls the basket size.
DAY_AHEAD_TOP_N = max(1, int(os.getenv("NEXT_DAY_TOP_N", "15")))
TOP15_COUNT = DAY_AHEAD_TOP_N
TOP5_COUNT = DAY_AHEAD_TOP_N

# First-stage volume-shocker gate. These are raw volume-behaviour gates, not
# replacements for the existing indicator mathematics.
VOLUME_SHOCKER_LOOKBACK_DAYS = 5
VOLUME_SHOCKER_MIN_CONSECUTIVE_DAYS = max(2, int(os.getenv("NEXT_DAY_VOLUME_CONSECUTIVE_DAYS", "3")))
VOLUME_SHOCKER_MIN_DAILY_PCT = float(os.getenv("NEXT_DAY_VOLUME_MIN_PCT", "5.0"))
MAJOR_FILTER_MAX_CANDIDATES = max(50, int(os.getenv("NEXT_DAY_MAJOR_FILTER_MAX", "50")))

# ----------------------------- Morning confirmation ------------------------

MORNING_CONFIRMATION_MIN_SCORE = 90.0
MORNING_WATCH_SCORE = 72.0

OPENING_MINUTES = 5
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
CONFIRMATION_END_MINUTE = 20

# ----------------------------- Risk / quality ------------------------------

MAX_GAP_PCT = 5.0
MAX_ATR_PCT = 8.0
MIN_ATR_PCT = 0.45

MAX_OPENING_RANGE_PCT = 4.0

# ----------------------------- Diversification -----------------------------

MAX_TOP5_PER_SECTOR = 2
SECTOR_REPEAT_PENALTY = 7.0

# ----------------------------- Catalyst ------------------------------------

ENABLE_CATALYST = os.getenv("NEXT_DAY_ENABLE_CATALYST", "1") != "0"
NEWS_LOOKBACK_HOURS = 36

# ----------------------------- Polling -------------------------------------

LIVE_REFRESH_SECONDS = 30
DAY_AHEAD_RUN_HOUR = 15
# ----------------------------- Resilience -----------------------------------
API_MAX_RETRIES = max(1, int(os.getenv("NEXT_DAY_API_MAX_RETRIES", "4")))
API_BACKOFF_BASE = max(0.1, float(os.getenv("NEXT_DAY_API_BACKOFF_BASE", "0.8")))
API_BACKOFF_MAX = max(API_BACKOFF_BASE, float(os.getenv("NEXT_DAY_API_BACKOFF_MAX", "12")))
FEED_MAX_AGE_SECONDS = max(1, float(os.getenv("NEXT_DAY_FEED_MAX_AGE_SECONDS", "15")))
LOG_FILE = ROOT / "next_day_alpha.log"
LOG_MAX_BYTES = max(1_000_000, int(os.getenv("NEXT_DAY_LOG_MAX_BYTES", str(5 * 1024 * 1024))))
LOG_BACKUP_COUNT = max(1, int(os.getenv("NEXT_DAY_LOG_BACKUP_COUNT", "5")))
NEXT_DAY_REQUIRE_KOTAK = os.getenv("NEXT_DAY_REQUIRE_KOTAK", "0") == "1"
DAY_AHEAD_RUN_MINUTE = 35
DAY_AHEAD_SNAPSHOT_START_MINUTE = 15
DAY_AHEAD_SNAPSHOT_END_MINUTE = 30

LOCK = threading.Lock()

def _build_logger() -> logging.Logger:
    logger = logging.getLogger("next_day_alpha")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
        "%Y-%m-%d %H:%M:%S%z",
    )
    try:
        handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except Exception:
        fallback = logging.StreamHandler()
        fallback.setFormatter(formatter)
        logger.addHandler(fallback)
    return logger

LOGGER = _build_logger()

def retry_api_call(func=None, *, retries=API_MAX_RETRIES, base=API_BACKOFF_BASE,
                   max_backoff=API_BACKOFF_MAX, retry_statuses=(429, 500, 502, 503, 504)):
    """Retry transient network/API failures with exponential backoff + jitter."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, retries + 1):
                try:
                    response = fn(*args, **kwargs)
                    status = getattr(response, "status_code", None)
                    if status in retry_statuses and attempt < retries:
                        delay = min(max_backoff, base * (2 ** (attempt - 1))) + random.uniform(0, base)
                        LOGGER.warning("Transient HTTP %s from %s; retry %d/%d in %.2fs",
                                       status, fn.__name__, attempt, retries, delay)
                        time.sleep(delay)
                        continue
                    return response
                except Exception as exc:
                    last_exc = exc
                    if attempt >= retries:
                        raise
                    delay = min(max_backoff, base * (2 ** (attempt - 1))) + random.uniform(0, base)
                    LOGGER.warning("Transient exception in %s; retry %d/%d in %.2fs: %s",
                                   fn.__name__, attempt, retries, delay, exc)
                    time.sleep(delay)
            if last_exc:
                raise last_exc
        return wrapper
    return decorator(func) if func else decorator

def _atomic_write_text(path: Path, text: str) -> None:
    """Durable atomic write: flush + fsync + replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.replace(path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass

def _parse_feed_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.tz_localize(IST)
        else:
            ts = ts.tz_convert(IST)
        return ts.to_pydatetime()
    except Exception:
        return None

def _freshness(ts: Optional[datetime], max_age_seconds: float = FEED_MAX_AGE_SECONDS) -> Tuple[bool, Optional[float]]:
    if ts is None:
        return False, None
    age = (now_ist() - ts).total_seconds()
    return age <= max_age_seconds, age

def validate_config() -> Dict[str, Any]:
    """Validate startup configuration before background work begins."""
    errors, warnings = [], []
    numeric_checks = {
        "NEXT_DAY_KOTAK_CAPTURE_SECONDS": KOTAK_CAPTURE_SECONDS,
        "NEXT_DAY_KOTAK_MAX_QUOTE_BATCH": KOTAK_MAX_QUOTE_BATCH,
        "NEXT_DAY_API_MAX_RETRIES": API_MAX_RETRIES,
        "NEXT_DAY_API_BACKOFF_BASE": API_BACKOFF_BASE,
        "NEXT_DAY_API_BACKOFF_MAX": API_BACKOFF_MAX,
        "NEXT_DAY_FEED_MAX_AGE_SECONDS": FEED_MAX_AGE_SECONDS,
    }
    for name, value in numeric_checks.items():
        if not isinstance(value, (int, float)) or not np.isfinite(value) or value <= 0:
            errors.append(f"{name} must be a positive number")
    for directory in (ROOT, RAW_CACHE_DIR, SHARED_RAW_CACHE_DIR):
        try:
            directory.mkdir(parents=True, exist_ok=True)
            if not directory.is_dir():
                errors.append(f"Not a directory: {directory}")
        except Exception as exc:
            errors.append(f"Cannot access directory {directory}: {exc}")
    if KOTAK_USE_LIVE:
        missing = [k for k in ("KOTAK_CONSUMER_KEY", "KOTAK_MOBILE", "KOTAK_UCC", "KOTAK_TOTP", "KOTAK_MPIN")
                   if not os.getenv(k, "").strip()]
        if missing:
            message = "Missing Kotak credentials: " + ", ".join(missing)
            if NEXT_DAY_REQUIRE_KOTAK:
                errors.append(message)
            else:
                warnings.append(message + " (live Kotak disabled by fallback policy)")
    report = {"ok": not errors, "errors": errors, "warnings": warnings}
    if errors:
        for msg in errors:
            LOGGER.error("CONFIG ERROR: %s", msg)
        raise RuntimeError("Configuration validation failed: " + " | ".join(errors))
    for msg in warnings:
        LOGGER.warning("CONFIG WARNING: %s", msg)
    LOGGER.info("Configuration validation passed")
    return report


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Candidate:
    symbol: str
    industry: str
    direction: str
    day_ahead_score: float
    selection_score: float
    setup_type: str
    trend_score: float
    momentum_score: float
    relative_strength_score: float
    sector_score: float
    volume_score: float
    volatility_score: float
    catalyst_score: float
    anti_false_positive_score: float
    ltp: float
    atr_pct: float
    ret_1d: float
    ret_5d: float
    ret_20d: float
    rs_5d: float
    rs_20d: float


@dataclass
class Confirmation:
    symbol: str
    direction: str
    previous_day_score: float
    confirmation_score: float
    status: str
    reason: str

    prev_close: float
    open_price: float
    gap_pct: float
    nifty_gap_pct: float
    sector_gap_pct: float

    opening_high: float
    opening_low: float
    opening_range_pct: float

    vwap: float
    close_vs_vwap_pct: float
    opening_volume_ratio: float

    relative_strength_vs_nifty: float
    relative_strength_vs_sector: float

    acceptance: bool
    rejection: bool
    breakout: bool
    breakdown: bool


# ============================================================================
# BASIC HELPERS
# ============================================================================

def now_ist() -> datetime:
    return datetime.now(IST)


def safe_float(value: Any, default: float = np.nan) -> float:
    try:
        x = float(value)
        if np.isfinite(x):
            return x
    except Exception:
        pass
    return default


def clip(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    x = safe_float(value)
    if not np.isfinite(x):
        return lo
    return float(max(lo, min(hi, x)))


def safe_mean(values: List[float], default: float = 0.0) -> float:
    vals = [
        safe_float(v)
        for v in values
        if np.isfinite(safe_float(v))
    ]
    return float(np.mean(vals)) if vals else default


def pct_change(close: pd.Series, n: int) -> float:
    if len(close) <= n:
        return np.nan

    a = safe_float(close.iloc[-1])
    b = safe_float(close.iloc[-1 - n])

    if not np.isfinite(a) or not np.isfinite(b) or b == 0:
        return np.nan

    return (a / b - 1.0) * 100.0


def sign_score(value: float, scale: float = 1.0) -> float:
    x = safe_float(value)
    if not np.isfinite(x):
        return 50.0
    return clip(50.0 + x * scale)


def slope(series: pd.Series, n: int = 5) -> float:
    if len(series) < n:
        return np.nan

    y = pd.to_numeric(series.tail(n), errors="coerce").to_numpy(dtype=float)
    mask = np.isfinite(y)

    if mask.sum() < 3:
        return np.nan

    x = np.arange(len(y), dtype=float)
    return float(np.polyfit(x[mask], y[mask], 1)[0])


# ============================================================================
# INDICATORS
# ============================================================================

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(
        span=period,
        adjust=False,
        min_periods=period,
    ).mean()


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    previous_close = df["Close"].shift(1)

    true_range = pd.concat(
        [
            df["High"] - df["Low"],
            (df["High"] - previous_close).abs(),
            (df["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return true_range.ewm(
        alpha=1.0 / period,
        adjust=False,
        min_periods=period,
    ).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()

    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(
        alpha=1.0 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    avg_loss = loss.ewm(
        alpha=1.0 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)

    output = 100.0 - 100.0 / (1.0 + rs)
    output = output.where(avg_loss != 0, 100.0)

    return output


def macd(series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    fast = ema(series, 12)
    slow = ema(series, 26)

    line = fast - slow
    signal = ema(line, 9)
    histogram = line - signal

    return line, signal, histogram


def adx(
    df: pd.DataFrame,
    period: int = 14,
) -> Tuple[pd.Series, pd.Series, pd.Series]:

    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(
        np.where(
            (up_move > down_move) & (up_move > 0),
            up_move,
            0.0,
        ),
        index=df.index,
    )

    minus_dm = pd.Series(
        np.where(
            (down_move > up_move) & (down_move > 0),
            down_move,
            0.0,
        ),
        index=df.index,
    )

    previous_close = close.shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr_value = true_range.ewm(
        alpha=1.0 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    plus_di = (
        100.0
        * plus_dm.ewm(
            alpha=1.0 / period,
            adjust=False,
            min_periods=period,
        ).mean()
        / atr_value.replace(0, np.nan)
    )

    minus_di = (
        100.0
        * minus_dm.ewm(
            alpha=1.0 / period,
            adjust=False,
            min_periods=period,
        ).mean()
        / atr_value.replace(0, np.nan)
    )

    dx = (
        100.0
        * (plus_di - minus_di).abs()
        / (plus_di + minus_di).replace(0, np.nan)
    )

    adx_value = dx.ewm(
        alpha=1.0 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    return adx_value, plus_di, minus_di


def supertrend_direction(
    df: pd.DataFrame,
    period: int = 10,
    multiplier: float = 3.0,
) -> pd.Series:

    atr_value = atr(df, period)

    hl2 = (df["High"] + df["Low"]) / 2.0

    upper_basic = hl2 + multiplier * atr_value
    lower_basic = hl2 - multiplier * atr_value

    upper = upper_basic.copy()
    lower = lower_basic.copy()

    direction = pd.Series(
        1,
        index=df.index,
        dtype=int,
    )

    for i in range(1, len(df)):

        if (
            np.isfinite(safe_float(upper_basic.iloc[i]))
            and np.isfinite(safe_float(upper.iloc[i - 1]))
        ):
            if df["Close"].iloc[i - 1] <= upper.iloc[i - 1]:
                upper.iloc[i] = min(
                    upper_basic.iloc[i],
                    upper.iloc[i - 1],
                )
            else:
                upper.iloc[i] = upper_basic.iloc[i]

        if (
            np.isfinite(safe_float(lower_basic.iloc[i]))
            and np.isfinite(safe_float(lower.iloc[i - 1]))
        ):
            if df["Close"].iloc[i - 1] >= lower.iloc[i - 1]:
                lower.iloc[i] = max(
                    lower_basic.iloc[i],
                    lower.iloc[i - 1],
                )
            else:
                lower.iloc[i] = lower_basic.iloc[i]

        previous_direction = direction.iloc[i - 1]

        if (
            previous_direction < 0
            and df["Close"].iloc[i] > upper.iloc[i - 1]
        ):
            direction.iloc[i] = 1

        elif (
            previous_direction > 0
            and df["Close"].iloc[i] < lower.iloc[i - 1]
        ):
            direction.iloc[i] = -1

        else:
            direction.iloc[i] = previous_direction

    return direction


# ============================================================================
# UNIVERSE
# ============================================================================

@retry_api_call
def _requests_get(*args, **kwargs):
    import requests
    return requests.get(*args, **kwargs)

@retry_api_call
def _session_get(session, *args, **kwargs):
    return session.get(*args, **kwargs)

def load_nifty500_universe() -> pd.DataFrame:

    if UNIVERSE_CACHE.exists():

        try:
            age = time.time() - UNIVERSE_CACHE.stat().st_mtime

            if age < 7 * 86400:

                cached = pd.read_csv(
                    UNIVERSE_CACHE
                )

                if (
                    "Symbol" in cached.columns
                    and len(cached) >= 350
                ):
                    return cached

        except Exception:
            pass

    try:

        import requests
        from io import StringIO

        response = _requests_get(
            NSE_500_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/csv,*/*",
                "Referer": "https://www.nseindia.com/",
            },
            timeout=20,
        )

        response.raise_for_status()

        text = response.content.decode(
            "utf-8-sig",
            errors="replace",
        )

        df = pd.read_csv(
            StringIO(text)
        )

        df.columns = [
            str(c).strip()
            for c in df.columns
        ]

        if (
            "Symbol" not in df.columns
            or len(df) < 350
        ):
            raise RuntimeError(
                "Incomplete NIFTY-500 universe"
            )

        df["Symbol"] = (
            df["Symbol"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        df.to_csv(
            UNIVERSE_CACHE,
            index=False,
        )

        return df

    except Exception:

        if UNIVERSE_CACHE.exists():

            cached = pd.read_csv(
                UNIVERSE_CACHE
            )

            if (
                "Symbol" in cached.columns
                and len(cached) >= 350
            ):
                return cached

        raise


# ============================================================================
# RAW DATA LAYER
# ============================================================================

def raw_cache_path(symbol: str, date_string: str) -> Path:
    safe_symbol = (
        str(symbol)
        .replace("/", "_")
        .replace("&", "_")
        .replace(" ", "_")
    )

    return RAW_CACHE_DIR / f"{safe_symbol}_{date_string}.json"


def write_raw_cache(
    symbol: str,
    data: Dict[str, Any],
    date_string: Optional[str] = None,
) -> None:

    date_string = (
        date_string
        or now_ist().strftime("%Y%m%d")
    )

    path = raw_cache_path(
        symbol,
        date_string,
    )

    temporary = path.with_suffix(".tmp")

    payload = json.dumps(data, ensure_ascii=False, default=str)
    _atomic_write_text(path, payload)


def read_raw_cache(
    symbol: str,
    date_string: Optional[str] = None,
) -> Optional[Dict[str, Any]]:

    date_string = (
        date_string
        or now_ist().strftime("%Y%m%d")
    )

    path = raw_cache_path(
        symbol,
        date_string,
    )

    if not path.exists():
        return None

    try:

        with path.open(
            "r",
            encoding="utf-8",
        ) as handle:

            return json.load(handle)

    except Exception:

        return None


# ============================================================================
# HISTORICAL DATA FETCH
# ============================================================================

def fetch_yahoo_chart(
    ticker: str,
    days: int = 320,
    interval: str = "1d",
) -> Optional[pd.DataFrame]:

    try:

        import requests

        end = int(time.time())

        if interval == "1m":

            params = {
                "range": "1d",
                "interval": "1m",
                "events": "history",
            }

        else:

            start = end - days * 86400

            params = {
                "period1": start,
                "period2": end,
                "interval": interval,
                "events": "history",
            }

        url = YF_CHART.format(
            ticker=ticker
        )

        response = _requests_get(
            url,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0"
            },
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        result_list = (
            payload
            .get("chart", {})
            .get("result")
            or [None]
        )

        result = result_list[0]

        if not result:
            return None

        timestamps = (
            result.get("timestamp")
            or []
        )

        quote = (
            result
            .get("indicators", {})
            .get("quote", [{}])[0]
        )

        if not timestamps:
            return None

        index = pd.to_datetime(
            timestamps,
            unit="s",
            utc=True,
        ).tz_convert(IST)

        df = pd.DataFrame(
            {
                "DateTime": index,
                "Open": quote.get("open", []),
                "High": quote.get("high", []),
                "Low": quote.get("low", []),
                "Close": quote.get("close", []),
                "Volume": quote.get("volume", []),
            }
        )

        for column in [
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
        ]:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

        df = df.dropna(
            subset=["Close"]
        ).reset_index(drop=True)

        return df

    except Exception:

        return None


def fetch_history(
    symbols: List[str],
    days: int = 320,
) -> Dict[str, pd.DataFrame]:

    result: Dict[str, pd.DataFrame] = {}

    # First choice: yfinance batch.
    try:

        import yfinance as yf

        tickers = [
            f"{symbol}.NS"
            for symbol in symbols
        ]

        raw = yf.download(
            tickers=tickers,
            period=f"{days}d",
            interval="1d",
            group_by="column",
            auto_adjust=False,
            progress=False,
            threads=True,
            timeout=30,
        )

        if (
            isinstance(raw, pd.DataFrame)
            and not raw.empty
        ):

            for symbol in symbols:

                ticker = f"{symbol}.NS"

                try:

                    if isinstance(
                        raw.columns,
                        pd.MultiIndex,
                    ):

                        sub = raw.xs(
                            ticker,
                            axis=1,
                            level=-1,
                        ).copy()

                    else:

                        sub = raw.copy()

                    sub = sub.reset_index()

                    if (
                        "Datetime" in sub.columns
                        and "Date" not in sub.columns
                    ):
                        sub = sub.rename(
                            columns={
                                "Datetime": "Date"
                            }
                        )

                    required = [
                        "Open",
                        "High",
                        "Low",
                        "Close",
                        "Volume",
                    ]

                    if all(
                        c in sub.columns
                        for c in required
                    ):

                        sub = sub[
                            ["Date"] + required
                        ].dropna(
                            subset=["Close"]
                        )

                        if len(sub) >= MIN_HISTORY_DAYS:
                            result[symbol] = (
                                sub.reset_index(
                                    drop=True
                                )
                            )

                except Exception:
                    continue

    except Exception:
        pass

    # Fallback only for missing symbols.
    missing = [
        symbol
        for symbol in symbols
        if symbol not in result
    ]

    def one(symbol: str):

        return (
            symbol,
            fetch_yahoo_chart(
                f"{symbol}.NS",
                days=days,
                interval="1d",
            ),
        )

    if missing:

        with ThreadPoolExecutor(
            max_workers=8
        ) as executor:

            futures = [
                executor.submit(
                    one,
                    symbol,
                )
                for symbol in missing
            ]

            for future in as_completed(
                futures
            ):

                try:

                    symbol, df = (
                        future.result()
                    )

                    if (
                        df is not None
                        and len(df)
                        >= MIN_HISTORY_DAYS
                    ):

                        result[symbol] = df

                except Exception:
                    continue

    return result


# ============================================================================
# FEATURE EXTRACTION
# ============================================================================

def structure_features(
    df: pd.DataFrame,
    window: int = 5,
) -> Dict[str, Any]:

    if len(df) < window * 4:

        return {
            "Structure": "NEUTRAL",
            "StructureStrength": 50.0,
            "HHHL": 0,
            "LHLL": 0,
        }

    high = df["High"]
    low = df["Low"]

    previous_high = high.iloc[
        -2 * window:-window
    ].max()

    recent_high = high.iloc[
        -window:
    ].max()

    previous_low = low.iloc[
        -2 * window:-window
    ].min()

    recent_low = low.iloc[
        -window:
    ].min()

    hh = (
        np.isfinite(recent_high)
        and np.isfinite(previous_high)
        and recent_high > previous_high
    )

    hl = (
        np.isfinite(recent_low)
        and np.isfinite(previous_low)
        and recent_low > previous_low
    )

    lh = (
        np.isfinite(recent_high)
        and np.isfinite(previous_high)
        and recent_high < previous_high
    )

    ll = (
        np.isfinite(recent_low)
        and np.isfinite(previous_low)
        and recent_low < previous_low
    )

    if hh and hl:

        return {
            "Structure": "LONG",
            "StructureStrength": 90.0,
            "HHHL": 1,
            "LHLL": 0,
        }

    if lh and ll:

        return {
            "Structure": "SHORT",
            "StructureStrength": 90.0,
            "HHHL": 0,
            "LHLL": 1,
        }

    if hh or hl:

        return {
            "Structure": "LONG",
            "StructureStrength": 68.0,
            "HHHL": 0,
            "LHLL": 0,
        }

    if lh or ll:

        return {
            "Structure": "SHORT",
            "StructureStrength": 68.0,
            "HHHL": 0,
            "LHLL": 0,
        }

    return {
        "Structure": "NEUTRAL",
        "StructureStrength": 50.0,
        "HHHL": 0,
        "LHLL": 0,
    }


def build_features(
    symbol: str,
    df: pd.DataFrame,
    benchmark: Optional[pd.DataFrame],
    industry: str,
) -> Optional[Dict[str, Any]]:

    if (
        df is None
        or len(df) < MIN_HISTORY_DAYS
    ):
        return None

    d = df.copy()

    for column in [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ]:

        d[column] = pd.to_numeric(
            d[column],
            errors="coerce",
        )

    d = d.dropna(
        subset=[
            "Open",
            "High",
            "Low",
            "Close",
        ]
    ).reset_index(drop=True)

    if len(d) < MIN_HISTORY_DAYS:
        return None

    # ---------------- Trend ----------------

    d["EMA20"] = ema(
        d["Close"],
        20,
    )

    d["EMA50"] = ema(
        d["Close"],
        50,
    )

    d["EMA200"] = ema(
        d["Close"],
        200,
    )

    adx_value, plus_di, minus_di = adx(
        d,
        14,
    )

    d["ADX14"] = adx_value
    d["PlusDI"] = plus_di
    d["MinusDI"] = minus_di

    d["SuperTrendDirection"] = (
        supertrend_direction(
            d,
            10,
            3.0,
        )
    )

    # ---------------- Momentum ----------------

    d["RSI14"] = rsi(
        d["Close"],
        14,
    )

    (
        d["MACD"],
        d["MACDSignal"],
        d["MACDHist"],
    ) = macd(
        d["Close"]
    )

    # ---------------- Volatility ----------------

    d["ATR14"] = atr(
        d,
        14,
    )

    d["ATRpct"] = (
        d["ATR14"]
        / d["Close"]
        * 100.0
    )

    d["RangePct"] = (
        (d["High"] - d["Low"])
        / d["Close"]
        * 100.0
    )

    # ---------------- Volume ----------------

    d["Turnover"] = (
        d["Close"]
        * d["Volume"]
    )

    last = d.iloc[-1]

    close = safe_float(
        last["Close"]
    )

    if (
        not np.isfinite(close)
        or close < MIN_PRICE
    ):
        return None

    avg_turnover_cr = (
        d["Turnover"].tail(20).mean()
        / 1e7
    )

    avg_volume = (
        d["Volume"]
        .tail(20)
        .mean()
    )

    if (
        not np.isfinite(avg_turnover_cr)
        or avg_turnover_cr
        < MIN_AVG_TURNOVER_CR
    ):
        return None

    if (
        not np.isfinite(avg_volume)
        or avg_volume < MIN_AVG_VOLUME
    ):
        return None

    ret_1d = pct_change(
        d["Close"],
        1,
    )

    ret_5d = pct_change(
        d["Close"],
        5,
    )

    ret_20d = pct_change(
        d["Close"],
        20,
    )

    ret_60d = pct_change(
        d["Close"],
        60,
    )

    # ---------------- Relative strength ----------------

    if (
        benchmark is not None
        and len(benchmark) >= 70
    ):

        benchmark_close = (
            benchmark["Close"]
            .dropna()
            .reset_index(drop=True)
        )

        nifty_ret_1d = pct_change(
            benchmark_close,
            1,
        )

        nifty_ret_5d = pct_change(
            benchmark_close,
            5,
        )

        nifty_ret_20d = pct_change(
            benchmark_close,
            20,
        )

    else:

        nifty_ret_1d = np.nan
        nifty_ret_5d = np.nan
        nifty_ret_20d = np.nan

    rs_1d = (
        ret_1d - nifty_ret_1d
        if np.isfinite(ret_1d)
        and np.isfinite(nifty_ret_1d)
        else np.nan
    )

    rs_5d = (
        ret_5d - nifty_ret_5d
        if np.isfinite(ret_5d)
        and np.isfinite(nifty_ret_5d)
        else np.nan
    )

    rs_20d = (
        ret_20d - nifty_ret_20d
        if np.isfinite(ret_20d)
        and np.isfinite(nifty_ret_20d)
        else np.nan
    )

    # ---------------- Trend values ----------------

    ema20 = safe_float(
        last["EMA20"]
    )

    ema50 = safe_float(
        last["EMA50"]
    )

    ema200 = safe_float(
        last["EMA200"]
    )

    atr14 = safe_float(
        last["ATR14"]
    )

    atr_pct = safe_float(
        last["ATRpct"]
    )

    rsi14 = safe_float(
        last["RSI14"]
    )

    adx14 = safe_float(
        last["ADX14"]
    )

    plus = safe_float(
        last["PlusDI"]
    )

    minus = safe_float(
        last["MinusDI"]
    )

    macd_hist = safe_float(
        last["MACDHist"]
    )

    st_dir = int(
        safe_float(
            last["SuperTrendDirection"],
            0,
        )
    )

    ema20_slope = slope(
        d["EMA20"].dropna(),
        5,
    )

    ema50_slope = slope(
        d["EMA50"].dropna(),
        5,
    )

    ema200_slope = slope(
        d["EMA200"].dropna(),
        10,
    )

    structure = structure_features(
        d,
        5,
    )

    # ---------------- Breakouts ----------------

    previous_20_high = (
        d["High"]
        .iloc[-21:-1]
        .max()
    )

    previous_20_low = (
        d["Low"]
        .iloc[-21:-1]
        .min()
    )

    previous_60_high = (
        d["High"]
        .iloc[-61:-1]
        .max()
    )

    previous_60_low = (
        d["Low"]
        .iloc[-61:-1]
        .min()
    )

    breakout_20_up = int(
        np.isfinite(previous_20_high)
        and close > previous_20_high
    )

    breakout_20_down = int(
        np.isfinite(previous_20_low)
        and close < previous_20_low
    )

    breakout_60_up = int(
        np.isfinite(previous_60_high)
        and close > previous_60_high
    )

    breakout_60_down = int(
        np.isfinite(previous_60_low)
        and close < previous_60_low
    )

    # ---------------- Volume quality ----------------

    average_volume_20 = (
        d["Volume"]
        .iloc[-21:-1]
        .mean()
    )

    average_volume_5 = (
        d["Volume"]
        .iloc[-6:-1]
        .mean()
    )

    latest_volume = safe_float(
        last["Volume"]
    )

    volume_ratio_20 = (
        latest_volume
        / average_volume_20
        if average_volume_20 > 0
        else np.nan
    )

    volume_ratio_5 = (
        latest_volume
        / average_volume_5
        if average_volume_5 > 0
        else np.nan
    )

    # ---------------- Volatility regime ----------------

    atr20_average = (
        d["ATRpct"]
        .iloc[-21:-1]
        .mean()
    )

    atr_expansion = (
        atr_pct / atr20_average
        if (
            np.isfinite(atr20_average)
            and atr20_average > 0
        )
        else np.nan
    )

    # ---------------- Direction vote ----------------

    long_votes = [
        int(
            np.isfinite(ema20)
            and close > ema20
        ),
        int(
            np.isfinite(ema50)
            and close > ema50
        ),
        int(
            np.isfinite(ema200)
            and close > ema200
        ),
        int(
            np.isfinite(ema20_slope)
            and ema20_slope > 0
        ),
        int(
            np.isfinite(ema50_slope)
            and ema50_slope > 0
        ),
        int(
            np.isfinite(adx14)
            and adx14 >= 20
            and plus > minus
        ),
        int(st_dir > 0),
        int(
            structure["Structure"]
            == "LONG"
        ),
        int(
            breakout_20_up
            or breakout_60_up
        ),
    ]

    short_votes = [
        int(
            np.isfinite(ema20)
            and close < ema20
        ),
        int(
            np.isfinite(ema50)
            and close < ema50
        ),
        int(
            np.isfinite(ema200)
            and close < ema200
        ),
        int(
            np.isfinite(ema20_slope)
            and ema20_slope < 0
        ),
        int(
            np.isfinite(ema50_slope)
            and ema50_slope < 0
        ),
        int(
            np.isfinite(adx14)
            and adx14 >= 20
            and minus > plus
        ),
        int(st_dir < 0),
        int(
            structure["Structure"]
            == "SHORT"
        ),
        int(
            breakout_20_down
            or breakout_60_down
        ),
    ]

    long_vote_pct = (
        sum(long_votes)
        / len(long_votes)
        * 100.0
    )

    short_vote_pct = (
        sum(short_votes)
        / len(short_votes)
        * 100.0
    )

    if (
        long_vote_pct
        >= short_vote_pct + 12
    ):

        direction = "LONG"

    elif (
        short_vote_pct
        >= long_vote_pct + 12
    ):

        direction = "SHORT"

    else:

        direction = "NEUTRAL"

    return {
        "Symbol": symbol,
        "Industry": industry,

        "LTP": close,

        "AvgTurnoverCr": avg_turnover_cr,
        "AvgVolume20": avg_volume,

        "Ret1D": ret_1d,
        "Ret5D": ret_5d,
        "Ret20D": ret_20d,
        "Ret60D": ret_60d,

        "NiftyRet1D": nifty_ret_1d,
        "NiftyRet5D": nifty_ret_5d,
        "NiftyRet20D": nifty_ret_20d,

        "RS1D": rs_1d,
        "RS5D": rs_5d,
        "RS20D": rs_20d,

        "EMA20": ema20,
        "EMA50": ema50,
        "EMA200": ema200,

        "AboveEMA20": int(
            np.isfinite(ema20)
            and close > ema20
        ),

        "AboveEMA50": int(
            np.isfinite(ema50)
            and close > ema50
        ),

        "AboveEMA200": int(
            np.isfinite(ema200)
            and close > ema200
        ),

        "EMA20Slope": ema20_slope,
        "EMA50Slope": ema50_slope,
        "EMA200Slope": ema200_slope,

        "ATR14": atr14,
        "ATRpct": atr_pct,
        "ATRExpansion": atr_expansion,
        "RangePct": safe_float(
            last["RangePct"]
        ),

        "RSI14": rsi14,

        "MACD": safe_float(
            last["MACD"]
        ),

        "MACDSignal": safe_float(
            last["MACDSignal"]
        ),

        "MACDHist": macd_hist,

        "ADX14": adx14,
        "PlusDI": plus,
        "MinusDI": minus,

        "SuperTrendDirection": st_dir,

        **structure,

        "Breakout20Up": breakout_20_up,
        "Breakout20Down": breakout_20_down,
        "Breakout60Up": breakout_60_up,
        "Breakout60Down": breakout_60_down,

        "VolumeRatio20": volume_ratio_20,
        "VolumeRatio5": volume_ratio_5,

        "LongVotes": sum(long_votes),
        "ShortVotes": sum(short_votes),

        "LongVotePct": long_vote_pct,
        "ShortVotePct": short_vote_pct,

        "Direction": direction,
    }


# ============================================================================
# SECTOR ENGINE
# ============================================================================

def add_sector_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:

    x = frame.copy()

    if "Industry" not in x.columns:
        x["Industry"] = "UNKNOWN"

    x["Industry"] = (
        x["Industry"]
        .fillna("UNKNOWN")
        .astype(str)
        .str.strip()
    )

    sector_rows = []

    for industry, group in x.groupby(
        "Industry"
    ):

        if (
            industry == "UNKNOWN"
            or len(group) < 3
        ):
            continue

        sector_rows.append(
            {
                "Industry": industry,
                "SectorCount": len(group),

                "SectorRet1D": group[
                    "Ret1D"
                ].median(),

                "SectorRet5D": group[
                    "Ret5D"
                ].median(),

                "SectorRet20D": group[
                    "Ret20D"
                ].median(),

                "SectorRS5D": group[
                    "RS5D"
                ].median(),

                "SectorRS20D": group[
                    "RS20D"
                ].median(),

                "SectorLongBreadth": (
                    group["LongVotePct"]
                    >= 55
                ).mean() * 100.0,

                "SectorShortBreadth": (
                    group["ShortVotePct"]
                    >= 55
                ).mean() * 100.0,
            }
        )

    sectors = pd.DataFrame(
        sector_rows
    )

    if sectors.empty:

        x["SectorCount"] = 0
        x["SectorRet1D"] = np.nan
        x["SectorRet5D"] = np.nan
        x["SectorRet20D"] = np.nan
        x["SectorRS5D"] = np.nan
        x["SectorRS20D"] = np.nan
        x["SectorLongBreadth"] = 50.0
        x["SectorShortBreadth"] = 50.0

        return x

    return x.merge(
        sectors,
        on="Industry",
        how="left",
    )


# ============================================================================
# TRUSTED CATALYST ENGINE
# ============================================================================

# These terms are used only AFTER a trusted source has identified a real event.
# A generic news headline by itself cannot create a strong catalyst score.
CATALYST_BULLISH = (
    "order win", "order wins", "large order", "new order", "major contract",
    "contract awarded", "approval", "approved", "partnership", "acquisition",
    "buyback", "dividend", "capacity expansion", "fund raise", "fundraising",
    "strong results", "profit rises", "revenue rises", "guidance raised",
    "positive outlook", "new project", "major deal", "debt reduction",
)
CATALYST_BEARISH = (
    "downgrade", "fraud", "penalty", "fine", "probe", "investigation",
    "resignation", "guidance cut", "profit falls", "revenue falls", "default",
    "delay", "regulatory action", "warning", "weak results", "loss widens",
    "order cancellation", "cancelled order", "adverse order", "license revoked",
)


def _trusted_event_direction(text: str) -> Tuple[str, float]:
    t = str(text or "").lower()
    bull = sum(k in t for k in CATALYST_BULLISH)
    bear = sum(k in t for k in CATALYST_BEARISH)
    if bull > bear:
        return "BULLISH", float(bull - bear)
    if bear > bull:
        return "BEARISH", float(bear - bull)
    return "MIXED", 0.0


def fetch_nse_corporate_events(symbol: str, lookback_hours: int = NEWS_LOOKBACK_HOURS) -> List[Dict[str, Any]]:
    """Fetch recent NSE corporate announcements for one symbol.

    This is a source-of-truth event layer. If the NSE endpoint changes or is
    unavailable, the function fails closed and returns no event rather than
    manufacturing a catalyst.
    """
    try:
        import requests
        end = now_ist()
        start_dt = end - timedelta(hours=lookback_hours)
        params = {
            "index": "equities",
            "from_date": start_dt.strftime("%d-%m-%Y"),
            "to_date": end.strftime("%d-%m-%Y"),
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nseindia.com/",
        }
        session = requests.Session()
        _session_get(session, "https://www.nseindia.com/", headers=headers, timeout=10)
        response = _session_get(session, NSE_CORPORATE_API, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        payload = response.json()
        rows = payload if isinstance(payload, list) else payload.get("data", [])
        out = []
        wanted = str(symbol).upper().strip()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            row_symbol = str(row.get("symbol", row.get("Symbol", ""))).upper().strip()
            if row_symbol != wanted:
                continue
            text = " ".join(str(row.get(k, "")) for k in ("subject", "desc", "description", "companyName"))
            direction, strength = _trusted_event_direction(text)
            out.append({
                "source": "NSE",
                "source_type": "exchange_filing",
                "symbol": wanted,
                "published_at": str(row.get("an_dt", row.get("date", row.get("timestamp", "")))),
                "title": str(row.get("subject", row.get("desc", "Corporate announcement")))[:300],
                "direction": direction,
                "strength": strength,
                "url": str(row.get("attchmntFile", row.get("attachment", ""))),
            })
        return out[:20]
    except Exception:
        return []


def fetch_sebi_context(symbol: str) -> List[Dict[str, Any]]:
    """Lightweight SEBI source marker; does not scrape generic search results into a signal.

    SEBI is treated as a regulatory verification source. A concrete filing must be
    manually/API-resolved before it can receive a directional score.
    """
    return []


def catalyst_for_symbol(symbol: str) -> Dict[str, Any]:
    neutral = {
        "CatalystScore": 50.0,
        "CatalystDirection": "UNKNOWN",
        "CatalystCount": 0,
        "CatalystText": "",
        "CatalystSources": [],
        "CatalystMateriality": "NONE",
    }
    if not ENABLE_CATALYST:
        return neutral

    events = fetch_nse_corporate_events(symbol)
    events.extend(fetch_sebi_context(symbol))
    if not events:
        return neutral

    bullish = sum(max(0.0, safe_float(e.get("strength"), 0.0)) for e in events if e.get("direction") == "BULLISH")
    bearish = sum(max(0.0, safe_float(e.get("strength"), 0.0)) for e in events if e.get("direction") == "BEARISH")
    if bullish > bearish:
        direction = "BULLISH"
    elif bearish > bullish:
        direction = "BEARISH"
    else:
        direction = "MIXED"

    # Exchange filing gets higher confidence than generic news. Score remains
    # bounded and is a quality component, not a probability.
    score = clip(50.0 + (bullish - bearish) * 12.0, 15.0, 85.0)
    materiality = "HIGH" if abs(bullish - bearish) >= 2 else "MEDIUM" if abs(bullish - bearish) >= 1 else "LOW"
    texts = [str(e.get("title", ""))[:180] for e in events[:4] if e.get("title")]
    sources = sorted(set(str(e.get("source", "")) for e in events if e.get("source")))
    return {
        "CatalystScore": score,
        "CatalystDirection": direction,
        "CatalystCount": len(events),
        "CatalystText": " | ".join(texts),
        "CatalystSources": sources,
        "CatalystMateriality": materiality,
    }


# ============================================================================
# SCORING
# ============================================================================

def trend_score(
    row: pd.Series,
    direction: str,
) -> float:

    if direction == "LONG":

        alignment = np.mean(
            [
                row.get(
                    "AboveEMA20",
                    0,
                ),
                row.get(
                    "AboveEMA50",
                    0,
                ),
                row.get(
                    "AboveEMA200",
                    0,
                ),
            ]
        ) * 100.0

        directional = safe_mean(
            [
                row.get(
                    "LongVotePct",
                    50,
                ),

                row.get(
                    "StructureStrength",
                    50,
                )
                if row.get(
                    "Structure"
                ) == "LONG"
                else 100
                - row.get(
                    "StructureStrength",
                    50,
                ),

                clip(
                    row.get(
                        "ADX14",
                        0,
                    ) * 2.0
                )
                if row.get(
                    "PlusDI",
                    0,
                )
                > row.get(
                    "MinusDI",
                    0,
                )
                else 20.0,

                100.0
                if row.get(
                    "SuperTrendDirection",
                    0,
                ) > 0
                else 0.0,
            ]
        )

        return clip(
            0.55 * directional
            + 0.45 * alignment
        )

    alignment = np.mean(
        [
            1
            - row.get(
                "AboveEMA20",
                0,
            ),

            1
            - row.get(
                "AboveEMA50",
                0,
            ),

            1
            - row.get(
                "AboveEMA200",
                0,
            ),
        ]
    ) * 100.0

    directional = safe_mean(
        [
            row.get(
                "ShortVotePct",
                50,
            ),

            row.get(
                "StructureStrength",
                50,
            )
            if row.get(
                "Structure"
            ) == "SHORT"
            else 100
            - row.get(
                "StructureStrength",
                50,
            ),

            clip(
                row.get(
                    "ADX14",
                    0,
                ) * 2.0
            )
            if row.get(
                "MinusDI",
                0,
            )
            > row.get(
                "PlusDI",
                0,
            )
            else 20.0,

            100.0
            if row.get(
                "SuperTrendDirection",
                0,
            ) < 0
            else 0.0,
        ]
    )

    return clip(
        0.55 * directional
        + 0.45 * alignment
    )


def momentum_score(
    row: pd.Series,
    direction: str,
) -> float:

    returns = safe_mean(
        [
            row.get(
                "Ret1D",
                0,
            ),

            row.get(
                "Ret5D",
                0,
            ),

            row.get(
                "Ret20D",
                0,
            ) * 0.8,
        ]
    )

    if direction == "SHORT":
        returns = -returns

    return_score = sign_score(
        returns,
        9.0,
    )

    rsi_value = safe_float(
        row.get(
            "RSI14"
        )
    )

    if not np.isfinite(
        rsi_value
    ):

        rsi_score = 50.0

    elif direction == "LONG":

        rsi_score = clip(
            50.0
            + (
                rsi_value
                - 50.0
            )
            * 1.35
        )

    else:

        rsi_score = clip(
            50.0
            + (
                50.0
                - rsi_value
            )
            * 1.35
        )

    macd_hist = safe_float(
        row.get(
            "MACDHist"
        )
    )

    atr_value = max(
        safe_float(
            row.get(
                "ATR14"
            ),
            1.0,
        ),
        1e-9,
    )

    macd_directional = (
        macd_hist
        if direction == "LONG"
        else -macd_hist
    )

    macd_score = sign_score(
        macd_directional
        / atr_value,
        80.0,
    )

    return clip(
        0.38 * return_score
        + 0.32 * rsi_score
        + 0.30 * macd_score
    )


def relative_strength_score(
    row: pd.Series,
    direction: str,
) -> float:

    value = safe_mean(
        [
            row.get(
                "RS1D",
                0,
            ),

            row.get(
                "RS5D",
                0,
            ),

            row.get(
                "RS20D",
                0,
            ),

            row.get(
                "StockVsSector5D",
                0,
            ),

            row.get(
                "StockVsSector20D",
                0,
            ),
        ]
    )

    score = sign_score(
        value,
        16.0,
    )

    if direction == "SHORT":
        score = 100.0 - score

    return clip(score)


def sector_score(
    row: pd.Series,
    direction: str,
) -> float:

    sector_return = safe_mean(
        [
            row.get(
                "SectorRet1D",
                0,
            ),

            row.get(
                "SectorRet5D",
                0,
            ),

            row.get(
                "SectorRet20D",
                0,
            ),
        ]
    )

    sector_rs = safe_mean(
        [
            row.get(
                "SectorRS5D",
                0,
            ),

            row.get(
                "SectorRS20D",
                0,
            ),
        ]
    )

    if direction == "LONG":

        return clip(
            safe_mean(
                [
                    sign_score(
                        sector_return,
                        10.0,
                    ),

                    sign_score(
                        sector_rs,
                        14.0,
                    ),

                    row.get(
                        "SectorLongBreadth",
                        50.0,
                    ),
                ]
            )
        )

    return clip(
        safe_mean(
            [
                sign_score(
                    -sector_return,
                    10.0,
                ),

                sign_score(
                    -sector_rs,
                    14.0,
                ),

                row.get(
                    "SectorShortBreadth",
                    50.0,
                ),
            ]
        )
    )


def volume_score(
    row: pd.Series,
) -> float:

    parts = []

    ratio_20 = safe_float(
        row.get(
            "VolumeRatio20"
        )
    )

    ratio_5 = safe_float(
        row.get(
            "VolumeRatio5"
        )
    )

    if np.isfinite(
        ratio_20
    ):

        parts.append(
            clip(
                50.0
                + math.log(
                    max(
                        ratio_20,
                        0.05,
                    )
                )
                * 35.0
            )
        )

    if np.isfinite(
        ratio_5
    ):

        parts.append(
            clip(
                50.0
                + math.log(
                    max(
                        ratio_5,
                        0.05,
                    )
                )
                * 30.0
            )
        )

    return safe_mean(
        parts,
        50.0,
    )


def volatility_score(
    row: pd.Series,
) -> float:

    atr_pct = safe_float(
        row.get(
            "ATRpct"
        )
    )

    expansion = safe_float(
        row.get(
            "ATRExpansion"
        )
    )

    if not np.isfinite(
        atr_pct
    ):
        return 50.0

    if (
        atr_pct < MIN_ATR_PCT
    ):

        score = 35.0

    elif atr_pct <= 4.5:

        score = 88.0

    elif atr_pct <= MAX_ATR_PCT:

        score = (
            70.0
            - (
                atr_pct
                - 4.5
            )
            * 7.0
        )

    else:

        score = 20.0

    if np.isfinite(
        expansion
    ):

        if (
            1.05
            <= expansion
            <= 1.80
        ):

            score += 8.0

        elif expansion > 2.5:

            score -= 15.0

    return clip(score)


def setup_score(
    row: pd.Series,
    direction: str,
) -> float:

    if direction == "LONG":

        breakout = (
            row.get(
                "Breakout20Up",
                0,
            )
            + row.get(
                "Breakout60Up",
                0,
            )
        )

    else:

        breakout = (
            row.get(
                "Breakout20Down",
                0,
            )
            + row.get(
                "Breakout60Down",
                0,
            )
        )

    breakout_score = clip(
        50.0
        + breakout * 25.0
    )

    structure_value = row.get(
        "StructureStrength",
        50.0,
    )

    if (
        row.get(
            "Structure"
        )
        == direction
    ):

        structure_score = (
            structure_value
        )

    else:

        structure_score = (
            100.0
            - structure_value
        )

    return clip(
        0.55 * structure_score
        + 0.45 * breakout_score
    )


def anti_false_positive_score(
    row: pd.Series,
    direction: str,
) -> float:

    penalty = 0.0

    atr_pct = safe_float(
        row.get(
            "ATRpct"
        )
    )

    if (
        np.isfinite(atr_pct)
        and atr_pct > MAX_ATR_PCT
    ):

        penalty += 18.0

    one_day_move = abs(
        safe_float(
            row.get(
                "Ret1D"
            ),
            0.0,
        )
    )

    if one_day_move > 5.0:

        penalty += min(
            15.0,
            (
                one_day_move
                - 5.0
            )
            * 3.0,
        )

    volume_ratio = safe_float(
        row.get(
            "VolumeRatio20"
        )
    )

    if (
        np.isfinite(volume_ratio)
        and volume_ratio < 0.65
    ):

        penalty += 10.0

    rs = relative_strength_score(
        row,
        direction,
    )

    if rs < 38.0:
        penalty += 14.0

    sector = sector_score(
        row,
        direction,
    )

    if sector < 38.0:
        penalty += 14.0

    if (
        row.get(
            "Direction"
        )
        != direction
    ):

        penalty += 12.0

    return clip(
        100.0 - penalty
    )


def score_candidates(
    frame: pd.DataFrame,
) -> pd.DataFrame:

    x = frame.copy()

    x["StockVsSector5D"] = (
        x["Ret5D"]
        - x["SectorRet5D"]
    )

    x["StockVsSector20D"] = (
        x["Ret20D"]
        - x["SectorRet20D"]
    )

    output = []

    for _, row in x.iterrows():

        direction = row.get(
            "Direction",
            "NEUTRAL",
        )

        if direction == "NEUTRAL":

            output.append(
                {
                    **row.to_dict(),

                    "TrendScore": 50.0,
                    "MomentumScore": 50.0,
                    "RelativeStrengthScore": 50.0,
                    "SectorScore": 50.0,
                    "VolumeScore": volume_score(
                        row
                    ),
                    "VolatilityScore": volatility_score(
                        row
                    ),
                    "SetupScore": 50.0,
                    "CatalystScoreFinal": 50.0,
                    "AntiFalsePositiveScore": 35.0,
                    "DayAheadScore": 35.0,
                    "SetupType": "NO_DIRECTION",
                }
            )

            continue

        trend = trend_score(
            row,
            direction,
        )

        momentum = momentum_score(
            row,
            direction,
        )

        relative = (
            relative_strength_score(
                row,
                direction,
            )
        )

        sector = sector_score(
            row,
            direction,
        )

        volume = volume_score(
            row
        )

        volatility = volatility_score(
            row
        )

        setup = setup_score(
            row,
            direction,
        )

        anti = (
            anti_false_positive_score(
                row,
                direction,
            )
        )

        catalyst_data = catalyst_for_symbol(
            str(row["Symbol"])
        )

        catalyst = safe_float(
            catalyst_data.get(
                "CatalystScore",
                50.0,
            ),
            50.0,
        )

        catalyst_direction = str(
            catalyst_data.get(
                "CatalystDirection",
                "UNKNOWN",
            )
        )

        if (
            direction == "LONG"
            and catalyst_direction
            == "BEARISH"
        ):

            catalyst = (
                100.0
                - catalyst
            )

        elif (
            direction == "SHORT"
            and catalyst_direction
            == "BULLISH"
        ):

            catalyst = (
                100.0
                - catalyst
            )

        score = (
            trend * 0.20
            + momentum * 0.15
            + relative * 0.20
            + sector * 0.10
            + volume * 0.10
            + volatility * 0.08
            + catalyst * 0.07
            + setup * 0.10
        )

        # Quality multiplier.
        score *= (
            0.78
            + 0.22
            * (
                anti
                / 100.0
            )
        )

        if (
            setup >= 75
            and relative >= 70
        ):

            if direction == "LONG":

                setup_type = (
                    "RELATIVE_STRENGTH_BREAKOUT"
                )

            else:

                setup_type = (
                    "RELATIVE_WEAKNESS_BREAKDOWN"
                )

        elif (
            trend >= 78
            and momentum >= 68
        ):

            setup_type = (
                "TREND_CONTINUATION"
            )

        elif (
            volatility >= 78
            and volume >= 70
        ):

            setup_type = (
                "VOLATILITY_EXPANSION"
            )

        elif (
            catalyst >= 72
            and momentum >= 65
        ):

            setup_type = (
                "CATALYST_MOMENTUM"
            )

        elif (
            row.get(
                "Breakout20Up",
                0,
            )
            or row.get(
                "Breakout20Down",
                0,
            )
        ):

            setup_type = "BREAKOUT"

        else:

            setup_type = (
                "STRUCTURED_MOMENTUM"
            )

        output.append(
            {
                **row.to_dict(),

                "TrendScore": trend,
                "MomentumScore": momentum,
                "RelativeStrengthScore": relative,
                "SectorScore": sector,
                "VolumeScore": volume,
                "VolatilityScore": volatility,
                "CatalystScoreFinal": catalyst,
                "CatalystDirection": catalyst_direction,
                "CatalystCount": catalyst_data.get(
                    "CatalystCount",
                    0,
                ),
                "CatalystText": catalyst_data.get(
                    "CatalystText",
                    "",
                ),
                "SetupScore": setup,
                "AntiFalsePositiveScore": anti,
                "DayAheadScore": clip(
                    score
                ),
                "SetupType": setup_type,
            }
        )

    return (
        pd.DataFrame(output)
        .sort_values(
            "DayAheadScore",
            ascending=False,
        )
        .reset_index(drop=True)
    )


# ============================================================================
# TOP-5 SELECTION
# ============================================================================

def select_top5(
    scored: pd.DataFrame,
) -> pd.DataFrame:

    if scored.empty:
        return scored

    candidates = scored[
        scored["Direction"].isin(
            [
                "LONG",
                "SHORT",
            ]
        )
        & (
            scored["DayAheadScore"]
            >= DAY_AHEAD_MIN_SCORE
        )
    ].copy()

    if candidates.empty:

        return scored.head(
            TOP5_COUNT
        ).copy()

    selected = []
    sector_count: Dict[
        str,
        int,
    ] = {}

    for _, row in candidates.iterrows():

        sector = str(
            row.get(
                "Industry",
                "UNKNOWN",
            )
        ).strip()

        if not sector:
            sector = "UNKNOWN"

        repeated = (
            sector_count.get(
                sector,
                0,
            )
            >= 1
        )

        adjusted = (
            row["DayAheadScore"]
            - (
                SECTOR_REPEAT_PENALTY
                if repeated
                else 0.0
            )
        )

        if (
            sector_count.get(
                sector,
                0,
            )
            >= MAX_TOP5_PER_SECTOR
        ):

            if adjusted < 82.0:
                continue

        item = row.copy()

        item["SelectionScore"] = (
            adjusted
        )

        selected.append(
            item
        )

        sector_count[
            sector
        ] = (
            sector_count.get(
                sector,
                0,
            )
            + 1
        )

        if len(selected) >= TOP5_COUNT:
            break

    # If strict selection produced fewer than five,
    # fill with strongest remaining candidates.
    if len(selected) < TOP5_COUNT:

        existing = {
            str(
                item["Symbol"]
            )
            for item in selected
        }

        for _, row in candidates.iterrows():

            symbol = str(
                row["Symbol"]
            )

            if symbol in existing:
                continue

            item = row.copy()
            item["SelectionScore"] = (
                row["DayAheadScore"]
            )

            selected.append(
                item
            )

            existing.add(symbol)

            if len(selected) >= TOP5_COUNT:
                break

    return (
        pd.DataFrame(selected)
        .sort_values(
            "SelectionScore",
            ascending=False,
        )
        .reset_index(drop=True)
    )


# ============================================================================
# DAY-AHEAD ENGINE
# ============================================================================

def build_day_ahead_watchlist() -> Dict[str, Any]:

    timestamp = now_ist()

    universe = (
        load_nifty500_universe()
    )

    if "Symbol" not in universe.columns:
        raise RuntimeError(
            "Universe has no Symbol column"
        )

    symbols = (
        universe["Symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    # Benchmark is fetched once.
    benchmark = fetch_yahoo_chart(
        NIFTY_TICKER,
        days=320,
        interval="1d",
    )

    histories = fetch_history(
        symbols,
        days=320,
    )

    rows = []

    for _, item in universe.iterrows():

        symbol = str(
            item["Symbol"]
        ).upper().strip()

        df = histories.get(
            symbol
        )

        if df is None:
            continue

        industry = str(
            item.get(
                "Industry",
                item.get(
                    "Industry",
                    "UNKNOWN",
                ),
            )
        )

        features = build_features(
            symbol,
            df,
            benchmark,
            industry,
        )

        if features is not None:
            rows.append(
                features
            )

    if not rows:
        raise RuntimeError(
            "No usable stock data available"
        )

    frame = pd.DataFrame(
        rows
    )

    frame = add_sector_features(
        frame
    )

    scored = score_candidates(
        frame
    )

    top5 = select_top5(
        scored
    )

    # 15:15-15:30 market-close snapshot: Kotak is preferred for live raw
    # fields. The snapshot is advisory/raw only; scoring remains this engine's
    # own calculation.
    kotak_snapshot = capture_kotak_day_ahead_snapshot(
        [str(x).upper() for x in top5["Symbol"].tolist()] if not top5.empty else []
    )
    if kotak_snapshot and not top5.empty:
        for idx, row in top5.iterrows():
            symbol = str(row["Symbol"]).upper()
            q = kotak_snapshot.get(symbol, {})
            ltp = safe_float(q.get("ltp"))
            if np.isfinite(ltp) and ltp > 0:
                top5.loc[idx, "LTP"] = ltp


    candidates: List[
        Dict[str, Any]
    ] = []

    for rank, (_, row) in enumerate(
        top5.iterrows(),
        start=1,
    ):

        candidates.append(
            {
                "rank": rank,
                "symbol": str(
                    row["Symbol"]
                ),
                "industry": str(
                    row.get(
                        "Industry",
                        "UNKNOWN",
                    )
                ),
                "direction": str(
                    row["Direction"]
                ),
                "day_ahead_score": round(
                    safe_float(
                        row[
                            "DayAheadScore"
                        ],
                        0.0,
                    ),
                    2,
                ),
                "selection_score": round(
                    safe_float(
                        row.get(
                            "SelectionScore",
                            row[
                                "DayAheadScore"
                            ],
                        ),
                        0.0,
                    ),
                    2,
                ),
                "setup_type": str(
                    row.get(
                        "SetupType",
                        "UNKNOWN",
                    )
                ),

                "trend_score": round(
                    safe_float(
                        row.get(
                            "TrendScore",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "momentum_score": round(
                    safe_float(
                        row.get(
                            "MomentumScore",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "relative_strength_score": round(
                    safe_float(
                        row.get(
                            "RelativeStrengthScore",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "sector_score": round(
                    safe_float(
                        row.get(
                            "SectorScore",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "volume_score": round(
                    safe_float(
                        row.get(
                            "VolumeScore",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "volatility_score": round(
                    safe_float(
                        row.get(
                            "VolatilityScore",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "catalyst_score": round(
                    safe_float(
                        row.get(
                            "CatalystScoreFinal",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "anti_false_positive_score": round(
                    safe_float(
                        row.get(
                            "AntiFalsePositiveScore",
                            50.0,
                        ),
                        50.0,
                    ),
                    2,
                ),

                "ltp": round(
                    safe_float(
                        row.get(
                            "LTP",
                            np.nan,
                        ),
                    ),
                    2,
                ),

                "atr_pct": round(
                    safe_float(
                        row.get(
                            "ATRpct",
                            np.nan,
                        ),
                    ),
                    3,
                ),

                "ret_1d": round(
                    safe_float(
                        row.get(
                            "Ret1D",
                            np.nan,
                        ),
                    ),
                    3,
                ),

                "ret_5d": round(
                    safe_float(
                        row.get(
                            "Ret5D",
                            np.nan,
                        ),
                    ),
                    3,
                ),

                "ret_20d": round(
                    safe_float(
                        row.get(
                            "Ret20D",
                            np.nan,
                        ),
                    ),
                    3,
                ),

                "rs_5d": round(
                    safe_float(
                        row.get(
                            "RS5D",
                            np.nan,
                        ),
                    ),
                    3,
                ),

                "rs_20d": round(
                    safe_float(
                        row.get(
                            "RS20D",
                            np.nan,
                        ),
                    ),
                    3,
                ),
            }
        )

    result = {
        "engine": "NEXT_DAY_ALPHA_ENGINE",
        "version": "FINAL_V2_ISOLATED_KOTAK_TRUSTED_CATALYST",
        "generated_at": timestamp.isoformat(),
        "data_as_of": timestamp.strftime(
            "%Y-%m-%d"
        ),

        "architecture": {
            "nifty_3min_engine_modified": False,
            "shared_raw_data_allowed": True,
            "shared_calculated_features": False,
            "shared_scores": False,
            "shared_decisions": False,
            "shared_labels": False,
            "shared_predictions": False,
            "shared_raw_fields_only": True,
            "next_day_can_write_to_nifty_engine": False,
            "next_day_can_read_nifty_calculations": False,
            "live_intraday_primary": "KOTAK_NEO",
            "historical_fallback": "YFINANCE_OPTIONAL",
            "catalyst_primary": "NSE_CORPORATE_FILINGS",
            "catalyst_regulatory_context": "SEBI",
        },

        "day_ahead": {
            "universe_size": len(
                symbols
            ),
            "usable_symbols": len(
                frame
            ),
            "scored_symbols": len(
                scored
            ),
            "top5_count": len(
                candidates
            ),
            "top5": candidates,
        },

        "morning_confirmation": {
            "status": "PENDING",
            "final": [],
        },

        "probability_note": (
            "DayAheadScore is a quality score, "
            "not a calibrated win probability."
        ),
    }

    with LOCK:

        temporary = CACHE_JSON.with_suffix(
            ".tmp"
        )

        _atomic_write_text(
            CACHE_JSON,
            json.dumps(result, ensure_ascii=True, indent=2, default=str),
        )

    return result


# ============================================================================
# COMMON RAW-DATA BRIDGE + KOTAK NEO LIVE ADAPTER
# ============================================================================

_RAW_ALLOWED = {
    "timestamp", "received_at", "timestamp_source", "feed_age_seconds", "feed_stale", "token", "exchange", "symbol", "display_symbol", "ltp",
    "open", "high", "low", "close", "volume", "oi", "vwap", "raw_source",
}


def _raw_only(record: Dict[str, Any]) -> Dict[str, Any]:
    return {k: record[k] for k in _RAW_ALLOWED if k in record}


def shared_raw_path(symbol: str, date_string: Optional[str] = None) -> Path:
    date_string = date_string or now_ist().strftime("%Y%m%d")
    safe = str(symbol).replace("/", "_").replace("&", "_").replace(" ", "_").upper()
    return SHARED_RAW_CACHE_DIR / f"{safe}_{date_string}_raw.jsonl"


def write_shared_raw(symbol: str, records: List[Dict[str, Any]]) -> None:
    path = shared_raw_path(symbol)
    payload = "".join(
        json.dumps(_raw_only(record), ensure_ascii=False, default=str) + "\n"
        for record in records
    )
    _atomic_write_text(path, payload)


def read_shared_raw_intraday(symbol: str, max_age_seconds: Optional[float] = None) -> Optional[pd.DataFrame]:
    path = shared_raw_path(symbol)
    if not path.exists():
        return None
    rows = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    rows.append(json.loads(line))
        if not rows:
            return None
        raw = pd.DataFrame(rows)
        raw["DateTime"] = pd.to_datetime(raw.get("timestamp"), errors="coerce")
        if raw["DateTime"].dt.tz is None:
            raw["DateTime"] = raw["DateTime"].dt.tz_localize(IST, ambiguous="NaT", nonexistent="NaT")
        else:
            raw["DateTime"] = raw["DateTime"].dt.tz_convert(IST)
        raw["LTP"] = pd.to_numeric(raw.get("ltp"), errors="coerce")
        raw["VolumeRaw"] = pd.to_numeric(raw.get("volume"), errors="coerce").fillna(0.0)
        raw = raw.dropna(subset=["DateTime", "LTP"]).sort_values("DateTime")
        if raw.empty:
            return None
        if max_age_seconds is not None:
            latest_ts = raw["DateTime"].iloc[-1].to_pydatetime()
            fresh, age = _freshness(latest_ts, float(max_age_seconds))
            if not fresh:
                LOGGER.warning("STALE SHARED FEED %s: age=%s sec limit=%s sec",
                                symbol, "unknown" if age is None else round(age, 2), max_age_seconds)
                return None

        # Kotak quote volume is generally cumulative. Convert it to incremental
        # volume before building 1-minute bars; never treat repeated cumulative
        # volume as fresh traded volume.
        raw["VolumeDelta"] = raw["VolumeRaw"].diff()
        first_vol = raw["VolumeRaw"].iloc[0]
        raw.loc[raw.index[0], "VolumeDelta"] = max(first_vol, 0.0)
        raw["VolumeDelta"] = raw["VolumeDelta"].where(raw["VolumeDelta"] >= 0, 0.0)
        raw["Minute"] = raw["DateTime"].dt.floor("min")

        grouped = raw.groupby("Minute", sort=True)
        bars = grouped.agg(
            Open=("LTP", "first"),
            High=("LTP", "max"),
            Low=("LTP", "min"),
            Close=("LTP", "last"),
            Volume=("VolumeDelta", "sum"),
        ).reset_index().rename(columns={"Minute": "DateTime"})
        return normalize_intraday(bars[["DateTime", "Open", "High", "Low", "Close", "Volume"]])
    except Exception:
        return None


def generate_live_totp(secret_or_otp: str) -> str:
    raw = str(secret_or_otp or "").strip().replace(" ", "").upper()
    if raw.isdigit() and len(raw) == 6:
        return raw
    try:
        if len(raw) % 8:
            raw += "=" * (8 - len(raw) % 8)
        key = base64.b32decode(raw, casefold=True)
        counter = int(time.time() // 30)
        digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
        offset = digest[19] & 15
        token = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000
        return f"{token:06d}"
    except Exception:
        return raw


def _record_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "records", "result", "response"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                nested = _record_list(value)
                if nested:
                    return nested
    return []


def _first(row: Dict[str, Any], keys: Tuple[str, ...], default=None):
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


class KotakRawAdapter:
    """Small isolated Kotak adapter used only by this engine.

    It never imports or calls the NIFTY 3-Min Engine. It writes only raw quote
    fields into the shared raw cache; all indicators and scores stay local.
    """
    def __init__(self):
        self.client = None
        self.connected = False
        self.consumer_key = os.getenv("KOTAK_CONSUMER_KEY", "")
        self.mobile = os.getenv("KOTAK_MOBILE", "")
        self.ucc = os.getenv("KOTAK_UCC", "")
        self.totp = os.getenv("KOTAK_TOTP", "")
        self.mpin = os.getenv("KOTAK_MPIN", "")
        self.token_cache: Dict[str, str] = {}

    def login(self) -> bool:
        if not KOTAK_USE_LIVE or NeoAPI is None:
            return False
        if not all([self.consumer_key, self.mobile, self.ucc, self.totp, self.mpin]):
            return False
        self.client = NeoAPI(
            environment=KOTAK_ENVIRONMENT,
            access_token=None,
            neo_fin_key=None,
            consumer_key=self.consumer_key,
        )
        step1 = self.client.totp_login(
            mobile_number=self.mobile,
            ucc=self.ucc,
            totp=generate_live_totp(self.totp),
        )
        if isinstance(step1, dict) and step1.get("error"):
            raise RuntimeError(str(step1))
        step2 = self.client.totp_validate(mpin=self.mpin)
        if isinstance(step2, dict) and step2.get("error"):
            raise RuntimeError(str(step2))
        self.connected = True
        return True

    def resolve_token(self, symbol: str) -> Optional[str]:
        symbol = str(symbol).upper().strip()
        if symbol in self.token_cache:
            return self.token_cache[symbol]
        if not self.connected or self.client is None:
            return None
        try:
            response = self.client.search_scrip(
                exchange_segment="nse_cm",
                symbol=symbol,
                expiry=None,
                option_type=None,
                strike_price=None,
            )
            rows = _record_list(response)
            for row in rows:
                token = _first(row, ("tk", "token", "instrument_token", "pSymbol"))
                display = str(_first(row, ("display_symbol", "pTrdSymbol", "tradingSymbol", "symbol"), "")).upper()
                if token and (symbol in display or not display):
                    self.token_cache[symbol] = str(token)
                    return str(token)
            if rows:
                token = _first(rows[0], ("tk", "token", "instrument_token", "pSymbol"))
                if token:
                    self.token_cache[symbol] = str(token)
                    return str(token)
        except Exception:
            return None
        return None

    def quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        token = self.resolve_token(symbol)
        if not token or not self.connected or self.client is None:
            return None
        try:
            payload = self.client.quotes(
                instrument_tokens=[{"instrument_token": token, "exchange_segment": "nse_cm"}],
                quote_type="all",
            )
            rows = _record_list(payload)
            if not rows:
                return None
            r = rows[0]
            received_at = now_ist()
            feed_ts = _parse_feed_timestamp(_first(
                r, ("timestamp", "feed_timestamp", "lastTradedTime", "ltt", "lttime", "exchangeTime")
            ))
            if feed_ts is None:
                feed_ts = received_at
                timestamp_source = "RECEIPT_TIME"
            else:
                timestamp_source = "EXCHANGE_FEED"
            fresh, age = _freshness(feed_ts)
            if timestamp_source == "EXCHANGE_FEED" and not fresh:
                LOGGER.warning("STALE KOTAK QUOTE %s: age=%.2fs", symbol, age or -1)
                return None
            return _raw_only({
                "timestamp": feed_ts.isoformat(),
                "received_at": received_at.isoformat(),
                "timestamp_source": timestamp_source,
                "feed_age_seconds": round(age, 3) if age is not None else None,
                "feed_stale": False,
                "token": token,
                "exchange": "nse_cm",
                "symbol": symbol,
                "display_symbol": _first(r, ("display_symbol", "pTrdSymbol", "tradingSymbol", "symbol"), symbol),
                "ltp": safe_float(_first(r, ("ltp", "lastPrice", "iv", "c"))),
                "open": safe_float(_first(r, ("o", "open", "openingPrice"))),
                "high": safe_float(_first(r, ("h", "high", "highPrice"))),
                "low": safe_float(_first(r, ("l", "low", "lowPrice"))),
                "close": safe_float(_first(r, ("c", "close", "previousClose", "pdc"))),
                "volume": safe_float(_first(r, ("v", "volume", "tradedVolume")), 0.0),
                "oi": safe_float(_first(r, ("oi", "openInterest", "pOpenInterest"))),
                "vwap": safe_float(_first(r, ("ap", "vwap"))),
                "raw_source": "KOTAK_NEO",
            })
        except Exception:
            return None

    def get_intraday_capture(self, symbol: str) -> Optional[pd.DataFrame]:
        # Prefer previously captured raw data; no API replay is needed.
        cached = read_shared_raw_intraday(symbol)
        if cached is not None and not cached.empty:
            return cached
        return None

    def capture_snapshot(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        out = {}
        for symbol in symbols:
            row = self.quote(symbol)
            if row:
                out[symbol] = row
                write_shared_raw(symbol, [row])
        return out


_KOTAK_SINGLETON = None


def get_kotak_adapter() -> Optional[KotakRawAdapter]:
    global _KOTAK_SINGLETON
    if _KOTAK_SINGLETON is None:
        _KOTAK_SINGLETON = KotakRawAdapter()
        try:
            _KOTAK_SINGLETON.login()
        except Exception as exc:
            LOGGER.exception("[NEXT-DAY KOTAK] login unavailable")
            _KOTAK_SINGLETON = None
    return _KOTAK_SINGLETON


def capture_kotak_day_ahead_snapshot(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    adapter = get_kotak_adapter()
    if adapter is None or not adapter.connected:
        return {}
    try:
        return adapter.capture_snapshot(symbols)
    except Exception as exc:
        LOGGER.exception("[NEXT-DAY KOTAK] snapshot failed")
        return {}


def capture_kotak_opening_window(symbols: List[str]) -> None:
    """Capture 09:15-09:20 raw quotes for only the frozen TOP 15.

    This keeps the critical morning confirmation on Kotak rather than a
    delayed 1-minute provider. The loop is harmless if the engine is not live.
    """
    adapter = get_kotak_adapter()
    if adapter is None or not adapter.connected:
        return
    records: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
    start = datetime(now_ist().year, now_ist().month, now_ist().day, 9, 15, tzinfo=IST)
    end = datetime(now_ist().year, now_ist().month, now_ist().day, 9, 20, tzinfo=IST)
    while now_ist() < start:
        time.sleep(1)
    while now_ist() < end:
        for symbol in symbols:
            row = adapter.quote(symbol)
            if row:
                records[symbol].append(row)
        time.sleep(max(1, KOTAK_CAPTURE_SECONDS))
    for symbol, rows in records.items():
        if rows:
            write_shared_raw(symbol, rows)


# ============================================================================
# LIVE DATA
# ============================================================================

def fetch_intraday(symbol: str) -> Optional[pd.DataFrame]:
    """Kotak-first intraday source; Yahoo only as explicit fallback."""
    kotak = get_kotak_adapter()
    if kotak is not None and kotak.connected:
        try:
            return kotak.get_intraday_capture(symbol)
        except Exception:
            pass

    shared = read_shared_raw_intraday(symbol)
    if shared is not None and not shared.empty:
        return shared

    return fetch_yahoo_chart(f"{symbol}.NS", days=1, interval="1m")


def normalize_intraday(
    df: pd.DataFrame,
) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame()

    x = df.copy()

    if "DateTime" not in x.columns:

        for candidate in [
            "Datetime",
            "Date",
            "index",
        ]:

            if candidate in x.columns:

                x = x.rename(
                    columns={
                        candidate: "DateTime"
                    }
                )

                break

    if "DateTime" not in x.columns:
        return pd.DataFrame()

    x["DateTime"] = pd.to_datetime(
        x["DateTime"],
        errors="coerce",
    )

    if (
        getattr(
            x["DateTime"].dt,
            "tz",
            None,
        )
        is None
    ):

        x["DateTime"] = (
            x["DateTime"]
            .dt.tz_localize(
                IST,
                ambiguous="NaT",
                nonexistent="NaT",
            )
        )

    else:

        x["DateTime"] = (
            x["DateTime"]
            .dt.tz_convert(IST)
        )

    x = x.dropna(
        subset=["DateTime"]
    )

    for column in [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ]:

        if column in x.columns:

            x[column] = pd.to_numeric(
                x[column],
                errors="coerce",
            )

    return (
        x.sort_values(
            "DateTime"
        )
        .reset_index(drop=True)
    )


def opening_slice(
    df: pd.DataFrame,
) -> pd.DataFrame:

    x = normalize_intraday(
        df
    )

    if x.empty:
        return x

    today = now_ist().date()

    start = datetime(
        today.year,
        today.month,
        today.day,
        MARKET_OPEN_HOUR,
        MARKET_OPEN_MINUTE,
        tzinfo=IST,
    )

    end = start + timedelta(
        minutes=OPENING_MINUTES
    )

    return x[
        (
            x["DateTime"]
            >= start
        )
        & (
            x["DateTime"]
            < end
        )
    ].copy()


def intraday_vwap(
    df: pd.DataFrame,
) -> float:

    if df.empty:
        return np.nan

    typical = (
        df["High"]
        + df["Low"]
        + df["Close"]
    ) / 3.0

    volume = df["Volume"].fillna(
        0.0
    )

    denominator = volume.sum()

    if denominator <= 0:
        return safe_float(
            df["Close"].iloc[-1]
        )

    return safe_float(
        (
            typical * volume
        ).sum()
        / denominator
    )


# ============================================================================
# MORNING CONFIRMATION
# ============================================================================

def market_gap(ticker: str) -> float:
    # For NIFTY opening gap, use Kotak raw quote when available.
    if ticker == NIFTY_TICKER:
        adapter = get_kotak_adapter()
        if adapter is not None and adapter.connected:
            q = adapter.quote("Nifty 50")
            if q:
                previous_close = safe_float(q.get("close"))
                open_price = safe_float(q.get("open"))
                if np.isfinite(previous_close) and previous_close != 0 and np.isfinite(open_price):
                    return (open_price / previous_close - 1.0) * 100.0

    df = fetch_yahoo_chart(ticker, days=5, interval="1d")
    if df is None or len(df) < 2:
        return np.nan
    previous_close = safe_float(df["Close"].iloc[-2])
    today_open = safe_float(df["Open"].iloc[-1])
    if not np.isfinite(previous_close) or previous_close == 0 or not np.isfinite(today_open):
        return np.nan
    return (today_open / previous_close - 1.0) * 100.0


def confirm_candidate(
    candidate: Dict[str, Any],
    nifty_open_gap: float,
    sector_gap: float = np.nan,
) -> Confirmation:

    symbol = str(
        candidate["symbol"]
    )

    direction = str(
        candidate["direction"]
    )

    previous_day_score = safe_float(
        candidate.get(
            "day_ahead_score",
            0.0,
        ),
        0.0,
    )

    daily = fetch_yahoo_chart(
        f"{symbol}.NS",
        days=5,
        interval="1d",
    )

    intra = fetch_intraday(
        symbol
    )

    opening = opening_slice(
        intra
    )

    if (
        daily is None
        or len(daily) < 2
        or opening.empty
    ):

        return Confirmation(
            symbol=symbol,
            direction=direction,
            previous_day_score=previous_day_score,
            confirmation_score=0.0,
            status="DATA_NOT_READY",
            reason="Opening data unavailable",
            prev_close=np.nan,
            open_price=np.nan,
            gap_pct=np.nan,
            nifty_gap_pct=nifty_open_gap,
            sector_gap_pct=sector_gap,
            opening_high=np.nan,
            opening_low=np.nan,
            opening_range_pct=np.nan,
            vwap=np.nan,
            close_vs_vwap_pct=np.nan,
            opening_volume_ratio=np.nan,
            relative_strength_vs_nifty=np.nan,
            relative_strength_vs_sector=np.nan,
            acceptance=False,
            rejection=False,
            breakout=False,
            breakdown=False,
        )

    previous_close = safe_float(
        daily["Close"].iloc[-2]
    )

    open_price = safe_float(
        opening["Open"].iloc[0]
    )

    opening_high = safe_float(
        opening["High"].max()
    )

    opening_low = safe_float(
        opening["Low"].min()
    )

    last_price = safe_float(
        opening["Close"].iloc[-1]
    )

    if (
        not np.isfinite(
            open_price
        )
        or not np.isfinite(
            previous_close
        )
        or previous_close == 0
    ):

        return Confirmation(
            symbol=symbol,
            direction=direction,
            previous_day_score=previous_day_score,
            confirmation_score=0.0,
            status="DATA_NOT_READY",
            reason="Invalid opening price",
            prev_close=previous_close,
            open_price=open_price,
            gap_pct=np.nan,
            nifty_gap_pct=nifty_open_gap,
            sector_gap_pct=sector_gap,
            opening_high=opening_high,
            opening_low=opening_low,
            opening_range_pct=np.nan,
            vwap=np.nan,
            close_vs_vwap_pct=np.nan,
            opening_volume_ratio=np.nan,
            relative_strength_vs_nifty=np.nan,
            relative_strength_vs_sector=np.nan,
            acceptance=False,
            rejection=False,
            breakout=False,
            breakdown=False,
        )

    gap_pct = (
        open_price
        / previous_close
        - 1.0
    ) * 100.0

    opening_range_pct = (
        (
            opening_high
            - opening_low
        )
        / open_price
        * 100.0
    )

    vwap = intraday_vwap(
        opening
    )

    close_vs_vwap_pct = (
        (
            last_price
            / vwap
            - 1.0
        )
        * 100.0
        if (
            np.isfinite(vwap)
            and vwap != 0
        )
        else np.nan
    )

    # Opening volume compared with recent daily average.
    avg_volume = (
        daily["Volume"]
        .tail(4)
        .mean()
    )

    opening_volume = (
        opening["Volume"]
        .fillna(0.0)
        .sum()
    )

    # Approximate 5-minute participation relative to one trading day's
    # average divided by ~75 five-minute blocks.
    expected_opening_volume = (
        avg_volume / 75.0
        if (
            np.isfinite(
                avg_volume
            )
            and avg_volume > 0
        )
        else np.nan
    )

    opening_volume_ratio = (
        opening_volume
        / expected_opening_volume
        if (
            np.isfinite(
                expected_opening_volume
            )
            and expected_opening_volume > 0
        )
        else np.nan
    )

    stock_gap_relative = (
        gap_pct
        - safe_float(
            nifty_open_gap,
            0.0,
        )
    )

    sector_gap_relative = (
        gap_pct
        - safe_float(
            sector_gap,
            0.0,
        )
        if np.isfinite(
            safe_float(
                sector_gap
            )
        )
        else stock_gap_relative
    )

    # Price behaviour after open.
    price_change_from_open = (
        (
            last_price
            / open_price
            - 1.0
        )
        * 100.0
        if open_price != 0
        else 0.0
    )

    if direction == "LONG":

        gap_quality = clip(
            50.0
            + stock_gap_relative
            * 16.0
        )

        vwap_quality = (
            90.0
            if (
                np.isfinite(
                    close_vs_vwap_pct
                )
                and close_vs_vwap_pct
                > 0.10
            )
            else 30.0
        )

        momentum_quality = clip(
            50.0
            + price_change_from_open
            * 18.0
        )

        relative_quality = clip(
            50.0
            + stock_gap_relative
            * 14.0
        )

        acceptance = bool(
            last_price > vwap
            and price_change_from_open > 0
            and last_price
            >= opening_high * 0.997
        )

        rejection = bool(
            last_price < vwap
            and price_change_from_open < -0.20
        )

        breakout = bool(
            last_price
            >= opening_high
            * 0.999
        )

        breakdown = bool(
            last_price
            <= opening_low
            * 1.001
        )

    else:

        gap_quality = clip(
            50.0
            - stock_gap_relative
            * 16.0
        )

        vwap_quality = (
            90.0
            if (
                np.isfinite(
                    close_vs_vwap_pct
                )
                and close_vs_vwap_pct
                < -0.10
            )
            else 30.0
        )

        momentum_quality = clip(
            50.0
            - price_change_from_open
            * 18.0
        )

        relative_quality = clip(
            50.0
            - stock_gap_relative
            * 14.0
        )

        acceptance = bool(
            last_price < vwap
            and price_change_from_open < 0
            and last_price
            <= opening_low * 1.003
        )

        rejection = bool(
            last_price > vwap
            and price_change_from_open > 0.20
        )

        breakout = bool(
            last_price
            <= opening_low * 1.001
        )

        breakdown = bool(
            last_price
            >= opening_high * 0.999
        )

    volume_quality = (
        clip(
            45.0
            + math.log(
                max(
                    opening_volume_ratio,
                    0.05,
                )
            )
            * 28.0
        )
        if np.isfinite(
            opening_volume_ratio
        )
        else 50.0
    )

    range_quality = (
        78.0
        if (
            0.15
            <= opening_range_pct
            <= MAX_OPENING_RANGE_PCT
        )
        else 40.0
    )

    confirmation_score = (
        gap_quality * 0.15
        + vwap_quality * 0.20
        + momentum_quality * 0.20
        + relative_quality * 0.15
        + volume_quality * 0.10
        + range_quality * 0.05
        + (
            100.0
            if acceptance
            else 35.0
        )
        * 0.10
        + (
            100.0
            if breakout
            else 40.0
        )
        * 0.05
    )

    # Hard contradiction penalty.
    if rejection:
        confirmation_score -= 25.0

    if direction == "LONG":
        if (
            gap_pct > MAX_GAP_PCT
            and price_change_from_open < 0
        ):
            confirmation_score -= 15.0

    else:
        if (
            gap_pct < -MAX_GAP_PCT
            and price_change_from_open > 0
        ):
            confirmation_score -= 15.0

    confirmation_score = clip(
        confirmation_score
    )

    if rejection:

        status = "REJECTED"
        reason = (
            "Opening behaviour rejected "
            "the previous-day thesis"
        )

    elif (
        confirmation_score
        >= MORNING_CONFIRMATION_MIN_SCORE
        and (
            acceptance
            or breakout
        )
    ):

        status = "CONFIRMED"
        reason = (
            "Opening acceptance + VWAP + "
            "relative strength structure aligned"
        )

    elif (
        confirmation_score
        >= MORNING_WATCH_SCORE
    ):

        status = "WATCH"
        reason = (
            "Partial confirmation; "
            "not strong enough for final trade"
        )

    else:

        status = "REJECTED"
        reason = (
            "Insufficient morning confirmation"
        )

    return Confirmation(
        symbol=symbol,
        direction=direction,
        previous_day_score=previous_day_score,
        confirmation_score=round(
            confirmation_score,
            2,
        ),
        status=status,
        reason=reason,

        prev_close=round(
            previous_close,
            2,
        ),

        open_price=round(
            open_price,
            2,
        ),

        gap_pct=round(
            gap_pct,
            3,
        ),

        nifty_gap_pct=round(
            safe_float(
                nifty_open_gap
            ),
            3,
        ),

        sector_gap_pct=round(
            safe_float(
                sector_gap
            ),
            3,
        ),

        opening_high=round(
            opening_high,
            2,
        ),

        opening_low=round(
            opening_low,
            2,
        ),

        opening_range_pct=round(
            opening_range_pct,
            3,
        ),

        vwap=round(
            safe_float(vwap),
            2,
        ),

        close_vs_vwap_pct=round(
            safe_float(
                close_vs_vwap_pct
            ),
            3,
        ),

        opening_volume_ratio=round(
            safe_float(
                opening_volume_ratio
            ),
            3,
        ),

        relative_strength_vs_nifty=round(
            stock_gap_relative,
            3,
        ),

        relative_strength_vs_sector=round(
            sector_gap_relative,
            3,
        ),

        acceptance=acceptance,
        rejection=rejection,
        breakout=breakout,
        breakdown=breakdown,
    )


# ============================================================================
# MORNING FINAL 2
# ============================================================================

def run_morning_confirmation() -> Dict[str, Any]:

    latest = load_latest()

    if not latest:
        return {
            "status": "NO_DAY_AHEAD_DATA",
            "final": [],
        }

    candidates = (
        latest
        .get(
            "day_ahead",
            {}
        )
        .get(
            "top5",
            [],
        )
    )

    if not candidates:

        result = {
            "status": "NO_CANDIDATES",
            "final": [],
            "confirmations": [],
        }

        persist_morning(
            latest,
            result,
        )

        return result

    nifty_gap = market_gap(
        NIFTY_TICKER
    )

    confirmations = []

    for candidate in candidates:

        confirmation = (
            confirm_candidate(
                candidate,
                nifty_gap,
            )
        )

        confirmations.append(
            asdict(
                confirmation
            )
        )

    confirmed = [
        item
        for item in confirmations
        if item["status"]
        == "CONFIRMED"
    ]

    confirmed.sort(
        key=lambda item: (
            item[
                "confirmation_score"
            ],
            item[
                "previous_day_score"
            ],
        ),
        reverse=True,
    )

    final = confirmed[
        :2
    ]

    if final:

        status = "FINAL_2"

        if len(final) == 1:
            status = "FINAL_1"

    else:

        status = "NO_TRADE"

    result = {
        "status": status,
        "generated_at": now_ist().isoformat(),
        "final": final,
        "confirmations": confirmations,
    }

    persist_morning(
        latest,
        result,
    )

    return result


def persist_morning(
    latest: Dict[str, Any],
    morning: Dict[str, Any],
) -> None:

    latest["morning_confirmation"] = (
        morning
    )

    with LOCK:

        temporary = CACHE_JSON.with_suffix(
            ".tmp"
        )

        _atomic_write_text(
            CACHE_JSON,
            json.dumps(latest, ensure_ascii=False, indent=2, default=str),
        )


# ============================================================================
# OUTCOME LEDGER
# ============================================================================

def append_outcome(
    record: Dict[str, Any],
) -> None:

    record = {
        "timestamp": now_ist().isoformat(),
        **record,
    }

    with OUTCOME_JSONL.open(
        "a",
        encoding="utf-8",
    ) as handle:

        handle.write(
            json.dumps(
                record,
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )
        handle.flush()
        os.fsync(handle.fileno())


# ============================================================================
# V7 FINAL HARDENING / MTF / MACRO / SECTOR / RISK EXTENSION
# ============================================================================
# This extension is intentionally appended to the existing V4 implementation.
# It does not delete or replace the original engines. Later definitions below
# override only the day-ahead/morning orchestration points, while preserving all
# existing adapters, helpers, catalyst logic, UI and storage contracts.

import contextlib

V7_VERSION = "FINAL_V7_FULL_MTF_MACRO_RISK_AUDIT"
V7_BASKET_SIZE = max(10, int(os.getenv("NEXT_DAY_V7_BASKET_SIZE", "30")))
V7_MAX_STOCKS_PER_SECTOR = max(1, int(os.getenv("NEXT_DAY_V7_MAX_PER_SECTOR", "4")))
V7_MTF_MIN_SCORE = float(os.getenv("NEXT_DAY_V7_MTF_MIN_SCORE", "62"))
V7_MIN_RR = max(1.0, float(os.getenv("NEXT_DAY_V7_MIN_RR", "1.5")))
V7_RR_TARGET_MULT = max(1.5, float(os.getenv("NEXT_DAY_V7_RR_TARGET_MULT", "2.0")))
V7_MAX_INVALIDATION_DISTANCE_ATR = max(0.25, float(os.getenv("NEXT_DAY_V7_MAX_INVALIDATION_DISTANCE_ATR", "2.5")))
V7_MIN_INVALIDATION_DISTANCE_ATR = max(0.05, float(os.getenv("NEXT_DAY_V7_MIN_INVALIDATION_DISTANCE_ATR", "0.25")))
V7_MTF_THREADS = max(2, int(os.getenv("NEXT_DAY_V7_MTF_THREADS", "6")))
V7_VIX_CAUTION = float(os.getenv("NEXT_DAY_V7_VIX_CAUTION", "18"))
V7_VIX_HIGH = float(os.getenv("NEXT_DAY_V7_VIX_HIGH", "20"))
V7_VIX_SPIKE_PCT = float(os.getenv("NEXT_DAY_V7_VIX_SPIKE_PCT", "12"))
V7_VIX_HIGH_CONFIRM_BONUS = float(os.getenv("NEXT_DAY_V7_VIX_HIGH_CONFIRM_BONUS", "5"))
V7_VIX_CAUTION_CONFIRM_BONUS = float(os.getenv("NEXT_DAY_V7_VIX_CAUTION_CONFIRM_BONUS", "2"))
V7_AUDIT_FILE = ROOT / "audit_summary.json"
V7_MTF_CACHE_DIR = ROOT / "mtf_cache"
V7_MTF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
V7_LOCK_FILE = SHARED_RAW_CACHE_DIR / ".shared_raw.lock"

# Extra raw fields only. These are still raw quote values; no calculated field
# from either engine is allowed across the isolation boundary.
_RAW_ALLOWED.update({
    "upper_circuit", "lower_circuit", "upper_price_band", "lower_price_band",
    "bid", "ask", "bid_qty", "ask_qty", "last_traded_time",
})

@contextlib.contextmanager
def _v7_process_lock(path: Path):
    """Best-effort cross-process lock; fcntl on Linux, thread lock fallback."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = None
    try:
        fh = path.open("a+")
        try:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            yield fh
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except ImportError:
            with LOCK:
                yield fh
    finally:
        if fh is not None:
            fh.close()


def write_shared_raw(symbol: str, records: List[Dict[str, Any]]) -> None:
    """Append raw JSONL safely; never writes calculated features/scores."""
    if not records:
        return
    path = shared_raw_path(symbol)
    payload = "".join(
        json.dumps(_raw_only(record), ensure_ascii=False, default=str) + "\n"
        for record in records
    )
    with _v7_process_lock(V7_LOCK_FILE):
        with path.open("a", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())


def _v7_sector_bucket(industry: Any) -> str:
    s = str(industry or "UNKNOWN").upper()
    mapping = {
        "BANK": "BANKING", "FINANC": "BANKING", "NBFC": "BANKING",
        "IT": "IT", "SOFTWARE": "IT", "TECH": "IT",
        "PHARMA": "HEALTHCARE_PHARMA", "HEALTH": "HEALTHCARE_PHARMA",
        "HOSPITAL": "HEALTHCARE_PHARMA", "BIOTECH": "HEALTHCARE_PHARMA",
        "AUTO": "AUTOMOBILE", "TYRE": "AUTOMOBILE", "AUTOMOB": "AUTOMOBILE",
        "CHEM": "CHEMICAL", "FERTIL": "CHEMICAL", "SPECIALTY": "CHEMICAL",
        "DEFENCE": "DEFENCE", "DEFENSE": "DEFENCE", "AEROSPACE": "DEFENCE",
        "RAIL": "RAILWAY", "TRAVEL": "RAILWAY", "LOGISTICS": "RAILWAY",
        "FMCG": "FMCG", "FOOD": "FMCG", "CONSUMER": "FMCG",
        "ENERGY": "ENERGY", "OIL": "ENERGY", "GAS": "ENERGY", "POWER": "ENERGY",
        "METAL": "METALS_MINING", "MINING": "METALS_MINING", "STEEL": "METALS_MINING",
        "CEMENT": "CEMENT_CONSTRUCTION", "CONSTRUCTION": "CEMENT_CONSTRUCTION",
        "INFRA": "CEMENT_CONSTRUCTION", "REAL ESTATE": "REALTY",
        "TELECOM": "TELECOM", "MEDIA": "MEDIA", "TEXTILE": "TEXTILES",
    }
    for key, value in mapping.items():
        if key in s:
            return value
    return str(industry or "UNKNOWN").strip().upper() or "UNKNOWN"


def _v7_resample_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    if "DateTime" not in x.columns:
        return pd.DataFrame()
    x["DateTime"] = pd.to_datetime(x["DateTime"], errors="coerce")
    x = x.dropna(subset=["DateTime"]).set_index("DateTime")
    for c in ("Open", "High", "Low", "Close", "Volume"):
        if c not in x.columns:
            x[c] = np.nan
        x[c] = pd.to_numeric(x[c], errors="coerce")
    out = x.resample(rule).agg({
        "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"
    }).dropna(subset=["Open", "High", "Low", "Close"])
    return out.reset_index()


def _v7_local_levels(df: pd.DataFrame) -> Dict[str, float]:
    if df is None or len(df) < 8:
        return {"support": np.nan, "resistance": np.nan, "atr": np.nan, "distance_support_pct": np.nan, "distance_resistance_pct": np.nan}
    x = df.copy()
    close = safe_float(x["Close"].iloc[-1])
    a = atr(x, 14)
    av = safe_float(a.iloc[-1]) if len(a) else np.nan
    lows = pd.to_numeric(x["Low"], errors="coerce").tail(min(60, len(x)))
    highs = pd.to_numeric(x["High"], errors="coerce").tail(min(60, len(x)))
    supports = [safe_float(v) for v in lows if np.isfinite(safe_float(v)) and safe_float(v) < close]
    resistances = [safe_float(v) for v in highs if np.isfinite(safe_float(v)) and safe_float(v) > close]
    support = max(supports) if supports else safe_float(lows.min())
    resistance = min(resistances) if resistances else safe_float(highs.max())
    return {
        "support": support,
        "resistance": resistance,
        "atr": av,
        "distance_support_pct": ((close / support) - 1) * 100 if np.isfinite(close) and np.isfinite(support) and support else np.nan,
        "distance_resistance_pct": ((resistance / close) - 1) * 100 if np.isfinite(close) and np.isfinite(resistance) and close else np.nan,
    }


def _v7_mw_pattern(df: pd.DataFrame) -> Tuple[str, float]:
    """Conservative M/W heuristic from swing highs/lows; not a chart-image claim."""
    if df is None or len(df) < 25:
        return "NONE", 0.0
    c = pd.to_numeric(df["Close"], errors="coerce").dropna().tail(80)
    if len(c) < 25:
        return "NONE", 0.0
    vals = c.values
    peaks, troughs = [], []
    w = 2
    for i in range(w, len(vals) - w):
        window = vals[i-w:i+w+1]
        if vals[i] == max(window): peaks.append((i, vals[i]))
        if vals[i] == min(window): troughs.append((i, vals[i]))
    if len(peaks) >= 2:
        a, b = peaks[-2], peaks[-1]
        if a[0] < b[0] and abs(a[1]-b[1]) / max(abs(a[1]), 1e-9) < 0.025:
            neckline = min(vals[a[0]:b[0]+1])
            if vals[-1] < max(a[1], b[1]) * 0.995:
                return "M_TOP", 82.0
    if len(troughs) >= 2:
        a, b = troughs[-2], troughs[-1]
        if a[0] < b[0] and abs(a[1]-b[1]) / max(abs(a[1]), 1e-9) < 0.025:
            if vals[-1] > min(a[1], b[1]) * 1.005:
                return "W_BOTTOM", 82.0
    return "NONE", 0.0


def _v7_mtf_fetch(symbol: str) -> Dict[str, Any]:
    """Fetch expensive MTF data only for the 30-stock bet basket."""
    ticker = f"{symbol}.NS"
    result: Dict[str, Any] = {"symbol": symbol}
    try:
        daily = fetch_yahoo_chart(ticker, days=320, interval="1d")
        hourly = fetch_yahoo_chart(ticker, days=180, interval="1h")
        mins15 = fetch_yahoo_chart(ticker, days=55, interval="15m")
        if daily is not None and not daily.empty:
            weekly = _v7_resample_ohlc(daily, "W-FRI")
        else:
            weekly = pd.DataFrame()
        four_h = _v7_resample_ohlc(hourly, "4h") if hourly is not None else pd.DataFrame()
        frames = {"W": weekly, "D": daily, "4H": four_h, "1H": hourly, "15M": mins15}
        levels = {}
        score_parts = []
        for name, frame in frames.items():
            lv = _v7_local_levels(frame)
            pat, pat_score = _v7_mw_pattern(frame)
            lv["pattern"] = pat
            lv["pattern_score"] = pat_score
            levels[name] = lv
            if pat == "M_TOP": score_parts.append(25.0)
            elif pat == "W_BOTTOM": score_parts.append(75.0)
            else: score_parts.append(50.0)
        result["mtf"] = levels
        result["mtf_score"] = float(np.mean(score_parts)) if score_parts else 50.0
    except Exception as exc:
        LOGGER.warning("MTF enrichment failed for %s: %s", symbol, exc)
        result["mtf"] = {}
        result["mtf_score"] = 50.0
    return result


def _v7_directional_mtf(row: pd.Series, mtf: Dict[str, Any]) -> Dict[str, Any]:
    direction = str(row.get("Direction", "NEUTRAL"))
    price = safe_float(row.get("LTP"), np.nan)
    av = safe_float(row.get("ATR"), np.nan)
    if not np.isfinite(av) or av <= 0:
        atrpct = safe_float(row.get("ATRpct"), np.nan)
        av = price * atrpct / 100.0 if np.isfinite(price) and np.isfinite(atrpct) else np.nan

    support_candidates, resistance_candidates = [], []
    pattern_conflicts, pattern_supports = 0, 0

    for tf, lv in mtf.items():
        s = safe_float(lv.get("support"))
        r = safe_float(lv.get("resistance"))
        if np.isfinite(s) and np.isfinite(price) and s < price:
            support_candidates.append(s)
        if np.isfinite(r) and np.isfinite(price) and r > price:
            resistance_candidates.append(r)

        p = lv.get("pattern")
        if direction == "LONG":
            if p == "M_TOP":
                pattern_conflicts += 1
            elif p == "W_BOTTOM":
                pattern_supports += 1
        elif direction == "SHORT":
            if p == "W_BOTTOM":
                pattern_conflicts += 1
            elif p == "M_TOP":
                pattern_supports += 1

    structural_support = max(support_candidates) if support_candidates else np.nan
    structural_resistance = min(resistance_candidates) if resistance_candidates else np.nan

    min_risk = (
        V7_MIN_INVALIDATION_DISTANCE_ATR * av
        if np.isfinite(av) and av > 0 else np.nan
    )

    if direction == "LONG":
        structural_risk = (
            price - structural_support
            if np.isfinite(price) and np.isfinite(structural_support) else np.nan
        )
        if np.isfinite(structural_risk) and np.isfinite(min_risk) and structural_risk >= min_risk:
            invalidation = structural_support
            invalidation_source = "STRUCTURAL_SR"
            risk = structural_risk
        elif np.isfinite(price) and np.isfinite(min_risk):
            invalidation = price - min_risk
            invalidation_source = "ATR_FLOOR"
            risk = min_risk
        else:
            invalidation = structural_support
            invalidation_source = "STRUCTURAL_SR"
            risk = structural_risk

        target = (
            structural_resistance
            if np.isfinite(structural_resistance)
            else (price + V7_RR_TARGET_MULT * av if np.isfinite(price) and np.isfinite(av) else np.nan)
        )
        reward = target - price if np.isfinite(target) and np.isfinite(price) else np.nan

    else:
        structural_risk = (
            structural_resistance - price
            if np.isfinite(price) and np.isfinite(structural_resistance) else np.nan
        )
        if np.isfinite(structural_risk) and np.isfinite(min_risk) and structural_risk >= min_risk:
            invalidation = structural_resistance
            invalidation_source = "STRUCTURAL_SR"
            risk = structural_risk
        elif np.isfinite(price) and np.isfinite(min_risk):
            invalidation = price + min_risk
            invalidation_source = "ATR_FLOOR"
            risk = min_risk
        else:
            invalidation = structural_resistance
            invalidation_source = "STRUCTURAL_SR"
            risk = structural_risk

        target = (
            structural_support
            if np.isfinite(structural_support)
            else (price - V7_RR_TARGET_MULT * av if np.isfinite(price) and np.isfinite(av) else np.nan)
        )
        reward = price - target if np.isfinite(target) and np.isfinite(price) else np.nan

    rr = reward / risk if np.isfinite(reward) and np.isfinite(risk) and risk > 0 else np.nan
    invalidation_atr = risk / av if np.isfinite(risk) and np.isfinite(av) and av > 0 else np.nan

    mtf_score = 50.0 + pattern_supports * 6.0 - pattern_conflicts * 9.0
    for tf in ("W", "D", "4H", "1H", "15M"):
        lv = mtf.get(tf, {})
        if direction == "LONG" and np.isfinite(price) and np.isfinite(lv.get("support", np.nan)) and np.isfinite(lv.get("resistance", np.nan)):
            if price > lv["support"]:
                mtf_score += 2.0
        if direction == "SHORT" and np.isfinite(price) and np.isfinite(lv.get("resistance", np.nan)) and np.isfinite(lv.get("support", np.nan)):
            if price < lv["resistance"]:
                mtf_score += 2.0

    hard_rr_pass = bool(
        np.isfinite(rr)
        and rr >= V7_MIN_RR
        and np.isfinite(invalidation_atr)
        and invalidation_atr <= V7_MAX_INVALIDATION_DISTANCE_ATR
        and invalidation_atr >= V7_MIN_INVALIDATION_DISTANCE_ATR
    )

    return {
        "mtf_score": clip(mtf_score),
        "support": structural_support,
        "resistance": structural_resistance,
        "invalidation": invalidation,
        "invalidation_source": invalidation_source,
        "target": target,
        "risk_points": risk,
        "reward_points": reward,
        "rr": rr,
        "invalidation_atr": invalidation_atr,
        "pattern_conflicts": pattern_conflicts,
        "pattern_supports": pattern_supports,
        "hard_rr_pass": hard_rr_pass,
    }


def _v7_vix_context() -> Dict[str, Any]:
    try:
        vix = fetch_yahoo_chart("^INDIAVIX", days=320, interval="1d")
        if vix is None or vix.empty or "Close" not in vix:
            return {"status": "UNAVAILABLE", "directional_predictor": False}
        c = pd.to_numeric(vix["Close"], errors="coerce").dropna()
        if c.empty: return {"status": "UNAVAILABLE", "directional_predictor": False}
        level = safe_float(c.iloc[-1])
        prev5 = safe_float(c.iloc[-6]) if len(c) >= 6 else np.nan
        change5 = (level / prev5 - 1.0) * 100 if np.isfinite(prev5) and prev5 else np.nan
        percentile = float((c <= level).mean() * 100)
        if level >= V7_VIX_HIGH or (np.isfinite(change5) and change5 >= V7_VIX_SPIKE_PCT):
            regime = "HIGH_VOLATILITY"
            confirm_bonus = V7_VIX_HIGH_CONFIRM_BONUS
            risk_multiplier = 0.65
        elif level >= V7_VIX_CAUTION:
            regime = "CAUTION"
            confirm_bonus = V7_VIX_CAUTION_CONFIRM_BONUS
            risk_multiplier = 0.80
        else:
            regime = "NORMAL"
            confirm_bonus = 0.0
            risk_multiplier = 1.0
        return {
            "status": "OK", "level": round(level, 3), "change_5d_pct": round(change5, 3) if np.isfinite(change5) else None,
            "percentile": round(percentile, 2), "regime": regime, "confirmation_bonus": confirm_bonus,
            "risk_multiplier": risk_multiplier, "directional_predictor": False,
            "note": "VIX controls caution/confirmation/risk only; it does not predict LONG or SHORT direction.",
        }
    except Exception as exc:
        LOGGER.warning("VIX context unavailable: %s", exc)
        return {"status": "UNAVAILABLE", "directional_predictor": False}


def _v7_preselect_30(scored: pd.DataFrame) -> pd.DataFrame:
    if scored is None or scored.empty:
        return pd.DataFrame()
    x = scored[scored["Direction"].isin(["LONG", "SHORT"])].copy()
    x = x[x["DayAheadScore"] >= DAY_AHEAD_MIN_SCORE].sort_values("DayAheadScore", ascending=False)
    if x.empty: return x
    selected, counts = [], {}
    for _, row in x.iterrows():
        sector = _v7_sector_bucket(row.get("Industry"))
        if counts.get(sector, 0) >= V7_MAX_STOCKS_PER_SECTOR:
            continue
        item = row.copy()
        item["SectorBucket"] = sector
        selected.append(item)
        counts[sector] = counts.get(sector, 0) + 1
        if len(selected) >= V7_BASKET_SIZE: break
    if len(selected) < V7_BASKET_SIZE:
        used = {str(r["Symbol"]) for r in selected}
        for _, row in x.iterrows():
            if str(row["Symbol"]) in used: continue
            item = row.copy(); item["SectorBucket"] = _v7_sector_bucket(row.get("Industry")); selected.append(item); used.add(str(row["Symbol"]))
            if len(selected) >= V7_BASKET_SIZE: break
    return pd.DataFrame(selected).reset_index(drop=True)


def _v7_enrich_basket(basket: pd.DataFrame) -> pd.DataFrame:
    if basket.empty: return basket
    jobs = {}
    with ThreadPoolExecutor(max_workers=V7_MTF_THREADS) as pool:
        for symbol in basket["Symbol"].astype(str): jobs[symbol] = pool.submit(_v7_mtf_fetch, symbol)
        rows = []
        for _, row in basket.iterrows():
            symbol = str(row["Symbol"])
            try: mtf = jobs[symbol].result()
            except Exception: mtf = {"mtf": {}, "mtf_score": 50.0}
            risk = _v7_directional_mtf(row, mtf.get("mtf", {}))
            out = row.to_dict(); out.update(risk); out["MTF"] = mtf.get("mtf", {})
            # MTF is an additional confirmation layer, not a replacement for the original score.
            base = safe_float(out.get("DayAheadScore"), 0.0)
            out["V7Score"] = clip(base * 0.72 + safe_float(risk.get("mtf_score"), 50.0) * 0.18 + safe_float(out.get("AntiFalsePositiveScore"), 50.0) * 0.10)
            rows.append(out)
    return pd.DataFrame(rows).sort_values("V7Score", ascending=False).reset_index(drop=True)


def _v7_select_final5(enriched: pd.DataFrame) -> pd.DataFrame:
    if enriched.empty: return enriched
    x = enriched.copy()
    # Hard exclusions: no direction, no structural RR, or invalidation too close.
    x = x[x["Direction"].isin(["LONG", "SHORT"])].copy()
    x = x[x["hard_rr_pass"] == True].copy()
    if x.empty: return x
    selected, sector_count = [], {}
    for _, row in x.sort_values(["V7Score", "Symbol"], ascending=[False, True]).iterrows():
        sector = str(row.get("SectorBucket", "UNKNOWN"))
        if sector_count.get(sector, 0) >= 2: continue
        selected.append(row)
        sector_count[sector] = sector_count.get(sector, 0) + 1
        if len(selected) >= TOP5_COUNT: break
    return pd.DataFrame(selected).reset_index(drop=True).head(TOP5_COUNT)


def _v7_risk_profile(row: Dict[str, Any], vix: Dict[str, Any]) -> Dict[str, Any]:
    direction = str(row.get("direction", row.get("Direction", "LONG")))
    entry = safe_float(row.get("ltp", row.get("LTP", np.nan)))
    atrv = safe_float(row.get("ATR", np.nan))
    if not np.isfinite(atrv) or atrv <= 0:
        atrpct = safe_float(row.get("atr_pct", row.get("ATRpct", np.nan)))
        atrv = entry * atrpct / 100 if np.isfinite(entry) and np.isfinite(atrpct) else np.nan
    stop = safe_float(row.get("invalidation", np.nan))
    target = safe_float(row.get("target", np.nan))
    if not np.isfinite(stop) and np.isfinite(entry) and np.isfinite(atrv): stop = entry - 1.5*atrv if direction == "LONG" else entry + 1.5*atrv
    if not np.isfinite(target) and np.isfinite(entry) and np.isfinite(atrv): target = entry + 2.25*atrv if direction == "LONG" else entry - 2.25*atrv
    risk = abs(entry-stop) if np.isfinite(entry) and np.isfinite(stop) else np.nan
    reward = abs(target-entry) if np.isfinite(entry) and np.isfinite(target) else np.nan
    rr = reward/risk if np.isfinite(risk) and risk > 0 and np.isfinite(reward) else np.nan
    return {
        "entry_reference": round(entry, 2) if np.isfinite(entry) else None,
        "atr": round(atrv, 4) if np.isfinite(atrv) else None,
        "suggested_stop": round(stop, 2) if np.isfinite(stop) else None,
        "target_1": round(entry + (risk*1.5 if direction == "LONG" else -risk*1.5), 2) if np.isfinite(entry) and np.isfinite(risk) else None,
        "target_2": round(target, 2) if np.isfinite(target) else None,
        "rr": round(rr, 3) if np.isfinite(rr) else None,
        "risk_multiplier": vix.get("risk_multiplier", 1.0),
        "distance_to_invalidation_atr": row.get("invalidation_atr"),
            "invalidation_source": row.get("invalidation_source", "UNKNOWN"),
    }


def _v7_circuit_gate(symbol: str, opening: pd.DataFrame) -> Tuple[bool, str]:
    """Reject only when a raw circuit/price-band field proves a lock.
    Unknown is not treated as locked because some broker quote schemas omit bands.
    """
    adapter = get_kotak_adapter()
    if adapter is not None and adapter.connected:
        q = adapter.quote(symbol)
        if q:
            ltp = safe_float(q.get("ltp")); upper = safe_float(q.get("upper_circuit", q.get("upper_price_band"))); lower = safe_float(q.get("lower_circuit", q.get("lower_price_band")))
            if np.isfinite(ltp) and np.isfinite(upper) and upper > 0 and ltp >= upper * 0.999: return False, "CIRCUIT_LOCKED_UPPER"
            if np.isfinite(ltp) and np.isfinite(lower) and lower > 0 and ltp <= lower * 1.001: return False, "CIRCUIT_LOCKED_LOWER"
    return True, "OK_OR_BAND_UNAVAILABLE"


def _v7_sector_regimes(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    groups: Dict[str, List[float]] = {}
    for c in candidates:
        sec = str(c.get("sector_bucket", _v7_sector_bucket(c.get("industry"))))
        intra = fetch_intraday(str(c.get("symbol", "")))
        opening = opening_slice(intra)
        ret = np.nan
        if not opening.empty:
            op = safe_float(opening["Open"].iloc[0]); last = safe_float(opening["Close"].iloc[-1])
            if np.isfinite(op) and op: ret = (last/op-1)*100
        if np.isfinite(ret): groups.setdefault(sec, []).append(ret)
    regimes = {}
    for sec, vals in groups.items():
        r = float(np.mean(vals)); regimes[sec] = {"return_5m_pct": round(r,3), "breadth": round(float(np.mean(np.array(vals)>0)),3), "regime": "BULLISH" if r > 0.15 and np.mean(np.array(vals)>0) >= .60 else ("BEARISH" if r < -0.15 and np.mean(np.array(vals)<0) >= .60 else "NEUTRAL")}
    return regimes


def _v7_confirm_sector_filter(confirmations: List[Dict[str, Any]], regimes: Dict[str, Any]) -> List[Dict[str, Any]]:
    out=[]
    for c in confirmations:
        sec = str(c.get("sector_bucket", "UNKNOWN")); reg = regimes.get(sec, {}).get("regime", "NEUTRAL")
        direction = c.get("direction")
        if reg == "BULLISH" and direction == "LONG": c["sector_live_alignment"] = True; c["confirmation_score"] = clip(safe_float(c.get("confirmation_score"),0)+5)
        elif reg == "BEARISH" and direction == "SHORT": c["sector_live_alignment"] = True; c["confirmation_score"] = clip(safe_float(c.get("confirmation_score"),0)+5)
        elif reg == "NEUTRAL": c["sector_live_alignment"] = False
        else: c["sector_live_alignment"] = False; c["confirmation_score"] = max(0.0, safe_float(c.get("confirmation_score"),0)-10)
        out.append(c)
    return out


def _v7_audit() -> Dict[str, Any]:
    rows=[]
    if OUTCOME_JSONL.exists():
        try:
            with OUTCOME_JSONL.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if line.strip(): rows.append(json.loads(line))
        except Exception as exc:
            return {"status":"ERROR", "error":str(exc)}
    if not rows: return {"status":"NO_DATA", "trades":0}
    df=pd.DataFrame(rows)
    def mean_col(cols):
        for c in cols:
            if c in df.columns:
                x=pd.to_numeric(df[c],errors="coerce").dropna()
                if not x.empty:return float(x.mean())
        return None
    result={"status":"OK","trades":len(df),"avg_slippage_pct":mean_col(["execution_slippage_pct","slippage_pct"]),"avg_slippage_points":mean_col(["execution_slippage_points","slippage_points"])}
    if "sector" in df.columns:
        result["sector_breakdown"]=df.groupby("sector").size().to_dict()
    if "quality_score" in df.columns and "win" in df.columns:
        try: result["quality_score_win_correlation"]=float(df[["quality_score","win"]].corr().iloc[0,1])
        except Exception: result["quality_score_win_correlation"]=None
    _atomic_write_text(V7_AUDIT_FILE, json.dumps(result,ensure_ascii=False,indent=2,default=str))
    return result


def _v7_build_day_ahead_watchlist() -> Dict[str, Any]:
    """V7 orchestration: original 500 scan -> 30 bet basket -> expensive MTF -> final 5."""
    timestamp = now_ist()
    universe = load_nifty500_universe()
    symbols = universe["Symbol"].astype(str).str.upper().str.strip().drop_duplicates().tolist()
    benchmark = fetch_yahoo_chart(NIFTY_TICKER, days=320, interval="1d")
    histories = fetch_history(symbols, days=320)
    rows=[]
    for _, item in universe.iterrows():
        symbol=str(item["Symbol"]).upper().strip(); df=histories.get(symbol)
        if df is None: continue
        industry=str(item.get("Industry","UNKNOWN"))
        features=build_features(symbol,df,benchmark,industry)
        if features is not None: rows.append(features)
    if not rows: raise RuntimeError("No usable stock data available")
    frame=add_sector_features(pd.DataFrame(rows)); scored=score_candidates(frame)
    basket=_v7_preselect_30(scored)
    enriched=_v7_enrich_basket(basket)
    vix=_v7_vix_context()
    if not enriched.empty:
        enriched["MacroVIXRegime"] = vix.get("regime","UNAVAILABLE")
    top5=_v7_select_final5(enriched)
    kotak_snapshot=capture_kotak_day_ahead_snapshot([str(x).upper() for x in top5["Symbol"].tolist()] if not top5.empty else [])
    candidates=[]
    for rank,(_,row) in enumerate(top5.iterrows(),start=1):
        d=row.to_dict(); sym=str(row["Symbol"]); q=kotak_snapshot.get(sym,{})
        if np.isfinite(safe_float(q.get("ltp"))): d["LTP"]=safe_float(q.get("ltp"))
        d["sector_bucket"]=_v7_sector_bucket(d.get("Industry")); d["risk_profile"]=_v7_risk_profile(d,vix)
        candidates.append({
            "rank":rank,"symbol":sym,"industry":str(d.get("Industry","UNKNOWN")),"sector_bucket":d["sector_bucket"],"direction":str(d.get("Direction")),
            "day_ahead_score":round(safe_float(d.get("DayAheadScore"),0),2),"v7_score":round(safe_float(d.get("V7Score"),0),2),
            "selection_score":round(safe_float(d.get("V7Score"),0),2),"setup_type":str(d.get("SetupType","UNKNOWN")),
            "trend_score":round(safe_float(d.get("TrendScore"),50),2),"momentum_score":round(safe_float(d.get("MomentumScore"),50),2),
            "relative_strength_score":round(safe_float(d.get("RelativeStrengthScore"),50),2),"sector_score":round(safe_float(d.get("SectorScore"),50),2),
            "volume_score":round(safe_float(d.get("VolumeScore"),50),2),"volatility_score":round(safe_float(d.get("VolatilityScore"),50),2),
            "catalyst_score":round(safe_float(d.get("CatalystScoreFinal"),50),2),"anti_false_positive_score":round(safe_float(d.get("AntiFalsePositiveScore"),50),2),
            "ltp":round(safe_float(d.get("LTP"),np.nan),2),"atr_pct":round(safe_float(d.get("ATRpct"),np.nan),3),
            "ret_1d":round(safe_float(d.get("Ret1D"),np.nan),3),"ret_5d":round(safe_float(d.get("Ret5D"),np.nan),3),"ret_20d":round(safe_float(d.get("Ret20D"),np.nan),3),
            "rs_5d":round(safe_float(d.get("RS5D"),np.nan),3),"rs_20d":round(safe_float(d.get("RS20D"),np.nan),3),
            "mtf_score":round(safe_float(d.get("mtf_score"),50),2),"support":safe_float(d.get("support"),np.nan),"resistance":safe_float(d.get("resistance"),np.nan),
            "invalidation":safe_float(d.get("invalidation"),np.nan),"target":safe_float(d.get("target"),np.nan),"rr":round(safe_float(d.get("rr"),np.nan),3),
            "invalidation_atr":round(safe_float(d.get("invalidation_atr"),np.nan),3),"pattern_conflicts":int(d.get("pattern_conflicts",0)),"pattern_supports":int(d.get("pattern_supports",0)),
            "risk_profile":d["risk_profile"],"thesis":"THESIS_PENDING_MORNING_CONFIRMATION","invalidation_rule":"Structural S/R or ATR fallback; hard R:R gate applies."
        })
    basket_records=[]
    for _,row in enriched.iterrows():
        basket_records.append({"symbol":str(row["Symbol"]),"sector_bucket":str(row.get("SectorBucket",_v7_sector_bucket(row.get("Industry")))),"direction":str(row.get("Direction")),"day_ahead_score":round(safe_float(row.get("DayAheadScore"),0),2),"v7_score":round(safe_float(row.get("V7Score"),0),2),"mtf_score":round(safe_float(row.get("mtf_score"),50),2),"rr":round(safe_float(row.get("rr"),np.nan),3),"hard_rr_pass":bool(row.get("hard_rr_pass",False))})
    result={"engine":"NEXT_DAY_ALPHA_ENGINE","version":V7_VERSION,"generated_at":timestamp.isoformat(),"data_as_of":timestamp.strftime("%Y-%m-%d"),"architecture":{"nifty_3min_engine_modified":False,"shared_raw_data_allowed":True,"shared_calculated_features":False,"shared_scores":False,"shared_regime_decisions":False,"shared_decisions":False,"shared_labels":False,"shared_predictions":False,"shared_raw_fields_only":True,"next_day_can_write_to_nifty_engine":False,"next_day_can_read_nifty_calculations":False},"macro_regime":vix,"day_ahead":{"universe_size":len(symbols),"usable_symbols":len(frame),"scored_symbols":len(scored),"bet_basket_size":len(basket_records),"bet_basket_30":basket_records,"top5_count":min(TOP5_COUNT,len(candidates)),"top5":candidates[:TOP5_COUNT]},"morning_confirmation":{"status":"PENDING","final":[]},"probability_note":"Quality scores are not win probabilities. VIX is not a directional predictor. Historical calibration is required before any probability claim.","quality_controls":{"hard_rr_gate":V7_MIN_RR,"min_invalidation_atr":V7_MIN_INVALIDATION_DISTANCE_ATR,"max_invalidation_atr":V7_MAX_INVALIDATION_DISTANCE_ATR,"mtf_timeframes":["W","D","4H","1H","15M"],"sector_basket_max_per_sector":V7_MAX_STOCKS_PER_SECTOR,"no_trade_allowed":True}}
    _atomic_write_text(CACHE_JSON,json.dumps(result,ensure_ascii=False,indent=2,default=str))
    return result


# Override the original orchestration without removing its implementation.
build_day_ahead_watchlist = _v7_build_day_ahead_watchlist


def _v7_run_morning_confirmation() -> Dict[str, Any]:
    latest=load_latest()
    if not latest: return {"status":"NO_DAY_AHEAD_DATA","final":[]}
    candidates=latest.get("day_ahead",{}).get("top5",[])
    if not candidates: return {"status":"NO_CANDIDATES","final":[],"confirmations":[]}
    nifty_gap=market_gap(NIFTY_TICKER)
    vix=latest.get("macro_regime",{})
    # Morning sector regime is calculated from the 30-stock raw basket first.
    basket=candidates
    regimes=_v7_sector_regimes(basket)
    confirmations=[]
    for c in candidates:
        sec=str(c.get("sector_bucket",_v7_sector_bucket(c.get("industry"))))
        sector_ret=safe_float(regimes.get(sec,{}).get("return_5m_pct"),np.nan)
        conf=confirm_candidate(c,nifty_gap,sector_ret)
        item=asdict(conf); item["sector_bucket"]=sec; item["sector_live_regime"]=regimes.get(sec,{}).get("regime","UNKNOWN"); item["vix_regime"]=vix.get("regime","UNKNOWN")
        ok, circuit_reason=_v7_circuit_gate(str(c["symbol"]),pd.DataFrame()); item["circuit_gate"]=circuit_reason
        if not ok: item["status"]="REJECTED"; item["reason"]=circuit_reason; item["confirmation_score"]=0.0
        confirmations.append(item)
    confirmations=_v7_confirm_sector_filter(confirmations,regimes)
    bonus=safe_float(vix.get("confirmation_bonus"),0)
    required=min(99.0,MORNING_CONFIRMATION_MIN_SCORE+bonus)
    confirmed=[]
    for item in confirmations:
        score=safe_float(item.get("confirmation_score"),0)
        sector_ok=item.get("sector_live_alignment") is True
        # In neutral sectors the original confirmation may still stand; opposite live sectors are hard rejection.
        if item.get("sector_live_regime") in ("BULLISH","BEARISH") and not sector_ok:
            item["status"]="REJECTED"; item["reason"]="Live sector regime contradicts thesis"
        elif score >= required and item.get("status") not in ("REJECTED","DATA_NOT_READY") and (item.get("acceptance") or item.get("breakout")):
            item["status"]="CONFIRMED"; item["reason"]="Thesis + morning price acceptance + sector + VWAP aligned"; confirmed.append(item)
        elif score >= MORNING_WATCH_SCORE and item.get("status") not in ("REJECTED","DATA_NOT_READY"):
            item["status"]="WATCH"
        else:
            item["status"]="REJECTED"
    confirmed.sort(key=lambda x:(safe_float(x.get("confirmation_score"),0),safe_float(x.get("previous_day_score"),0)),reverse=True)
    # FINAL-2 correlation gate: avoid two same-sector names unless market breadth is exceptionally strong.
    final=[]; sector_used={}
    for item in confirmed:
        sec=item.get("sector_bucket","UNKNOWN")
        if sector_used.get(sec,0)>=1: continue
        final.append(item); sector_used[sec]=sector_used.get(sec,0)+1
        if len(final)>=2: break
    if len(final)<2:
        for item in confirmed:
            if item in final: continue
            final.append(item)
            if len(final)>=2: break
    status="FINAL_2" if len(final)>=2 else ("FINAL_1" if len(final)==1 else "NO_TRADE")
    result={"status":status,"generated_at":now_ist().isoformat(),"required_confirmation_score":required,"vix_regime":vix,"sector_regimes":regimes,"final":final,"confirmations":confirmations,"no_trade_reason":"No candidate satisfied thesis, sector, circuit, R:R and morning confirmation gates." if not final else ""}
    latest["morning_confirmation"]=result
    _atomic_write_text(CACHE_JSON,json.dumps(latest,ensure_ascii=True,indent=2,default=str))
    return result

run_morning_confirmation = _v7_run_morning_confirmation


# ============================================================================
# PUBLIC ENGINE
# ============================================================================

class NextDayAlphaEngine:

    def __init__(
        self,
        refresh_seconds: int = LIVE_REFRESH_SECONDS,
    ):

        self.refresh_seconds = (
            refresh_seconds
        )

        self._thread = None
        self._stop = threading.Event()

    def latest(
        self,
    ) -> Dict[str, Any]:

        return load_latest()

    def today_snapshot(self) -> Dict[str, Any]:
        """Return only today's frozen day-ahead snapshot; never a stale prior-day cache."""
        return _load_frozen_day_ahead_snapshot(now_ist().strftime("%Y-%m-%d"))

    def run_if_due(
        self,
    ) -> Optional[Dict[str, Any]]:

        current = now_ist()

        # Day-ahead scan after market close.
        if (
            current.hour > DAY_AHEAD_RUN_HOUR
            or (
                current.hour
                == DAY_AHEAD_RUN_HOUR
                and current.minute
                >= DAY_AHEAD_RUN_MINUTE
            )
        ):

            latest = load_latest()

            current_date = (
                current.strftime(
                    "%Y-%m-%d"
                )
            )

            generated = str(
                latest.get(
                    "generated_at",
                    "",
                )
            )

            if not generated.startswith(
                current_date
            ):

                return (
                    build_day_ahead_watchlist()
                )

        return None

    def run_day_ahead(
        self,
    ) -> Dict[str, Any]:

        return (
            build_day_ahead_watchlist()
        )

    def run_morning(
        self,
    ) -> Dict[str, Any]:

        return (
            run_morning_confirmation()
        )

    def live_top5(
        self,
    ) -> List[Dict[str, Any]]:

        latest = load_latest()

        return (
            latest
            .get(
                "day_ahead",
                {},
            )
            .get(
                "top5",
                [],
            )
        )

    def live_basket30(self) -> List[Dict[str, Any]]:
        latest = load_latest()
        return latest.get("day_ahead", {}).get("bet_basket_30", [])

    def start_if_due_background(
        self,
    ) -> None:

        if (
            self._thread
            and self._thread.is_alive()
        ):
            return

        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="next-day-alpha-engine",
        )

        self._thread.start()

    def stop(
        self,
    ) -> None:

        self._stop.set()

    def _loop(
        self,
    ) -> None:

        while not self._stop.is_set():

            try:

                self.run_if_due()

                current = now_ist()

                # Capture raw opening-window quotes for TOP 5 using Kotak.
                # Only raw fields are written to the shared cache.
                if current.hour == 9 and 15 <= current.minute < 20:
                    try:
                        basket_now = self.live_basket30()
                        symbols_now = [str(x.get("symbol", "")) for x in basket_now if x.get("symbol")]
                        adapter = get_kotak_adapter()
                        if adapter is not None and adapter.connected:
                            for symbol in symbols_now:
                                row = adapter.quote(symbol)
                                if row:
                                    existing = read_shared_raw_intraday(symbol)
                                    rows = []
                                    if existing is not None and not existing.empty:
                                        for _, rr in existing.iterrows():
                                            rows.append({"timestamp": str(rr.get("DateTime")), "ltp": rr.get("Close"), "open": rr.get("Open"), "high": rr.get("High"), "low": rr.get("Low"), "volume": rr.get("Volume")})
                                    rows.append(row)
                                    write_shared_raw(symbol, rows[-300:])
                    except Exception as exc:
                        LOGGER.exception("[NEXT-DAY OPEN CAPTURE] failed")

                # Automatic morning confirmation after 09:20.
                if (
                    current.hour == 9
                    and current.minute >= 20
                    and current.minute <= 25
                ):

                    latest = load_latest()

                    morning = latest.get(
                        "morning_confirmation",
                        {},
                    )

                    if morning.get(
                        "status"
                    ) in (
                        "PENDING",
                        None,
                        "",
                    ):

                        run_morning_confirmation()

            except Exception as exc:

                LOGGER.exception("[NEXT-DAY ERROR] background cycle failed")

            self._stop.wait(
                self.refresh_seconds
            )


# ============================================================================
# STORAGE
# ============================================================================

def load_latest() -> Dict[str, Any]:

    if not CACHE_JSON.exists():
        return {}

    try:

        with CACHE_JSON.open(
            "r",
            encoding="utf-8",
        ) as handle:

            return json.load(
                handle
            )

    except Exception:

        return {}


# ============================================================================
# TERMINAL UI
# ============================================================================

def print_day_ahead(
    result: Dict[str, Any],
) -> None:

    print()
    print(
        "=" * 72
    )

    print(
        "NEXT-DAY STOCK ALPHA ENGINE"
    )

    print(
        "DAY-AHEAD TOP 15"
    )

    print(
        "=" * 72
    )

    top5 = (
        result
        .get(
            "day_ahead",
            {}
        )
        .get(
            "top5",
            [],
        )
    )

    if not top5:

        print(
            "NO QUALIFIED CANDIDATE"
        )

        return

    for item in top5:

        print(
            f"{item['rank']}. "
            f"{item['symbol']:<12} "
            f"{item['direction']:<5} "
            f"Score={item['day_ahead_score']:>6.2f} "
            f"{item['setup_type']}"
        )

    print(
        "=" * 72
    )


def print_morning(
    result: Dict[str, Any],
) -> None:

    print()
    print(
        "=" * 72
    )

    print(
        "09:15-09:20 MORNING CONFIRMATION"
    )

    print(
        "=" * 72
    )

    print(
        "STATUS:",
        result.get(
            "status"
        ),
    )

    print()

    final = result.get(
        "final",
        [],
    )

    if final:

        print(
            "FINAL TRADE CANDIDATES:"
        )

        for rank, item in enumerate(
            final,
            start=1,
        ):

            print(
                f"{rank}. "
                f"{item['symbol']:<12} "
                f"{item['direction']:<5} "
                f"Confirmation="
                f"{item['confirmation_score']:.2f}"
            )

    else:

        print(
            "NO TRADE"
        )

    print(
        "=" * 72
    )


# ============================================================================
# OPTIONAL STREAMLIT DASHBOARD
# ============================================================================

def run_streamlit_dashboard() -> None:
    try:
        import streamlit as st
    except ImportError:
        raise RuntimeError("Streamlit is not installed.")

    st.set_page_config(page_title="Next-Day Stock Alpha Engine", layout="wide")
    st.title("NEXT-DAY STOCK ALPHA ENGINE")
    st.caption("Standalone - Raw-data sharing only - Kotak Neo live - Trusted catalyst layer")

    result = load_latest()
    day = result.get("day_ahead", {})
    morning = result.get("morning_confirmation", {})

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TOP 5", len(day.get("top5", [])))
    c2.metric("Morning Status", morning.get("status", "PENDING"))
    c3.metric("FINAL", len(morning.get("final", [])))
    c4.metric("Engine", result.get("version", "UNKNOWN"))

    st.subheader("DAY-AHEAD TOP 15")
    top5 = day.get("top5", [])
    if top5:
        st.dataframe(pd.DataFrame(top5), use_container_width=True, hide_index=True)
    else:
        st.warning("NO QUALIFIED CANDIDATE")

    st.subheader("09:15-09:20 CONFIRMATION")
    confirmations = morning.get("confirmations", [])
    if confirmations:
        st.dataframe(pd.DataFrame(confirmations), use_container_width=True, hide_index=True)
    final = morning.get("final", [])
    if final:
        st.success("FINAL TRADE CANDIDATES")
        st.dataframe(pd.DataFrame(final), use_container_width=True, hide_index=True)
    else:
        st.info("NO TRADE - engine never forces two trades.")

    st.caption("A score is a quality score, not a guaranteed win probability. Historical calibration is required before any probability claim.")


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:

    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Standalone NIFTY Next-Day "
            "Stock Alpha Engine"
        )
    )

    parser.add_argument(
        "--day-ahead",
        action="store_true",
        help=(
            "Run the complete day-ahead "
            "stock scan now"
        ),
    )

    parser.add_argument(
        "--morning",
        action="store_true",
        help=(
            "Run the 09:15-09:20 "
            "confirmation now"
        ),
    )

    parser.add_argument(
        "--show",
        action="store_true",
        help=(
            "Show latest saved result"
        ),
    )

    parser.add_argument(
        "--background",
        action="store_true",
        help=(
            "Start background engine"
        ),
    )

    parser.add_argument(
        "--streamlit",
        action="store_true",
        help="Launch the optional Streamlit dashboard",
    )

    parser.add_argument(
        "--audit",
        action="store_true",
        help="Audit outcomes.jsonl for slippage, score correlation and sector results",
    )

    args = parser.parse_args()

    # Fail fast: validate directories, environment/configuration, and
    # critical runtime settings before any market/session work starts.
    # This is intentionally performed at application boot so configuration
    # errors cannot surface for the first time during market hours.
    validate_config()

    engine = (
        NextDayAlphaEngine()
    )

    if args.streamlit:
        run_streamlit_dashboard()
        return

    if args.audit:
        print(json.dumps(_v7_audit(), indent=2, ensure_ascii=False, default=str))
        return

    if args.day_ahead:

        result = (
            engine.run_day_ahead()
        )

        print_day_ahead(
            result
        )

        return

    if args.morning:

        result = (
            engine.run_morning()
        )

        print_morning(
            result
        )

        return

    if args.show:

        result = (
            engine.latest()
        )

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
            )
        )

        return

    if args.background:

        print(
            "NEXT-DAY ALPHA ENGINE "
            "BACKGROUND MODE"
        )

        print(
            "Press Ctrl+C to stop."
        )

        engine.start_if_due_background()

        try:

            while True:
                time.sleep(1)

        except KeyboardInterrupt:

            engine.stop()

        return

    # Default: run appropriate operation.
    result = engine.run_if_due()

    if result:

        print_day_ahead(
            result
        )

    else:

        latest = engine.latest()

        if latest:

            print_day_ahead(
                latest
            )

        else:

            print(
                "No day-ahead result yet."
            )

            print(
                "Run with --day-ahead "
                "after market close."
            )




# ============================================================================
# SUPABASE RAW DATA BUS INTEGRATION
# ============================================================================
# ARCHITECTURAL RULE:
#   Kotak Neo + yfinance -> raw_data_producer -> Supabase RAW BUS
#   -> this engine -> existing V7 calculations
#
# This section is DATA INGESTION ONLY.
# It does not calculate indicators, scores, regimes, signals, labels,
# probabilities, targets, risk/reward, or strategy decisions.
#
# Existing V7 source functions are intentionally left in the file for
# provenance/rollback. The active runtime bindings are replaced at the very
# end of this section.

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip()
SUPABASE_RAW_TABLE = os.getenv("SUPABASE_RAW_TABLE", "raw_market_data").strip()

SUPABASE_DATASETS = {
    "daily": os.getenv(
        "NEXT_DAY_SUPABASE_DAILY_DATASET",
        "next_day_stock_daily",
    ),
    "mtf_daily": os.getenv(
        "NEXT_DAY_SUPABASE_MTF_DAILY_DATASET",
        "next_day_mtf_daily",
    ),
    "mtf_hourly": os.getenv(
        "NEXT_DAY_SUPABASE_MTF_HOURLY_DATASET",
        "next_day_mtf_hourly",
    ),
    "mtf_15m": os.getenv(
        "NEXT_DAY_SUPABASE_MTF_15M_DATASET",
        "next_day_mtf_15m",
    ),
    "nifty_daily": os.getenv(
        "NEXT_DAY_SUPABASE_NIFTY_DATASET",
        "nifty_spot_daily",
    ),
    "vix_daily": os.getenv(
        "NEXT_DAY_SUPABASE_VIX_DATASET",
        "india_vix_daily",
    ),
    "live": os.getenv(
        "NEXT_DAY_SUPABASE_LIVE_DATASET",
        "kotak_live",
    ),
}

def _supabase_raw_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Accept": "application/json",
    }

def _supabase_raw_read(
    dataset: str,
    symbol: Optional[str] = None,
    start_ts: Optional[datetime] = None,
    end_ts: Optional[datetime] = None,
    limit: int = 10000,
) -> List[Dict[str, Any]]:
    """
    Read raw observations from Supabase.

    No transformation beyond filtering/order selection occurs here.
    """
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        LOGGER.warning("Supabase RAW BUS is not configured.")
        return []

    try:
        import requests
    except Exception as exc:
        LOGGER.error("requests unavailable for Supabase RAW BUS: %s", exc)
        return []

    params: List[Tuple[str, str]] = [
        ("select", "*"),
        ("dataset", f"eq.{dataset}"),
        ("order", "event_timestamp.asc"),
        ("limit", str(int(limit))),
    ]

    if symbol:
        params.append(("symbol", f"eq.{symbol}"))

    if start_ts is not None:
        params.append(
            (
                "event_timestamp",
                f"gte.{start_ts.astimezone(timezone.utc).isoformat()}",
            )
        )

    if end_ts is not None:
        params.append(
            (
                "event_timestamp",
                f"lte.{end_ts.astimezone(timezone.utc).isoformat()}",
            )
        )

    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_RAW_TABLE}",
            headers=_supabase_raw_headers(),
            params=params,
            timeout=20,
        )
        if response.status_code >= 400:
            LOGGER.warning(
                "Supabase RAW BUS read failed dataset=%s symbol=%s "
                "status=%s body=%s",
                dataset,
                symbol,
                response.status_code,
                response.text[:300],
            )
            return []

        payload = response.json()
        return payload if isinstance(payload, list) else []
    except Exception as exc:
        LOGGER.warning(
            "Supabase RAW BUS read exception dataset=%s symbol=%s: %s",
            dataset,
            symbol,
            exc,
        )
        return []

def _raw_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("raw")
    return raw if isinstance(raw, dict) else row

def _raw_timestamp(row: Dict[str, Any]) -> Optional[pd.Timestamp]:
    value = row.get("event_timestamp")
    if value is None:
        value = row.get("observation_timestamp")
    if value is None:
        return None
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.tz_convert(IST)

def _raw_ohlcv_frame(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert stored RAW OHLCV fields into the exact frame shape expected by
    the existing V7 functions. This is schema adaptation, not indicator math.
    """
    records: List[Dict[str, Any]] = []

    for row in rows:
        ts = _raw_timestamp(row)
        if ts is None:
            continue

        raw = _raw_payload(row)

        def pick(*keys):
            for key in keys:
                if key in raw:
                    value = safe_float(raw.get(key), np.nan)
                    if np.isfinite(value):
                        return value
            return np.nan

        records.append(
            {
                "Datetime": ts,
                "Open": pick("open", "o", "pOpen", "openPrice"),
                "High": pick("high", "h", "pHigh", "highPrice"),
                "Low": pick("low", "l", "pLow", "lowPrice"),
                "Close": pick(
                    "close",
                    "c",
                    "ltp",
                    "lp",
                    "last_price",
                    "lastPrice",
                ),
                "Volume": pick(
                    "volume",
                    "v",
                    "vol",
                    "tradedVolume",
                    "vtt",
                ),
            }
        )

    if not records:
        return pd.DataFrame(
            columns=["Datetime", "Open", "High", "Low", "Close", "Volume"]
        )

    frame = pd.DataFrame(records)
    frame["Datetime"] = pd.to_datetime(
        frame["Datetime"],
        errors="coerce",
        utc=True,
    )
    frame = frame.dropna(subset=["Datetime"])
    frame = frame.sort_values("Datetime")
    frame = frame.drop_duplicates("Datetime", keep="last")
    frame["Datetime"] = frame["Datetime"].dt.tz_convert(IST)

    return frame.reset_index(drop=True)

def _supabase_history(
    symbol: str,
    days: int,
    interval: str,
    dataset: str,
) -> pd.DataFrame:
    end_ts = datetime.now(IST)
    start_ts = end_ts - timedelta(days=int(days))

    rows = _supabase_raw_read(
        dataset=dataset,
        symbol=symbol,
        start_ts=start_ts,
        end_ts=end_ts,
        limit=50000,
    )

    # No resampling here. The engine receives the stored raw timeframe.
    return _raw_ohlcv_frame(rows)

def _supabase_nifty_daily(days: int = 320) -> pd.DataFrame:
    return _supabase_history(
        "NIFTY_SPOT",
        days,
        "1d",
        SUPABASE_DATASETS["nifty_daily"],
    )

def _supabase_vix_daily(days: int = 320) -> pd.DataFrame:
    return _supabase_history(
        "INDIAVIX",
        days,
        "1d",
        SUPABASE_DATASETS["vix_daily"],
    )

def _supabase_live_frame(symbol: str) -> pd.DataFrame:
    clean = _canonical_equity_symbol(symbol)
    since = datetime.now(IST) - timedelta(minutes=30)
    rows = _raw_bus_read(
        source="kotak_live",
        symbol=clean,
        since=since,
        limit=5000,
    )
    records = []
    for row in rows:
        raw = _bus_raw(row)
        ts = _bus_timestamp(row)
        ltp = _bus_number(raw, "ltp", "lp", "last_price", "lastPrice", "c")
        if ts is None or not np.isfinite(ltp):
            continue
        records.append({
            "Datetime": ts,
            "Open": ltp,
            "High": ltp,
            "Low": ltp,
            "Close": ltp,
            "Volume": _bus_number(raw, "volume", "v", "tradedVolume", default=0.0),
        })
    if not records:
        return pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
    return pd.DataFrame(records).sort_values("Datetime").drop_duplicates("Datetime", keep="last").reset_index(drop=True)


def _supabase_fetch_yahoo_compat(
    ticker: str,
    days: int = 30,
    interval: str = "1d",
) -> pd.DataFrame:
    """
    Compatibility boundary for all existing V7 historical calls.

    The existing callers still ask for 320d/180d/55d exactly as before.
    The producer/Supabase layer determines actual source coverage. This
    adapter never fabricates missing observations and never converts one
    timeframe into another.
    """
    if ticker == "^NSEI":
        return _supabase_nifty_daily(days)

    if ticker == "^INDIAVIX":
        return _supabase_vix_daily(days)

    symbol = str(ticker)
    if symbol.endswith(".NS"):
        symbol = symbol[:-3]

    if interval == "1d":
        dataset = SUPABASE_DATASETS["daily"]
    elif interval == "1h":
        dataset = SUPABASE_DATASETS["mtf_hourly"]
    elif interval == "15m":
        dataset = SUPABASE_DATASETS["mtf_15m"]
    else:
        # No unsupported timeframe is silently substituted.
        LOGGER.warning(
            "Supabase RAW BUS has no mapped dataset for interval=%s "
            "ticker=%s",
            interval,
            ticker,
        )
        return pd.DataFrame()

    return _supabase_history(
        symbol,
        days,
        interval,
        dataset,
    )

def _supabase_fetch_intraday_compat(
    symbol: str,
) -> pd.DataFrame:
    """
    Existing V7 morning-confirmation boundary.

    The existing function's expected OHLCV frame is preserved. No new
    confirmation calculation is introduced here.
    """
    clean = str(symbol).replace(".NS", "")
    return _supabase_live_frame(clean)

def _supabase_market_gap_compat(
    ticker: str = NIFTY_TICKER,
) -> float:
    frame = _supabase_nifty_daily(days=5)

    if frame is None or len(frame) < 2:
        return np.nan

    prev_close = safe_float(
        frame["Close"].iloc[-2],
        np.nan,
    )
    latest_open = safe_float(
        frame["Open"].iloc[-1],
        np.nan,
    )

    if (
        not np.isfinite(prev_close)
        or prev_close == 0
        or not np.isfinite(latest_open)
    ):
        return np.nan

    # This is the existing market-gap calculation, retained only because
    # the original V7 function already performs it. The data source changes;
    # the calculation itself is not changed.
    return float((latest_open / prev_close - 1.0) * 100.0)

def supabase_raw_contract_status() -> Dict[str, Any]:
    """
    Data-contract visibility only. It does not inspect or calculate a signal.
    """
    return {
        "enabled": bool(SUPABASE_URL and SUPABASE_ANON_KEY),
        "table": SUPABASE_RAW_TABLE,
        "datasets": dict(SUPABASE_DATASETS),
        "requested_windows": {
            "daily_days": 320,
            "hourly_days": 180,
            "15m_days": 55,
            "vix_days": 320,
        },
        "hourly_policy": (
            "180d x 1h remains the requested contract. Consume only actual "
            "raw observations available in Supabase; never fabricate, "
            "duplicate, relabel, or silently resample missing history."
        ),
        "architecture": (
            "Kotak Neo + yfinance -> Raw Data Producer -> Supabase -> "
            "Next-Day Alpha Engine"
        ),
    }

# ============================================================================
# FINAL SUPABASE RAW BUS BINDINGS - LOCKED
# ============================================================================
# One-way architecture:
#   Kotak LIVE producer -> Supabase raw_observations
#   yFinance HIST producer -> Supabase raw_observations
#   this engine -> Supabase raw_observations
#
# This engine NEVER logs into Kotak. The dedicated Kotak producer is the
# single authenticated Kotak gateway.

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", os.getenv("SUPABASE_ANON_KEY", "")).strip()
SUPABASE_RAW_TABLE = os.getenv("SUPABASE_RAW_TABLE", "raw_observations").strip()


def _raw_bus_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }


def _canonical_equity_symbol(symbol: str) -> str:
    """Canonical internal equity symbol used by the Next-Day engine."""
    value = str(symbol or "").upper().strip()
    for suffix in (".NS", "-EQ"):
        if value.endswith(suffix):
            value = value[: -len(suffix)]
    return value.strip()


def _supabase_symbol_variants(symbol: str) -> List[str]:
    """Return only producer-compatible symbol variants; never invent tokens."""
    canonical = _canonical_equity_symbol(symbol)
    if not canonical:
        return []
    # Yahoo producer stores canonical symbols; Kotak equity producer commonly
    # stores the exchange suffix. Query both in one server-side IN predicate.
    return list(dict.fromkeys([canonical, f"{canonical}-EQ"]))


def _raw_bus_read(
    *,
    source: Optional[str] = None,
    symbol: Optional[str] = None,
    dataset: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    event_since: Optional[datetime] = None,
    event_until: Optional[datetime] = None,
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    """Read only the RAW rows actually required by the consumer.

    Important contract:
      * historical market time comes from raw.event_timestamp;
      * live freshness may use observation_timestamp;
      * candidate symbols are filtered server-side;
      * Kotak ``ABC-EQ`` and Yahoo ``ABC`` are treated as the same internal
        equity symbol, while the returned row's real instrument_token remains
        authoritative.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []

    params: List[Tuple[str, str]] = [
        ("select", "*"),
        ("order", "observation_timestamp.asc"),
        ("limit", str(max(1, int(limit)))),
    ]
    if source:
        params.append(("source", f"eq.{source}"))
    if symbol:
        variants = _supabase_symbol_variants(symbol)
        if len(variants) == 1:
            params.append(("symbol", f"eq.{variants[0]}"))
        elif variants:
            params.append(("symbol", "in.(" + ",".join(variants) + ")"))
    if dataset:
        params.append(("raw->>dataset", f"eq.{dataset}"))

    # Historical producer writes the true candle timestamp inside raw.
    if event_since is not None:
        params.append(("raw->>event_timestamp", f"gte.{event_since.astimezone(timezone.utc).isoformat()}"))
    if event_until is not None:
        params.append(("raw->>event_timestamp", f"lte.{event_until.astimezone(timezone.utc).isoformat()}"))

    # Live freshness window can safely use the table observation timestamp.
    if since is not None:
        params.append(("observation_timestamp", f"gte.{since.astimezone(timezone.utc).isoformat()}"))
    if until is not None:
        params.append(("observation_timestamp", f"lte.{until.astimezone(timezone.utc).isoformat()}"))

    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_RAW_TABLE}",
            headers=_raw_bus_headers(),
            params=params,
            timeout=20,
        )
        if response.status_code >= 400:
            LOGGER.warning("RAW BUS read failed status=%s body=%s", response.status_code, response.text[:250])
            return []
        payload = response.json()
        return payload if isinstance(payload, list) else []
    except Exception as exc:
        LOGGER.warning("RAW BUS read exception: %s", exc)
        return []


def _bus_raw(row: Dict[str, Any]) -> Dict[str, Any]:
    value = row.get("raw")
    return value if isinstance(value, dict) else {}


def _bus_timestamp(row: Dict[str, Any]) -> Optional[pd.Timestamp]:
    raw = _bus_raw(row)
    value = raw.get("event_timestamp") or raw.get("timestamp") or raw.get("received_at") or row.get("observation_timestamp")
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.tz_convert(IST)


def _bus_number(raw: Dict[str, Any], *keys: str, default=np.nan) -> float:
    for key in keys:
        try:
            value = float(raw.get(key))
            if np.isfinite(value):
                return value
        except Exception:
            pass
    return default


def _historical_frame(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    records = []
    for row in rows:
        raw = _bus_raw(row)
        ts = _bus_timestamp(row)
        if ts is None:
            continue
        records.append({
            "Datetime": ts,
            "Open": _bus_number(raw, "open", "o", "pOpen", "openPrice"),
            "High": _bus_number(raw, "high", "h", "pHigh", "highPrice"),
            "Low": _bus_number(raw, "low", "l", "pLow", "lowPrice"),
            "Close": _bus_number(raw, "close", "c", "ltp", "lp", "last_price", "lastPrice"),
            "Volume": _bus_number(raw, "volume", "v", "vol", "tradedVolume", "vtt", default=0.0),
        })
    if not records:
        return pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
    frame = pd.DataFrame(records).sort_values("Datetime").drop_duplicates("Datetime", keep="last")
    return frame.reset_index(drop=True)


def _supabase_history(symbol: str, days: int, interval: str, dataset: str) -> pd.DataFrame:
    since = datetime.now(IST) - timedelta(days=int(days))
    rows = _raw_bus_read(source="yahoo_historical", symbol=symbol, dataset=dataset, since=since)
    return _historical_frame(rows)


def _supabase_fetch_yahoo_compat(ticker: str, days: int = 320, interval: str = "1d") -> pd.DataFrame:
    ticker = str(ticker)
    if ticker == "^NSEI":
        return _supabase_history("NIFTY_SPOT", days, interval, "nifty_spot_daily")
    if ticker == "^INDIAVIX":
        return _supabase_history("INDIAVIX", days, interval, "india_vix_daily")
    symbol = ticker[:-3] if ticker.upper().endswith(".NS") else ticker
    dataset = {
        "1d": "next_day_stock_daily",
        "1h": "next_day_mtf_hourly",
        "15m": "next_day_mtf_15m",
    }.get(interval)
    if not dataset:
        return pd.DataFrame()
    return _supabase_history(symbol, days, interval, dataset)


def _supabase_live_frame(symbol: str) -> pd.DataFrame:
    clean = str(symbol).replace(".NS", "").upper().strip()
    since = datetime.now(IST) - timedelta(minutes=30)
    rows = _raw_bus_read(source="kotak_live", symbol=clean, since=since, limit=10000)
    records = []
    for row in rows:
        raw = _bus_raw(row)
        ts = _bus_timestamp(row)
        ltp = _bus_number(raw, "ltp", "lp", "last_price", "lastPrice", "c")
        if ts is None or not np.isfinite(ltp):
            continue
        # Producer snapshots are quote observations, not candles. For the
        # opening-window consumer, LTP is the raw observation price.
        records.append({
            "Datetime": ts,
            "Open": ltp,
            "High": ltp,
            "Low": ltp,
            "Close": ltp,
            "Volume": _bus_number(raw, "volume", "v", "tradedVolume", default=0.0),
        })
    if not records:
        return pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
    return pd.DataFrame(records).sort_values("Datetime").drop_duplicates("Datetime", keep="last").reset_index(drop=True)


def _supabase_fetch_intraday_compat(symbol: str) -> pd.DataFrame:
    return _supabase_live_frame(symbol)


def _supabase_market_gap_compat(ticker: str = NIFTY_TICKER) -> float:
    frame = _supabase_history("NIFTY_SPOT", 5, "1d", "nifty_spot_daily")
    if frame is None or len(frame) < 2:
        return np.nan
    prev_close = safe_float(frame["Close"].iloc[-2], np.nan)
    latest_open = safe_float(frame["Open"].iloc[-1], np.nan)
    if not np.isfinite(prev_close) or prev_close == 0 or not np.isfinite(latest_open):
        return np.nan
    return float((latest_open / prev_close - 1.0) * 100.0)


def _supabase_kotak_snapshot(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for symbol in symbols:
        frame = _supabase_live_frame(symbol)
        if frame.empty:
            continue
        row = frame.iloc[-1]
        out[str(symbol).upper()] = {
            "symbol": str(symbol).upper(),
            "ltp": safe_float(row.get("Close"), np.nan),
            "timestamp": row.get("Datetime").isoformat() if hasattr(row.get("Datetime"), "isoformat") else None,
            "raw_source": "KOTAK_NEO_VIA_SUPABASE_RAW_BUS",
        }
    return out


def get_kotak_adapter() -> Optional[Any]:
    # HARD LOCK: Next-Day Alpha never authenticates with Kotak.
    return None


def capture_kotak_day_ahead_snapshot(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    return _supabase_kotak_snapshot(symbols)


def capture_kotak_opening_window(symbols: List[str]) -> None:
    # The dedicated Kotak producer already publishes the live raw stream.
    return None


def supabase_raw_contract_status() -> Dict[str, Any]:
    return {
        "enabled": bool(SUPABASE_URL and SUPABASE_KEY),
        "table": SUPABASE_RAW_TABLE,
        "live_source": "kotak_live",
        "historical_source": "yahoo_historical",
        "architecture": "Kotak + yFinance producers -> Supabase RAW BUS -> Next-Day Alpha",
        "direct_kotak_login": False,
        "totp_required_here": False,
    }


# Keep all existing calculation/orchestration function names unchanged.
fetch_yahoo_chart = _supabase_fetch_yahoo_compat
fetch_intraday = _supabase_fetch_intraday_compat
market_gap = _supabase_market_gap_compat


# ============================================================================
# FINAL LOCKED DAY-AHEAD PIPELINE
# ============================================================================
# This layer changes ONLY data selection/orchestration/persistence. Existing
# indicator formulas, scoring mathematics, MTF calculations and confirmation
# calculations above are deliberately reused unchanged.

DAY_AHEAD_SNAPSHOT_DIR = ROOT / "snapshots"
DAY_AHEAD_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
DAY_AHEAD_STATUS_JSON = DAY_AHEAD_SNAPSHOT_DIR / "latest_status.json"


def _day_ahead_snapshot_paths(trading_date: str) -> Tuple[Path, Path]:
    return (
        DAY_AHEAD_SNAPSHOT_DIR / f"{trading_date}_top15.json",
        DAY_AHEAD_SNAPSHOT_DIR / f"{trading_date}_top15.csv",
    )


def _freeze_day_ahead_snapshot(result: Dict[str, Any]) -> None:
    trading_date = str(result.get("data_as_of") or now_ist().strftime("%Y-%m-%d"))
    json_path, csv_path = _day_ahead_snapshot_paths(trading_date)

    # Never overwrite an existing daily snapshot. This is what makes the
    # overnight TOP 15 deterministic across dashboard refreshes/restarts.
    if not json_path.exists():
        _atomic_write_text(json_path, json.dumps(result, ensure_ascii=True, indent=2, default=str))

    rows = result.get("day_ahead", {}).get("top15", [])
    if rows and not csv_path.exists():
        pd.DataFrame(rows).to_csv(csv_path, index=False)


def _load_frozen_day_ahead_snapshot(trading_date: Optional[str] = None) -> Dict[str, Any]:
    trading_date = trading_date or now_ist().strftime("%Y-%m-%d")
    json_path, _ = _day_ahead_snapshot_paths(trading_date)
    if not json_path.exists():
        return {}
    try:
        with json_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _volume_shocker_profile(df: pd.DataFrame) -> Optional[Dict[str, float]]:
    """Measure only the last five completed daily volume changes."""
    if df is None or df.empty or "Volume" not in df.columns:
        return None
    x = df.copy()
    x["Volume"] = pd.to_numeric(x["Volume"], errors="coerce")
    x = x.dropna(subset=["Volume"]).tail(VOLUME_SHOCKER_LOOKBACK_DAYS)
    if len(x) < VOLUME_SHOCKER_LOOKBACK_DAYS:
        return None
    volumes = x["Volume"].to_numpy(dtype=float)
    if np.any(~np.isfinite(volumes)) or np.any(volumes <= 0):
        return None
    pct = (pd.Series(volumes).pct_change() * 100.0).iloc[1:].to_numpy(dtype=float)

    run = 0
    for value in pct[::-1]:
        if np.isfinite(value) and value >= VOLUME_SHOCKER_MIN_DAILY_PCT:
            run += 1
        else:
            break

    if run < VOLUME_SHOCKER_MIN_CONSECUTIVE_DAYS:
        return None

    consecutive = pct[-run:]
    avg_pct = float(np.mean(consecutive))
    cumulative_pct = float((volumes[-1] / volumes[-run-1] - 1.0) * 100.0)
    prior_avg = float(np.mean(volumes[:-run])) if len(volumes) > run else np.nan
    latest_ratio = float(volumes[-1] / prior_avg) if np.isfinite(prior_avg) and prior_avg > 0 else np.nan
    shock_score = float(avg_pct * run + max(cumulative_pct, 0.0) * 0.35 + max(latest_ratio - 1.0, 0.0) * 20.0)
    return {
        "volume_shock_consecutive_days": float(run),
        "volume_shock_avg_daily_pct": avg_pct,
        "volume_shock_cumulative_pct": cumulative_pct,
        "volume_shock_latest_vs_prior_ratio": latest_ratio,
        "volume_shock_score": shock_score,
    }


def _volume_shocker_candidates(
    universe: pd.DataFrame,
    histories: Dict[str, pd.DataFrame],
) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    """First gate: last five completed sessions and consecutive volume expansion."""
    candidates = []
    profiles: Dict[str, Dict[str, float]] = {}
    for _, item in universe.iterrows():
        symbol = _canonical_equity_symbol(item.get("Symbol", ""))
        df = histories.get(symbol)
        if not symbol or df is None or len(df) < VOLUME_SHOCKER_LOOKBACK_DAYS:
            continue
        shock = _volume_shocker_profile(df)
        if shock is None:
            continue
        profiles[symbol] = dict(shock)
        candidates.append((symbol, shock["volume_shock_score"]))
    candidates.sort(key=lambda x: (-x[1], x[0]))
    return [symbol for symbol, _ in candidates], profiles


def _major_filter_candidates(
    universe: pd.DataFrame,
    histories: Dict[str, pd.DataFrame],
    shock_profiles: Dict[str, Dict[str, float]],
) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    """Second gate: objective tradability/quality filters after volume shock."""
    candidates = []
    profiles = {}
    lookup = universe.set_index(universe["Symbol"].astype(str).map(_canonical_equity_symbol))
    for symbol, shock in shock_profiles.items():
        df = histories.get(symbol)
        if df is None or len(df) < 5:
            continue
        d = df.copy()
        d["Close"] = pd.to_numeric(d["Close"], errors="coerce")
        d["Volume"] = pd.to_numeric(d["Volume"], errors="coerce")
        d = d.dropna(subset=["Close", "Volume"])
        if d.empty:
            continue
        close = float(d["Close"].iloc[-1])
        avg_volume = float(d["Volume"].tail(min(20, len(d))).mean())
        avg_turnover_cr = float((d["Close"].tail(min(20, len(d))) * d["Volume"].tail(min(20, len(d)))).mean() / 1e7)
        if close < MIN_PRICE or avg_volume < MIN_AVG_VOLUME or avg_turnover_cr < MIN_AVG_TURNOVER_CR:
            continue
        profile = dict(shock)
        profile.update({"avg_volume_20": avg_volume, "avg_turnover_20_cr": avg_turnover_cr, "last_close": close})
        profiles[symbol] = profile
        candidates.append((symbol, shock["volume_shock_score"]))
    candidates.sort(key=lambda x: (-x[1], x[0]))
    return [symbol for symbol, _ in candidates[:MAJOR_FILTER_MAX_CANDIDATES]], profiles


def _supabase_fetch_daily_window(
    symbols: List[str],
    calendar_days: int,
    *,
    minimum_rows: int = 1,
) -> Dict[str, pd.DataFrame]:
    """Fetch only the requested daily window and symbols from Supabase.

    This is deliberately server-side filtered. It never downloads the full
    historical table and then filters it locally.
    """
    if not symbols or not SUPABASE_URL or not SUPABASE_KEY:
        return {}

    end_ts = datetime.now(IST)
    start_ts = end_ts - timedelta(days=int(calendar_days))
    wanted = {_canonical_equity_symbol(s) for s in symbols if _canonical_equity_symbol(s)}
    variants = sorted({v for s in wanted for v in _supabase_symbol_variants(s)})
    if not variants:
        return {}

    rows: List[Dict[str, Any]] = []
    page_size = 5000
    offset = 0
    while True:
        params: List[Tuple[str, str]] = [
            ("select", "symbol,instrument_token,observation_timestamp,raw"),
            ("source", "eq.yahoo_historical"),
            ("raw->>dataset", "eq.next_day_stock_daily"),
            ("raw->>event_timestamp", f"gte.{start_ts.astimezone(timezone.utc).isoformat()}"),
            ("raw->>event_timestamp", f"lte.{end_ts.astimezone(timezone.utc).isoformat()}"),
            ("symbol", "in.(" + ",".join(variants) + ")"),
            ("order", "raw->>event_timestamp.asc"),
            ("limit", str(page_size)),
            ("offset", str(offset)),
        ]
        try:
            response = requests.get(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_RAW_TABLE}",
                headers=_raw_bus_headers(),
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            page = response.json()
        except Exception as exc:
            LOGGER.warning("Supabase daily window read failed offset=%s: %s", offset, exc)
            break
        if not isinstance(page, list) or not page:
            break
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        canonical = _canonical_equity_symbol(row.get("symbol", ""))
        if canonical in wanted:
            grouped.setdefault(canonical, []).append(row)

    result = {k: _historical_frame(v) for k, v in grouped.items()}
    return {k: v for k, v in result.items() if v is not None and len(v) >= minimum_rows}


def _supabase_fetch_history_all(symbols: List[str], days: int = 320) -> Dict[str, pd.DataFrame]:
    """Compatibility wrapper: server-side symbol/date filtering only."""
    return _supabase_fetch_daily_window(symbols, days, minimum_rows=MIN_HISTORY_DAYS)


def _supabase_fetch_history_locked(symbol: str, days: int = 320) -> pd.DataFrame:
    return _supabase_history(str(symbol).replace(".NS", "").upper(), days, "1d", "next_day_stock_daily")


# Historical data boundary: no direct yFinance read by the Next-Day engine.
def fetch_history(symbols: List[str], days: int = 320) -> Dict[str, pd.DataFrame]:
    result = _supabase_fetch_history_all(symbols, days)
    valid = {k: v for k, v in result.items() if v is not None and len(v) >= MIN_HISTORY_DAYS}
    _set_source_health(
        "SUPABASE_RAW",
        status="CONNECTED" if valid else "ERROR",
        last_success_ist=now_ist().isoformat() if valid else None,
        symbols_ok=len(valid),
        error=None if valid else "No qualifying daily historical RAW rows in Supabase",
    )
    return valid


def _build_final_day_ahead_v8() -> Dict[str, Any]:
    timestamp = now_ist()
    universe = load_nifty500_universe()
    symbols = universe["Symbol"].astype(str).str.upper().str.strip().drop_duplicates().tolist()
    benchmark = fetch_yahoo_chart(NIFTY_TICKER, days=320, interval="1d")

    # Stage 1: only the last week of daily RAW is needed for the first gate.
    # This avoids pulling hundreds of days for the entire universe.
    short_histories = _supabase_fetch_daily_window(
        symbols,
        calendar_days=14,
        minimum_rows=VOLUME_SHOCKER_LOOKBACK_DAYS,
    )
    if not short_histories:
        raise RuntimeError("Supabase RAW BUS returned no usable one-week volume history")

    shock_symbols_all, shock_profiles = _volume_shocker_candidates(universe, short_histories)
    if not shock_symbols_all:
        raise RuntimeError("Volume Shocker gate produced no qualifying stocks")

    # Stage 2: fetch a modest recent window only for volume-shocker survivors,
    # apply the major tradability/quality gates, and cap the population at ~50.
    major_histories = _supabase_fetch_daily_window(
        shock_symbols_all,
        calendar_days=60,
        minimum_rows=5,
    )
    shock_profiles_major = {s: shock_profiles[s] for s in major_histories if s in shock_profiles}
    major_symbols, major_profiles = _major_filter_candidates(
        universe,
        major_histories,
        shock_profiles_major,
    )
    if not major_symbols:
        raise RuntimeError("Major stock filters produced no qualifying stocks")

    # Stage 3: only the ~50 survivors receive the full historical depth needed
    # by the existing indicator/MTF mathematics.
    histories = _supabase_fetch_daily_window(
        major_symbols,
        calendar_days=420,
        minimum_rows=MIN_HISTORY_DAYS,
    )
    if not histories:
        raise RuntimeError("Supabase RAW BUS returned no usable full history for major-filter survivors")

    combined_profiles = {s: {**shock_profiles.get(s, {}), **major_profiles.get(s, {})} for s in histories}

    # Full existing indicator/scoring stack is deliberately executed ONLY on
    # the ~50 survivors. The formulas are unchanged above.
    rows = []
    universe_lookup = universe.set_index(universe["Symbol"].astype(str).map(_canonical_equity_symbol))
    for symbol in major_symbols:
        df = histories.get(symbol)
        if df is None:
            continue
        try:
            item = universe_lookup.loc[symbol]
            industry = str(item.get("Industry", "UNKNOWN"))
        except Exception:
            industry = "UNKNOWN"
        features = build_features(symbol, df, benchmark, industry)
        if features is not None:
            features.update(combined_profiles.get(symbol, {}))
            rows.append(features)

    if not rows:
        raise RuntimeError("No usable stocks survived the full indicator evaluation")

    frame = add_sector_features(pd.DataFrame(rows))
    scored = score_candidates(frame)

    # Full MTF layer is retained, but it is now applied to the complete ~50
    # post-shocker population rather than an old 30-stock intermediate basket.
    basket = scored.head(MAJOR_FILTER_MAX_CANDIDATES).copy()
    enriched = _v7_enrich_basket(basket)
    vix = _v7_vix_context()
    if not enriched.empty:
        enriched["MacroVIXRegime"] = vix.get("regime", "UNAVAILABLE")

    # Stage 4: deterministic TOP 15 selection. No live Kotak quote is needed
    # to determine the overnight basket; live Kotak RAW is reserved for morning.
    top15 = _v7_select_final5(enriched).head(DAY_AHEAD_TOP_N)

    candidates = []
    for rank, (_, row) in enumerate(top15.iterrows(), start=1):
        d = row.to_dict()
        sym = str(row["Symbol"]).upper()
        d["sector_bucket"] = _v7_sector_bucket(d.get("Industry"))
        d["risk_profile"] = _v7_risk_profile(d, vix)
        catalyst_text = str(d.get("CatalystText", "")).strip()
        direction = str(d.get("Direction", "NEUTRAL"))
        thesis = (
            f"{direction} thesis: existing quantitative setup is supported by "
            f"volume-shocker persistence, full indicator score and MTF structure. "
            f"Catalyst context: {catalyst_text if catalyst_text else 'No verified NSE catalyst found.'}"
        )
        candidates.append({
            "rank": rank, "symbol": sym, "industry": str(d.get("Industry", "UNKNOWN")),
            "sector_bucket": d["sector_bucket"], "direction": direction,
            "day_ahead_score": round(safe_float(d.get("DayAheadScore"), 0), 2),
            "v7_score": round(safe_float(d.get("V7Score"), 0), 2),
            "selection_score": round(safe_float(d.get("V7Score"), 0), 2),
            "setup_type": str(d.get("SetupType", "UNKNOWN")),
            "trend_score": round(safe_float(d.get("TrendScore"), 50), 2),
            "momentum_score": round(safe_float(d.get("MomentumScore"), 50), 2),
            "relative_strength_score": round(safe_float(d.get("RelativeStrengthScore"), 50), 2),
            "sector_score": round(safe_float(d.get("SectorScore"), 50), 2),
            "volume_score": round(safe_float(d.get("VolumeScore"), 50), 2),
            "volatility_score": round(safe_float(d.get("VolatilityScore"), 50), 2),
            "catalyst_score": round(safe_float(d.get("CatalystScoreFinal"), 50), 2),
            "anti_false_positive_score": round(safe_float(d.get("AntiFalsePositiveScore"), 50), 2),
            "volume_shock_consecutive_days": int(safe_float(d.get("volume_shock_consecutive_days"), 0)),
            "volume_shock_avg_daily_pct": round(safe_float(d.get("volume_shock_avg_daily_pct"), np.nan), 3),
            "volume_shock_cumulative_pct": round(safe_float(d.get("volume_shock_cumulative_pct"), np.nan), 3),
            "volume_shock_latest_vs_prior_ratio": round(safe_float(d.get("volume_shock_latest_vs_prior_ratio"), np.nan), 3),
            "ltp": round(safe_float(d.get("LTP"), np.nan), 2),
            "atr_pct": round(safe_float(d.get("ATRpct"), np.nan), 3),
            "ret_1d": round(safe_float(d.get("Ret1D"), np.nan), 3),
            "ret_5d": round(safe_float(d.get("Ret5D"), np.nan), 3),
            "ret_20d": round(safe_float(d.get("Ret20D"), np.nan), 3),
            "rs_5d": round(safe_float(d.get("RS5D"), np.nan), 3),
            "rs_20d": round(safe_float(d.get("RS20D"), np.nan), 3),
            "mtf_score": round(safe_float(d.get("mtf_score"), 50), 2),
            "support": safe_float(d.get("support"), np.nan), "resistance": safe_float(d.get("resistance"), np.nan),
            "invalidation": safe_float(d.get("invalidation"), np.nan), "target": safe_float(d.get("target"), np.nan),
            "rr": round(safe_float(d.get("rr"), np.nan), 3),
            "invalidation_atr": round(safe_float(d.get("invalidation_atr"), np.nan), 3),
            "pattern_conflicts": int(d.get("pattern_conflicts", 0)), "pattern_supports": int(d.get("pattern_supports", 0)),
            "hard_rr_pass": bool(d.get("hard_rr_pass", False)),
            "overnight_eligibility": str(d.get("OvernightEligibility", "WATCH_ONLY")),
            "catalyst_text": catalyst_text,
            "thesis": thesis,
            "invalidation_rule": "Structural S/R or ATR fallback; hard R:R gate applies.",
        })

    result = {
        "engine": "NEXT_DAY_ALPHA_ENGINE",
        "version": V7_VERSION + "_LOCKED_TOP15_VOLUME_SHOCKER",
        "generated_at": timestamp.isoformat(),
        "data_as_of": timestamp.strftime("%Y-%m-%d"),
        "selection_pipeline": {
            "volume_shocker_lookback_days": VOLUME_SHOCKER_LOOKBACK_DAYS,
            "volume_shocker_min_consecutive_days": VOLUME_SHOCKER_MIN_CONSECUTIVE_DAYS,
            "volume_shocker_min_daily_pct": VOLUME_SHOCKER_MIN_DAILY_PCT,
            "volume_shocker_survivors": len(shock_symbols_all),
            "major_filter_output": len(major_symbols),
            "full_indicator_population": len(scored),
            "full_history_symbols": len(histories),
            "overnight_top15": len(candidates),
            "snapshot_frozen": True,
        },
        "architecture": {
            "nifty_3min_engine_modified": False,
            "shared_raw_data_allowed": True,
            "shared_calculated_features": False,
            "shared_scores": False,
            "shared_regime_decisions": False,
            "shared_decisions": False,
            "shared_labels": False,
            "shared_predictions": False,
            "shared_raw_fields_only": True,
            "next_day_direct_broker_access": False,
            "historical_source": "Supabase:yahoo_historical",
            "live_source": "Supabase:kotak_live",
        },
        "macro_regime": vix,
        "day_ahead": {
            "universe_size": len(symbols),
            "volume_shocker_count": len(shock_symbols),
            "usable_symbols": len(frame),
            "scored_symbols": len(scored),
            "top15_count": len(candidates),
            "top15": candidates,
            # Compatibility aliases. Both point to the same frozen TOP 15.
            "top5_count": len(candidates),
            "top5": candidates,
            "bet_basket_size": len(basket_records) if 'basket_records' in locals() else len(enriched),
            "bet_basket_30": [],
        },
        "morning_confirmation": {"status": "PENDING", "final": []},
        "probability_note": "Quality scores are not win probabilities. VIX is not a directional predictor. Historical calibration is required before any probability claim.",
        "quality_controls": {
            "hard_rr_gate": V7_MIN_RR,
            "min_invalidation_atr": V7_MIN_INVALIDATION_DISTANCE_ATR,
            "max_invalidation_atr": V7_MAX_INVALIDATION_DISTANCE_ATR,
            "mtf_timeframes": ["W", "D", "4H", "1H", "15M"],
            "no_trade_allowed": True,
        },
    }
    _atomic_write_text(CACHE_JSON, json.dumps(result, ensure_ascii=True, indent=2, default=str))
    _freeze_day_ahead_snapshot(result)
    return result


# Active day-ahead builder: staged TOP-15 pipeline above.
build_day_ahead_watchlist = _build_final_day_ahead_v8


def _locked_live_top15() -> List[Dict[str, Any]]:
    latest = load_latest()
    day = latest.get("day_ahead", {}) if isinstance(latest, dict) else {}
    return day.get("top15", day.get("top5", []))


# Active public readers always expose TOP 15, never the latest arbitrary RAW symbol.
NextDayAlphaEngine.live_top15 = lambda self: _locked_live_top15()
NextDayAlphaEngine.live_top5 = lambda self: _locked_live_top15()

# Active morning confirmation consumes the frozen TOP 15.
def _morning_indicator_overlay(candidate: Dict[str, Any], intra: pd.DataFrame, nifty: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """Calculate existing indicator families on the 09:15-09:20 candidate window.

    This is an additional confirmation layer. It does not alter the existing
    indicator implementations or the day-ahead scoring formulas.
    """
    out = {
        "rsa": np.nan, "macd_hist": np.nan, "rsi": np.nan,
        "supertrend_direction": "UNKNOWN", "sma20": np.nan,
        "price_vs_sma20_pct": np.nan, "atr_pct": np.nan,
        "indicator_alignment": 50.0,
    }
    if intra is None or intra.empty or len(intra) < 3:
        return out

    x = intra.copy()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        x[col] = pd.to_numeric(x.get(col), errors="coerce")
    x = x.dropna(subset=["High", "Low", "Close"]).reset_index(drop=True)
    if len(x) < 3:
        return out

    close = x["Close"]
    rsi_series = rsi(close, 14)
    macd_line, macd_signal, macd_hist = macd(close)
    st_dir = supertrend_direction(x, 10, 3.0)
    sma20 = close.rolling(20, min_periods=1).mean()
    atr_series = atr(x, 14)

    last_close = safe_float(close.iloc[-1], np.nan)
    sma = safe_float(sma20.iloc[-1], np.nan)
    atr_value = safe_float(atr_series.iloc[-1], np.nan)
    out["rsi"] = safe_float(rsi_series.iloc[-1], np.nan)
    out["macd_hist"] = safe_float(macd_hist.iloc[-1], np.nan)
    out["supertrend_direction"] = "LONG" if safe_float(st_dir.iloc[-1], 0) > 0 else "SHORT"
    out["sma20"] = sma
    out["price_vs_sma20_pct"] = ((last_close / sma) - 1.0) * 100.0 if np.isfinite(last_close) and np.isfinite(sma) and sma else np.nan
    out["atr_pct"] = (atr_value / last_close) * 100.0 if np.isfinite(atr_value) and np.isfinite(last_close) and last_close else np.nan

    # RSA/relative-strength confirmation uses the candidate's opening return
    # relative to the NIFTY opening return, using only raw price observations.
    stock_ret = ((last_close / safe_float(x["Open"].iloc[0], np.nan)) - 1.0) * 100.0 if np.isfinite(last_close) and safe_float(x["Open"].iloc[0], np.nan) else np.nan
    nifty_ret = np.nan
    if nifty is not None and not nifty.empty and "Close" in nifty:
        n = pd.to_numeric(nifty["Close"], errors="coerce").dropna()
        if len(n) >= 2 and safe_float(n.iloc[0], np.nan):
            nifty_ret = ((safe_float(n.iloc[-1], np.nan) / safe_float(n.iloc[0], np.nan)) - 1.0) * 100.0
    out["rsa"] = stock_ret - nifty_ret if np.isfinite(stock_ret) and np.isfinite(nifty_ret) else np.nan

    direction = str(candidate.get("direction", "LONG"))
    votes = []
    if direction == "LONG":
        votes += [out["macd_hist"] > 0, out["supertrend_direction"] == "LONG", out["price_vs_sma20_pct"] > 0, out["rsi"] >= 50, out["rsa"] > 0]
    else:
        votes += [out["macd_hist"] < 0, out["supertrend_direction"] == "SHORT", out["price_vs_sma20_pct"] < 0, out["rsi"] <= 50, out["rsa"] < 0]
    valid = [v for v in votes if isinstance(v, (bool, np.bool_))]
    out["indicator_alignment"] = float(np.mean(valid) * 100.0) if valid else 50.0
    return out


def _locked_morning_confirmation() -> Dict[str, Any]:
    latest = load_latest()
    candidates = latest.get("day_ahead", {}).get("top15", []) if latest else []
    if not candidates:
        return {"status": "NO_CANDIDATES", "final": [], "confirmations": []}

    # First run the existing confirmation mathematics on the frozen TOP 15.
    base = _v7_run_morning_confirmation()
    confirmations = base.get("confirmations", [])

    # Overlay the existing RSI/MACD/SuperTrend/SMA/ATR/relative-strength
    # families on the same 09:15-09:20 raw window, then use the overlay only as
    # a confirmation gate/ranking tie-breaker.
    nifty = fetch_yahoo_chart(NIFTY_TICKER, days=5, interval="1d")
    candidate_map = {str(c.get("symbol")): c for c in candidates}
    for item in confirmations:
        sym = str(item.get("symbol"))
        overlay = _morning_indicator_overlay(candidate_map.get(sym, {}), fetch_intraday(sym), nifty)
        item["morning_indicators"] = overlay
        item["indicator_alignment_score"] = round(safe_float(overlay.get("indicator_alignment"), 50.0), 2)
        # Directional conflict is a rejection; neutral/partial alignment remains
        # subject to the existing confirmation score and acceptance/breakout gates.
        direction = str(item.get("direction", "LONG"))
        if overlay.get("indicator_alignment", 50.0) < 40.0:
            item["status"] = "REJECTED"
            item["reason"] = "Morning indicator alignment contradicts thesis"
        elif overlay.get("indicator_alignment", 50.0) >= 80.0:
            item["confirmation_score"] = clip(safe_float(item.get("confirmation_score"), 0.0) + 5.0)
            item["indicator_confirmation"] = "STRONG"
        else:
            item["indicator_confirmation"] = "PARTIAL"

    required = safe_float(base.get("required_confirmation_score"), MORNING_CONFIRMATION_MIN_SCORE)
    confirmed = [
        x for x in confirmations
        if x.get("status") not in ("REJECTED", "DATA_NOT_READY")
        and safe_float(x.get("confirmation_score"), 0.0) >= required
        and (x.get("acceptance") or x.get("breakout"))
        and safe_float(x.get("indicator_alignment_score"), 0.0) >= 60.0
    ]
    confirmed.sort(key=lambda x: (safe_float(x.get("confirmation_score"), 0.0), safe_float(x.get("indicator_alignment_score"), 0.0), safe_float(x.get("previous_day_score"), 0.0)), reverse=True)

    top5_confirmed = confirmed[:5]
    final = []
    sector_used = {}
    for item in top5_confirmed:
        sec = item.get("sector_bucket", "UNKNOWN")
        if sector_used.get(sec, 0) >= 1:
            continue
        final.append(item)
        sector_used[sec] = sector_used.get(sec, 0) + 1
        if len(final) >= 2:
            break
    if len(final) < 2:
        for item in top5_confirmed:
            if item not in final:
                final.append(item)
            if len(final) >= 2:
                break

    status = "FINAL_2" if len(final) >= 2 else ("FINAL_1" if len(final) == 1 else "NO_TRADE")
    result = {
        **base,
        "status": status,
        "top5_confirmed": top5_confirmed,
        "final": final,
        "confirmations": confirmations,
        "generated_at": now_ist().isoformat(),
    }
    latest["morning_confirmation"] = result
    _atomic_write_text(CACHE_JSON, json.dumps(latest, ensure_ascii=False, indent=2, default=str))
    return result

run_morning_confirmation = _locked_morning_confirmation
NextDayAlphaEngine.run_morning = lambda self: run_morning_confirmation()

run_morning_confirmation = _locked_morning_confirmation
NextDayAlphaEngine.run_morning = lambda self: run_morning_confirmation()



def _locked_run_if_due(self) -> Optional[Dict[str, Any]]:
    current = now_ist()
    if current.hour > DAY_AHEAD_RUN_HOUR or (current.hour == DAY_AHEAD_RUN_HOUR and current.minute >= DAY_AHEAD_RUN_MINUTE):
        today = current.strftime("%Y-%m-%d")
        frozen = _load_frozen_day_ahead_snapshot(today)
        if frozen:
            return frozen
        return build_day_ahead_watchlist()
    return None

NextDayAlphaEngine.run_if_due = _locked_run_if_due



# ============================================================================
# FINAL HARDENED RUNTIME OVERRIDES
# ============================================================================
# The historical implementation above is retained for provenance. The block
# below is the single active runtime contract. It fixes the previously observed
# ordering/override problems without changing the isolation boundary.

# --- Canonical RAW BUS contract ---------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY", os.getenv("SUPABASE_KEY", "")).strip()
SUPABASE_RAW_TABLE = os.getenv("SUPABASE_RAW_TABLE", "raw_observations").strip()
SUPABASE_DATASETS = {
    "daily": os.getenv("NEXT_DAY_SUPABASE_DAILY_DATASET", "next_day_stock_daily"),
    "hourly": os.getenv("NEXT_DAY_SUPABASE_MTF_HOURLY_DATASET", "next_day_mtf_hourly"),
    "15m": os.getenv("NEXT_DAY_SUPABASE_MTF_15M_DATASET", "next_day_mtf_15m"),
    "nifty_daily": os.getenv("NEXT_DAY_SUPABASE_NIFTY_DATASET", "nifty_spot_daily"),
    "vix_daily": os.getenv("NEXT_DAY_SUPABASE_VIX_DATASET", "india_vix_daily"),
}


def _nd_canonical(symbol: Any) -> str:
    s = str(symbol or "").upper().strip()
    for suffix in (".NS", "-EQ", "_EQ"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    return s.strip()


def _nd_is_equity(symbol: Any) -> bool:
    s = _nd_canonical(symbol)
    if not s:
        return False
    if s in {"NIFTY_SPOT", "NIFTY 50", "NIFTY50", "^NSEI", "INDIAVIX", "^INDIAVIX"}:
        return False
    if s.endswith(("CE", "PE", "FUT")) or "FUT" in s:
        return False
    if re.search(r"\d{2}[A-Z]{3}\d+", s):
        return False
    if re.search(r"\d{4,}", s):
        return False
    if any(x in s for x in ("BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")):
        return False
    return not bool(re.search(r"[^A-Z0-9&._-]", s))


# Keep compatibility for every caller in the original file.
_is_equity_symbol = _nd_is_equity
_canonical_equity_symbol = _nd_canonical


def _nd_raw_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }


def _nd_raw(row: Dict[str, Any]) -> Dict[str, Any]:
    value = row.get("raw")
    return value if isinstance(value, dict) else {}


def _nd_ts(row: Dict[str, Any]) -> Optional[pd.Timestamp]:
    raw = _nd_raw(row)
    for value in (
        raw.get("event_timestamp"), raw.get("timestamp"),
        raw.get("last_traded_time"), raw.get("received_at"),
        row.get("observation_timestamp"),
    ):
        try:
            ts = pd.to_datetime(value, errors="coerce", utc=True)
            if not pd.isna(ts):
                return ts.tz_convert(IST)
        except Exception:
            pass
    return None


def _nd_num(raw: Dict[str, Any], *keys: str, default=np.nan) -> float:
    for key in keys:
        if key not in raw:
            continue
        try:
            x = float(raw[key])
            if np.isfinite(x):
                return x
        except Exception:
            pass
    return default


def _nd_symbol(row: Dict[str, Any]) -> str:
    raw = _nd_raw(row)
    return _nd_canonical(row.get("symbol") or raw.get("symbol") or raw.get("display_symbol"))


def _nd_bus_read(
    source: str,
    dataset: Optional[str] = None,
    symbols: Optional[List[str]] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    wanted = {_nd_canonical(x) for x in (symbols or []) if _nd_is_equity(x)}
    variants = sorted({v for x in wanted for v in (x, f"{x}-EQ")})
    rows: List[Dict[str, Any]] = []
    offset = 0
    page_size = min(5000, max(500, int(limit)))

    while len(rows) < limit:
        take = min(page_size, limit - len(rows))
        params: List[Tuple[str, str]] = [
            ("select", "*"),
            ("source", f"eq.{source}"),
            ("order", "observation_timestamp.asc"),
            ("limit", str(take)),
            ("offset", str(offset)),
        ]
        if dataset:
            params.append(("raw->>dataset", f"eq.{dataset}"))
        if variants:
            params.append(("symbol", "in.(" + ",".join(variants) + ")"))
        if since is not None:
            params.append(("observation_timestamp", f"gte.{since.astimezone(timezone.utc).isoformat()}"))
        if until is not None:
            params.append(("observation_timestamp", f"lte.{until.astimezone(timezone.utc).isoformat()}"))

        try:
            response = requests.get(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_RAW_TABLE}",
                headers=_nd_raw_headers(), params=params, timeout=30,
            )
            response.raise_for_status()
            page = response.json()
        except Exception as exc:
            LOGGER.warning("RAW BUS read failed source=%s dataset=%s offset=%s: %s", source, dataset, offset, exc)
            break
        if not isinstance(page, list) or not page:
            break
        rows.extend(x for x in page if isinstance(x, dict))
        if len(page) < take:
            break
        offset += len(page)

    out = []
    for row in rows:
        raw = _nd_raw(row)
        symbol = _nd_symbol(row)
        raw_symbol = _nd_canonical(raw.get("symbol") or raw.get("display_symbol") or symbol)
        if symbol != raw_symbol:
            continue
        if wanted and symbol not in wanted:
            continue
        # Live source must never leak derivatives into this engine.
        if source == "kotak_live" and not _nd_is_equity(symbol):
            continue
        if dataset and str(raw.get("dataset", "")) != dataset:
            continue
        ts = _nd_ts(row)
        if ts is None:
            continue
        if since is not None and ts < pd.Timestamp(since):
            continue
        if until is not None and ts > pd.Timestamp(until):
            continue
        out.append(row)
    return out


def _nd_frame(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    records = []
    for row in rows:
        raw = _nd_raw(row)
        ts = _nd_ts(row)
        if ts is None:
            continue
        records.append({
            "DateTime": ts,
            "Open": _nd_num(raw, "open", "o", "pOpen", "openPrice"),
            "High": _nd_num(raw, "high", "h", "pHigh", "highPrice"),
            "Low": _nd_num(raw, "low", "l", "pLow", "lowPrice"),
            "Close": _nd_num(raw, "close", "c", "ltp", "lp", "last_price", "lastPrice"),
            "Volume": _nd_num(raw, "volume", "v", "vol", "tradedVolume", "vtt", default=0.0),
        })
    if not records:
        return pd.DataFrame(columns=["DateTime", "Open", "High", "Low", "Close", "Volume"])
    x = pd.DataFrame(records)
    for c in ("Open", "High", "Low", "Close", "Volume"):
        x[c] = pd.to_numeric(x[c], errors="coerce")
    return x.dropna(subset=["Open", "High", "Low", "Close"]).sort_values("DateTime").drop_duplicates("DateTime", keep="last").reset_index(drop=True)


def _nd_history(symbol: str, days: int = 320, interval: str = "1d") -> pd.DataFrame:
    s = _nd_canonical(symbol)
    if s == "NIFTY_SPOT":
        dataset = SUPABASE_DATASETS["nifty_daily"]
        symbols = None
    elif s == "INDIAVIX":
        dataset = SUPABASE_DATASETS["vix_daily"]
        symbols = None
    else:
        if not _nd_is_equity(s):
            return pd.DataFrame()
        dataset = {"1d": SUPABASE_DATASETS["daily"], "1h": SUPABASE_DATASETS["hourly"], "15m": SUPABASE_DATASETS["15m"]}.get(interval)
        if not dataset:
            return pd.DataFrame()
        symbols = [s]
    end = now_ist()
    rows = _nd_bus_read("yahoo_historical", dataset, symbols, end - timedelta(days=int(days)), end, 100000)
    frame = _nd_frame(rows)
    if s in ("NIFTY_SPOT", "INDIAVIX") and not frame.empty:
        # The dataset is authoritative; keep only its market-reference symbol.
        frame = frame.reset_index(drop=True)
    return frame


def _nd_fetch_daily_window(symbols: List[str], days: int, minimum_rows: int) -> Dict[str, pd.DataFrame]:
    clean = sorted({_nd_canonical(x) for x in symbols if _nd_is_equity(x)})
    if not clean:
        return {}
    end = now_ist()
    rows = _nd_bus_read("yahoo_historical", SUPABASE_DATASETS["daily"], clean, end - timedelta(days=days), end, 150000)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        s = _nd_symbol(row)
        if s in clean:
            grouped.setdefault(s, []).append(row)
    return {s: _nd_frame(rs) for s, rs in grouped.items() if len(_nd_frame(rs)) >= minimum_rows}


def fetch_yahoo_chart(ticker: str, days: int = 320, interval: str = "1d") -> pd.DataFrame:
    t = str(ticker).upper().strip()
    if t == "^NSEI":
        return _nd_history("NIFTY_SPOT", days, interval)
    if t == "^INDIAVIX":
        return _nd_history("INDIAVIX", days, interval)
    return _nd_history(_nd_canonical(t), days, interval)


def fetch_history(symbols: List[str], days: int = 320) -> Dict[str, pd.DataFrame]:
    data = _nd_fetch_daily_window(symbols, days, MIN_HISTORY_DAYS)
    return {s: df for s, df in data.items() if _nd_is_equity(s) and len(df) >= MIN_HISTORY_DAYS}


# --- Correct indicator math --------------------------------------------------

def _nd_slope_norm(series: pd.Series, n: int, atr_value: float) -> float:
    y = pd.to_numeric(series, errors="coerce").dropna().tail(n)
    if len(y) < 3 or not np.isfinite(atr_value) or atr_value <= 0:
        return np.nan
    xx = np.arange(len(y), dtype=float)
    return float(np.polyfit(xx, y.to_numpy(dtype=float), 1)[0]) / atr_value


def _nd_structure(df: pd.DataFrame, window: int = 5) -> Dict[str, Any]:
    if len(df) < window * 4:
        return {"Structure": "NEUTRAL", "StructureStrength": 50.0, "HHHL": 0, "LHLL": 0}
    h = pd.to_numeric(df["High"], errors="coerce")
    l = pd.to_numeric(df["Low"], errors="coerce")
    ph, rh = h.iloc[-2*window:-window].max(), h.iloc[-window:].max()
    pl, rl = l.iloc[-2*window:-window].min(), l.iloc[-window:].min()
    hh, hl = rh > ph, rl > pl
    lh, ll = rh < ph, rl < pl
    if hh and hl: return {"Structure":"LONG","StructureStrength":90.0,"HHHL":1,"LHLL":0}
    if lh and ll: return {"Structure":"SHORT","StructureStrength":90.0,"HHHL":0,"LHLL":1}
    if hh or hl: return {"Structure":"LONG","StructureStrength":68.0,"HHHL":int(hh),"LHLL":0}
    if lh or ll: return {"Structure":"SHORT","StructureStrength":68.0,"HHHL":0,"LHLL":int(lh or ll)}
    return {"Structure":"NEUTRAL","StructureStrength":50.0,"HHHL":0,"LHLL":0}


def build_features(symbol: str, df: pd.DataFrame, benchmark: Optional[pd.DataFrame], industry: str) -> Optional[Dict[str, Any]]:
    s = _nd_canonical(symbol)
    if not _nd_is_equity(s) or df is None or len(df) < MIN_HISTORY_DAYS:
        return None
    d = df.copy()
    for c in ("Open","High","Low","Close","Volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["Open","High","Low","Close"]).sort_values("DateTime").drop_duplicates("DateTime", keep="last").reset_index(drop=True)
    if len(d) < MIN_HISTORY_DAYS:
        return None

    c = d["Close"]
    d["EMA20"], d["EMA50"], d["EMA200"] = ema(c,20), ema(c,50), ema(c,200)
    d["ATR14"] = atr(d,14)
    d["ATRpct"] = d["ATR14"] / c * 100.0
    d["ADX14"], d["PlusDI"], d["MinusDI"] = adx(d,14)
    d["SuperTrendDirection"] = supertrend_direction(d,10,3.0)
    d["RSI14"] = rsi(c,14)
    d["MACD"], d["MACDSignal"], d["MACDHist"] = macd(c)
    d["RangePct"] = (d["High"]-d["Low"]) / c * 100.0
    d["Turnover"] = c * d["Volume"].fillna(0.0)

    last = d.iloc[-1]
    close = safe_float(last["Close"])
    avg_turnover = safe_float(d["Turnover"].tail(20).mean() / 1e7)
    avg_volume = safe_float(d["Volume"].tail(20).mean())
    if not np.isfinite(close) or close < MIN_PRICE or avg_turnover < MIN_AVG_TURNOVER_CR or avg_volume < MIN_AVG_VOLUME:
        return None

    def ret(n):
        return _return_pct(c,n)
    ret1, ret5, ret20, ret60 = ret(1), ret(5), ret(20), ret(60)

    nr1 = nr5 = nr20 = np.nan
    if benchmark is not None and len(benchmark) >= 70:
        bc = pd.to_numeric(benchmark["Close"], errors="coerce").dropna()
        nr1, nr5, nr20 = _return_pct(bc,1), _return_pct(bc,5), _return_pct(bc,20)
    rs1 = ret1-nr1 if np.isfinite(ret1) and np.isfinite(nr1) else np.nan
    rs5 = ret5-nr5 if np.isfinite(ret5) and np.isfinite(nr5) else np.nan
    rs20 = ret20-nr20 if np.isfinite(ret20) and np.isfinite(nr20) else np.nan

    a14 = safe_float(last["ATR14"])
    ema20v, ema50v, ema200v = safe_float(last["EMA20"]), safe_float(last["EMA50"]), safe_float(last["EMA200"])
    slope20 = _nd_slope_norm(d["EMA20"],5,a14)
    slope50 = _nd_slope_norm(d["EMA50"],5,a14)
    slope200 = _nd_slope_norm(d["EMA200"],10,a14)
    structure = _nd_structure(d,5)

    p20h, p20l = d["High"].iloc[-21:-1].max(), d["Low"].iloc[-21:-1].min()
    p60h, p60l = d["High"].iloc[-61:-1].max(), d["Low"].iloc[-61:-1].min()
    b20u, b20d = int(close > p20h), int(close < p20l)
    b60u, b60d = int(close > p60h), int(close < p60l)

    av20, av5 = d["Volume"].iloc[-21:-1].mean(), d["Volume"].iloc[-6:-1].mean()
    latest_vol = safe_float(last["Volume"],0)
    vr20 = latest_vol/av20 if np.isfinite(av20) and av20>0 else np.nan
    vr5 = latest_vol/av5 if np.isfinite(av5) and av5>0 else np.nan
    atr20 = d["ATRpct"].iloc[-21:-1].mean()
    expansion = safe_float(last["ATRpct"])/atr20 if np.isfinite(atr20) and atr20>0 else np.nan

    row = {
        "LTP":close,"EMA20":ema20v,"EMA50":ema50v,"EMA200":ema200v,
        "EMA20SlopeNorm":slope20,"EMA50SlopeNorm":slope50,"EMA200SlopeNorm":slope200,
        "ADX14":safe_float(last["ADX14"]),"PlusDI":safe_float(last["PlusDI"]),"MinusDI":safe_float(last["MinusDI"]),
        "SuperTrendDirection":safe_float(last["SuperTrendDirection"],0),"Structure":structure["Structure"],
        "Breakout20Up":b20u,"Breakout20Down":b20d,"Breakout60Up":b60u,"Breakout60Down":b60d,
    }
    long_checks = [close>ema20v if np.isfinite(ema20v) else False, close>ema50v if np.isfinite(ema50v) else False, close>ema200v if np.isfinite(ema200v) else False, slope20>0 if np.isfinite(slope20) else False, slope50>0 if np.isfinite(slope50) else False, safe_float(last["ADX14"])>=20 and safe_float(last["PlusDI"])>safe_float(last["MinusDI"]), safe_float(last["SuperTrendDirection"])>0, structure["Structure"]=="LONG", bool(b20u or b60u)]
    short_checks = [close<ema20v if np.isfinite(ema20v) else False, close<ema50v if np.isfinite(ema50v) else False, close<ema200v if np.isfinite(ema200v) else False, slope20<0 if np.isfinite(slope20) else False, slope50<0 if np.isfinite(slope50) else False, safe_float(last["ADX14"])>=20 and safe_float(last["MinusDI"])>safe_float(last["PlusDI"]), safe_float(last["SuperTrendDirection"])<0, structure["Structure"]=="SHORT", bool(b20d or b60d)]
    long_pct, short_pct = np.mean(long_checks)*100.0, np.mean(short_checks)*100.0
    direction = "LONG" if long_pct >= short_pct+12 else "SHORT" if short_pct >= long_pct+12 else "NEUTRAL"

    return {"Symbol":s,"Industry":industry or "UNKNOWN","LTP":close,"AvgTurnoverCr":avg_turnover,"AvgVolume20":avg_volume,"Ret1D":ret1,"Ret5D":ret5,"Ret20D":ret20,"Ret60D":ret60,"NiftyRet1D":nr1,"NiftyRet5D":nr5,"NiftyRet20D":nr20,"RS1D":rs1,"RS5D":rs5,"RS20D":rs20,"EMA20":ema20v,"EMA50":ema50v,"EMA200":ema200v,"AboveEMA20":int(np.isfinite(ema20v) and close>ema20v),"AboveEMA50":int(np.isfinite(ema50v) and close>ema50v),"AboveEMA200":int(np.isfinite(ema200v) and close>ema200v),"EMA20SlopeNorm":slope20,"EMA50SlopeNorm":slope50,"EMA200SlopeNorm":slope200,"ATR14":a14,"ATRpct":safe_float(last["ATRpct"]),"ATRExpansion":expansion,"RangePct":safe_float(last["RangePct"]),"RSI14":safe_float(last["RSI14"]),"MACD":safe_float(last["MACD"]),"MACDSignal":safe_float(last["MACDSignal"]),"MACDHist":safe_float(last["MACDHist"]),"ADX14":safe_float(last["ADX14"]),"PlusDI":safe_float(last["PlusDI"]),"MinusDI":safe_float(last["MinusDI"]),"SuperTrendDirection":int(safe_float(last["SuperTrendDirection"],0)),**structure,"Breakout20Up":b20u,"Breakout20Down":b20d,"Breakout60Up":b60u,"Breakout60Down":b60d,"VolumeRatio20":vr20,"VolumeRatio5":vr5,"LongVotes":sum(long_checks),"ShortVotes":sum(short_checks),"LongVotePct":long_pct,"ShortVotePct":short_pct,"Direction":direction}


def _nd_score_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    x = frame.copy()
    x["StockVsSector5D"] = x["Ret5D"] - x["SectorRet5D"]
    x["StockVsSector20D"] = x["Ret20D"] - x["SectorRet20D"]
    rows=[]
    for _, r in x.iterrows():
        direction=str(r.get("Direction","NEUTRAL"))
        if direction not in ("LONG","SHORT"):
            continue
        sign=1 if direction=="LONG" else -1
        align=np.mean([r.get("AboveEMA20",0) if sign>0 else 1-r.get("AboveEMA20",0), r.get("AboveEMA50",0) if sign>0 else 1-r.get("AboveEMA50",0), r.get("AboveEMA200",0) if sign>0 else 1-r.get("AboveEMA200",0)])*100
        slope_score=safe_mean([clip(50+safe_float(r.get("EMA20SlopeNorm"))*sign*35),clip(50+safe_float(r.get("EMA50SlopeNorm"))*sign*35),clip(50+safe_float(r.get("EMA200SlopeNorm"))*sign*25)],50)
        adxv=safe_float(r.get("ADX14")); diok=(safe_float(r.get("PlusDI"))>safe_float(r.get("MinusDI"))) if sign>0 else (safe_float(r.get("MinusDI"))>safe_float(r.get("PlusDI")))
        adxscore=clip(45+max(adxv-15,0)*2.75) if np.isfinite(adxv) else 50
        trend=clip(.25*align+.20*slope_score+.20*adxscore+.15*(90 if diok else 20)+.10*(90 if sign*safe_float(r.get("SuperTrendDirection"),0)>0 else 20)+.10*(safe_float(r.get("StructureStrength"),50) if r.get("Structure")==direction else 100-safe_float(r.get("StructureStrength"),50)))
        retcomp=safe_mean([safe_float(r.get("Ret1D"))*.30,safe_float(r.get("Ret5D"))*.45,safe_float(r.get("Ret20D"))*.25])
        ret_score=clip(50+retcomp*sign*10)
        rv=safe_float(r.get("RSI14"));
        if not np.isfinite(rv): rsis=50
        elif direction=="LONG": rsis=90 if 60<=rv<=70 else clip(50+(rv-50)*1.3) if rv<=75 else max(30,70-(rv-75)*4)
        else: rsis=90 if 30<=rv<=40 else clip(50+(50-rv)*1.3) if rv>=25 else max(30,70-(25-rv)*4)
        macdh=safe_float(r.get("MACDHist")); a=safe_float(r.get("ATR14")); macds=clip(50+(macdh/a if np.isfinite(macdh) and np.isfinite(a) and a>0 else 0)*sign*80)
        momentum=clip(.38*ret_score+.32*rsis+.30*macds)
        rsval=safe_mean([r.get("RS1D"),r.get("RS5D"),r.get("RS20D"),r.get("StockVsSector5D"),r.get("StockVsSector20D")],0); relative=clip(50+rsval*sign*14)
        sec_ret=safe_mean([r.get("SectorRet1D"),r.get("SectorRet5D"),r.get("SectorRet20D")],0); sec_rs=safe_mean([r.get("SectorRS5D"),r.get("SectorRS20D")],0); breadth=safe_float(r.get("SectorLongBreadth" if sign>0 else "SectorShortBreadth"),50); sector=clip(.35*clip(50+sec_ret*10*sign)+.30*clip(50+sec_rs*14*sign)+.35*breadth)
        ratios=[safe_float(r.get("VolumeRatio20")),safe_float(r.get("VolumeRatio5"))]; volume=safe_mean([clip(50+math.log(max(v,.05))*30) for v in ratios if np.isfinite(v) and v>0],50)
        ap=safe_float(r.get("ATRpct")); ex=safe_float(r.get("ATRExpansion"));
        if not np.isfinite(ap): volat=50
        elif ap<MIN_ATR_PCT: volat=35
        elif ap<=4.5: volat=88
        elif ap<=MAX_ATR_PCT: volat=88-(ap-4.5)*7
        else: volat=20
        if np.isfinite(ex): volat += 8 if 1.05<=ex<=1.8 else -15 if ex>2.5 else -8 if ex<.75 else 0
        volatility=clip(volat)
        b=int(r.get("Breakout20Up" if sign>0 else "Breakout20Down",0))+int(r.get("Breakout60Up" if sign>0 else "Breakout60Down",0)); setup=clip(.60*(safe_float(r.get("StructureStrength"),50) if r.get("Structure")==direction else 100-safe_float(r.get("StructureStrength"),50))+.40*clip(50+b*25))
        penalty=0; one=abs(safe_float(r.get("Ret1D"),0));
        if np.isfinite(ap) and ap>MAX_ATR_PCT: penalty+=18
        if one>5: penalty+=min(15,(one-5)*3)
        if np.isfinite(safe_float(r.get("VolumeRatio20"))) and safe_float(r.get("VolumeRatio20"))<.65: penalty+=10
        if relative<38: penalty+=14
        if sector<38: penalty+=14
        if direction!=r.get("Direction"): penalty+=12
        if direction=="LONG" and np.isfinite(rv) and rv>82: penalty+=10
        if direction=="SHORT" and np.isfinite(rv) and rv<18: penalty+=10
        anti=clip(100-penalty)
        cat=catalyst_for_symbol(str(r["Symbol"]))
        catalyst=safe_float(cat.get("CatalystScore"),50); cd=str(cat.get("CatalystDirection","UNKNOWN"));
        if (direction=="LONG" and cd=="BEARISH") or (direction=="SHORT" and cd=="BULLISH"): catalyst=100-catalyst
        base=trend*.20+momentum*.15+relative*.20+sector*.10+volume*.10+volatility*.08+catalyst*.07+setup*.10
        score=clip(base*(.78+.22*anti/100))
        setup_type="RELATIVE_STRENGTH_BREAKOUT" if setup>=75 and relative>=70 and direction=="LONG" else "RELATIVE_WEAKNESS_BREAKDOWN" if setup>=75 and relative>=70 else "TREND_CONTINUATION" if trend>=78 and momentum>=68 else "VOLATILITY_EXPANSION" if volatility>=78 and volume>=70 else "CATALYST_MOMENTUM" if catalyst>=72 and momentum>=65 else "BREAKOUT" if (r.get("Breakout20Up") or r.get("Breakout20Down")) else "STRUCTURED_MOMENTUM"
        out=r.to_dict(); out.update({"TrendScore":trend,"MomentumScore":momentum,"RelativeStrengthScore":relative,"SectorScore":sector,"VolumeScore":volume,"VolatilityScore":volatility,"SetupScore":setup,"CatalystScoreFinal":catalyst,"CatalystDirection":cd,"CatalystCount":cat.get("CatalystCount",0),"CatalystText":cat.get("CatalystText",""),"CatalystSources":cat.get("CatalystSources",[]),"AntiFalsePositiveScore":anti,"DayAheadScore":score,"SetupType":setup_type}); rows.append(out)
    return pd.DataFrame(rows).sort_values(["DayAheadScore","RelativeStrengthScore"],ascending=False).reset_index(drop=True) if rows else pd.DataFrame()


# --- Structural MTF hardening ------------------------------------------------
def _nd_mtf_fetch(symbol: str) -> Dict[str, Any]:
    daily=_nd_history(symbol,420,"1d"); hourly=_nd_history(symbol,180,"1h"); mins15=_nd_history(symbol,55,"15m")
    frames={"W":_resample_ohlc(daily,"W-FRI"),"D":daily,"4H":_resample_ohlc(hourly,"4h"),"1H":hourly,"15M":mins15}
    levels={}; parts=[]
    for tf,frame in frames.items():
        lv=_local_levels(frame) if frame is not None and not frame.empty else {"support":np.nan,"resistance":np.nan,"atr":np.nan}
        pat,ps=_mw_pattern(frame) if frame is not None and not frame.empty else ("NONE",0)
        lv.update({"pattern":pat,"pattern_score":ps}); levels[tf]=lv; parts.append(25 if pat=="M_TOP" else 75 if pat=="W_BOTTOM" else 50)
    return {"symbol":symbol,"mtf":levels,"mtf_score":float(np.mean(parts)) if parts else 50.0}


def _nd_directional_mtf(row: pd.Series, mtf: Dict[str, Any]) -> Dict[str, Any]:
    direction=str(row.get("Direction","NEUTRAL")); price=safe_float(row.get("LTP")); av=safe_float(row.get("ATR14"));
    if not np.isfinite(av) or av<=0:
        ap=safe_float(row.get("ATRpct")); av=price*ap/100 if np.isfinite(price) and np.isfinite(ap) else np.nan
    supports=[]; resistances=[]; conflicts=0; supports_count=0
    for lv in mtf.values():
        s=safe_float(lv.get("support")); r=safe_float(lv.get("resistance"));
        if np.isfinite(s) and np.isfinite(price) and s<price: supports.append(s)
        if np.isfinite(r) and np.isfinite(price) and r>price: resistances.append(r)
        p=lv.get("pattern")
        if direction=="LONG": conflicts+=p=="M_TOP"; supports_count+=p=="W_BOTTOM"
        elif direction=="SHORT": conflicts+=p=="W_BOTTOM"; supports_count+=p=="M_TOP"
    ss=max(supports) if supports else np.nan; rr=min(resistances) if resistances else np.nan; minrisk=max(1.0*av,.25*av) if np.isfinite(av) and av>0 else np.nan
    if direction=="LONG":
        risk=price-ss if np.isfinite(price) and np.isfinite(ss) else np.nan
        if not np.isfinite(risk) or not np.isfinite(minrisk) or risk<minrisk: stop=price-minrisk if np.isfinite(price) and np.isfinite(minrisk) else np.nan; risk=minrisk; source="ATR_FLOOR"
        else: stop=ss; source="STRUCTURAL_SR"
        target=rr if np.isfinite(rr) else price*0+price+2*av if np.isfinite(price) and np.isfinite(av) else np.nan; reward=target-price if np.isfinite(target) else np.nan
    elif direction=="SHORT":
        risk=rr-price if np.isfinite(price) and np.isfinite(rr) else np.nan
        if not np.isfinite(risk) or not np.isfinite(minrisk) or risk<minrisk: stop=price+minrisk if np.isfinite(price) and np.isfinite(minrisk) else np.nan; risk=minrisk; source="ATR_FLOOR"
        else: stop=rr; source="STRUCTURAL_SR"
        target=ss if np.isfinite(ss) else price-2*av if np.isfinite(price) and np.isfinite(av) else np.nan; reward=price-target if np.isfinite(target) else np.nan
    else:
        return {"mtf_score":50.0,"support":ss,"resistance":rr,"invalidation":np.nan,"target":np.nan,"risk_points":np.nan,"reward_points":np.nan,"rr":np.nan,"invalidation_atr":np.nan,"hard_rr_pass":False,"pattern_conflicts":conflicts,"pattern_supports":supports_count}
    rratio=reward/risk if np.isfinite(reward) and np.isfinite(risk) and risk>0 else np.nan; invatr=risk/av if np.isfinite(risk) and np.isfinite(av) and av>0 else np.nan
    score=clip(50+supports_count*6-conflicts*9)
    hard=bool(np.isfinite(rratio) and rratio>=V7_MIN_RR and np.isfinite(invatr) and V7_MIN_INVALIDATION_DISTANCE_ATR<=invatr<=V7_MAX_INVALIDATION_DISTANCE_ATR)
    return {"mtf_score":score,"support":ss,"resistance":rr,"invalidation":stop,"invalidation_source":source,"target":target,"risk_points":risk,"reward_points":reward,"rr":rratio,"invalidation_atr":invatr,"hard_rr_pass":hard,"pattern_conflicts":conflicts,"pattern_supports":supports_count}


def _nd_enrich(basket: pd.DataFrame) -> pd.DataFrame:
    if basket.empty: return basket
    jobs={}
    with ThreadPoolExecutor(max_workers=V7_MTF_THREADS) as pool:
        for s in basket["Symbol"].astype(str): jobs[s]=pool.submit(_nd_mtf_fetch,s)
        rows=[]
        for _,r in basket.iterrows():
            m=jobs[str(r["Symbol"])].result(); risk=_nd_directional_mtf(r,m.get("mtf",{})); out=r.to_dict(); out.update(risk); out["MTF"]=m.get("mtf",{}); out["V7Score"]=clip(safe_float(out.get("DayAheadScore"))*0.72+safe_float(risk.get("mtf_score"),50)*0.18+safe_float(out.get("AntiFalsePositiveScore"),50)*0.10); rows.append(out)
    return pd.DataFrame(rows).sort_values(["V7Score","DayAheadScore"],ascending=False).reset_index(drop=True)


def _nd_select_top15(enriched: pd.DataFrame) -> pd.DataFrame:
    if enriched.empty: return enriched
    x=enriched[enriched["Direction"].isin(["LONG","SHORT"])].copy(); x=x[x["DayAheadScore"]>=DAY_AHEAD_MIN_SCORE]; x=x[x["hard_rr_pass"]==True]
    if x.empty: return x
    selected=[]; counts={}
    for _,r in x.iterrows():
        sec=_sector_bucket(r.get("Industry"));
        if counts.get(sec,0)>=2: continue
        z=r.copy(); z["SelectionScore"]=safe_float(r["V7Score"])-(SECTOR_REPEAT_PENALTY if counts.get(sec,0)>=1 else 0); selected.append(z); counts[sec]=counts.get(sec,0)+1
        if len(selected)>=DAY_AHEAD_TOP_N: break
    if len(selected)<DAY_AHEAD_TOP_N:
        used={str(r["Symbol"]) for r in selected}
        for _,r in x.iterrows():
            if str(r["Symbol"]) in used: continue
            z=r.copy(); z["SelectionScore"]=safe_float(r["V7Score"]); selected.append(z); used.add(str(r["Symbol"]))
            if len(selected)>=DAY_AHEAD_TOP_N: break
    return pd.DataFrame(selected).sort_values(["SelectionScore","V7Score"],ascending=False).head(DAY_AHEAD_TOP_N).reset_index(drop=True)


def _nd_vix() -> Dict[str, Any]:
    v=_nd_history("INDIAVIX",320,"1d")
    if v.empty: return {"status":"UNAVAILABLE","directional_predictor":False}
    c=pd.to_numeric(v["Close"],errors="coerce").dropna(); level=safe_float(c.iloc[-1]); prev=safe_float(c.iloc[-6]) if len(c)>=6 else np.nan; change=(level/prev-1)*100 if np.isfinite(prev) and prev else np.nan
    regime="HIGH_VOLATILITY" if level>=VIX_HIGH or (np.isfinite(change) and change>=VIX_SPIKE_PCT) else "CAUTION" if level>=VIX_CAUTION else "NORMAL"
    return {"status":"OK","level":round(level,3),"change_5d_pct":round(change,3) if np.isfinite(change) else None,"percentile":round(float((c<=level).mean()*100),2),"regime":regime,"confirmation_bonus":5.0 if regime=="HIGH_VOLATILITY" else 2.0 if regime=="CAUTION" else 0.0,"risk_multiplier":.65 if regime=="HIGH_VOLATILITY" else .80 if regime=="CAUTION" else 1.0,"directional_predictor":False}


def build_day_ahead_watchlist() -> Dict[str, Any]:
    validate_config()
    timestamp=now_ist(); universe=_clean_universe(load_nifty500_universe())
    symbols=[_nd_canonical(s) for s in universe["Symbol"].astype(str) if _nd_is_equity(s)]
    benchmark=_nd_history("NIFTY_SPOT",320,"1d")
    if benchmark.empty or len(benchmark)<70: raise RuntimeError("RAW BUS: insufficient NIFTY spot history")

    short=_nd_fetch_daily_window(symbols,14,VOLUME_SHOCKER_LOOKBACK_DAYS)
    shock=[]; shock_profiles={}
    for s,df in short.items():
        prof=_volume_shocker_profile(df)
        if prof: shock.append((s,prof["volume_shock_score"])); shock_profiles[s]=prof
    shock.sort(key=lambda z:(-z[1],z[0])); shock_symbols=[s for s,_ in shock]
    if not shock_symbols: raise RuntimeError("Volume Shocker gate produced no qualifying equities")

    major_hist=_nd_fetch_daily_window(shock_symbols,60,5); major=[]; major_profiles={}
    for s,df in major_hist.items():
        close=safe_float(df["Close"].iloc[-1]); av=safe_float(df["Volume"].tail(20).mean()); turn=safe_float((df["Close"].tail(20)*df["Volume"].tail(20)).mean()/1e7)
        if np.isfinite(close) and close>=MIN_PRICE and np.isfinite(av) and av>=MIN_AVG_VOLUME and np.isfinite(turn) and turn>=MIN_AVG_TURNOVER_CR:
            major.append((s,shock_profiles.get(s,{}).get("volume_shock_score",0))); major_profiles[s]={**shock_profiles.get(s,{}),"avg_volume_20":av,"avg_turnover_20_cr":turn,"last_close":close}
    major.sort(key=lambda z:(-z[1],z[0])); major_symbols=[s for s,_ in major[:MAJOR_FILTER_MAX_CANDIDATES]]
    if not major_symbols: raise RuntimeError("Major liquidity filter produced no qualifying equities")

    histories=_nd_fetch_daily_window(major_symbols,420,MIN_HISTORY_DAYS)
    lookup=universe.drop_duplicates("Symbol").set_index("Symbol")
    rows=[]
    for s,df in histories.items():
        try: industry=str(lookup.loc[s,"Industry"])
        except Exception: industry="UNKNOWN"
        f=build_features(s,df,benchmark,industry)
        if f: f.update({**shock_profiles.get(s,{}),**major_profiles.get(s,{})}); rows.append(f)
    if not rows: raise RuntimeError("No usable stocks survived full indicator evaluation")
    frame=add_sector_features(pd.DataFrame(rows)); scored=_nd_score_candidates(frame)
    if scored.empty: raise RuntimeError("No directional candidate survived scoring")
    enriched=_nd_enrich(scored.head(MAJOR_FILTER_MAX_CANDIDATES)); vix=_nd_vix(); top=_nd_select_top15(enriched)

    candidates=[]
    for rank,(_,r) in enumerate(top.iterrows(),1):
        if not _nd_is_equity(r["Symbol"]): continue
        d=r.to_dict(); candidates.append({"rank":rank,"symbol":str(r["Symbol"]),"industry":str(r.get("Industry","UNKNOWN")),"sector_bucket":_sector_bucket(r.get("Industry")),"direction":str(r["Direction"]),"day_ahead_score":round(safe_float(r.get("DayAheadScore")),2),"v7_score":round(safe_float(r.get("V7Score")),2),"selection_score":round(safe_float(r.get("SelectionScore",r.get("V7Score"))),2),"setup_type":str(r.get("SetupType","UNKNOWN")),"trend_score":round(safe_float(r.get("TrendScore"),50),2),"momentum_score":round(safe_float(r.get("MomentumScore"),50),2),"relative_strength_score":round(safe_float(r.get("RelativeStrengthScore"),50),2),"sector_score":round(safe_float(r.get("SectorScore"),50),2),"volume_score":round(safe_float(r.get("VolumeScore"),50),2),"volatility_score":round(safe_float(r.get("VolatilityScore"),50),2),"catalyst_score":round(safe_float(r.get("CatalystScoreFinal"),50),2),"anti_false_positive_score":round(safe_float(r.get("AntiFalsePositiveScore"),50),2),"ltp":round(safe_float(r.get("LTP")),2),"atr14":round(safe_float(r.get("ATR14")),4),"atr_pct":round(safe_float(r.get("ATRpct")),3),"ret_1d":round(safe_float(r.get("Ret1D")),3),"ret_5d":round(safe_float(r.get("Ret5D")),3),"ret_20d":round(safe_float(r.get("Ret20D")),3),"rs_5d":round(safe_float(r.get("RS5D")),3),"rs_20d":round(safe_float(r.get("RS20D")),3),"volume_shock_score":round(safe_float(r.get("volume_shock_score")),2),"volume_shock_consecutive_days":int(safe_float(r.get("volume_shock_consecutive_days"),0)),"mtf_score":round(safe_float(r.get("mtf_score"),50),2),"support":safe_float(r.get("support")),"resistance":safe_float(r.get("resistance")),"invalidation":safe_float(r.get("invalidation")),"target":safe_float(r.get("target")),"rr":round(safe_float(r.get("rr")),3),"invalidation_atr":round(safe_float(r.get("invalidation_atr")),3),"hard_rr_pass":bool(r.get("hard_rr_pass",False)),"catalyst_direction":str(r.get("CatalystDirection","UNKNOWN")),"catalyst_count":int(safe_float(r.get("CatalystCount"),0)),"catalyst_text":str(r.get("CatalystText","")),"macro_vix_regime":vix.get("regime","UNAVAILABLE"),"thesis":f"{r['Direction']} thesis from independent quantitative stock-selection calculations; morning confirmation remains mandatory.","invalidation_rule":"Structural S/R or ATR floor; hard R:R gate applies."})

    result={"engine":"NEXT_DAY_ALPHA_ENGINE","version":VERSION+"_LOCKED","generated_at":timestamp.isoformat(),"data_as_of":timestamp.strftime("%Y-%m-%d"),"architecture":{"raw_bus":"SUPABASE","raw_table":SUPABASE_RAW_TABLE,"historical":"yahoo_historical_via_raw_bus","live":"kotak_live_via_raw_bus","nifty_3min_engine_modified":False,"shared_calculated_features":False,"shared_scores":False,"shared_regime_decisions":False,"shared_labels":False,"shared_predictions":False,"shared_decisions":False,"option_selection":False},"pipeline":{"universe":len(symbols),"volume_shocker_survivors":len(shock_symbols),"major_filter_survivors":len(major_symbols),"full_history_survivors":len(histories),"indicator_scored":len(scored),"mtf_enriched":len(enriched),"hard_rr_survivors":int(enriched["hard_rr_pass"].sum()) if "hard_rr_pass" in enriched else 0},"macro_regime":vix,"day_ahead":{"universe_size":len(symbols),"top15_count":len(candidates),"top15":candidates,"top5_count":len(candidates),"top5":candidates},"morning_confirmation":{"status":"PENDING","final":[],"confirmations":[]},"probability_note":"Quality/ranking score only; not a calibrated win probability."}
    _atomic_write_text(CACHE_JSON,json.dumps(result,ensure_ascii=False,indent=2,default=str)); _freeze_day_ahead_snapshot(result); _atomic_write_text(AUDIT_FILE,json.dumps(result["pipeline"],indent=2))
    return result


# --- Day-ahead execution status / failure audit -----------------------------
# The dashboard must distinguish "not run yet" from "ran and failed".
_ORIGINAL_BUILD_DAY_AHEAD_WATCHLIST = build_day_ahead_watchlist

def _write_day_ahead_status(status: str, error: str = "", result: Optional[Dict[str, Any]] = None) -> None:
    payload = {
        "status": status,
        "updated_at": now_ist().isoformat(),
        "trading_date": now_ist().strftime("%Y-%m-%d"),
        "error": error,
        "snapshot_exists": _day_ahead_snapshot_paths(now_ist().strftime("%Y-%m-%d"))[0].exists(),
    }
    if result:
        payload["top15_count"] = len(result.get("day_ahead", {}).get("top15", []))
    try:
        _atomic_write_text(DAY_AHEAD_STATUS_JSON, json.dumps(payload, ensure_ascii=True, indent=2, default=str))
    except Exception:
        LOGGER.exception("Unable to write day-ahead status")

def _build_day_ahead_watchlist_safe() -> Dict[str, Any]:
    try:
        result = _ORIGINAL_BUILD_DAY_AHEAD_WATCHLIST()
        _write_day_ahead_status("SUCCESS", result=result)
        return result
    except Exception as exc:
        _write_day_ahead_status("FAILED", error=str(exc))
        LOGGER.exception("Day-ahead scan failed")
        raise

build_day_ahead_watchlist = _build_day_ahead_watchlist_safe


def day_ahead_status() -> Dict[str, Any]:
    """Return execution/snapshot status for the current trading date."""
    today = now_ist().strftime("%Y-%m-%d")
    snapshot = _load_frozen_day_ahead_snapshot(today)
    status = {}
    try:
        if DAY_AHEAD_STATUS_JSON.exists():
            with DAY_AHEAD_STATUS_JSON.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
                if isinstance(raw, dict):
                    status = raw
    except Exception:
        status = {}
    status["trading_date"] = today
    status["snapshot_exists"] = bool(snapshot)
    status["snapshot_top15_count"] = len(snapshot.get("day_ahead", {}).get("top15", [])) if snapshot else 0
    if snapshot:
        status["status"] = "SNAPSHOT_READY"
    elif status.get("status") not in {"FAILED"}:
        status["status"] = "NOT_RUN"
    return status


# --- Live RAW BUS aggregation ------------------------------------------------
def _nd_live_bars(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    rows=_nd_bus_read("kotak_live",None,[symbol],start,end,30000)
    records=[]
    for row in rows:
        raw=_nd_raw(row); ts=_nd_ts(row); ltp=_nd_num(raw,"ltp","lp","last_price","lastPrice","c")
        if ts is None or not np.isfinite(ltp) or ltp<=0: continue
        records.append({"DateTime":ts,"LTP":ltp,"VolumeRaw":_nd_num(raw,"volume","v","tradedVolume","vtt",default=np.nan)})
    if not records: return pd.DataFrame()
    x=pd.DataFrame(records).sort_values("DateTime").drop_duplicates("DateTime",keep="last").reset_index(drop=True)
    cv=pd.to_numeric(x["VolumeRaw"],errors="coerce"); delta=cv.diff();
    if len(delta): delta.iloc[0]=cv.iloc[0] if np.isfinite(cv.iloc[0]) else 0
    reset=(cv.notna() & cv.shift(1).notna() & (delta<0)); delta=delta.where(~reset,cv).clip(lower=0).fillna(0)
    x["VolumeDelta"]=delta; x["Minute"]=x["DateTime"].dt.floor("min")
    return x.groupby("Minute",sort=True).agg(Open=("LTP","first"),High=("LTP","max"),Low=("LTP","min"),Close=("LTP","last"),Volume=("VolumeDelta","sum")).reset_index().rename(columns={"Minute":"DateTime"})


def fetch_intraday(symbol: str) -> pd.DataFrame:
    clean=_nd_canonical(symbol)
    if not _nd_is_equity(clean): return pd.DataFrame()
    today=now_ist().date(); start=datetime(today.year,today.month,today.day,9,15,tzinfo=IST); end=datetime(today.year,today.month,today.day,15,30,tzinfo=IST)
    return _nd_live_bars(clean,start,end)


def market_gap(ticker: str = NIFTY_TICKER) -> float:
    if ticker != NIFTY_TICKER: return np.nan
    today=now_ist().date(); start=datetime(today.year,today.month,today.day,9,0,tzinfo=IST); end=datetime(today.year,today.month,today.day,9,20,tzinfo=IST)
    rows=_nd_bus_read("kotak_live",None,None,start,end,30000)
    candidates=[]
    for row in rows:
        raw=_nd_raw(row); sym=str(row.get("symbol") or raw.get("symbol") or raw.get("display_symbol") or "").upper();
        if sym not in {"NIFTY_SPOT","NIFTY 50","NIFTY50","NIFTY 50-EQ"}: continue
        ts=_nd_ts(row); op=_nd_num(raw,"open","o","openPrice"); pc=_nd_num(raw,"close","c","previousClose","pdc")
        if ts is not None and np.isfinite(op) and np.isfinite(pc) and pc>0: candidates.append((ts,(op/pc-1)*100))
    return sorted(candidates,key=lambda z:z[0])[0][1] if candidates else np.nan


def _nd_previous_close(symbol: str) -> float:
    d=_nd_history(symbol,10,"1d"); return safe_float(d["Close"].iloc[-1]) if not d.empty else np.nan


def _nd_opening_slice(bars: pd.DataFrame) -> pd.DataFrame:
    if bars is None or bars.empty: return pd.DataFrame()
    today=now_ist().date(); start=datetime(today.year,today.month,today.day,9,15,tzinfo=IST); end=start+timedelta(minutes=5)
    return bars[(bars["DateTime"]>=start)&(bars["DateTime"]<end)].copy()


def confirm_candidate(candidate: Dict[str,Any], nifty_open_gap: float, opening: pd.DataFrame, previous_close: float, sector_gap: float=np.nan, vix: Optional[Dict[str,Any]]=None) -> Confirmation:
    symbol=str(candidate.get("symbol","")); direction=str(candidate.get("direction","NEUTRAL")); prevscore=safe_float(candidate.get("day_ahead_score"),0)
    if not _nd_is_equity(symbol) or opening is None or opening.empty or not np.isfinite(previous_close) or previous_close<=0:
        return Confirmation(symbol,direction,prevscore,0.0,"DATA_NOT_READY","Opening RAW BUS data unavailable",previous_close,np.nan,np.nan,nifty_open_gap,sector_gap,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,False,False,False,False)
    x=opening.copy();
    for c in ("Open","High","Low","Close","Volume"): x[c]=pd.to_numeric(x[c],errors="coerce")
    x=x.dropna(subset=["Open","High","Low","Close"])
    op=safe_float(x["Open"].iloc[0]); hi=safe_float(x["High"].max()); lo=safe_float(x["Low"].min()); last=safe_float(x["Close"].iloc[-1])
    gap=(op/previous_close-1)*100 if np.isfinite(op) and op>0 else np.nan; rng=(hi-lo)/op*100 if np.isfinite(op) and op>0 else np.nan
    typical=(x["High"]+x["Low"]+x["Close"])/3; vol=x["Volume"].fillna(0); denom=vol.sum(); vw=safe_float((typical*vol).sum()/denom) if denom>0 else last; cvw=(last/vw-1)*100 if np.isfinite(vw) and vw>0 else np.nan
    daily=_nd_history(symbol,10,"1d"); avgvol=safe_float(daily["Volume"].tail(5).mean()) if not daily.empty else np.nan; expected=avgvol/75 if np.isfinite(avgvol) and avgvol>0 else np.nan; ov=safe_float(vol.sum(),0); vr=ov/expected if np.isfinite(expected) and expected>0 else np.nan
    rsn=gap-nifty_open_gap if np.isfinite(nifty_open_gap) else np.nan; rss=gap-sector_gap if np.isfinite(sector_gap) else np.nan; po=(last/op-1)*100 if np.isfinite(op) and op>0 else np.nan; sign=1 if direction=="LONG" else -1
    gapq=clip(50+safe_float(rsn,0)*14*sign); secq=clip(50+safe_float(rss,0)*12*sign); vq=92 if np.isfinite(cvw) and cvw*sign>.10 else 30; mq=clip(50+po*18*sign); volq=clip(45+math.log(max(vr,.05))*28) if np.isfinite(vr) else 50; rangeq=82 if np.isfinite(rng) and .15<=rng<=MAX_OPENING_RANGE_PCT else 40
    acceptance=bool(np.isfinite(vw) and last*sign>vw*sign and po*sign>0); rejection=bool(np.isfinite(vw) and last*sign<vw*sign and po*sign<-.20); breakout=bool(last>=hi*.999 if direction=="LONG" else last<=lo*1.001); breakdown=bool(last<=lo*1.001 if direction=="LONG" else last>=hi*.999)
    score=gapq*.12+secq*.10+vq*.20+mq*.18+volq*.10+rangeq*.05+(100 if acceptance else 35)*.15+(100 if breakout else 40)*.10
    if rejection: score-=25
    if direction=="LONG" and gap>MAX_GAP_PCT and po<0: score-=15
    if direction=="SHORT" and gap<-MAX_GAP_PCT and po>0: score-=15
    if np.isfinite(rng) and rng>MAX_OPENING_RANGE_PCT: score-=10
    vrg=(vix or {}).get("regime"); score-=5 if vrg=="HIGH_VOLATILITY" else 2 if vrg=="CAUTION" else 0; score=clip(score)
    if rejection: status,reason="REJECTED","Opening behaviour contradicted the overnight thesis"
    elif score>=MORNING_FINAL_MIN_SCORE and acceptance and (breakout or np.isfinite(cvw)): status,reason="CONFIRMED","Price acceptance + VWAP + directional momentum aligned"
    elif score>=MORNING_WATCH_SCORE: status,reason="WATCH","Partial confirmation; below final threshold"
    else: status,reason="REJECTED","Insufficient morning confirmation"
    return Confirmation(symbol,direction,prevscore,round(score,2),status,reason,round(previous_close,2),round(op,2),round(gap,3),round(safe_float(nifty_open_gap),3),round(safe_float(sector_gap),3),round(hi,2),round(lo,2),round(rng,3),round(vw,2),round(cvw,3),round(vr,3),round(rsn,3),round(rss,3),acceptance,rejection,breakout,breakdown)


def run_morning_confirmation() -> Dict[str,Any]:
    today=now_ist(); frozen=_load_frozen_day_ahead_snapshot(today.strftime("%Y-%m-%d")); latest=frozen or load_latest(); candidates=[c for c in latest.get("day_ahead",{}).get("top15",[]) if _nd_is_equity(c.get("symbol"))]
    if not candidates: return {"status":"NO_CANDIDATES","final":[],"confirmations":[]}
    nifty=market_gap(NIFTY_TICKER); vix=latest.get("macro_regime",{}); opening={}; prev={}
    start=datetime(today.year,today.month,today.day,9,15,tzinfo=IST); end=start+timedelta(minutes=5)
    with ThreadPoolExecutor(max_workers=min(8,len(candidates))) as pool:
        jobs={pool.submit(_nd_live_bars,c["symbol"],start,end):c["symbol"] for c in candidates}
        for f in as_completed(jobs):
            try: opening[jobs[f]]=_nd_opening_slice(f.result())
            except Exception: opening[jobs[f]]=pd.DataFrame()
    with ThreadPoolExecutor(max_workers=min(8,len(candidates))) as pool:
        jobs={pool.submit(_nd_previous_close,c["symbol"]):c["symbol"] for c in candidates}
        for f in as_completed(jobs):
            try: prev[jobs[f]]=f.result()
            except Exception: prev[jobs[f]]=np.nan
    confirmations=[]
    for c in candidates:
        sector=str(c.get("sector_bucket","UNKNOWN")); peer_gaps=[]
        for p in candidates:
            if p is c or p.get("sector_bucket")!=sector: continue
            b=opening.get(p["symbol"]); pc=prev.get(p["symbol"])
            if b is not None and not b.empty and np.isfinite(pc) and pc>0:
                oo=safe_float(b["Open"].iloc[0]);
                if np.isfinite(oo): peer_gaps.append((oo/pc-1)*100)
        sg=float(np.median(peer_gaps)) if peer_gaps else np.nan
        item=asdict(confirm_candidate(c,nifty,opening.get(c["symbol"],pd.DataFrame()),prev.get(c["symbol"],np.nan),sg,vix)); item["sector_bucket"]=sector; confirmations.append(item)
    confirmed=[x for x in confirmations if x["status"]=="CONFIRMED" and safe_float(x["confirmation_score"])>=MORNING_FINAL_MIN_SCORE and (x["acceptance"] or x["breakout"])]
    confirmed.sort(key=lambda x:(safe_float(x["confirmation_score"]),safe_float(x["previous_day_score"])),reverse=True)
    final=[]; used=set()
    for x in confirmed:
        if x.get("sector_bucket") in used: continue
        final.append(x); used.add(x.get("sector_bucket"));
        if len(final)>=2: break
    if len(final)<2:
        for x in confirmed:
            if x not in final: final.append(x)
            if len(final)>=2: break
    status="FINAL_2" if len(final)>=2 else "FINAL_1" if len(final)==1 else "NO_TRADE"
    result={"status":status,"generated_at":now_ist().isoformat(),"nifty_gap_pct":nifty,"vix_regime":vix,"final":final,"confirmations":confirmations}
    latest["morning_confirmation"]=result; _atomic_write_text(CACHE_JSON,json.dumps(latest,ensure_ascii=True,indent=2,default=str)); return result


# --- Locked public methods ----------------------------------------------------
def _nd_live_top15(self):
    frozen=_load_frozen_day_ahead_snapshot(now_ist().strftime("%Y-%m-%d"))
    return [x for x in frozen.get("day_ahead",{}).get("top15",[]) if _nd_is_equity(x.get("symbol"))]

NextDayAlphaEngine.live_top15=_nd_live_top15
NextDayAlphaEngine.live_top5=_nd_live_top15
NextDayAlphaEngine.run_day_ahead=lambda self: build_day_ahead_watchlist()
NextDayAlphaEngine.run_morning=lambda self: run_morning_confirmation()


def _nd_run_if_due(self):
    now=now_ist()
    if now.hour>DAY_AHEAD_RUN_HOUR or (now.hour==DAY_AHEAD_RUN_HOUR and now.minute>=DAY_AHEAD_RUN_MINUTE):
        frozen=_load_frozen_day_ahead_snapshot(now.strftime("%Y-%m-%d"))
        return frozen if frozen else build_day_ahead_watchlist()
    return None
NextDayAlphaEngine.run_if_due=_nd_run_if_due


def validate_config() -> Dict[str,Any]:
    errors=[]
    if not SUPABASE_URL: errors.append("SUPABASE_URL is missing")
    if not SUPABASE_KEY: errors.append("SUPABASE_ANON_KEY/SUPABASE_KEY is missing")
    if errors: raise RuntimeError("Configuration validation failed: "+" | ".join(errors))
    return {"ok":True,"errors":[]}


def _nd_main():
    import argparse
    parser=argparse.ArgumentParser(description="Standalone NIFTY Next-Day Stock Alpha Engine")
    parser.add_argument("--day-ahead",action="store_true")
    parser.add_argument("--morning",action="store_true")
    parser.add_argument("--show",action="store_true")
    parser.add_argument("--background",action="store_true")
    parser.add_argument("--streamlit",action="store_true")
    args=parser.parse_args(); engine=NextDayAlphaEngine()
    if args.streamlit: return run_streamlit_dashboard()
    if args.day_ahead: return print_day_ahead(engine.run_day_ahead())
    if args.morning: return print_morning(engine.run_morning())
    if args.show: return print(json.dumps(engine.latest(),indent=2,ensure_ascii=True,default=str))
    if args.background:
        engine.start_if_due_background()
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt: engine.stop()
        return
    result=engine.run_if_due(); print_day_ahead(result or engine.latest())

if __name__ == "__main__":
    _nd_main()
