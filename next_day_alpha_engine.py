#!/usr/bin/env python3
"""
NIFTY NEXT-DAY STOCK ALPHA ENGINE
=================================

FINAL STANDALONE VERSION

Purpose
-------
A completely isolated stock-selection engine for:

1. Market-close / day-ahead scan
2. Selecting up to TOP 15 high-quality stock candidates
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
optional: yfinance (historical fallback only)
common raw-data producer (live/intraday; external to this engine)

PUBLIC API
----------
engine = NextDayAlphaEngine()

engine.run_if_due()
engine.latest()
engine.live_top15()
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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

# IMPORTANT ARCHITECTURE NOTE:
# This engine deliberately has NO broker SDK, Kotak credentials, TOTP/MPIN,
# broker login, quote call, or order API. Live raw observations are supplied
# by the common raw-data producer and read from the shared raw-data boundary.

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

# COMMON RAW-DATA SOURCE
# ----------------------
# The producer is outside this engine. It may be app.py/Kotak, a websocket,
# another approved raw-feed service, or a replay source. This engine never
# knows broker credentials or broker API details.
COMMON_RAW_SOURCE_NAME = os.getenv("COMMON_RAW_SOURCE_NAME", "COMMON_RAW_MARKET_SOURCE")
COMMON_RAW_MAX_AGE_SECONDS = max(1.0, float(os.getenv("COMMON_RAW_MAX_AGE_SECONDS", "30")))

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
# Day-ahead display/selection is TOP 15. Quality gates may still yield fewer
# than 15; the engine never fabricates candidates.
DAY_AHEAD_TOP_N = max(1, int(os.getenv("NEXT_DAY_TOP_N", "15")))
TOP15_COUNT = DAY_AHEAD_TOP_N
# Backward-compatible alias for existing callers/UI code. It now means the
# complete day-ahead basket size, not five.
TOP5_COUNT = DAY_AHEAD_TOP_N

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
DAY_AHEAD_RUN_MINUTE = 31
DAY_AHEAD_SNAPSHOT_START_MINUTE = 15
DAY_AHEAD_SNAPSHOT_END_MINUTE = 30

LOCK = threading.Lock()

_DATA_SOURCE_HEALTH: Dict[str, Any] = {
    "YFINANCE": {"status": "NOT_TESTED", "last_success_ist": None, "last_attempt_ist": None, "symbols_ok": 0, "error": None, "mode": "HISTORICAL_RAW"},
    "COMMON_RAW": {"status": "NOT_TESTED", "last_success_ist": None, "last_attempt_ist": None, "quotes_ok": 0, "error": None, "mode": "LIVE_RAW_READ_ONLY", "source": COMMON_RAW_SOURCE_NAME},
}

def _set_source_health(source: str, **updates: Any) -> None:
    with LOCK:
        current = dict(_DATA_SOURCE_HEALTH.get(source, {}))
        current.update(updates)
        _DATA_SOURCE_HEALTH[source] = current

def get_data_source_health() -> Dict[str, Dict[str, Any]]:
    with LOCK:
        return {k: dict(v) for k, v in _DATA_SOURCE_HEALTH.items()}

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
        "COMMON_RAW_MAX_AGE_SECONDS": COMMON_RAW_MAX_AGE_SECONDS,
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
    # No broker credential validation is permitted here. The common producer
    # owns authentication; this engine only consumes raw observations.
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
    _set_source_health(
        "YFINANCE",
        status="FETCHING",
        last_attempt_ist=now_ist().isoformat(),
        error=None,
    )

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

    # Record the actual yfinance batch result before fallback requests.
    if result:
        _set_source_health("YFINANCE", status="CONNECTED", last_success_ist=now_ist().isoformat(), symbols_ok=len(result), error=None)

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

    if result:
        _set_source_health("YFINANCE", status="CONNECTED", last_success_ist=now_ist().isoformat(), symbols_ok=len(result), error=None)
    else:
        _set_source_health("YFINANCE", status="ERROR", symbols_ok=0, error="No historical symbols could be fetched from yfinance/Yahoo.")

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
# DAY-AHEAD SHORTLIST SELECTION
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

        if len(selected) >= DAY_AHEAD_TOP_N:
            break

    # If strict selection produced fewer than the requested shortlist size,
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

            if len(selected) >= DAY_AHEAD_TOP_N:
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
        "version": "FINAL_V2_COMMON_RAW_TRUSTED_CATALYST",
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
            "live_intraday_primary": "COMMON_RAW_SOURCE",
            "historical_raw_source": "YFINANCE_EXTERNAL_RAW",
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
            "top15_count": len(candidates),
            "top15": candidates,
            "top5_count": len(candidates),
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
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
        )

    return result


# ============================================================================
# COMMON RAW-DATA BRIDGE
# ============================================================================

# Only raw market observations are permitted to cross this boundary.
# No alpha, score, prediction, regime, signal, decision, or engine opinion.
_RAW_ALLOWED = {
    "timestamp", "received_at", "timestamp_source", "feed_age_seconds", "feed_stale",
    "token", "exchange", "symbol", "display_symbol", "ltp", "open", "high", "low",
    "close", "volume", "oi", "vwap", "upper_circuit", "lower_circuit",
    "upper_price_band", "lower_price_band", "bid", "ask", "bid_qty", "ask_qty",
    "last_traded_time", "raw_source", "instrument_type",
}

_FORBIDDEN_CROSS_ENGINE_FIELDS = {
    "alpha", "alpha_score", "alpha_probability", "confidence", "confidence_score",
    "prediction", "predicted_direction", "predicted_return", "signal", "signal_score",
    "signal_type", "regime", "regime_label", "regime_score", "external_regime",
    "position", "position_size", "decision", "trade_decision", "entry_signal",
    "exit_signal", "model_score", "model_prediction", "engine_opinion", "recommendation",
    "weight", "weights", "selection_score", "day_ahead_score", "v7_score",
}


def _raw_only(record: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(record, dict):
        return {}
    keys = {str(k).lower() for k in record.keys()}
    if keys & _FORBIDDEN_CROSS_ENGINE_FIELDS:
        raise ValueError("Opinion-contaminated observation rejected at common raw boundary")
    return {k: record[k] for k in _RAW_ALLOWED if k in record}


def shared_raw_path(symbol: str, date_string: Optional[str] = None) -> Path:
    date_string = date_string or now_ist().strftime("%Y%m%d")
    safe = str(symbol).replace("/", "_").replace("&", "_").replace(" ", "_").upper()
    return SHARED_RAW_CACHE_DIR / f"{safe}_{date_string}_raw.jsonl"


def _read_shared_raw_records(symbol: str) -> List[Dict[str, Any]]:
    path = shared_raw_path(symbol)
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    clean = _raw_only(row)
                    if clean:
                        rows.append(clean)
                except Exception:
                    continue
    except Exception:
        return []
    return rows


# IMPORTANT: there is intentionally NO writer in this engine.
# The common raw producer owns publication of observations.


def read_shared_raw_intraday(symbol: str, max_age_seconds: Optional[float] = None) -> Optional[pd.DataFrame]:
    rows = _read_shared_raw_records(symbol)
    if not rows:
        return None
    try:
        raw = pd.DataFrame(rows)
        ts_col = raw.get("timestamp")
        if ts_col is None:
            return None
        raw["DateTime"] = pd.to_datetime(ts_col, errors="coerce")
        if raw["DateTime"].dt.tz is None:
            raw["DateTime"] = raw["DateTime"].dt.tz_localize(IST, ambiguous="NaT", nonexistent="NaT")
        else:
            raw["DateTime"] = raw["DateTime"].dt.tz_convert(IST)
        raw["LTP"] = pd.to_numeric(raw.get("ltp"), errors="coerce")
        raw["VolumeRaw"] = pd.to_numeric(raw.get("volume"), errors="coerce").fillna(0.0)
        raw = raw.dropna(subset=["DateTime", "LTP"]).sort_values("DateTime")
        if raw.empty:
            return None
        latest_ts = raw["DateTime"].iloc[-1].to_pydatetime()
        fresh, age = _freshness(latest_ts, COMMON_RAW_MAX_AGE_SECONDS if max_age_seconds is None else float(max_age_seconds))
        if not fresh:
            _set_source_health("COMMON_RAW", status="STALE", last_attempt_ist=now_ist().isoformat(), error=f"latest raw observation age={age}")
            return None
        raw["VolumeDelta"] = raw["VolumeRaw"].diff()
        raw.loc[raw.index[0], "VolumeDelta"] = max(safe_float(raw["VolumeRaw"].iloc[0], 0.0), 0.0)
        raw["VolumeDelta"] = raw["VolumeDelta"].where(raw["VolumeDelta"] >= 0, 0.0)
        raw["Minute"] = raw["DateTime"].dt.floor("min")
        bars = raw.groupby("Minute", sort=True).agg(
            Open=("LTP", "first"), High=("LTP", "max"), Low=("LTP", "min"),
            Close=("LTP", "last"), Volume=("VolumeDelta", "sum")
        ).reset_index().rename(columns={"Minute": "DateTime"})
        _set_source_health("COMMON_RAW", status="CONNECTED", last_success_ist=latest_ts.isoformat(), quotes_ok=len(raw), error=None)
        return normalize_intraday(bars[["DateTime", "Open", "High", "Low", "Close", "Volume"]])
    except Exception as exc:
        _set_source_health("COMMON_RAW", status="ERROR", error=f"{type(exc).__name__}: {exc}")
        return None


def read_shared_raw_quote(symbol: str) -> Optional[Dict[str, Any]]:
    rows = _read_shared_raw_records(symbol)
    if not rows:
        return None
    rows.sort(key=lambda x: str(x.get("timestamp", "")))
    row = rows[-1]
    ts = _parse_feed_timestamp(row.get("timestamp"))
    fresh, age = _freshness(ts, COMMON_RAW_MAX_AGE_SECONDS)
    if not fresh:
        return None
    return row


class CommonRawDataSource:
    """Read-only source facade for the common raw-data boundary.

    This class deliberately contains no broker login, credentials, SDK, token
    resolution, order API, or data-derived opinion. A producer outside this
    engine is responsible for publishing raw observations.
    """
    def quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        return read_shared_raw_quote(symbol)

    def intraday(self, symbol: str) -> Optional[pd.DataFrame]:
        return read_shared_raw_intraday(symbol)

    def health(self) -> Dict[str, Any]:
        health = get_data_source_health().get("COMMON_RAW", {}).copy()
        health["source"] = COMMON_RAW_SOURCE_NAME
        health["credentials_owned_by_engine"] = False
        return health


_COMMON_RAW_SOURCE = CommonRawDataSource()


def get_common_raw_source() -> CommonRawDataSource:
    return _COMMON_RAW_SOURCE


# Backward-compatible function names. They now mean common raw-source reads;
# they do NOT instantiate or contact Kotak.
def fetch_intraday(symbol: str) -> Optional[pd.DataFrame]:
    return get_common_raw_source().intraday(symbol)


def capture_kotak_day_ahead_snapshot(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Compatibility shim: read existing common raw quotes only."""
    out: Dict[str, Dict[str, Any]] = {}
    for symbol in symbols:
        q = get_common_raw_source().quote(symbol)
        if q:
            out[str(symbol).upper()] = q
    return out


def capture_kotak_opening_window(symbols: List[str]) -> None:
    """No-op by design: opening capture belongs to the common raw producer."""
    return None


# ============================================================================
# LIVE DATA
# ============================================================================

def fetch_intraday(symbol: str) -> Optional[pd.DataFrame]:
    """Read live intraday observations from the common raw source only."""
    return get_common_raw_source().intraday(symbol)


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
    """Read the opening gap from the common raw source only."""
    symbol = "Nifty 50" if ticker == NIFTY_TICKER else str(ticker).replace(".NS", "")
    q = get_common_raw_source().quote(symbol)
    if q:
        previous_close = safe_float(q.get("close"), np.nan)
        open_price = safe_float(q.get("open"), np.nan)
        if np.isfinite(previous_close) and previous_close != 0 and np.isfinite(open_price):
            return (open_price / previous_close - 1.0) * 100.0
    return np.nan


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

# Extra raw fields only. These are still raw quote values; no calculated field
# from either engine is allowed across the isolation boundary.
_RAW_ALLOWED.update({
    "upper_circuit", "lower_circuit", "upper_price_band", "lower_price_band",
    "bid", "ask", "bid_qty", "ask_qty", "last_traded_time",
})


# Common raw source remains producer-owned; this engine is read-only.


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
    """Build the overnight TOP 15 shortlist.

    Hard R:R is a trade-readiness gate, not a reason to collapse the overnight
    research shortlist to five names. Hard-pass candidates are ranked first;
    remaining directional candidates can stay in the shortlist as WATCH_ONLY.
    Morning confirmation still requires the hard R:R gate before FINAL 1/2.
    """
    if enriched.empty:
        return enriched
    x = enriched[enriched["Direction"].isin(["LONG", "SHORT"])].copy()
    if x.empty:
        return x

    x["hard_rr_pass"] = x["hard_rr_pass"].fillna(False).astype(bool)
    x = x.sort_values(["hard_rr_pass", "V7Score"], ascending=[False, False])

    selected = []
    sector_count: Dict[str, int] = {}
    for _, row in x.iterrows():
        sector = str(row.get("SectorBucket", "UNKNOWN"))
        if sector_count.get(sector, 0) >= 2:
            continue
        item = row.copy()
        item["OvernightEligibility"] = "TRADE_READY" if bool(item.get("hard_rr_pass")) else "WATCH_ONLY"
        selected.append(item)
        sector_count[sector] = sector_count.get(sector, 0) + 1
        if len(selected) >= DAY_AHEAD_TOP_N:
            break

    # If sector diversification prevents 15, fill remaining slots without the
    # sector cap; never fabricate a candidate.
    if len(selected) < DAY_AHEAD_TOP_N:
        used = {str(r["Symbol"]) for r in selected}
        for _, row in x.iterrows():
            symbol = str(row["Symbol"])
            if symbol in used:
                continue
            item = row.copy()
            item["OvernightEligibility"] = "TRADE_READY" if bool(item.get("hard_rr_pass")) else "WATCH_ONLY"
            selected.append(item)
            used.add(symbol)
            if len(selected) >= DAY_AHEAD_TOP_N:
                break

    return pd.DataFrame(selected).reset_index(drop=True).head(DAY_AHEAD_TOP_N)


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
    """Use only raw circuit/price-band fields from the common raw source."""
    q = get_common_raw_source().quote(symbol)
    if q:
        ltp = safe_float(q.get("ltp"), np.nan)
        upper = safe_float(q.get("upper_circuit", q.get("upper_price_band")), np.nan)
        lower = safe_float(q.get("lower_circuit", q.get("lower_price_band")), np.nan)
        if np.isfinite(ltp) and np.isfinite(upper) and upper > 0 and ltp >= upper * 0.999:
            return False, "CIRCUIT_LOCKED_UPPER"
        if np.isfinite(ltp) and np.isfinite(lower) and lower > 0 and ltp <= lower * 1.001:
            return False, "CIRCUIT_LOCKED_LOWER"
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
    common_raw_snapshot=capture_kotak_day_ahead_snapshot([str(x).upper() for x in top5["Symbol"].tolist()] if not top5.empty else [])
    candidates=[]
    for rank,(_,row) in enumerate(top5.iterrows(),start=1):
        d=row.to_dict(); sym=str(row["Symbol"]); q=common_raw_snapshot.get(sym,{})
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
            "hard_rr_pass":bool(d.get("hard_rr_pass",False)),"overnight_eligibility":str(d.get("OvernightEligibility","WATCH_ONLY")),
            "risk_profile":d["risk_profile"],"thesis":"THESIS_PENDING_MORNING_CONFIRMATION","invalidation_rule":"Structural S/R or ATR fallback; hard R:R gate applies."
        })
    basket_records=[]
    for _,row in enriched.iterrows():
        basket_records.append({"symbol":str(row["Symbol"]),"sector_bucket":str(row.get("SectorBucket",_v7_sector_bucket(row.get("Industry")))),"direction":str(row.get("Direction")),"day_ahead_score":round(safe_float(row.get("DayAheadScore"),0),2),"v7_score":round(safe_float(row.get("V7Score"),0),2),"mtf_score":round(safe_float(row.get("mtf_score"),50),2),"rr":round(safe_float(row.get("rr"),np.nan),3),"hard_rr_pass":bool(row.get("hard_rr_pass",False))})
    result={"engine":"NEXT_DAY_ALPHA_ENGINE","version":V7_VERSION + "_COMMON_RAW","generated_at":timestamp.isoformat(),"data_as_of":timestamp.strftime("%Y-%m-%d"),"architecture":{"nifty_3min_engine_modified":False,"shared_raw_data_allowed":True,"shared_calculated_features":False,"shared_scores":False,"shared_regime_decisions":False,"shared_decisions":False,"shared_labels":False,"shared_predictions":False,"shared_raw_fields_only":True,"common_raw_source":COMMON_RAW_SOURCE_NAME,"broker_credentials_in_engine":False,"next_day_can_write_to_nifty_engine":False,"next_day_can_read_nifty_calculations":False,"next_day_direct_broker_access":False},"macro_regime":vix,"day_ahead":{"universe_size":len(symbols),"usable_symbols":len(frame),"scored_symbols":len(scored),"bet_basket_size":len(basket_records),"bet_basket_30":basket_records,"top15_count":min(DAY_AHEAD_TOP_N,len(candidates)),"top15":candidates[:DAY_AHEAD_TOP_N],
            "top5_count":min(DAY_AHEAD_TOP_N,len(candidates)),"top5":candidates[:DAY_AHEAD_TOP_N]},"morning_confirmation":{"status":"PENDING","final":[]},"probability_note":"Quality scores are not win probabilities. VIX is not a directional predictor. Historical calibration is required before any probability claim.","quality_controls":{"hard_rr_gate":V7_MIN_RR,"min_invalidation_atr":V7_MIN_INVALIDATION_DISTANCE_ATR,"max_invalidation_atr":V7_MAX_INVALIDATION_DISTANCE_ATR,"mtf_timeframes":["W","D","4H","1H","15M"],"sector_basket_max_per_sector":V7_MAX_STOCKS_PER_SECTOR,"no_trade_allowed":True}}
    _atomic_write_text(CACHE_JSON,json.dumps(result,ensure_ascii=False,indent=2,default=str))
    return result


# Override the original orchestration with the validated V7 implementation.
build_day_ahead_watchlist = _v7_build_day_ahead_watchlist


def _v7_run_morning_confirmation() -> Dict[str, Any]:
    latest=load_latest()
    if not latest: return {"status":"NO_DAY_AHEAD_DATA","final":[]}
    candidates=latest.get("day_ahead",{}).get("top15", latest.get("day_ahead",{}).get("top5",[]))
    if not candidates: return {"status":"NO_CANDIDATES","final":[],"confirmations":[]}
    nifty_gap=market_gap(NIFTY_TICKER)
    vix=latest.get("macro_regime",{})
    # Morning sector regime is calculated from the 30-stock raw basket first.
    basket=latest.get("day_ahead",{}).get("bet_basket_30",[])
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
        candidate_hard_rr = bool(next((c.get("hard_rr_pass",False) for c in candidates if str(c.get("symbol")) == str(item.get("symbol"))), False))
        # In neutral sectors the original confirmation may still stand; opposite live sectors are hard rejection.
        if not candidate_hard_rr:
            item["status"]="REJECTED"; item["reason"]="Overnight R:R gate not passed"
        elif item.get("sector_live_regime") in ("BULLISH","BEARISH") and not sector_ok:
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
    _atomic_write_text(CACHE_JSON,json.dumps(latest,ensure_ascii=False,indent=2,default=str))
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

    def data_source_health(self) -> Dict[str, Dict[str, Any]]:
        health = get_data_source_health()
        health["COMMON_RAW"] = get_common_raw_source().health()
        return health

    def latest(
        self,
    ) -> Dict[str, Any]:

        return load_latest()

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

    def common_raw_health(self, probe_symbol: str = "Nifty 50") -> Dict[str, Any]:
        q = get_common_raw_source().quote(probe_symbol)
        health = get_common_raw_source().health()
        health["quote_received"] = bool(q)
        health["probe_symbol"] = probe_symbol
        health["probe_ltp"] = safe_float(q.get("ltp"), np.nan) if q else np.nan
        return health

    # Backward-compatible API name. It reports common raw health and never
    # attempts a broker login or reads broker credentials.
    def kotak_health(self, probe_symbol: str = "NIFTY 50") -> Dict[str, Any]:
        return self.common_raw_health("Nifty 50" if probe_symbol == "NIFTY 50" else probe_symbol)

    def live_top15(self) -> List[Dict[str, Any]]:
        latest = load_latest()
        day = latest.get("day_ahead", {})
        return day.get("top15", day.get("top5", []))

    def live_top5(self) -> List[Dict[str, Any]]:
        # Compatibility alias; the day-ahead engine now publishes TOP 15.
        return self.live_top15()

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

                # The common raw producer owns 09:15-09:20 capture. This engine
                # only reads the resulting raw observations.

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

    top15 = (
        result
        .get(
            "day_ahead",
            {}
        )
        .get(
            "top15",
            result.get("day_ahead", {}).get("top5", []),
        )
    )

    if not top15:

        print(
            "NO QUALIFIED CANDIDATE"
        )

        return

    for item in top15:

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
    st.caption("Standalone | Common raw-data source only | Trusted catalyst layer")

    result = load_latest()
    day = result.get("day_ahead", {})
    morning = result.get("morning_confirmation", {})

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TOP 15", len(day.get("top15", day.get("top5", []))))
    c2.metric("Morning Status", morning.get("status", "PENDING"))
    c3.metric("FINAL", len(morning.get("final", [])))
    c4.metric("Engine", result.get("version", "UNKNOWN"))

    st.subheader("DAY-AHEAD TOP 15")
    top15 = day.get("top15", day.get("top5", []))
    if top15:
        st.dataframe(pd.DataFrame(top15), use_container_width=True, hide_index=True)
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

    st.subheader("COMMON RAW DATA SOURCE HEALTH")
    st.json(get_common_raw_source().health())


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


if __name__ == "__main__":
    main()
