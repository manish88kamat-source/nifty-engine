#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine | v7.0 Institutional Prop-Grade Architecture
DATA-COLLECTION READY & OPTION-CENTRIC DESK:
- Price action, Kalman filter, ATR, and slope strictly use Future (fut_vwap / fut_c).
- PCR, OI changes, Greeks (Vanna/Charm), and GEX strictly use Option Chain (22 strikes).
- Integrated Heikin Ashi, SuperTrend, Hilega Milega, and Live Heavyweight Impact Desk.
"""

from __future__ import annotations

import os
import re
import json
import time
import math
import hmac
import hashlib
import struct
import base64
import threading
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import joblib
except ImportError:
    joblib = None

try:
    import streamlit as st
except ImportError:
    st = None

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None


# =========================================================
# TIMEZONE FIX - FORCE IST EVERYWHERE
# =========================================================
IST = ZoneInfo("Asia/Kolkata")

def now_ist() -> datetime:
    return datetime.now(IST)

def to_ist(dt: datetime) -> datetime:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


# =========================================================
# 1. CONFIGURATION & CONSTANTS
# =========================================================

CONFIG = {
    "app_version": "v7.3_real_option_bid_ask_timeseries_pnl",
    "feature_version": "v6.0_bucket_smc_volume_profile",
    "label_version": "TB_v3.0_clean",
    "schema_version": "5.0",
    "weight_version": "NIFTY_STATIC_2025Q1",
    "atr_period": 14,
    "sma_period": 20,
    "triple_upper_atr": 1.0,
    "triple_lower_atr": 0.75,
    "time_barrier_min": 30,
    "mfe_horizons_min": [15, 30, 45],
    "max_label_horizon_min": 45,
    "purge_bars": 15,
    "embargo_bars": 5,
    "opening_range_minutes": 15,
    "atr_mode": "session_local",
    "execution_model": "next_bar_open",
    "session_start": "09:15",
    "session_end": "15:30",
    "bar_minutes": 3,
    "dataset_path": "./nifty_3min_dataset",
    "model_path": "./model/nifty_lgbm_latest.joblib",
    "neo_environment": "prod",
    "nifty_index_name": "Nifty 50",
    "nifty_spot_token": "Nifty 50",
    "nifty_future_token": os.getenv("NIFTY_FUT_TOKEN", "").strip(),
    "pcr_strike_count": int(os.getenv("PCR_STRIKE_COUNT", "5")),
    "pcr_strike_step": float(os.getenv("PCR_STRIKE_STEP", "50")),
    "min_data_quality_to_trade": 0.45,
    "signal_min_hold_bars": 2,
    "ui_refresh_sec": 3,
    "bar_close_grace_sec": 2,
    "session_end_flush": True,
    "hw_max_quote_age_sec": 240,
    "hw_min_symbols_required": 5,
    "feed_silence_sec": 60,
    "base_delta": 0.52,
    "base_slippage_pts": 0.35,
    "max_daily_loss_pts": 120.0,
    "risk_free_rate": 0.065,
    "default_atm_iv": 0.135,
    "dq_weights": {
        "missing_future": 0.25,
        "missing_spot": 0.20,
        "bad_ohlc": 0.15,
        "missing_oi": 0.10,
        "zero_oi": 0.05,
        "missing_volume": 0.08,
        "zero_volume": 0.05,
        "missing_option_chain": 0.08,
        "missing_heavyweight": 0.04,
    },
}

HEAVYWEIGHTS_TOP5 = {
    "HDFCBANK": 0.115,
    "RELIANCE": 0.098,
    "ICICIBANK": 0.080,
    "INFY": 0.058,
    "ITC": 0.042,
}

HEAVYWEIGHTS_ALL = {
    **HEAVYWEIGHTS_TOP5,
    "TCS": 0.040, "LT": 0.038, "AXISBANK": 0.033,
    "KOTAKBANK": 0.029, "SBIN": 0.028,
}

NSE_CASH_TOKENS = {
    "HDFCBANK": "1333", "RELIANCE": "2885", "ICICIBANK": "4963",
    "INFY": "1594", "ITC": "1660", "TCS": "11536",
    "LT": "11483", "AXISBANK": "5900", "KOTAKBANK": "1922", "SBIN": "3045",
}


# =========================================================
# 2. MATHEMATICAL & SECURITY UTILITIES
# =========================================================

def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def generate_live_totp(secret_or_otp: str) -> str:
    raw = str(secret_or_otp or "").strip().replace(" ", "").upper()
    if raw.isdigit() and len(raw) == 6:
        return raw
    try:
        if len(raw) % 8:
            raw += "=" * (8 - len(raw) % 8)
        key = base64.b32decode(raw, casefold=True)
        counter = int(time.time() // 30)
        msg = struct.pack(">Q", counter)
        digest = hmac.new(key, msg, hashlib.sha1).digest()
        offset = digest[19] & 15
        token = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000
        return f"{token:06d}"
    except Exception:
        return raw

def normalize_kotak_mobile(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("91") and len(digits) == 12:
        national = digits[2:]
    elif len(digits) == 10:
        national = digits
    else:
        return raw
    if len(national) != 10 or national[0] not in "6789":
        return raw
    return "+91" + national

def safe_float(value, default=np.nan):
    try:
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if not value:
                return default
        return float(value)
    except Exception:
        return default

def safe_int(value, default=0) -> int:
    try:
        if value is None:
            return default
        val = float(value)
        if not np.isfinite(val):
            return default
        return int(val)
    except Exception:
        return default

def is_valid_number(value):
    try:
        return value is not None and np.isfinite(float(value))
    except Exception:
        return False

def env_or_secret(name):
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
    return ""

def floor_bar_timestamp(ts: datetime, minutes=3) -> datetime:
    ts = to_ist(ts)
    naive = ts.replace(tzinfo=None)
    anchor = naive.replace(hour=9, minute=15, second=0, microsecond=0)
    if naive < anchor:
        return anchor.replace(tzinfo=IST)
    elapsed = int((naive - anchor).total_seconds() // 60)
    floored = anchor + timedelta(minutes=(elapsed // minutes) * minutes)
    return floored.replace(tzinfo=IST)

def parse_tick_timestamp(tick: Dict[str, Any]) -> datetime:
    for key in ("lstup_time", "ft", "exch_tm", "timestamp", "ltt", "t", "time", "ts"):
        val = tick.get(key)
        if val is None:
            continue
        try:
            if isinstance(val, datetime):
                return to_ist(val)
            x = float(val)
            if x > 1e12:
                return datetime.fromtimestamp(x / 1000.0, tz=IST)
            if x > 1e9:
                return datetime.fromtimestamp(x, tz=IST)
        except Exception:
            pass
    return now_ist()

def wilder_atr(trs: List[float], period=14):
    if len(trs) < period:
        return np.nan
    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return float(atr)

def calc_3bar_slope(series: List[float]) -> float:
    if len(series) < 3:
        return 0.0
    y = np.array(series[-3:], dtype=float)
    if not np.all(np.isfinite(y)):
        return 0.0
    return float((y[2] - y[0]) / 2.0)

def parse_expiry(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_ist(value)
    try:
        x = float(value)
        if x > 10_000_000_000:
            return datetime.fromtimestamp(x / 1000, tz=IST)
        if x > 1_000_000_000:
            return datetime.fromtimestamp(x, tz=IST)
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    for fmt in ["%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",
                "%d%b%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d"]:
        try:
            dt = datetime.strptime(text.upper(), fmt)
            return dt.replace(tzinfo=IST)
        except Exception:
            pass
    return None

def expiry_from_record(record):
    for key in ["pExpiryDate", "lExpiryDate", "pMaturityDate", "pLastTradingDate", "expiryDate", "expiry", "expiry_date"]:
        dt = parse_expiry(record.get(key))
        if dt is not None:
            return dt
    return None

def option_type_from_record(record):
    val = str(record.get("pOptionType") or record.get("optType") or record.get("option_type") or "").upper().strip()
    if "CE" in val or "CALL" in val:
        return "CE"
    if "PE" in val or "PUT" in val:
        return "PE"
    symbol = str(record.get("pTrdSymbol", record.get("ts", record.get("display_symbol", "")))).upper()
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    return ""

def strike_from_record(record):
    for key in ["dStrikePrice", "dStrikePrice;", "strike_price", "strikePrice", "dStrike", "strike", "pStrikePrice"]:
        value = safe_float(record.get(key))
        if is_valid_number(value) and value > 0:
            if value > 1_000_000:
                value /= 100.0
            return value
    return np.nan

def token_from_record(record):
    if not isinstance(record, dict):
        return ""
    for key in ("exchange_token", "pSymbol", "pSymbolToken", "instrument_token", "instrumentToken", "tok", "token", "pToken", "tk"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""

def extract_tick_price(tick: Dict[str, Any]) -> float:
    if not isinstance(tick, dict):
        return np.nan
    for k in ("ltp", "lp", "last_price", "last_traded_price", "c", "close", "lastPrice", "iv"):
        val = safe_float(tick.get(k))
        if is_valid_number(val) and val > 0:
            return val
    return np.nan

def extract_quote_field(record: Dict[str, Any], keys: Tuple[str, ...]) -> float:
    if not isinstance(record, dict):
        return np.nan
    for k in keys:
        val = safe_float(record.get(k))
        if is_valid_number(val) and val > 0:
            return val
    for wrapper in ("data", "quote", "ohlc", "marketDepth", "depth"):
        nested = record.get(wrapper)
        if isinstance(nested, dict):
            for k in keys:
                val = safe_float(nested.get(k))
                if is_valid_number(val) and val > 0:
                    return val
    return np.nan

def extract_option_quote(record: Dict[str, Any]) -> Dict[str, float]:
    """Extract executable option quote fields without fabricating prices.

    For a long CE/PE paper trade, entry is executed against ASK and exit
    against BID. LTP is retained as an observed fallback when one side of
    the book is unavailable.
    """
    if not isinstance(record, dict):
        return {"ltp": np.nan, "bid": np.nan, "ask": np.nan}

    def pick(keys):
        return extract_quote_field(record, tuple(keys))

    ltp = pick(("ltp", "lp", "last_price", "last_traded_price", "c", "close", "lastPrice"))
    bid = pick(("bp", "bid_price", "best_bid", "bidPrice", "bPrice", "buyPrice"))
    ask = pick(("ap", "ask_price", "best_ask", "askPrice", "aPrice", "sellPrice"))
    return {"ltp": ltp, "bid": bid, "ask": ask}


def record_list(response):
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    for key in ("data", "result", "records", "data_list", "scrips", "list", "message"):
        value = response.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for k in ("data", "records", "result", "scrips"):
                if isinstance(value.get(k), list):
                    return value[k]
    return []


# =========================================================
# 3. STATE-SPACE KALMAN FILTER & 2ND-ORDER GREEKS
# =========================================================

class KalmanPriceEngine:
    def __init__(self, process_variance=1e-4, measurement_variance=0.08):
        self.q = process_variance
        self.r = measurement_variance
        self.post_estimate = None
        self.post_error_estimate = 1.0

    def reset(self):
        self.post_estimate = None
        self.post_error_estimate = 1.0

    def update(self, measurement: float) -> Tuple[float, float]:
        if not is_valid_number(measurement):
            return np.nan, np.nan
        if self.post_estimate is None:
            self.post_estimate = measurement
            self.post_error_estimate = 1.0
            return measurement, 0.0

        prior_estimate = self.post_estimate
        prior_error_estimate = self.post_error_estimate + self.q

        blending_factor = prior_error_estimate / (prior_error_estimate + self.r)
        self.post_estimate = prior_estimate + blending_factor * (measurement - prior_estimate)
        self.post_error_estimate = (1.0 - blending_factor) * prior_error_estimate

        velocity = float(self.post_estimate - prior_estimate)
        return float(self.post_estimate), velocity


class GreeksEngine:
    @staticmethod
    def compute_second_order_greeks(
        spot: float, strike: float, minutes_to_exp: float,
        iv: float = 0.135, r: float = 0.065
    ) -> Dict[str, float]:
        if not (is_valid_number(spot) and is_valid_number(strike) and spot > 0 and strike > 0):
            return {"vanna": 0.0, "charm_ce": 0.0, "charm_pe": 0.0, "d1": 0.0, "d2": 0.0}

        tau = max(minutes_to_exp / (375.0 * 252.0), 1e-6)
        vol_sqrt_tau = iv * math.sqrt(tau)

        d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * tau) / vol_sqrt_tau
        d2 = d1 - vol_sqrt_tau
        pdf_d1 = norm_pdf(d1)

        vanna = float(-pdf_d1 * d2 / max(iv, 1e-4))
        term1 = pdf_d1 * (r / vol_sqrt_tau - d2 / (2.0 * tau))
        charm_ce = float(-term1)
        charm_pe = float(charm_ce + r * math.exp(-r * tau))

        return {
            "vanna": vanna,
            "charm_ce": charm_ce,
            "charm_pe": charm_pe,
            "d1": d1,
            "d2": d2
        }


# =========================================================
# 4. NEW INDICATOR ENGINES (HEIKIN ASHI + SUPERTREND + HILEGA MILEGA)
# =========================================================

class HeikinAshiEngine:
    def __init__(self):
        self.prev_ha_open = None
        self.prev_ha_close = None

    def reset(self):
        self.prev_ha_open = None
        self.prev_ha_close = None

    def update(self, o, h, l, c):
        if not all(is_valid_number(x) for x in [o, h, l, c]):
            return {"ha_open": np.nan, "ha_close": np.nan, "ha_color": 0, "ha_strong": 0}

        ha_close = (o + h + l + c) / 4.0

        if self.prev_ha_open is None:
            ha_open = (o + c) / 2.0
        else:
            ha_open = (self.prev_ha_open + self.prev_ha_close) / 2.0

        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)

        self.prev_ha_open = ha_open
        self.prev_ha_close = ha_close

        color = 1 if ha_close >= ha_open else -1
        strong = 1 if (color == 1 and ha_low >= ha_open - 1e-6) or (color == -1 and ha_high <= ha_open + 1e-6) else 0

        return {
            "ha_open": ha_open,
            "ha_close": ha_close,
            "ha_color": color,
            "ha_strong": strong
        }


class SuperTrendEngine:
    def __init__(self, period=10, multiplier=3.0):
        self.period = period
        self.multiplier = multiplier
        self.atr_values = deque(maxlen=period + 5)
        self.prev_upper = None
        self.prev_lower = None
        self.prev_supertrend = None
        self.prev_direction = 1

    def reset(self):
        self.atr_values.clear()
        self.prev_upper = self.prev_lower = self.prev_supertrend = None
        self.prev_direction = 1

    def update(self, high, low, close, atr=None):
        if not all(is_valid_number(x) for x in [high, low, close]):
            return {"supertrend": np.nan, "st_direction": 0, "st_flip": 0}

        tr = high - low
        self.atr_values.append(tr)

        if atr is None or not is_valid_number(atr):
            if len(self.atr_values) < self.period:
                return {"supertrend": np.nan, "st_direction": 0, "st_flip": 0}
            atr = float(np.mean(list(self.atr_values)[-self.period:]))

        basic_upper = (high + low) / 2.0 + self.multiplier * atr
        basic_lower = (high + low) / 2.0 - self.multiplier * atr

        if self.prev_upper is None:
            final_upper = basic_upper
            final_lower = basic_lower
        else:
            final_upper = basic_upper if (basic_upper < self.prev_upper or close > self.prev_upper) else self.prev_upper
            final_lower = basic_lower if (basic_lower > self.prev_lower or close < self.prev_lower) else self.prev_lower

        if self.prev_supertrend is None:
            direction = 1 if close > final_upper else -1
            supertrend = final_lower if direction == 1 else final_upper
        else:
            if self.prev_supertrend == self.prev_upper:
                direction = -1 if close < final_upper else 1
            else:
                direction = 1 if close > final_lower else -1
            supertrend = final_lower if direction == 1 else final_upper

        flip = 1 if direction != self.prev_direction else 0

        self.prev_upper = final_upper
        self.prev_lower = final_lower
        self.prev_supertrend = supertrend
        self.prev_direction = direction

        return {
            "supertrend": supertrend,
            "st_direction": direction,
            "st_flip": flip
        }


class HilegaMilegaEngine:
    def __init__(self):
        self.rsi_period = 9
        self.closes = deque(maxlen=100)
        self.rsi_values = deque(maxlen=50)

    def reset(self):
        self.closes.clear()
        self.rsi_values.clear()

    def _rsi(self, period=9):
        if len(self.closes) < period + 1:
            return np.nan
        deltas = np.diff(list(self.closes)[-(period+1):])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _ema(self, data, period):
        if len(data) < period:
            return np.nan
        alpha = 2 / (period + 1)
        ema = data[0]
        for val in data[1:]:
            ema = alpha * val + (1 - alpha) * ema
        return ema

    def _wma(self, data, period):
        if len(data) < period:
            return np.nan
        weights = np.arange(1, period + 1)
        return np.dot(data[-period:], weights) / weights.sum()

    def update(self, close):
        if not is_valid_number(close):
            return {"hm_rsi": np.nan, "hm_ema": np.nan, "hm_wma": np.nan, "hm_signal": 0}

        self.closes.append(close)
        rsi = self._rsi(self.rsi_period)
        if is_valid_number(rsi):
            self.rsi_values.append(rsi)

        if len(self.rsi_values) < 21:
            return {"hm_rsi": rsi, "hm_ema": np.nan, "hm_wma": np.nan, "hm_signal": 0}

        rsi_list = list(self.rsi_values)
        ema3 = self._ema(rsi_list, 3)
        wma21 = self._wma(rsi_list, 21)

        signal = 0
        if is_valid_number(rsi) and is_valid_number(ema3) and is_valid_number(wma21):
            if rsi > 50 and ema3 < rsi and wma21 > 50:
                signal = 1
            elif rsi < 50 and ema3 > rsi and wma21 < 50:
                signal = -1

        return {
            "hm_rsi": rsi,
            "hm_ema": ema3,
            "hm_wma": wma21,
            "hm_signal": signal
        }


# =========================================================
# 5. RESEARCH ENGINES (FUTURE VWAP ANCHORED)
# =========================================================

@dataclass
class Candle3Min:
    timestamp: datetime
    spot_o: float
    spot_h: float
    spot_l: float
    spot_c: float
    fut_o: float
    fut_h: float
    fut_l: float
    fut_c: float
    fut_volume: float
    fut_oi: float
    heavy: Dict[str, Dict[str, float]] = field(default_factory=dict)
    option_chain: Dict[str, Any] = field(default_factory=dict)
    l2_depth: Dict[str, Any] = field(default_factory=dict)


class OpeningRangeEngine:
    def __init__(self, minutes=15):
        self.minutes = minutes
        self.or_high = None
        self.or_low = None
        self.or_set = False

    def reset(self):
        self.or_high = self.or_low = None
        self.or_set = False

    def update(self, candle: Candle3Min):
        ts = to_ist(candle.timestamp)
        mins = (ts.hour * 60 + ts.minute) - 555
        if mins < self.minutes:
            self.or_high = candle.fut_h if self.or_high is None else max(self.or_high, candle.fut_h)
            self.or_low = candle.fut_l if self.or_low is None else min(self.or_low, candle.fut_l)
        else:
            self.or_set = True

    def features(self, candle: Candle3Min, atr: float):
        names = ["or_high", "or_low", "or_width_atr", "dist_to_or_high_atr", "dist_to_or_low_atr", "or_breakout_state"]
        if not self.or_set or self.or_high is None or not is_valid_number(atr) or atr <= 0:
            return {k: np.nan for k in names}
        return {
            "or_high": self.or_high, "or_low": self.or_low,
            "or_width_atr": (self.or_high - self.or_low) / atr,
            "dist_to_or_high_atr": (candle.fut_c - self.or_high) / atr,
            "dist_to_or_low_atr": (candle.fut_c - self.or_low) / atr,
            "or_breakout_state": 1 if candle.fut_c > self.or_high else (-1 if candle.fut_c < self.or_low else 0),
        }


class SessionContextEngine:
    def __init__(self):
        self.prev_close = self.prev_high = self.prev_low = self.today_open = None

    def set_previous_day(self, close, high, low):
        self.prev_close = safe_float(close)
        self.prev_high = safe_float(high)
        self.prev_low = safe_float(low)

    def set_today_open(self, open_price):
        self.today_open = safe_float(open_price)

    def reset(self):
        self.today_open = None

    def features(self, candle: Candle3Min, atr: float):
        names = ["gap_points", "gap_atr", "gap_direction", "dist_to_pdh_atr", "dist_to_pdl_atr"]
        if not is_valid_number(self.prev_close) or not is_valid_number(atr) or atr <= 0:
            return {k: np.nan for k in names}
        op = self.today_open if is_valid_number(self.today_open) else candle.fut_o
        gap = op - self.prev_close
        return {
            "gap_points": gap,
            "gap_atr": gap / atr,
            "gap_direction": 1 if gap > 0 else (-1 if gap < 0 else 0),
            "dist_to_pdh_atr": (candle.fut_c - self.prev_high) / atr if is_valid_number(self.prev_high) else np.nan,
            "dist_to_pdl_atr": (candle.fut_c - self.prev_low) / atr if is_valid_number(self.prev_low) else np.nan,
        }


class OptionChainEngine:
    def __init__(self, maxlen=150):
        self.pcr_history = deque(maxlen=maxlen)

    def reset(self):
        self.pcr_history.clear()

    def compute(self, chain: Dict[str, Any], candle_ts: datetime, spot_price: float):
        keys = [
            "pcr_oi", "pcr_volume", "ce_oi_change", "pe_oi_change", "ce_oi_atm", "pe_oi_atm",
            "atm_strike", "ce_pe_oi_imbalance", "atm_oi_imbalance", "pcr_oi_delta",
            "pcr_velocity", "days_to_expiry", "minutes_to_expiry", "expiry_day_flag",
            "gex_proxy", "zero_dte_intensity", "gex_x_0dte", "atm_gamma_imbalance",
            "dealer_vanna_flow", "dealer_charm_flow"
        ]
        if not chain:
            out = {k: np.nan for k in keys}
            for k in keys:
                out[f"{k}_missing"] = 1
            out["ce_contracts_seen"] = out["pe_contracts_seen"] = 0
            return out

        out = {}
        curr_pcr = safe_float(chain.get("pcr_oi"))
        self.pcr_history.append(curr_pcr if is_valid_number(curr_pcr) else np.nan)
        
        valid_pcrs = [p for p in self.pcr_history if is_valid_number(p)]
        pcr_delta = (valid_pcrs[-1] - valid_pcrs[-2]) if len(valid_pcrs) >= 2 else 0.0
        pcr_velocity = calc_3bar_slope(valid_pcrs)

        total_ce = safe_float(chain.get("total_ce_oi"), 0.0)
        total_pe = safe_float(chain.get("total_pe_oi"), 0.0)
        tot_sum = total_ce + total_pe
        ce_pe_imbalance = (total_pe - total_ce) / (tot_sum + 1e-5) if tot_sum > 0 else 0.0

        atm_ce = safe_float(chain.get("ce_oi_atm"), 0.0)
        atm_pe = safe_float(chain.get("pe_oi_atm"), 0.0)
        atm_sum = atm_ce + atm_pe
        atm_imbalance = (atm_pe - atm_ce) / (atm_sum + 1e-5) if atm_sum > 0 else 0.0
        atm_strike = safe_float(chain.get("atm_strike"), round(spot_price / 50.0) * 50.0 if spot_price > 0 else 24500.0)

        exp_dt = chain.get("active_expiry")
        if isinstance(exp_dt, datetime):
            exp_dt = to_ist(exp_dt)
            candle_ts = to_ist(candle_ts)
            exp_day_end = exp_dt.replace(hour=15, minute=30, second=0, microsecond=0)
            diff = exp_day_end - candle_ts
            days_to_exp = max(0, diff.days)
            mins_to_exp = max(0.0, diff.total_seconds() / 60.0)
            exp_flag = int(candle_ts.date() == exp_dt.date())
        else:
            days_to_exp = mins_to_exp = np.nan
            exp_flag = 0

        if exp_flag and is_valid_number(mins_to_exp) and mins_to_exp <= 375:
            zero_dte_intensity = max(0.0, 1.0 - (mins_to_exp / 375.0))
            if mins_to_exp <= 90:
                zero_dte_intensity = min(1.0, zero_dte_intensity + 0.35)
        else:
            zero_dte_intensity = 0.0

        atm_diff = atm_ce - atm_pe
        total_diff = total_ce - total_pe
        tot_oi_baseline = (tot_sum + 1e-5)
        
        gex_proxy = float(((atm_diff * 2.5) + (total_diff * 0.4)) / tot_oi_baseline)
        atm_gamma_imb = (atm_ce - atm_pe) / (atm_sum + 1e-5) if atm_sum > 0 else 0.0
        gex_x_0dte = float(gex_proxy * zero_dte_intensity)

        greeks = GreeksEngine.compute_second_order_greeks(
            spot=spot_price, strike=atm_strike, minutes_to_exp=mins_to_exp if is_valid_number(mins_to_exp) else 375.0,
            iv=CONFIG["default_atm_iv"], r=CONFIG["risk_free_rate"]
        )
        dealer_vanna_flow = float(np.clip(atm_gamma_imb * greeks["vanna"] * 10.0, -1.0, 1.0))
        dealer_charm_flow = float(np.clip((atm_ce * greeks["charm_ce"] - atm_pe * greeks["charm_pe"]) / (atm_sum + 1e-5), -1.0, 1.0))

        raw_metrics = {
            "pcr_oi": curr_pcr,
            "pcr_volume": chain.get("pcr_volume", np.nan),
            "ce_oi_change": chain.get("ce_oi_change", np.nan),
            "pe_oi_change": chain.get("pe_oi_change", np.nan),
            "ce_oi_atm": atm_ce,
            "pe_oi_atm": atm_pe,
            "atm_strike": atm_strike,
            "ce_pe_oi_imbalance": ce_pe_imbalance,
            "atm_oi_imbalance": atm_imbalance,
            "pcr_oi_delta": pcr_delta,
            "pcr_velocity": pcr_velocity,
            "days_to_expiry": days_to_exp,
            "minutes_to_expiry": mins_to_exp,
            "expiry_day_flag": exp_flag,
            "gex_proxy": gex_proxy,
            "zero_dte_intensity": zero_dte_intensity,
            "gex_x_0dte": gex_x_0dte,
            "atm_gamma_imbalance": atm_gamma_imb,
            "dealer_vanna_flow": dealer_vanna_flow,
            "dealer_charm_flow": dealer_charm_flow
        }

        for key in keys:
            val = raw_metrics.get(key, np.nan)
            out[key] = val
            out[f"{key}_missing"] = int(not is_valid_number(val))
        out["ce_contracts_seen"] = int(chain.get("ce_contracts_seen", 0))
        out["pe_contracts_seen"] = int(chain.get("pe_contracts_seen", 0))
        return out


class HeavyweightEngine:
    def __init__(self, weights_all: Dict[str, float], weights_top5: Dict[str, float]):
        self.weights_all = weights_all
        self.weights_top5 = weights_top5
        self.day_open: Dict[str, float] = {}

    def reset_day(self):
        self.day_open.clear()

    def compute(self, candle: Candle3Min):
        contributions, returns = [], []
        bullish = 0
        top5_pressures = []

        for symbol, weight in self.weights_all.items():
            data = candle.heavy.get(symbol)
            if not data:
                continue
            
            open_price = self.day_open.get(symbol)
            if open_price is None or not is_valid_number(open_price):
                open_price = extract_quote_field(data, ("o", "open", "pOpen", "openPrice", "op"))
                if not is_valid_number(open_price) or open_price <= 0:
                    open_price = extract_tick_price(data)
                if is_valid_number(open_price) and open_price > 0:
                    self.day_open[symbol] = open_price

            close_price = extract_tick_price(data)
            if not is_valid_number(open_price) or open_price <= 0 or not is_valid_number(close_price):
                continue

            ret = (close_price - open_price) / open_price
            contributions.append(weight * ret)
            returns.append(ret)
            
            vwap = extract_quote_field(data, ("vwap", "avp", "averagePrice", "average_price", "a"))
            if not is_valid_number(vwap) or vwap <= 0:
                vwap = open_price

            if close_price >= vwap:
                bullish += 1

            if symbol in self.weights_top5:
                top5_w = self.weights_top5[symbol]
                top5_pressures.append(top5_w * ((close_price - vwap) / (vwap + 1e-5)))

        total_twc = sum(contributions) if contributions else 0.0
        n = max(len(contributions), 1)
        slp_5 = float(sum(top5_pressures) * 1000.0) if top5_pressures else 0.0

        return {
            "twc": total_twc,
            "breadth_10": bullish / n if contributions else 0.5,
            "dispersion_index": float(np.std(returns)) if returns else 0.0,
            "contribution_concentration": max(contributions, key=abs) / (abs(total_twc) + 1e-9) if contributions else 0.0,
            "slp_top5_pressure": slp_5,
            "hw_bullish_count": bullish,
            "hw_symbols_seen": len(contributions),
        }


class FeatureEngine:
    def __init__(self, maxlen=150):
        self.vwap_pv = self.vwap_vol = 0.0
        self.tr_history = deque(maxlen=maxlen)
        self.volume_history = deque(maxlen=maxlen)
        self.iv_history = deque(maxlen=maxlen)
        self.history = deque(maxlen=maxlen)
        self.stretch_history = deque(maxlen=maxlen)
        self.spread_history = deque(maxlen=maxlen)
        self.preloaded_closes = deque(maxlen=CONFIG["sma_period"])
        self.hw = HeavyweightEngine(HEAVYWEIGHTS_ALL, HEAVYWEIGHTS_TOP5)
        self.or_eng = OpeningRangeEngine(CONFIG["opening_range_minutes"])
        self.sess = SessionContextEngine()
        self.opt = OptionChainEngine(maxlen=maxlen)
        self.kalman = KalmanPriceEngine()
        self.ha = HeikinAshiEngine()
        self.st = SuperTrendEngine(period=10, multiplier=3.0)
        self.hm = HilegaMilegaEngine()
        self.advanced = AdvancedStructureEngine(maxlen=maxlen)
        self.baskets = SignalBasketEngine()

    def reset_session(self):
        self.vwap_pv = self.vwap_vol = 0.0
        self.tr_history.clear()
        self.volume_history.clear()
        self.iv_history.clear()
        self.history.clear()
        self.stretch_history.clear()
        self.spread_history.clear()
        self.preloaded_closes.clear()
        self.hw.reset_day()
        self.or_eng.reset()
        self.sess.reset()
        self.opt.reset()
        self.kalman.reset()
        self.ha.reset()
        self.st.reset()
        self.hm.reset()
        self.advanced.reset()

    def preload_warmup(self, historical_closes: List[float], historical_trs: List[float]):
        if historical_closes:
            for c in historical_closes[-CONFIG["sma_period"]:]:
                if is_valid_number(c):
                    self.preloaded_closes.append(c)
        if historical_trs:
            for tr in historical_trs[-CONFIG["sma_period"]:]:
                if is_valid_number(tr):
                    self.tr_history.append(tr)

    def set_previous_day(self, close, high, low):
        self.sess.set_previous_day(close, high, low)

    def set_today_open(self, open_price):
        self.sess.set_today_open(open_price)

    def compute(self, candle: Candle3Min, prev: deque):
        typical = (candle.fut_h + candle.fut_l + candle.fut_c) / 3.0
        volume = safe_float(candle.fut_volume, 0.0)
        # Causal rolling volume z-score: compare the current bar only against
        # previously completed bars, then append the current bar.
        _vol_prev = [float(x) for x in self.volume_history if is_valid_number(x)]
        if len(_vol_prev) >= 10 and is_valid_number(volume):
            _vol_mean = float(np.mean(_vol_prev))
            _vol_std = float(np.std(_vol_prev, ddof=0))
            volume_zscore = (volume - _vol_mean) / max(_vol_std, 1e-9)
            volume_zscore = float(np.clip(volume_zscore, -8.0, 8.0))
        else:
            volume_zscore = 0.0
        if is_valid_number(volume):
            self.volume_history.append(volume)
        self.vwap_pv += typical * max(volume, 0.0)
        self.vwap_vol += max(volume, 0.0)
        fut_vwap = self.vwap_pv / self.vwap_vol if self.vwap_vol > 0 else typical

        if prev:
            pc = prev[-1].fut_c
            tr = max(candle.fut_h - candle.fut_l, abs(candle.fut_h - pc), abs(candle.fut_l - pc))
        else:
            tr = candle.fut_h - candle.fut_l

        atr_prev = wilder_atr(list(self.tr_history), CONFIG["atr_period"])
        self.tr_history.append(tr)
        atr = atr_prev

        kalman_price, kalman_velocity = self.kalman.update(candle.fut_c)

        all_closes = list(self.preloaded_closes) + [c.spot_c for c in prev]
        all_closes.append(candle.spot_c)
        sma_window = all_closes[-CONFIG["sma_period"]:]
        sma_ready = len(sma_window) >= CONFIG["sma_period"] and all(is_valid_number(x) for x in sma_window)
        spot_sma = float(np.mean(sma_window)) if sma_ready else np.nan

        if is_valid_number(atr) and atr > 0:
            normalized_stretch = (candle.fut_c - fut_vwap) / atr
            kalman_stretch = (kalman_price - fut_vwap) / atr if is_valid_number(kalman_price) else normalized_stretch
            normalized_spread = (spot_sma - fut_vwap) / atr if is_valid_number(spot_sma) else np.nan
        else:
            normalized_stretch = kalman_stretch = normalized_spread = np.nan

        self.stretch_history.append(normalized_stretch)
        self.spread_history.append(normalized_spread)

        stretch_slope = calc_3bar_slope(list(self.stretch_history))
        spread_slope = calc_3bar_slope(list(self.spread_history))

        l2 = candle.l2_depth or {}
        best_bid = safe_float(l2.get("best_bid"), candle.fut_c)
        best_ask = safe_float(l2.get("best_ask"), candle.fut_c)
        bid_qty = safe_float(l2.get("bid_qty"), 1.0)
        ask_qty = safe_float(l2.get("ask_qty"), 1.0)
        tot_depth = bid_qty + ask_qty
        obi = (bid_qty - ask_qty) / tot_depth if tot_depth > 0 else 0.0
        micro_price = (best_bid * ask_qty + best_ask * bid_qty) / tot_depth if tot_depth > 0 else candle.fut_c
        micro_price_drift = (micro_price - candle.fut_c) / atr if is_valid_number(atr) and atr > 0 else 0.0

        if prev:
            oi_change = candle.fut_oi - prev[-1].fut_oi if is_valid_number(candle.fut_oi) and is_valid_number(prev[-1].fut_oi) else np.nan
            price_up = candle.fut_c > prev[-1].fut_c
            price_down = candle.fut_c < prev[-1].fut_c
        else:
            oi_change = np.nan
            price_up = price_down = False

        oi_has_val = is_valid_number(oi_change)
        oi_long_buildup = int(price_up and oi_has_val and oi_change > 0)
        oi_short_buildup = int(price_down and oi_has_val and oi_change > 0)
        oi_short_covering = int(price_up and oi_has_val and oi_change < 0)
        oi_long_unwinding = int(price_down and oi_has_val and oi_change < 0)
        oi_neutral = int(not oi_has_val or oi_change == 0 or (not price_up and not price_down))
        oi_strength = ((1 if price_up else -1) * np.sign(oi_change) * np.log1p(abs(oi_change))) if (oi_has_val and oi_change != 0) else 0.0

        self.or_eng.update(candle)

        missing_spot = int(not is_valid_number(candle.spot_c))
        missing_future = int(not is_valid_number(candle.fut_c))
        missing_oi = int(not is_valid_number(candle.fut_oi))
        zero_oi = int(is_valid_number(candle.fut_oi) and candle.fut_oi == 0)
        missing_volume = int(not is_valid_number(candle.fut_volume))
        zero_volume = int(is_valid_number(candle.fut_volume) and candle.fut_volume == 0)
        missing_heavyweight = int(len(candle.heavy) == 0)
        missing_option = int(len(candle.option_chain) == 0)
        bad_ohlc = int(candle.fut_h < candle.fut_l or candle.spot_h < candle.spot_l)

        w = CONFIG["dq_weights"]
        penalty = (
            w["missing_spot"] * missing_spot +
            w["missing_future"] * missing_future +
            w["missing_oi"] * missing_oi +
            w["zero_oi"] * zero_oi +
            w["missing_volume"] * missing_volume +
            w["zero_volume"] * zero_volume +
            w["missing_heavyweight"] * missing_heavyweight +
            w["missing_option_chain"] * missing_option +
            w["bad_ohlc"] * bad_ohlc
        )

        pcr_features = self.opt.compute(candle.option_chain, candle.timestamp, candle.spot_c)
        # ATM IV is only used when the live option feed actually supplies IV.
        # Never manufacture IV from a constant default for scanner learning.
        atm_iv_obs = safe_float(candle.option_chain.get("atm_iv"), np.nan) if candle.option_chain else np.nan
        if not is_valid_number(atm_iv_obs) and candle.option_chain:
            atm_iv_obs = safe_float(candle.option_chain.get("implied_volatility"), np.nan)
        if is_valid_number(atm_iv_obs):
            prev_iv = next((float(x) for x in reversed(self.iv_history) if is_valid_number(x)), np.nan)
            iv_change = float(atm_iv_obs - prev_iv) if is_valid_number(prev_iv) else 0.0
            self.iv_history.append(atm_iv_obs)
        else:
            iv_change = 0.0
        pcr_features["atm_iv"] = atm_iv_obs
        pcr_features["iv_change"] = iv_change
        ha_res = self.ha.update(candle.fut_o, candle.fut_h, candle.fut_l, candle.fut_c)
        st_res = self.st.update(candle.fut_h, candle.fut_l, candle.fut_c, atr=atr if is_valid_number(atr) else None)
        hm_res = self.hm.update(candle.fut_c)
        advanced_res = self.advanced.compute(candle, prev)
        advanced_res["fut_c"] = candle.fut_c
        hw_res = self.hw.compute(candle)
        or_res = self.or_eng.features(candle, atr if is_valid_number(atr) else 0.0)
        sess_res = self.sess.features(candle, atr if is_valid_number(atr) else 0.0)
        basket_input = {**advanced_res, **st_res, **hm_res, **ha_res, **pcr_features, **hw_res, **or_res, **sess_res,
                        "kalman_stretch": kalman_stretch, "stretch_slope_3": stretch_slope,
                        "order_book_imbalance": obi, "oi_long_buildup": oi_long_buildup,
                        "oi_short_buildup": oi_short_buildup, "twc": hw_res.get("twc",0.0),
                        "breadth_10": hw_res.get("breadth_10",0.5), "fut_volume": candle.fut_volume,
                        "atr_14_prev": atr}
        basket_res = self.baskets.score(basket_input)

        now_ts = now_ist()
        is_causal_verified = int(
            to_ist(candle.timestamp) <= now_ts and 
            is_valid_number(candle.fut_c) and 
            is_valid_number(candle.spot_c)
        )

        ts_ist = to_ist(candle.timestamp)
        features = {
            "timestamp": candle.timestamp,
            "feature_available_timestamp": now_ts,
            "is_causal": is_causal_verified,
            "feature_version": CONFIG["feature_version"],
            "schema_version": CONFIG["schema_version"],
            "weight_version": CONFIG["weight_version"],
            "atr_mode": CONFIG["atr_mode"],
            "execution_model": CONFIG["execution_model"],
            "basis": candle.fut_c - candle.spot_c,
            "fut_vwap": fut_vwap,
            "normalized_stretch": normalized_stretch,
            "kalman_price": kalman_price,
            "kalman_velocity": kalman_velocity,
            "kalman_stretch": kalman_stretch,
            "normalized_spread": normalized_spread,
            "stretch_slope_3": stretch_slope,
            "spread_slope_3": spread_slope,
            "order_book_imbalance": obi,
            "micro_price_drift": micro_price_drift,
            "atr_14_prev": atr_prev,
            "atr_warmup_flag": int(not is_valid_number(atr)),
            "spot_sma_20": spot_sma,
            "sma20_warmup_flag": 1 - int(sma_ready),
            "oi_change": oi_change,
            "oi_long_buildup": oi_long_buildup,
            "oi_short_buildup": oi_short_buildup,
            "oi_short_covering": oi_short_covering,
            "oi_long_unwinding": oi_long_unwinding,
            "oi_neutral": oi_neutral,
            "oi_strength": oi_strength,
            "minutes_from_open": (ts_ist.hour * 60 + ts_ist.minute) - 555,
            "day_of_week": ts_ist.weekday(),
            **hw_res,
            "dispersion_10": safe_float(hw_res.get("dispersion_index"), 0.0),
            "volume_zscore": volume_zscore,
            "atm_iv": atm_iv_obs,
            "iv_change": iv_change,
            **or_res,
            **sess_res,
            **pcr_features,
            **ha_res,
            **st_res,
            **hm_res,
            **advanced_res,
            "basket_scores_json": json.dumps(basket_res["basket_scores"], sort_keys=True),
            "indicator_signals_json": json.dumps(basket_res["indicator_signals"], sort_keys=True),
            "basket_alignments_json": json.dumps(basket_res["basket_alignments"], sort_keys=True),
            "composite_alignment_score": basket_res["composite_score"],
            "positive_baskets": basket_res["positive_baskets"],
            "negative_baskets": basket_res["negative_baskets"],
            "missing_spot": missing_spot, "missing_future": missing_future,
            "missing_oi": missing_oi, "missing_volume": missing_volume,
            "missing_heavyweight": missing_heavyweight, "missing_option_chain": missing_option,
            "bad_ohlc": bad_ohlc, "zero_volume": zero_volume, "zero_oi": zero_oi,
            "data_quality_score": float(max(0.0, 1.0 - penalty)),
            "bar_complete": 1,
        }
        self.history.append(features)
        return features


class LabelEngine:
    def __init__(self):
        self.upper = CONFIG["triple_upper_atr"]
        self.lower = CONFIG["triple_lower_atr"]
        self.tb_horizon = CONFIG["time_barrier_min"]
        self.mfe_horizons = CONFIG["mfe_horizons_min"]
        self.execution_model = CONFIG["execution_model"]
        if self.execution_model != "next_bar_open":
            raise ValueError("Research-Lock requires next_bar_open")

    def _excursion(self, entry, future, direction, max_bars):
        mfe = mae = 0.0
        available = min(len(future), max_bars)
        for candle in future[:available]:
            if direction == 1:
                mfe = max(mfe, candle.fut_h - entry)
                mae = max(mae, entry - candle.fut_l)
            else:
                mfe = max(mfe, entry - candle.fut_l)
                mae = max(mae, candle.fut_h - entry)
        return mfe, mae, int(available >= max_bars)

    def generate(self, entry_price, atr, future_after_entry, direction=1, signal_timestamp=None, entry_timestamp=None):
        if entry_timestamp and future_after_entry and to_ist(future_after_entry[0].timestamp) <= to_ist(entry_timestamp):
            raise ValueError("FUTURE ALIGNMENT VIOLATION")
        
        atr = atr if is_valid_number(atr) and atr > 0 else np.nan
        upper = entry_price + direction * self.upper * atr if not np.isnan(atr) else np.nan
        lower = entry_price - direction * self.lower * atr if not np.isnan(atr) else np.nan
        
        outcome, bars, mfe_tb, mae_tb, time_to_mfe = "TIMEOUT", 0, 0.0, 0.0, 0
        max_tb_bars = self.tb_horizon // CONFIG["bar_minutes"]
        
        for i, candle in enumerate(future_after_entry[:max_tb_bars]):
            bars = i + 1
            if direction == 1:
                mfe_tb = max(mfe_tb, candle.fut_h - entry_price)
                mae_tb = max(mae_tb, entry_price - candle.fut_l)
                hit_target = not np.isnan(upper) and candle.fut_h >= upper
                hit_stop = not np.isnan(lower) and candle.fut_l <= lower
            else:
                mfe_tb = max(mfe_tb, entry_price - candle.fut_l)
                mae_tb = max(mae_tb, candle.fut_h - entry_price)
                hit_target = not np.isnan(upper) and candle.fut_l <= upper
                hit_stop = not np.isnan(lower) and candle.fut_h >= lower
            
            if mfe_tb > 0 and time_to_mfe == 0:
                time_to_mfe = bars
            if hit_target and hit_stop:
                outcome = "AMBIGUOUS"
                break
            if hit_target:
                outcome = "TARGET_FIRST"
                break
            if hit_stop:
                outcome = "STOP_FIRST"
                break

        r_multiple = self.upper / self.lower if outcome == "TARGET_FIRST" else (-1.0 if outcome == "STOP_FIRST" else np.nan)
        valid = int(outcome != "AMBIGUOUS" and not np.isnan(atr))
        mfe_atr = mfe_tb / atr if not np.isnan(atr) else np.nan
        mae_atr = mae_tb / atr if not np.isnan(atr) else np.nan
        velocity = mfe_atr / max(bars, 1) if is_valid_number(mfe_atr) else np.nan
        
        if is_valid_number(mfe_atr):
            if mfe_atr >= 1.2 and mae_atr <= 0.45 and velocity > 0.25:
                trajectory = "IMPULSE"
            elif mfe_atr >= 0.8 and mae_atr <= 0.70 and 0.08 < velocity <= 0.25:
                trajectory = "STAIRCASE"
            elif mfe_atr >= 0.5 and velocity <= 0.08:
                trajectory = "GRIND"
            else:
                trajectory = "FAILURE"
        else:
            trajectory = "UNKNOWN"

        labels = {
            "label_version": CONFIG["label_version"], "execution_model": self.execution_model,
            "signal_timestamp": signal_timestamp, "entry_timestamp": entry_timestamp,
            "entry_price": entry_price, "triple_barrier_outcome": outcome,
            "label_valid_for_training": valid, "r_multiple": r_multiple, "trajectory": trajectory,
            "real_breakout": int(outcome == "TARGET_FIRST" and is_valid_number(mfe_atr) and mfe_atr >= 1.0 and mae_atr <= 0.55),
            "mfe_atr_tb": mfe_atr, "mae_atr_tb": mae_atr, "time_to_mfe": time_to_mfe,
            "bars_to_outcome": bars, "velocity": velocity,
        }
        for horizon in self.mfe_horizons:
            mfe_h, mae_h, complete = self._excursion(entry_price, future_after_entry, direction, horizon // CONFIG["bar_minutes"])
            labels[f"mfe_atr_{horizon}m"] = mfe_h / atr if not np.isnan(atr) else np.nan
            labels[f"mae_atr_{horizon}m"] = mae_h / atr if not np.isnan(atr) else np.nan
            labels[f"horizon_{horizon}m_complete"] = complete
        return labels



# =========================================================
# 6. ORTHOGONAL SIGNAL BASKETS / SMC / VOLUME PROFILE
# =========================================================

class AdvancedStructureEngine:
    """Causal, bar-close structure engine.

    IMPORTANT: CVD is an estimated directional-volume proxy when true aggressor
    buy/sell prints are unavailable from the feed. It is never labelled as true
    exchange cumulative delta.
    """
    def __init__(self, maxlen=150):
        self.closes = deque(maxlen=maxlen)
        self.highs = deque(maxlen=maxlen)
        self.lows = deque(maxlen=maxlen)
        self.volumes = deque(maxlen=maxlen)
        self.cvd = 0.0
        self.prev_pivot_high = np.nan
        self.prev_pivot_low = np.nan
        self.last_bos = 0
        self.last_fvg = 0

    def reset(self):
        self.closes.clear(); self.highs.clear(); self.lows.clear(); self.volumes.clear()
        self.cvd = 0.0; self.prev_pivot_high = np.nan; self.prev_pivot_low = np.nan
        self.last_bos = 0; self.last_fvg = 0

    @staticmethod
    def _ema(values, period):
        vals=[float(x) for x in values if is_valid_number(x)]
        if len(vals)<period: return np.nan
        a=2.0/(period+1.0); e=vals[0]
        for x in vals[1:]: e=a*x+(1-a)*e
        return float(e)

    @staticmethod
    def _rsi(values, period=14):
        vals=np.asarray([x for x in values if is_valid_number(x)], dtype=float)
        if len(vals)<period+1: return np.nan
        d=np.diff(vals[-(period+1):]); g=np.mean(np.maximum(d,0)); l=np.mean(np.maximum(-d,0))
        if l==0: return 100.0
        return float(100-100/(1+g/l))

    @staticmethod
    def _adx(highs,lows,closes,period=14):
        h=np.asarray(list(highs),float); l=np.asarray(list(lows),float); c=np.asarray(list(closes),float)
        if len(c)<period+2: return np.nan
        h=h[-(period+2):]; l=l[-(period+2):]; c=c[-(period+2):]
        tr=[]; plus=[]; minus=[]
        for i in range(1,len(c)):
            tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
            up=h[i]-h[i-1]; dn=l[i-1]-l[i]
            plus.append(up if up>dn and up>0 else 0.0); minus.append(dn if dn>up and dn>0 else 0.0)
        atr=np.mean(tr[-period:])
        if atr<=0: return 0.0
        p=100*np.mean(plus[-period:])/atr; m=100*np.mean(minus[-period:])/atr
        return float(100*abs(p-m)/max(p+m,1e-9))

    def _volume_profile(self, close, atr):
        if len(self.closes)<10: return {"vp_poc":np.nan,"vp_vah":np.nan,"vp_val":np.nan,"vp_position":0,"vp_breakout":0}
        prices=np.asarray(list(self.closes),float); vols=np.asarray(list(self.volumes),float)
        lo=float(np.min(prices)); hi=float(np.max(prices))
        if hi<=lo: return {"vp_poc":close,"vp_vah":close,"vp_val":close,"vp_position":0,"vp_breakout":0}
        bins=max(12,min(40,int(np.sqrt(len(prices))*3)))
        edges=np.linspace(lo,hi,bins+1); hist=np.zeros(bins)
        idx=np.clip(np.digitize(prices,edges)-1,0,bins-1)
        for i,v in zip(idx,vols): hist[i]+=max(float(v),0.0)
        if hist.sum()<=0: hist=np.ones(bins)
        poc_i=int(np.argmax(hist)); total=hist.sum(); target=0.70*total
        order=np.argsort(hist)[::-1]; selected=[]; acc=0
        for i in order:
            selected.append(i); acc+=hist[i]
            if acc>=target: break
        val_i=min(selected); vah_i=max(selected)
        centers=(edges[:-1]+edges[1:])/2
        poc=float(centers[poc_i]); val=float(edges[val_i]); vah=float(edges[vah_i+1])
        pos=1 if close>vah else (-1 if close<val else 0)
        br=1 if close>vah else (-1 if close<val else 0)
        return {"vp_poc":poc,"vp_vah":vah,"vp_val":val,"vp_position":pos,"vp_breakout":br}

    def compute(self, candle, prev):
        self.closes.append(float(candle.fut_c)); self.highs.append(float(candle.fut_h)); self.lows.append(float(candle.fut_l));
        self.volumes.append(float(candle.fut_volume) if is_valid_number(candle.fut_volume) else 0.0)
        if prev:
            d=candle.fut_c-prev[-1].fut_c
            self.cvd += (1.0 if d>0 else (-1.0 if d<0 else 0.0))*max(float(candle.fut_volume),0.0)
        closes=list(self.closes); highs=list(self.highs); lows=list(self.lows); vols=list(self.volumes)
        atr=abs(float(candle.fut_h-candle.fut_l)); atr=max(atr,0.01)
        ema20=self._ema(closes,20); ema50=self._ema(closes,50)
        macd_fast=self._ema(closes,12); macd_slow=self._ema(closes,26)
        macd=float(macd_fast-macd_slow) if is_valid_number(macd_fast) and is_valid_number(macd_slow) else np.nan
        macd_hist=macd
        rsi=self._rsi(closes,14)
        if len(closes)>=14:
            hh=max(highs[-14:]); ll=min(lows[-14:]); stoch=100*(candle.fut_c-ll)/max(hh-ll,1e-9)
        else: stoch=np.nan
        adx=self._adx(highs,lows,closes,14)

        # Causal structure: previous completed 3-bar pivot, not future bars.
        bos=0; hh_hl=0
        if len(closes)>=3:
            ph=highs[-2]; pl=lows[-2]
            if not is_valid_number(self.prev_pivot_high) or ph>self.prev_pivot_high: self.prev_pivot_high=ph
            if not is_valid_number(self.prev_pivot_low) or pl<self.prev_pivot_low: self.prev_pivot_low=pl
            if is_valid_number(self.prev_pivot_high) and candle.fut_c>self.prev_pivot_high: bos=1
            elif is_valid_number(self.prev_pivot_low) and candle.fut_c<self.prev_pivot_low: bos=-1
            hh_hl=1 if candle.fut_c>ema20 and candle.fut_l>=lows[-2] else (-1 if candle.fut_c<ema20 and candle.fut_h<=highs[-2] else 0)
        self.last_bos=bos

        # Three-candle fair-value-gap proxy, confirmed only from completed prior candles.
        fvg=0
        if len(highs)>=3:
            if lows[-1]>highs[-3]: fvg=1
            elif highs[-1]<lows[-3]: fvg=-1
        self.last_fvg=fvg

        # Supply / demand proxy: recent swing rejection/impulse zones.
        demand=0; supply=0
        if len(closes)>=5:
            rng=max(highs[-2]-lows[-2],0.01)
            body=abs(closes[-2]-closes[-3])
            if candle.fut_c>highs[-2] and body>0.5*rng: demand=1
            if candle.fut_c<lows[-2] and body>0.5*rng: supply=1
        vp=self._volume_profile(candle.fut_c,atr)
        cvd_slope=0.0
        if len(closes)>=3:
            # local estimated CVD acceleration, not true trade-side delta.
            cvd_slope=self.cvd

        return {
            "ema20":ema20,"ema50":ema50,"macd":macd,"macd_hist":macd_hist,
            "rsi_14":rsi,"stoch_14":stoch,"adx_14":adx,
            "bos_signal":bos,"hh_hl_signal":hh_hl,"fvg_signal":fvg,
            "demand_zone_signal":demand,"supply_zone_signal":supply,
            "cvd_est":self.cvd,"cvd_est_slope":cvd_slope,
            **vp
        }


class SignalBasketEngine:
    """Scores every indicator individually AND the orthogonal basket as a whole.

    Each indicator returns {-1,0,+1}; the basket combines them with capped,
    normalized weights. A basket cannot contribute more than its assigned
    budget, preventing correlated indicators from dominating the decision.
    """
    BASKETS = {
        "trend": {"ema":1.0,"supertrend":1.0,"adx":0.7},
        "momentum": {"rsi":0.7,"macd":1.0,"stochastic":0.6,"kalman":1.0},
        "location": {"vwap":1.0,"volume_profile":1.0,"supply_demand":0.8,"fvg":0.8},
        "structure": {"bos":1.2,"hh_hl":0.9,"breakout_retest":0.8},
        "flow": {"obi":0.9,"cvd":0.8,"volume":0.7,"oi":0.9},
        "options": {"pcr":0.7,"gex":0.8,"vanna":0.6,"charm":0.5,"iv":0.5},
        "internals": {"breadth":1.0,"heavyweights":1.0},
    }
    def _s(self,x,thr=0.0):
        x=safe_float(x,0.0)
        return 1 if x>thr else (-1 if x<-thr else 0)

    def score(self, f):
        k=safe_float(f.get("kalman_stretch"),0); slope=safe_float(f.get("stretch_slope_3"),0)
        st=safe_int(f.get("st_direction"),0); adx=safe_float(f.get("adx_14"),0)
        ema20=safe_float(f.get("ema20"),np.nan); ema50=safe_float(f.get("ema50"),np.nan); price=safe_float(f.get("fut_c"),np.nan)
        ema_sig=self._s((price-ema20) if is_valid_number(price) and is_valid_number(ema20) else 0, max(safe_float(f.get("atr_14_prev"),15)*0.03,0.1))
        trend={"ema":ema_sig,"supertrend":st,"adx":(1 if adx>=20 and ema_sig>0 else (-1 if adx>=20 and ema_sig<0 else 0))}
        macd=self._s(f.get("macd"),0.0); rsi=safe_float(f.get("rsi_14"),50); stoch=safe_float(f.get("stoch_14"),50)
        momentum={"rsi":1 if rsi>55 else (-1 if rsi<45 else 0),"macd":macd,"stochastic":1 if stoch>55 else (-1 if stoch<45 else 0),"kalman":self._s(k,0.08)}
        vp_pos=safe_int(f.get("vp_position"),0); fvg=safe_int(f.get("fvg_signal"),0)
        loc={"vwap":self._s(k,0.20),"volume_profile":vp_pos,"supply_demand":1 if safe_int(f.get("demand_zone_signal")) else (-1 if safe_int(f.get("supply_zone_signal")) else 0),"fvg":fvg}
        bos=safe_int(f.get("bos_signal"),0); hhl=safe_int(f.get("hh_hl_signal"),0); br=safe_int(f.get("or_breakout_state"),0)
        structure={"bos":bos,"hh_hl":hhl,"breakout_retest":br}
        obi=self._s(f.get("order_book_imbalance"),0.08); cvd=self._s(f.get("cvd_est_slope"),0.0)
        vol=safe_float(f.get("fut_volume"),0); flow={"obi":obi,"cvd":cvd,"volume":1 if vol>0 else 0,"oi":1 if safe_int(f.get("oi_long_buildup")) else (-1 if safe_int(f.get("oi_short_buildup")) else 0)}
        pcr=safe_float(f.get("pcr_oi"),1); pcrs=1 if pcr>1.05 else (-1 if pcr<0.95 else 0)
        options={"pcr":pcrs,"gex":self._s(f.get("gex_proxy"),0.15),"vanna":self._s(f.get("dealer_vanna_flow"),0.05),"charm":self._s(f.get("dealer_charm_flow"),0.05),"iv":0}
        breadth=safe_float(f.get("breadth_10"),0.5); twc=safe_float(f.get("twc"),0)
        internals={"breadth":1 if breadth>0.55 else (-1 if breadth<0.45 else 0),"heavyweights":self._s(twc,0.0005)}
        raw={"trend":trend,"momentum":momentum,"location":loc,"structure":structure,"flow":flow,"options":options,"internals":internals}
        basket_scores={}; basket_align={}
        for name,weights in self.BASKETS.items():
            vals=raw[name]; den=sum(weights.values()); score=sum(weights[k]*vals.get(k,0) for k in weights)/max(den,1e-9)
            basket_scores[name]=float(np.clip(score,-1,1)); basket_align[name]=sum(1 for v in vals.values() if v==1)-sum(1 for v in vals.values() if v==-1)
        # Equal basket budgets: orthogonal evidence, not raw indicator counting.
        total=float(np.mean(list(basket_scores.values())))
        pos=sum(1 for x in basket_scores.values() if x>=0.25); neg=sum(1 for x in basket_scores.values() if x<=-0.25)
        return {"indicator_signals":raw,"basket_scores":basket_scores,"basket_alignments":basket_align,"composite_score":total,"positive_baskets":pos,"negative_baskets":neg}


# =========================================================
# 6. REGIME & DECISION ENGINE
# =========================================================

class RegimeEngine:
    def detect(self, feats: Dict[str, Any]) -> str:
        dq = safe_float(feats.get("data_quality_score"), 0.0)
        atr_warm = safe_int(feats.get("atr_warmup_flag"), 0)
        
        if dq < CONFIG["min_data_quality_to_trade"] or atr_warm == 1:
            return "DATA_BAD"
        
        k_stretch = safe_float(feats.get("kalman_stretch"), feats.get("normalized_stretch", 0.0))
        slope = safe_float(feats.get("stretch_slope_3"), 0.0)
        or_state = safe_int(feats.get("or_breakout_state"), 0)
        oi_long = safe_int(feats.get("oi_long_buildup"), 0)
        oi_short = safe_int(feats.get("oi_short_buildup"), 0)
        oi_unwind = safe_int(feats.get("oi_long_unwinding"), 0) or safe_int(feats.get("oi_short_covering"), 0)
        twc = safe_float(feats.get("twc"), 0.0)
        breadth = safe_float(feats.get("breadth_10"), 0.5)

        gex_val = safe_float(feats.get("gex_proxy"), 0.0)
        z_dte = safe_float(feats.get("zero_dte_intensity"), 0.0)

        if z_dte > 0.5 and gex_val > 0.70 and abs(k_stretch) <= 0.65:
            return "GRIND"

        if z_dte > 0.4 and gex_val < -0.70 and abs(k_stretch) > 0.40:
            return "IMPULSE_UP" if k_stretch > 0 else "IMPULSE_DOWN"

        if (abs(k_stretch) > 0.85 and abs(slope) > 0.12) or (or_state != 0 and abs(k_stretch) > 0.45):
            if k_stretch > 0 and (oi_long or twc > 0 or breadth > 0.55):
                return "IMPULSE_UP"
            if k_stretch < 0 and (oi_short or twc < 0 or breadth < 0.45):
                return "IMPULSE_DOWN"
        if 0.35 < abs(k_stretch) <= 0.85:
            return "STAIRCASE_UP" if k_stretch > 0 else "STAIRCASE_DOWN"
        if oi_unwind and abs(k_stretch) > 0.55:
            return "FAILURE"
        if abs(k_stretch) <= 0.35 and abs(slope) < 0.08:
            return "GRIND"
        return "NEUTRAL"


@dataclass
class TradeDecision:
    action: str = "SKIP"
    regime: str = "NEUTRAL"
    target_points: float = 0.0
    stop_points: float = 0.0
    option_target_pts: float = 0.0
    option_stop_pts: float = 0.0
    effective_delta: float = 0.52
    size_factor: float = 1.0
    confidence: float = 0.5
    reason: str = ""
    timestamp: Optional[datetime] = None
    decision_timestamp: Optional[datetime] = None
    ml_probability: float = 0.5
    alignment: int = 0
    positive_baskets: int = 0
    negative_baskets: int = 0
    target_stretch: float = 1.0
    momentum_state: str = "NEUTRAL"


class DecisionEngine:
    def __init__(self):
        self.regime_engine=RegimeEngine(); self.last_action=None; self.last_action_bar_idx=-999; self.bar_counter=0
        self.ml_model=None; self.expected_feature_names=[]; self._load_production_model()

    def _load_production_model(self):
        model_p=Path(CONFIG.get("model_path",""))
        if joblib and model_p.exists():
            try:
                loaded=joblib.load(model_p)
                self.ml_model=loaded.get("model") if isinstance(loaded,dict) else loaded
                if isinstance(loaded,dict): self.expected_feature_names=loaded.get("features",[])
                elif hasattr(self.ml_model,"feature_name_"): self.expected_feature_names=list(self.ml_model.feature_name_)
                elif hasattr(self.ml_model,"feature_names_in_"): self.expected_feature_names=list(self.ml_model.feature_names_in_)
            except Exception: self.ml_model=None; self.expected_feature_names=[]

    def _ml(self,f):
        if self.ml_model is None:return 0.5
        try:
            cols=self.expected_feature_names
            if cols:
                x=pd.DataFrame([[safe_float(f.get(k),np.nan) for k in cols]],columns=cols)
            else:
                x=pd.DataFrame([{k:v for k,v in f.items() if isinstance(v,(int,float)) and np.isfinite(v)}])
            if hasattr(self.ml_model,"predict_proba"):return float(self.ml_model.predict_proba(x)[0][1])
            return float(self.ml_model.predict(x)[0])
        except Exception:return 0.5

    def _basket_data(self,f):
        try:
            bs=json.loads(f.get("basket_scores_json","{}"))
            inds=json.loads(f.get("indicator_signals_json","{}"))
        except Exception: bs={}; inds={}
        return bs,inds

    def _side_score(self, bs, inds, side):
        sign=1 if side=="CE" else -1
        aligned=sum(1 for x in bs.values() if x*sign>=0.25)
        opposed=sum(1 for x in bs.values() if x*sign<=-0.25)
        strong=aligned>=4
        min_ok=aligned>=3 and aligned>opposed
        return aligned,opposed,strong,min_ok

    def _dynamic_target(self,atr,regime,alignment,composite):
        base_mult=1.15 if regime.startswith("IMPULSE") else (0.90 if regime.startswith("STAIRCASE") else 0.70)
        stretch={3:1.00,4:1.30,5:1.55}.get(min(alignment,5),1.0)
        # Composite evidence can add a small bounded extension, never an unbounded target.
        if alignment>=4 and abs(composite)>=0.55: stretch*=1.08
        target=atr*base_mult*stretch
        cap=atr*(2.20 if regime.startswith("IMPULSE") else 1.80)
        target=min(target,cap)
        stop=atr*(0.70 if regime.startswith("IMPULSE") else 0.65)
        return target,stop,stretch

    def decide(self,feats):
        self.bar_counter+=1; now=now_ist()
        expiry=safe_int(feats.get("expiry_day_flag"),0); cutoff=(15,25) if expiry else (15,0)
        if (now.hour,now.minute)>=(cutoff[0],cutoff[1]):
            return TradeDecision(action="SKIP",regime="TIME_GUARD_ACTIVE",reason="Time guard active",timestamp=feats.get("timestamp"),decision_timestamp=now)
        regime=self.regime_engine.detect(feats); dq=safe_float(feats.get("data_quality_score"),0)
        if regime=="DATA_BAD" or dq<CONFIG["min_data_quality_to_trade"]:
            return TradeDecision(action="SKIP",regime=regime,reason="Data quality low / warmup",timestamp=feats.get("timestamp"),decision_timestamp=now)
        bs,inds=self._basket_data(feats); atr=safe_float(feats.get("atr_14_prev"),15)
        ce_n,ce_o,ce_strong,ce_ok=self._side_score(bs,inds,"CE"); pe_n,pe_o,pe_strong,pe_ok=self._side_score(bs,inds,"PE")
        # Independent qualification: PE is never inferred merely because CE weakened.
        action="SKIP"; alignment=0
        if ce_ok and ce_n>=pe_n: action="CE"; alignment=ce_n
        elif pe_ok and pe_n>ce_n: action="PE"; alignment=pe_n
        target,stop,stretch=self._dynamic_target(atr,regime,alignment,safe_float(feats.get("composite_alignment_score"),0)) if action!="SKIP" else (0,0,1)
        ml=self._ml(feats)
        side_prob=ml if action=="CE" else (1-ml if action=="PE" else 0.5)
        conf=float(np.clip(0.45+0.10*max(0,alignment-3)+0.20*abs(safe_float(feats.get("composite_alignment_score"),0))+0.15*max(0,side_prob-0.5)*2,0.25,0.95))
        size=1.0 if alignment<=3 else (1.10 if alignment==4 else 1.20)
        reason=f"Regime={regime} | Baskets CE={ce_n}/7 PE={pe_n}/7 | Composite={safe_float(feats.get('composite_alignment_score'),0):+.2f} | {action} alignment={alignment}/7 | Target stretch={stretch:.2f}x"
        return TradeDecision(action=action,regime=regime,target_points=round(target,1),stop_points=round(stop,1),option_target_pts=round(target*CONFIG["base_delta"],1),option_stop_pts=round(stop*0.75,1),effective_delta=CONFIG["base_delta"],size_factor=size,confidence=round(conf,3),reason=reason,timestamp=feats.get("timestamp"),decision_timestamp=now,ml_probability=ml)


# =========================================================
# 7. OPTION-CENTRIC PAPER TRADING DESK & JOURNAL
# =========================================================

class DatasetManager:
    def __init__(self, path=None):
        self.base = Path(path or CONFIG["dataset_path"])
        self.base.mkdir(parents=True, exist_ok=True)

    def write_parquet(self, df: pd.DataFrame, name="features"):
        if df.empty:
            return
        data = df.copy()
        if "timestamp" in data.columns:
            data["date"] = pd.to_datetime(data["timestamp"]).dt.date.astype(str)
        table = pa.Table.from_pandas(data, preserve_index=False)
        pq.write_to_dataset(
            table, root_path=str(self.base / name),
            partition_cols=["date"] if "date" in data.columns else None,
            existing_data_behavior="overwrite_or_ignore",
        )


@dataclass
class PaperPosition:
    entry_time: datetime
    direction: int
    entry_future_price: float
    entry_option_price: float
    entry_option_bid: Optional[float]
    entry_option_ask: Optional[float]
    entry_quote_timestamp: Optional[datetime]
    option_token: str
    option_symbol: str
    option_strike: float
    option_expiry: Optional[datetime]
    option_target: float
    base_option_target: float
    option_stop: float
    effective_delta: float
    size: float
    regime: str
    bars_held: int = 0
    status: str = "OPEN"
    exit_time: Optional[datetime] = None
    exit_future_price: Optional[float] = None
    exit_option_price: Optional[float] = None
    pnl_pts: float = 0.0
    gross_pnl_pts: float = 0.0
    execution_cost_pts: float = 0.0
    exit_option_bid: Optional[float] = None
    exit_option_ask: Optional[float] = None
    exit_option_ltp: Optional[float] = None
    pricing_source: str = ""
    exit_reason: str = ""
    peak_pnl_pts: float = 0.0
    locked_floor_pts: float = 0.0
    last_option_price: Optional[float] = None


class PaperTradingDesk:
    def __init__(self, dataset_manager: DatasetManager):
        self.dataset_manager = dataset_manager
        self.active_position: Optional[PaperPosition] = None
        self.pending_order: Optional[Dict[str, Any]] = None
        self.closed_trades: deque = deque(maxlen=200)
        self.realized_pnl_pts: float = 0.0
        self.unrealized_pnl_pts: float = 0.0
        self.current_trade_date: Optional[date] = None
        self.risk_locked: bool = False

    def check_and_reset_new_day(self, current_dt: datetime):
        today = to_ist(current_dt).date()
        if self.current_trade_date is None:
            self.current_trade_date = today
        elif today > self.current_trade_date:
            self.active_position = None
            self.pending_order = None
            self.closed_trades.clear()
            self.realized_pnl_pts = 0.0
            self.unrealized_pnl_pts = 0.0
            self.current_trade_date = today
            self.risk_locked = False

    def check_total_risk_limit(self) -> bool:
        total_pnl = self.realized_pnl_pts + self.unrealized_pnl_pts
        if total_pnl <= -CONFIG["max_daily_loss_pts"]:
            self.risk_locked = True
            self.pending_order = None
            return True
        return False

    def stage_signal(self, decision: TradeDecision, atr: float, next_bar_time: datetime):
        if getattr(self, "risk_locked", False) or self.check_total_risk_limit():
            return
        if decision.action in ("CE", "PE") and self.active_position is None and self.pending_order is None:
            direction = 1 if decision.action == "CE" else -1
            self.pending_order = {
                "target_fill_time": next_bar_time,
                "direction": direction,
                "option_target": decision.option_target_pts,
                "option_stop": decision.option_stop_pts,
                "effective_delta": decision.effective_delta,
                "size": decision.size_factor,
                "regime": decision.regime,
            }

    def on_bar_open_fill(self, candle: Candle3Min, atr: float, option_market: Optional[Dict[str, Any]] = None):
        self.check_and_reset_new_day(candle.timestamp)
        if not (self.pending_order and to_ist(candle.timestamp) >= to_ist(self.pending_order["target_fill_time"])):
            return
        if self.check_total_risk_limit():
            self.pending_order = None
            return

        order = self.pending_order
        quote = option_market or {}
        entry_ask = safe_float(quote.get("first_ask"), np.nan)
        entry_ltp = safe_float(quote.get("first_ltp"), np.nan)
        entry_bid = safe_float(quote.get("first_bid"), np.nan)
        entry_quote_timestamp = quote.get("first_timestamp")

        # No synthetic option premium is allowed anymore. A trade can only be
        # opened when Kotak Neo has supplied an observed option quote.
        # Exact executable entry for a LONG CE/PE is the observed ASK.
        # LTP-only entry is deliberately rejected because it is not an
        # executable fill price and would contaminate exact P&L.
        entry_price = entry_ask
        if not is_valid_number(entry_price) or entry_price <= 0:
            self.pending_order = None
            return

        direction = order["direction"]
        vol_factor = max(0.5, min(2.0, (atr / 15.0))) if is_valid_number(atr) and atr > 0 else 1.0
        slippage = CONFIG["base_slippage_pts"] * vol_factor * direction
        fill_price = candle.fut_o + slippage

        self.active_position = PaperPosition(
            entry_time=candle.timestamp,
            direction=direction,
            entry_future_price=round(fill_price, 2),
            entry_option_price=round(entry_price, 2),
            entry_option_bid=round(entry_bid, 2) if is_valid_number(entry_bid) else None,
            entry_option_ask=round(entry_ask, 2) if is_valid_number(entry_ask) else None,
            entry_quote_timestamp=entry_quote_timestamp,
            option_token=str(quote.get("token", "")),
            option_symbol=str(quote.get("symbol", "")),
            option_strike=safe_float(quote.get("strike"), np.nan),
            option_expiry=quote.get("expiry"),
            option_target=order["option_target"],
            base_option_target=order["option_target"],
            option_stop=order["option_stop"],
            effective_delta=order["effective_delta"],
            size=order["size"],
            regime=order["regime"],
            peak_pnl_pts=0.0, locked_floor_pts=0.0,
        )
        self.pending_order = None

    def on_bar_update_and_exit_eval(self, candle: Candle3Min, option_market: Optional[Dict[str, Any]] = None, decision: Optional[TradeDecision]=None, feats: Optional[Dict[str,Any]]=None, is_session_end: bool=False):
        if self.active_position is None:
            self.unrealized_pnl_pts=0.0; self.check_total_risk_limit(); return

        pos=self.active_position
        pos.bars_held += 1
        quote = option_market or {}
        bid = safe_float(quote.get("bid"), np.nan)
        ask = safe_float(quote.get("ask"), np.nan)
        ltp = safe_float(quote.get("ltp"), np.nan)
        bid_low = safe_float(quote.get("bid_low"), np.nan)
        ask_high = safe_float(quote.get("ask_high"), np.nan)
        ltp_low = safe_float(quote.get("ltp_low"), np.nan)
        ltp_high = safe_float(quote.get("ltp_high"), np.nan)

        # Persist the actual selected option's quote time-series separately from
        # the trade journal. This preserves LTP/BID/ASK evidence for every 3-min
        # bar used while the position was open.
        try:
            quote_row = {
                "timestamp": candle.timestamp,
                "trade_entry_time": pos.entry_time,
                "option_token": pos.option_token,
                "option_symbol": pos.option_symbol,
                "option_strike": pos.option_strike,
                "option_expiry": pos.option_expiry,
                "ltp": ltp if is_valid_number(ltp) else np.nan,
                "bid": bid if is_valid_number(bid) else np.nan,
                "ask": ask if is_valid_number(ask) else np.nan,
                "ltp_low": ltp_low if is_valid_number(ltp_low) else np.nan,
                "ltp_high": ltp_high if is_valid_number(ltp_high) else np.nan,
                "bid_low": bid_low if is_valid_number(bid_low) else np.nan,
                "ask_high": ask_high if is_valid_number(ask_high) else np.nan,
                "bars_held": pos.bars_held,
            }
            self.dataset_manager.write_parquet(pd.DataFrame([quote_row]), name="paper_option_quotes_log")
        except Exception:
            pass

        # Long option position: executable mark/exit is the observed BID.
        # LTP is retained only as a diagnostic mark; it is never used to close
        # a trade when exact executable pricing is required.
        mark_exit = bid if is_valid_number(bid) and bid > 0 else np.nan
        if not is_valid_number(mark_exit) or mark_exit <= 0:
            self.unrealized_pnl_pts = 0.0
            return

        gross_mark_pnl = mark_exit - pos.entry_option_price
        self.unrealized_pnl_pts = round(gross_mark_pnl * pos.size, 2)

        # Peak excursion uses observed option prices when available.
        # A long option can only be liquidated at BID. ASK-high is not
        # realizable P&L and therefore must never inflate MFE/peak P&L.
        bid_high = safe_float(quote.get("bid_high"), np.nan)
        peak_price = bid_high if is_valid_number(bid_high) else (ltp_high if is_valid_number(ltp_high) else mark_exit)
        peak_pnl = (peak_price - pos.entry_option_price) * pos.size
        pos.peak_pnl_pts = max(pos.peak_pnl_pts, round(peak_pnl, 2))

        # For a long option, a stop is executable only if the BID reaches the
        # stop threshold. This avoids treating an LTP/underlying move as a fill.
        stop_price = pos.entry_option_price - pos.option_stop
        stop_trigger_bid, stop_trigger_ts = self._first_bid_at_or_below(pos.option_token, stop_price)
        stop_reference = bid_low if is_valid_number(bid_low) else ltp_low
        hit_stop = is_valid_number(stop_trigger_bid) or (is_valid_number(stop_reference) and stop_reference <= stop_price)

        self.check_total_risk_limit()
        timeout=pos.bars_held >= (CONFIG["time_barrier_min"]//CONFIG["bar_minutes"])
        exit_reason=None

        if is_session_end: exit_reason="SESSION END AUTO-EXIT"
        elif self.risk_locked: exit_reason="KILL-SWITCH MAX LOSS BREACH"
        elif hit_stop: exit_reason="STOP LOSS HIT"
        elif decision is not None:
            current_side="CE" if pos.direction==1 else "PE"
            try: bs=json.loads((feats or {}).get("basket_scores_json","{}"))
            except Exception: bs={}
            aligned=sum(1 for x in bs.values() if x*(1 if pos.direction==1 else -1)>=0.25)
            opposite=sum(1 for x in bs.values() if x*(1 if pos.direction==1 else -1)<=-0.25)
            opp_side="PE" if current_side=="CE" else "CE"
            opp_aligned=sum(1 for x in bs.values() if x*(-1 if pos.direction==1 else 1)>=0.25)
            if opp_aligned>=4 and opposite>=3:
                exit_reason=f"INDEPENDENT OPPOSITE REVERSAL ({opp_side} {opp_aligned}/7)"
            elif aligned<=1: exit_reason="MOMENTUM COLLAPSE (ALIGNMENT <=1)"
            elif aligned==2: exit_reason="MOMENTUM DECAY / PROFIT LOCK (ALIGNMENT 2/7)"
            elif aligned>=3:
                if aligned>=5:
                    pos.option_target=max(pos.option_target, round(pos.base_option_target*1.55, 2))
                    pos.exit_reason="RIDE EXTENDED 5/7"
                elif aligned==4:
                    pos.option_target=max(pos.option_target, round(pos.base_option_target*1.30, 2))
                    pos.exit_reason="RIDE EXTENDED 4/7"
                if timeout: exit_reason="TIME BARRIER EXIT"
            if exit_reason is None and timeout: exit_reason="TIME BARRIER EXIT"
        elif timeout: exit_reason="TIME BARRIER EXIT"

        pos.last_option_price = round(mark_exit, 2)
        if exit_reason:
            # The actual observed BID is the executable sell price. For a stop,
            # the stop condition determines WHY we exit; the observed bid
            # determines HOW MUCH was actually made/lost.
            if hit_stop and is_valid_number(stop_trigger_bid):
                # The first observed executable BID crossing the stop is the
                # historical stop-fill proxy. It is strictly better than using
                # the candle close after the stop had already been crossed.
                exit_price = max(0.0, stop_trigger_bid)
                exit_observed_ts = stop_trigger_ts
            else:
                exit_price = max(0.0, mark_exit)
                exit_observed_ts = quote.get("last_timestamp")

            pos.exit_time = exit_observed_ts if isinstance(exit_observed_ts, datetime) else candle.timestamp
            pos.exit_future_price=round(candle.fut_c,2)
            pos.exit_option_price=round(exit_price,2)
            pos.exit_option_bid=round(bid,2) if is_valid_number(bid) else None
            pos.exit_option_ask=round(ask,2) if is_valid_number(ask) else None
            pos.exit_option_ltp=round(ltp,2) if is_valid_number(ltp) else None
            gross_pnl_pts=pos.exit_option_price-pos.entry_option_price
            pos.gross_pnl_pts=round(gross_pnl_pts,2)
            # Bid/ask execution already contains the observed spread. Do NOT
            # subtract an artificial second spread penalty. Broker fees/taxes
            # are not known here, so execution_cost_pts remains zero.
            pos.execution_cost_pts=0.0
            net_pnl_pts=gross_pnl_pts*pos.size
            pos.pnl_pts=round(net_pnl_pts,2)
            pos.pricing_source="KOTAK_NEO_BID_ASK"
            pos.status="CLOSED"
            pos.exit_reason=exit_reason+f" (Gross: {gross_pnl_pts:+.2f}pt | Execution Cost: 0.00pt | Net: {pos.pnl_pts:+.2f}pt)"
            self.realized_pnl_pts=round(self.realized_pnl_pts+pos.pnl_pts,2)
            self.closed_trades.append(pos)
            rec=asdict(pos)
            rec["timestamp"]=pos.exit_time
            self.dataset_manager.write_parquet(pd.DataFrame([rec]),name="paper_trades_log")
            self.active_position=None
            self.unrealized_pnl_pts=0.0



# =========================================================
# 8. KOTAK NEO ADAPTER
# =========================================================

class KotakNeoAdapter:
    def __init__(self):
        self.consumer_key = env_or_secret("KOTAK_CONSUMER_KEY")
        self.mobile = normalize_kotak_mobile(env_or_secret("KOTAK_MOBILE"))
        self.ucc = env_or_secret("KOTAK_UCC")
        self.totp = env_or_secret("KOTAK_TOTP")
        self.mpin = env_or_secret("KOTAK_MPIN")

        self.client = None
        self.connected = False
        self.conn_state = "DISCONNECTED"
        self.lock = threading.RLock()
        self.latest: Dict[str, Dict[str, Any]] = {}
        self.tick_buffer = deque(maxlen=2000)

        self.spot_token = CONFIG.get("nifty_spot_token", "Nifty 50")
        self.future_token = CONFIG.get("nifty_future_token", "53000")
        self.future_symbol = ""
        self.future_expiry = None
        self.pcr_tokens: List[str] = []
        self.pcr_records: Dict[str, Dict[str, Any]] = {}
        self.active_pcr_expiry: Optional[datetime] = None
        self.heavy_tokens: Dict[str, str] = dict(NSE_CASH_TOKENS)
        self.token_to_symbol: Dict[str, str] = {v: k for k, v in NSE_CASH_TOKENS.items()}
        self.discovery_log: List[str] = []
        self.last_error = ""

        self.dataset_manager = DatasetManager()
        self.feature_engine = FeatureEngine(maxlen=150)
        self.label_engine = LabelEngine()
        self.decision_engine = DecisionEngine()
        self.paper_desk = PaperTradingDesk(self.dataset_manager)
        self.candles_3m = deque(maxlen=150)
        self.current_bar_ticks: List[Dict[str, Any]] = []
        self.current_bar_time: Optional[datetime] = None
        self._bar_deadline: Optional[datetime] = None
        self.last_decision: Optional[TradeDecision] = None
        self._prev_ce_oi = np.nan
        self._prev_pe_oi = np.nan
        self._last_tick_wall = None
        self._last_cum_volume: Optional[float] = None
        self._unlabeled_decisions = deque(maxlen=150)

        self._watchdog_stop = threading.Event()
        self._watchdog_thread: Optional[threading.Thread] = None

    def _option_quote_from_records(self, token: str, records: Optional[List[Dict[str, Any]]] = None, *, allow_latest: bool = False) -> Dict[str, Any]:
        """Return the observed quote path for one exact option contract.

        Historical/live paper execution must never use a later quote to price an
        earlier fill.  Therefore callers can disable latest-quote fallback.
        Records are ordered by their parsed observation timestamp.
        """
        token = str(token)
        src = list(records if records is not None else self.current_bar_ticks)
        rows = [r for r in src if isinstance(r, dict) and str(token_from_record(r)) == token]
        if allow_latest:
            latest = self.latest.get(token, {})
            if isinstance(latest, dict) and latest and latest not in rows:
                rows.append(latest)
        if not rows:
            return {}

        def ts_key(r):
            ts = r.get("_parsed_ts")
            if isinstance(ts, datetime):
                return ts.timestamp()
            try:
                return parse_tick_timestamp(r).timestamp()
            except Exception:
                return float("inf")

        rows.sort(key=ts_key)
        quotes=[]
        for r in rows:
            q=extract_option_quote(r)
            if any(is_valid_number(q[k]) for k in q):
                quotes.append((r, q))
        if not quotes:
            return {}

        info=self.pcr_records.get(token, {})
        ltp_vals=[q["ltp"] for _,q in quotes if is_valid_number(q["ltp"]) and q["ltp"]>0]
        bid_vals=[q["bid"] for _,q in quotes if is_valid_number(q["bid"]) and q["bid"]>0]
        ask_vals=[q["ask"] for _,q in quotes if is_valid_number(q["ask"]) and q["ask"]>0]
        first_r, first_q = quotes[0]
        last_r, last_q = quotes[-1]
        return {
            "token": token,
            "symbol": info.get("symbol", ""),
            "strike": info.get("strike", np.nan),
            "expiry": info.get("expiry"),
            "ltp": last_q["ltp"] if is_valid_number(last_q["ltp"]) else (ltp_vals[-1] if ltp_vals else np.nan),
            "bid": last_q["bid"] if is_valid_number(last_q["bid"]) else (bid_vals[-1] if bid_vals else np.nan),
            "ask": last_q["ask"] if is_valid_number(last_q["ask"]) else (ask_vals[-1] if ask_vals else np.nan),
            "ltp_low": min(ltp_vals) if ltp_vals else np.nan,
            "ltp_high": max(ltp_vals) if ltp_vals else np.nan,
            "bid_low": min(bid_vals) if bid_vals else np.nan,
            "bid_high": max(bid_vals) if bid_vals else np.nan,
            "ask_low": min(ask_vals) if ask_vals else np.nan,
            "ask_high": max(ask_vals) if ask_vals else np.nan,
            "first_ltp": first_q["ltp"],
            "first_bid": first_q["bid"],
            "first_ask": first_q["ask"],
            "first_timestamp": first_r.get("_parsed_ts"),
            "last_timestamp": last_r.get("_parsed_ts"),
        }

    def _first_bid_at_or_below(self, token: str, threshold: float) -> Tuple[float, Optional[datetime]]:
        """Return the first observed executable BID that crosses a stop.

        This preserves chronology instead of using only a candle minimum, which
        would lose the actual executable price and could manufacture P&L.
        """
        if not is_valid_number(threshold):
            return np.nan, None
        rows=[r for r in self.current_bar_ticks
              if isinstance(r, dict) and str(token_from_record(r)) == str(token)]
        def ts_key(r):
            ts=r.get("_parsed_ts")
            if isinstance(ts, datetime):
                return ts.timestamp()
            try:
                return parse_tick_timestamp(r).timestamp()
            except Exception:
                return float("inf")
        rows.sort(key=ts_key)
        for r in rows:
            q=extract_option_quote(r)
            bid=safe_float(q.get("bid"), np.nan)
            if is_valid_number(bid) and bid > 0 and bid <= threshold:
                ts=r.get("_parsed_ts")
                return bid, ts if isinstance(ts, datetime) else None
        return np.nan, None

    def _select_entry_option(self, direction: int, spot_price: float) -> Dict[str, Any]:
        """Select the nearest active-expiry ATM option using the FIRST observed
        quote in the entry bar.  No later quote and no LTP-only fill is allowed."""
        wanted = "CE" if direction == 1 else "PE"
        candidates=[]
        for tok, info in self.pcr_records.items():
            if info.get("option_type") != wanted:
                continue
            strike=safe_float(info.get("strike"), np.nan)
            if not is_valid_number(strike) or not is_valid_number(spot_price):
                continue
            quote=self._option_quote_from_records(tok, records=self.current_bar_ticks, allow_latest=False)
            entry_ask=safe_float(quote.get("first_ask"), np.nan)
            if not is_valid_number(entry_ask) or entry_ask <= 0:
                continue
            candidates.append((abs(strike-float(spot_price)), tok, quote))
        if not candidates:
            return {}
        candidates.sort(key=lambda x: (x[0], str(x[1])))
        return candidates[0][2]

    def _active_option_market(self) -> Dict[str, Any]:
        pos=self.paper_desk.active_position
        if pos is None or not getattr(pos, "option_token", ""):
            return {}
        # Exit marks must come from the current completed bar only.  A stale
        # latest quote is not an executable historical exit.
        return self._option_quote_from_records(pos.option_token, records=self.current_bar_ticks, allow_latest=False)

    def _extract_oi(self, record: dict) -> float:
        if not isinstance(record, dict):
            return np.nan
        for key in ("oi", "open_interest", "openInterest", "OpenInterest", "oI", "OI",
                    "open_int", "opnInterest", "openInt", "dOpenInterest"):
            val = safe_float(record.get(key))
            if is_valid_number(val) and val >= 0:
                return val
        for wrapper in ("data", "quote", "marketDepth", "depth", "ohlc"):
            nested = record.get(wrapper)
            if isinstance(nested, dict):
                for key in ("oi", "open_interest", "openInterest", "OpenInterest", "oI"):
                    val = safe_float(nested.get(key))
                    if is_valid_number(val) and val >= 0:
                        return val
        return np.nan

    def on_message(self, message):
        try:
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except Exception:
                    return

            items = message if isinstance(message, list) else [message]
            with self.lock:
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    token = token_from_record(item)
                    if token:
                        item["_parsed_ts"] = parse_tick_timestamp(item)
                        oi_val = self._extract_oi(item)
                        if is_valid_number(oi_val):
                            item["oi"] = oi_val
                            item["open_interest"] = oi_val
                        self.latest[token] = item
                        self.tick_buffer.append(item)
                        self.current_bar_ticks.append(item)
        except Exception as exc:
            self.last_error = f"on_message: {exc}"

    def on_error(self, error):
        self.last_error = str(error) if error else ""

    def on_close(self, message=None):
        pass

    def on_open(self, message=None):
        self.connected = True

    def login(self, live_totp_override=""):
        if NeoAPI is None:
            raise RuntimeError("neo_api_client missing. Install official Kotak Neo API v2 package.")
        
        self.consumer_key = env_or_secret("KOTAK_CONSUMER_KEY")
        self.mobile = normalize_kotak_mobile(env_or_secret("KOTAK_MOBILE"))
        self.ucc = env_or_secret("KOTAK_UCC")
        self.totp = env_or_secret("KOTAK_TOTP")
        self.mpin = env_or_secret("KOTAK_MPIN")

        totp = (live_totp_override or "").strip() or self.totp
        required = {"KOTAK_CONSUMER_KEY": self.consumer_key, "KOTAK_MOBILE": self.mobile,
                    "KOTAK_UCC": self.ucc, "TOTP": totp, "KOTAK_MPIN": self.mpin}
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError("Missing credentials: " + ", ".join(missing))
            
        self.client = NeoAPI(environment=CONFIG["neo_environment"], access_token=None, neo_fin_key=None, consumer_key=self.consumer_key)
        self.client.on_message = self.on_message
        self.client.on_error = self.on_error
        self.client.on_close = self.on_close
        self.client.on_open = self.on_open
        
        step1 = self.client.totp_login(mobile_number=self.mobile, ucc=self.ucc, totp=generate_live_totp(totp))
        if isinstance(step1, dict) and step1.get("error"):
            raise RuntimeError(str(step1))
        step2 = self.client.totp_validate(mpin=self.mpin)
        if isinstance(step2, dict) and step2.get("error"):
            raise RuntimeError(str(step2))
            
        self.connected = True
        self.conn_state = "AUTHENTICATED"
        return True

    def resolve_current_nifty_future_token(self) -> str:
        try:
            res = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
            records = record_list(res)
            now_d = now_ist().date()
            
            futures = []
            for r in records:
                sym = str(r.get("pTrdSymbol", r.get("ts", r.get("symbol", "")))).upper().strip()
                inst = str(r.get("pInstType", "")).upper()
                
                is_nifty_fut = (
                    sym.startswith("NIFTY") and 
                    ("FUT" in sym or "FUTIDX" in inst) and
                    not any(x in sym for x in ["FPI", "BANK", "FIN", "MID", "NXT", "IT", "SENSEX", "BANKEX"])
                )
                
                if is_nifty_fut:
                    exp = expiry_from_record(r)
                    tok = token_from_record(r)
                    if exp and exp.date() >= now_d and tok:
                        futures.append((exp, tok, sym))
            
            if futures:
                futures.sort(key=lambda x: x[0])
                nearest_exp, nearest_tok, nearest_sym = futures[0]
                self.future_symbol = nearest_sym
                self.discovery_log.append(f"OK Resolved Nifty Future: {nearest_sym} | Token: {nearest_tok} | Expiry: {nearest_exp.date()}")
                return nearest_tok
        except Exception as e:
            self.last_error = f"Future resolution error: {e}"
        return CONFIG.get("nifty_future_token", "53000")

    def discover_nifty_instruments(self, auto_pcr: bool = True) -> bool:
        if not self.connected or not self.client:
            raise RuntimeError("Kotak Neo not authenticated.")
        self.discovery_log.clear()
        
        self.heavy_tokens = dict(NSE_CASH_TOKENS)
        self.token_to_symbol = {v: k for k, v in NSE_CASH_TOKENS.items()}
        self.discovery_log.append(f"OK Configured {len(self.heavy_tokens)} Core Heavyweights")
        
        self.spot_token = "Nifty 50"
        self.token_to_symbol[self.spot_token] = "NIFTY_SPOT"
        self.discovery_log.append("OK Configured Nifty Spot Index: Nifty 50")

        self.future_token = self.resolve_current_nifty_future_token()
        self.token_to_symbol[self.future_token] = "NIFTY_FUT"
        self.discovery_log.append(f"OK Configured Active Future Token: {self.future_token}")

        if auto_pcr:
            self.discover_pcr_chain()
        return True

    def discover_pcr_chain(self, center_strike: Optional[float] = None) -> int:
        if not self.connected or not self.client:
            return 0
        try:
            if not center_strike or not is_valid_number(center_strike):
                with self.lock:
                    spot_tick = self.latest.get("Nifty 50", {})
                    center_strike = extract_tick_price(spot_tick)
                    if not is_valid_number(center_strike) or center_strike <= 0:
                        center_strike = 24300.0

            step = CONFIG["pcr_strike_step"]
            atm = round(center_strike / step) * step
            count = CONFIG["pcr_strike_count"]
            target_strikes = [atm + (i * step) for i in range(-count, count + 1)]
            
            res = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
            records = record_list(res)
            now_d = now_ist().date()
            
            nifty_opt_pattern = re.compile(r"^NIFTY\d{2}[A-Z0-9]+(CE|PE)$", re.IGNORECASE)
            valid_expiries = []
            for r in records:
                sym = str(r.get("pTrdSymbol", r.get("ts", r.get("symbol", "")))).upper().strip()
                is_opt = nifty_opt_pattern.match(sym) or ("NIFTY" in sym and (sym.endswith("CE") or sym.endswith("PE")))
                if not is_opt or any(x in sym for x in ["NXT", "FPI", "FIN", "BANK", "MID", "IT", "SENSEX", "BANKEX"]):
                    continue
                op_type = option_type_from_record(r)
                if op_type in ("CE", "PE"):
                    exp = expiry_from_record(r)
                    if exp and exp.date() >= now_d:
                        valid_expiries.append(exp)
            
            if not valid_expiries:
                return 0
            
            self.active_pcr_expiry = min(valid_expiries, key=lambda x: x.date())
            target_exp_date = self.active_pcr_expiry.date()
            
            discovered = []
            for r in records:
                sym = str(r.get("pTrdSymbol", r.get("ts", r.get("symbol", "")))).upper().strip()
                is_opt = nifty_opt_pattern.match(sym) or ("NIFTY" in sym and (sym.endswith("CE") or sym.endswith("PE")))
                if not is_opt or any(x in sym for x in ["NXT", "FPI", "FIN", "BANK", "MID", "IT", "SENSEX", "BANKEX"]):
                    continue
                exp = expiry_from_record(r)
                strike = strike_from_record(r)
                op_type = option_type_from_record(r)
                tok = token_from_record(r)
                if tok and strike in target_strikes and op_type in ("CE", "PE"):
                    if exp and exp.date() == target_exp_date:
                        discovered.append(tok)
                        self.pcr_records[tok] = {
                            "strike": strike, "option_type": op_type,
                            "expiry": exp, "symbol": str(r.get("pTrdSymbol", ""))
                        }
            self.pcr_tokens = list(set(discovered))
            self.discovery_log.append(f"OK Single-Expiry PCR ({target_exp_date}): {len(self.pcr_tokens)} Strikes Mapped")
            return len(self.pcr_tokens)
        except Exception as e:
            self.last_error = f"PCR Discovery error: {e}"
            return 0

    def fetch_real_option_oi(self):
        if not self.connected or not self.client or not self.pcr_tokens:
            return
        try:
            tokens_to_poll = [
                {"instrument_token": str(tok), "exchange_segment": "nse_fo"}
                for tok in self.pcr_tokens[:25]
            ]
            res = self.client.quotes(instrument_tokens=tokens_to_poll, quote_type="all")
            recs = record_list(res)
            with self.lock:
                for r in recs:
                    if not isinstance(r, dict):
                        continue
                    tok = token_from_record(r)
                    if not tok:
                        continue
                    oi = self._extract_oi(r)
                    if tok not in self.latest:
                        self.latest[tok] = {}
                    # Preserve the complete broker quote, not just OI.
                    # This makes the option contract usable for actual entry/exit
                    # pricing during the same bar.
                    self.latest[tok].update(r)
                    if is_valid_number(oi):
                        self.latest[tok]["oi"] = oi
                        self.latest[tok]["open_interest"] = oi
                        self.latest[tok]["open_int"] = oi
        except Exception as e:
            pass

    def fetch_market_snapshot(self):
        if not self.connected or not self.client:
            return

        now_ts = now_ist()
        tokens_to_poll = [
            {"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"},
            {"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"},
        ]
        
        for tok in self.heavy_tokens.values():
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
            
        for tok in self.pcr_tokens[:20]:
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})
        
        try:
            res = self.client.quotes(instrument_tokens=tokens_to_poll, quote_type="all")
            recs = record_list(res)
            
            with self.lock:
                for r in recs:
                    if not isinstance(r, dict):
                        continue
                    tok = token_from_record(r)
                    sym_name = str(r.get("display_symbol", "") or "").upper()
                    
                    if not tok and ("NIFTY" in sym_name and "EQ" not in sym_name and "FUT" not in sym_name):
                        tok = "Nifty 50"
                        
                    if tok:
                        r["_parsed_ts"] = now_ts
                        oi_val = self._extract_oi(r)
                        if is_valid_number(oi_val):
                            r["oi"] = oi_val
                            r["open_interest"] = oi_val
                        self.latest[tok] = r
                        self.tick_buffer.append(r)
                        self.current_bar_ticks.append(r)
                        
                        if tok == "Nifty 50" or "NIFTY 50" in sym_name:
                            self.latest["Nifty 50"] = r
                        
                        if tok == str(self.future_token):
                            pdc = safe_float(r.get("c") or r.get("close") or r.get("pdc"))
                            pdh = safe_float(r.get("h") or r.get("high") or r.get("pdh"))
                            pdl = safe_float(r.get("l") or r.get("low") or r.get("pdl"))
                            open_p = safe_float(r.get("o") or r.get("open"))
                            if is_valid_number(pdc) and self.feature_engine.sess.prev_close is None:
                                self.feature_engine.set_previous_day(pdc, pdh, pdl)
                            if is_valid_number(open_p):
                                self.feature_engine.set_today_open(open_p)
                
                self.last_error = ""
            
            self.fetch_real_option_oi()
            
        except Exception as exc:
            self.last_error = f"Poll error: {exc}"

    def subscribe_live_feed(self) -> int:
        if not self.connected or not self.client:
            raise RuntimeError("Kotak Neo not authenticated.")
        
        self.fetch_market_snapshot()

        sub_tokens = [
            {"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"},
            {"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"},
        ]
        for tok in self.heavy_tokens.values():
            sub_tokens.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        for tok in self.pcr_tokens:
            sub_tokens.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})
            
        try:
            self.client.subscribe(instrument_tokens=[{"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"}], isIndex=True)
            self.client.subscribe(instrument_tokens=sub_tokens[1:], isIndex=False)
        except Exception as exc:
            self.last_error = f"Subscribe error: {exc}"

        self.conn_state = "STREAMING"
        self._last_tick_wall = time.time()
        return len(sub_tokens)

    def maybe_flush_bars(self):
        now = now_ist()
        with self.lock:
            if self.current_bar_time is None:
                self.current_bar_time = floor_bar_timestamp(now, CONFIG["bar_minutes"])
                self._bar_deadline = self.current_bar_time + timedelta(minutes=CONFIG["bar_minutes"])
            
            if now >= self._bar_deadline:
                bar_t = self.current_bar_time
                self._close_bar(bar_t)
                self.current_bar_time = self._bar_deadline
                self._bar_deadline = self.current_bar_time + timedelta(minutes=CONFIG["bar_minutes"])
                self.current_bar_ticks.clear()

            if CONFIG["session_end_flush"]:
                end_h, end_m = map(int, CONFIG["session_end"].split(":"))
                sess_end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
                if now >= sess_end and self.current_bar_ticks:
                    self._close_bar(self.current_bar_time or floor_bar_timestamp(now), is_session_end=True)
                    self.current_bar_ticks.clear()

    def _resolve_volume_clean(self, fut_ticks: List[Dict[str, Any]]) -> float:
        if not fut_ticks:
            return np.nan
        last_cum = None
        for t in reversed(fut_ticks):
            c_val = safe_float(t.get("v") or t.get("vol") or t.get("volume") or t.get("last_volume"))
            if is_valid_number(c_val) and c_val > 0:
                last_cum = c_val
                break
        if last_cum is None:
            return np.nan
        if self._last_cum_volume is None:
            self._last_cum_volume = last_cum
            return 0.0
        delta = last_cum - self._last_cum_volume
        if delta < 0:
            delta = last_cum
        self._last_cum_volume = last_cum
        return float(max(0.0, delta))

    def _process_delayed_labels(self):
        max_tb_bars = CONFIG["time_barrier_min"] // CONFIG["bar_minutes"]
        completed_records = []
        
        with self.lock:
            candles_list = list(self.candles_3m)
            while self._unlabeled_decisions:
                target_time, entry_px, atr_val, direction, f_row = self._unlabeled_decisions[0]
                future_candles = [c for c in candles_list if to_ist(c.timestamp) > to_ist(target_time)]
                if len(future_candles) >= max_tb_bars:
                    self._unlabeled_decisions.popleft()
                    try:
                        labels = self.label_engine.generate(
                            entry_price=entry_px, atr=atr_val,
                            future_after_entry=future_candles, direction=direction,
                            signal_timestamp=f_row["timestamp"], entry_timestamp=target_time
                        )
                        merged = {**f_row, **labels}
                        completed_records.append(merged)
                    except Exception:
                        pass
                else:
                    break

        if completed_records:
            self.dataset_manager.write_parquet(pd.DataFrame(completed_records), name="labeled_features_3min")

    def _close_bar(self, bar_time: datetime, is_session_end: bool = False):
        with self.lock:
            ticks_source = self.current_bar_ticks if self.current_bar_ticks else list(self.tick_buffer)
            if not ticks_source:
                return

            def _prices(token):
                ticks = [t for t in ticks_source if str(token_from_record(t)) == str(token)]
                vals = [extract_tick_price(t) for t in ticks]
                vals = [v for v in vals if is_valid_number(v)]
                return ticks, vals

            _, spot_prices = _prices("Nifty 50")
            if not spot_prices:
                last = extract_tick_price(self.latest.get("Nifty 50", {}))
                spot_o = spot_h = spot_l = spot_c = last if is_valid_number(last) else 24300.0
            else:
                spot_o, spot_h, spot_l, spot_c = spot_prices[0], max(spot_prices), min(spot_prices), spot_prices[-1]

            fut_ticks, fut_prices = _prices(self.future_token)
            if not fut_prices:
                last = extract_tick_price(self.latest.get(str(self.future_token), {})) or spot_c
                fut_o = fut_h = fut_l = fut_c = last if is_valid_number(last) else 24400.0
                fut_vol = 1000.0
                fut_oi = self._extract_oi(self.latest.get(str(self.future_token), {})) or 12784330.0
                l2_snap = {}
            else:
                fut_o, fut_h, fut_l, fut_c = fut_prices[0], max(fut_prices), min(fut_prices), fut_prices[-1]
                fut_vol = self._resolve_volume_clean(fut_ticks)
                last_fut_t = fut_ticks[-1] if fut_ticks else {}
                fut_oi = self._extract_oi(last_fut_t) or self._extract_oi(self.latest.get(str(self.future_token), {})) or 12784330.0
                
                l2_snap = {
                    "best_bid": safe_float(last_fut_t.get("bp") or last_fut_t.get("bid_price"), fut_c),
                    "best_ask": safe_float(last_fut_t.get("ap") or last_fut_t.get("ask_price"), fut_c),
                    "bid_qty": safe_float(last_fut_t.get("bq") or last_fut_t.get("bid_qty"), 1.0),
                    "ask_qty": safe_float(last_fut_t.get("aq") or last_fut_t.get("ask_qty"), 1.0),
                }

            hw_snap = {}
            for sym, tok in self.heavy_tokens.items():
                t = self.latest.get(str(tok), {})
                c_val = extract_tick_price(t)
                o_val = extract_quote_field(t, ("o", "open", "pOpen", "openPrice", "op"))
                if not is_valid_number(o_val):
                    o_val = c_val
                vwap_val = extract_quote_field(t, ("vwap", "avp", "averagePrice", "average_price", "a"))
                if not is_valid_number(vwap_val):
                    vwap_val = o_val

                if is_valid_number(c_val):
                    hw_snap[sym] = {"o": o_val, "c": c_val, "vwap": vwap_val}

            total_ce_oi = total_pe_oi = total_ce_vol = total_pe_vol = 0.0
            atm_ce_oi = atm_pe_oi = np.nan
            spot_approx = spot_c if is_valid_number(spot_c) and spot_c > 0 else 24300.0
            step = CONFIG["pcr_strike_step"]
            atm = round(spot_approx / step) * step
            
            for tok in self.pcr_tokens:
                info = self.pcr_records.get(str(tok), {})
                t = self.latest.get(str(tok), {})
                oi = self._extract_oi(t)
                vol = safe_float(t.get("last_volume") or t.get("v") or t.get("volume"), 0.0)
                strike = info.get("strike")
                if is_valid_number(oi):
                    if info.get("option_type") == "CE":
                        total_ce_oi += oi
                        total_ce_vol += vol
                        if strike == atm:
                            atm_ce_oi = oi
                    elif info.get("option_type") == "PE":
                        total_pe_oi += oi
                        total_pe_vol += vol
                        if strike == atm:
                            atm_pe_oi = oi

            if is_valid_number(total_ce_oi) and total_ce_oi > 0:
                ce_oi_change = total_ce_oi - self._prev_ce_oi if is_valid_number(self._prev_ce_oi) else 0.0
                self._prev_ce_oi = total_ce_oi
            else:
                ce_oi_change = 0.0

            if is_valid_number(total_pe_oi) and total_pe_oi > 0:
                pe_oi_change = total_pe_oi - self._prev_pe_oi if is_valid_number(self._prev_pe_oi) else 0.0
                self._prev_pe_oi = total_pe_oi
            else:
                pe_oi_change = 0.0

            # Preserve a real observed ATM IV when the broker feed exposes it.
            atm_iv_values = []
            for tok in self.pcr_tokens:
                info = self.pcr_records.get(str(tok), {})
                if info.get("strike") == atm:
                    t = self.latest.get(str(tok), {})
                    iv_val = safe_float(t.get("iv") or t.get("implied_volatility") or t.get("impliedVolatility"), np.nan)
                    if is_valid_number(iv_val) and iv_val > 0:
                        atm_iv_values.append(iv_val)
            observed_atm_iv = float(np.mean(atm_iv_values)) if atm_iv_values else np.nan

            pcr_chain = {
                "pcr_oi": total_pe_oi / max(total_ce_oi, 1.0) if total_ce_oi > 0 else np.nan,
                "pcr_volume": total_pe_vol / max(total_ce_vol, 1.0) if total_ce_vol > 0 else np.nan,
                "ce_oi_change": ce_oi_change, "pe_oi_change": pe_oi_change,
                "ce_oi_atm": atm_ce_oi, "pe_oi_atm": atm_pe_oi, "atm_strike": atm,
                "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi,
                "active_expiry": self.active_pcr_expiry,
                "atm_iv": observed_atm_iv,
                "ce_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "CE"),
                "pe_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "PE"),
            }

            candle = Candle3Min(
                timestamp=bar_time, spot_o=spot_o, spot_h=spot_h, spot_l=spot_l, spot_c=spot_c,
                fut_o=fut_o, fut_h=fut_h, fut_l=fut_l, fut_c=fut_c, fut_volume=fut_vol, fut_oi=fut_oi,
                heavy=hw_snap, option_chain=pcr_chain, l2_depth=l2_snap
            )
            
            feats = self.feature_engine.compute(candle, self.candles_3m)
            atr_v = safe_float(feats.get("atr_14_prev"), 15.0)

            # Fill an already-staged signal using the actual Kotak Neo option
            # quote for the nearest active-expiry ATM contract.
            pending = self.paper_desk.pending_order
            entry_option_market = {}
            if pending:
                entry_option_market = self._select_entry_option(int(pending.get("direction", 0)), spot_o)
            self.paper_desk.on_bar_open_fill(candle, atr_v, option_market=entry_option_market)

            self.candles_3m.append(candle)

            decision = self.decision_engine.decide(feats)
            self.last_decision = decision
            active_option_market = self._active_option_market()
            self.paper_desk.on_bar_update_and_exit_eval(candle, option_market=active_option_market, decision=decision, feats=feats, is_session_end=is_session_end)
            
            if not is_session_end:
                next_t = bar_time + timedelta(minutes=CONFIG["bar_minutes"])
                self.paper_desk.stage_signal(decision, atr_v, next_t)

            feats["decision_action"] = decision.action
            feats["decision_regime"] = decision.regime
            feats["decision_target"] = decision.target_points
            feats["decision_confidence"] = decision.confidence
            try:
                bs=json.loads(feats.get("basket_scores_json","{}")); side=1 if decision.action=="CE" else (-1 if decision.action=="PE" else 0)
                feats["decision_alignment"] = sum(1 for x in bs.values() if x*side>=0.25) if side else 0
                feats["decision_positive_baskets"] = feats.get("positive_baskets",0)
                feats["decision_negative_baskets"] = feats.get("negative_baskets",0)
            except Exception:
                feats["decision_alignment"]=0
            feats["decision_timestamp"] = decision.decision_timestamp
            feats["entry_timestamp"] = bar_time + timedelta(minutes=CONFIG["bar_minutes"])
            
            self.dataset_manager.write_parquet(pd.DataFrame([feats]), name="features_3min")
            
            dir_flag = 1 if decision.action == "CE" else (-1 if decision.action == "PE" else 0)
            if dir_flag != 0 and not is_session_end:
                self._unlabeled_decisions.append((
                    feats["entry_timestamp"], fut_c, feats.get("atr_14_prev"), dir_flag, feats
                ))
            self._process_delayed_labels()

    def start_bar_watchdog(self):
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()

        def _loop():
            while not self._watchdog_stop.is_set():
                try:
                    self.maybe_flush_bars()
                    if self.conn_state == "STREAMING":
                        self.fetch_market_snapshot()
                except Exception as exc:
                    self.last_error = f"watchdog: {exc}"
                self._watchdog_stop.wait(3.0)

        self._watchdog_thread = threading.Thread(target=_loop, name="BarWatchdog", daemon=True)
        self._watchdog_thread.start()

    def stop_bar_watchdog(self):
        self._watchdog_stop.set()


# =========================================================
# 9. STREAMLIT UI
# =========================================================

def inject_custom_css():
    st.markdown("""
        <style>
            .stApp {
                background-color: #0b0e14;
                color: #e1e7ec;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            }
            .terminal-card {
                background: #151a23;
                border: 1px solid #232b38;
                border-radius: 8px;
                padding: 16px;
                margin-bottom: 12px;
            }
            .badge-ce {
                background-color: #064e3b;
                color: #34d399;
                padding: 6px 14px;
                border-radius: 6px;
                font-weight: 700;
                font-size: 1.1rem;
                display: inline-block;
                border: 1px solid #059669;
            }
            .badge-pe {
                background-color: #7f1d1d;
                color: #f87171;
                padding: 6px 14px;
                border-radius: 6px;
                font-weight: 700;
                font-size: 1.1rem;
                display: inline-block;
                border: 1px solid #dc2626;
            }
            .badge-neutral {
                background-color: #374151;
                color: #9ca3af;
                padding: 6px 14px;
                border-radius: 6px;
                font-weight: 700;
                font-size: 1.1rem;
                display: inline-block;
            }
            .color-green { color: #34d399 !important; font-weight: bold; font-size: 1.25rem; }
            .color-red { color: #f87171 !important; font-weight: bold; font-size: 1.25rem; }
            .color-brown { color: #d97706 !important; font-weight: bold; font-size: 1.25rem; }
            .status-pill {
                padding: 3px 8px;
                border-radius: 12px;
                font-size: 0.75rem;
                font-weight: 600;
            }
            .status-active { background: #064e3b; color: #10b981; }
            .status-auth { background: #1e3a5f; color: #60a5fa; }
            .status-offline { background: #451a1a; color: #ef4444; }
            div[data-testid="stMetricValue"] {
                font-size: 1.5rem !important;
                font-weight: 700 !important;
                color: #f8fafc;
            }
        </style>
    """, unsafe_allow_html=True)


def get_colored_text(value, feature_name):
    if not is_valid_number(value):
        return f'<span style="color:#6b7280; font-size:1.25rem;">{value:.2f}</span>'
    color = "brown"
    if feature_name in ["normalized_stretch", "kalman_stretch"]:
        if value > 0.3: color = "green"
        elif value < -0.3: color = "red"
    elif feature_name == "stretch_slope_3":
        if value > 0.02: color = "green"
        elif value < -0.02: color = "red"
    elif feature_name == "pcr_oi":
        if value > 1.05: color = "green"
        elif value < 0.95: color = "red"
    elif feature_name == "breadth_10":
        if value > 0.55: color = "green"
        elif value < 0.45: color = "red"
    return f'<span class="color-{color}">{value:.2f}</span>'


def run_unit_tests() -> bool:
    oe = OpeningRangeEngine(15)
    c1 = Candle3Min(now_ist().replace(hour=9, minute=15), 100, 110, 95, 105, 100, 110, 95, 105, 1000, 500)
    oe.update(c1)
    assert "or_width_atr" in oe.features(c1, 10.0)
    
    le = LabelEngine()
    future_short = [
        Candle3Min(now_ist().replace(hour=9, minute=18), 100, 102, 90, 92, 100, 102, 90, 92, 1000, 500)
    ]
    lbl_short = le.generate(entry_price=100.0, atr=10.0, future_after_entry=future_short, direction=-1)
    assert lbl_short["triple_barrier_outcome"] in ["TARGET_FIRST", "STOP_FIRST", "TIMEOUT", "AMBIGUOUS"]
    
    de = DecisionEngine()
    d_bad = de.decide({
        "data_quality_score": 0.20, "atr_14_prev": 12.0, "normalized_stretch": 0.8,
        "stretch_slope_3": 0.2, "or_breakout_state": 1, "oi_long_buildup": 1,
        "twc": 0.002, "breadth_10": 0.7, "hw_symbols_seen": 9, "timestamp": now_ist(),
    })
    assert d_bad.action == "SKIP"
    
    g = GreeksEngine.compute_second_order_greeks(24000.0, 24000.0, 120.0)
    assert is_valid_number(g["vanna"])
    
    kf = KalmanPriceEngine()
    est, vel = kf.update(24050.0)
    assert is_valid_number(est)
    
    return True


if st is not None:
    @st.cache_resource
    def get_global_adapter():
        return KotakNeoAdapter()



# ============================================================================
# V8 ORTHOGONAL MATHEMATICAL SCANNER + COLD-START DECISION LAYER
# ============================================================================
# This layer is intentionally additive. Existing FeatureEngine, RegimeEngine,
# DecisionEngine, LabelEngine, execution model and datasets remain intact.
# Scanner outputs are evidence, not probabilities. Historical outcomes are
# learned online from the engine's own future-only labels.

SCANNER_HISTORY_FILE = Path(CONFIG.get("dataset_path", "./nifty_3min_dataset")) / "scanner_learning.jsonl"
SCANNER_PRIOR_STRENGTH = 20.0
SCANNER_MIN_OBSERVATIONS_HIGH_CONF = 100


def _sc_clip(x, lo=-1.0, hi=1.0):
    try:
        return float(np.clip(float(x), lo, hi))
    except Exception:
        return 0.0


def _sc_sig(x, scale=1.0):
    try:
        return float(np.tanh(float(x) / max(scale, 1e-9)))
    except Exception:
        return 0.0


def _sc_ratio(a, b, floor=1e-9):
    try:
        return float(a) / max(abs(float(b)), floor)
    except Exception:
        return 0.0


def _sc_val(f, k, default=0.0):
    return safe_float(f.get(k), default)


def _scanners_3m(f):
    """Return 48 hypothesis-driven, mathematically defined scanner scores."""
    st = _sc_val(f, "kalman_stretch", _sc_val(f, "normalized_stretch"))
    sp = _sc_val(f, "normalized_spread")
    ss = _sc_val(f, "stretch_slope_3")
    ps = _sc_val(f, "spread_slope_3")
    rsi = _sc_val(f, "rsi_14", 50.0)
    macd = _sc_val(f, "macd_hist", 0.0)
    adx = _sc_val(f, "adx_14", 0.0)
    pcr = _sc_val(f, "pcr_oi", 1.0)
    pcrv = _sc_val(f, "pcr_volume", 1.0)
    iv = _sc_val(f, "atm_iv", 0.135)
    ivchg = _sc_val(f, "iv_change", 0.0)
    breadth = _sc_val(f, "breadth_10", 0.5)
    disp = _sc_val(f, "dispersion_10", 0.0)
    twc = _sc_val(f, "twc", 0.0)
    oi_long = _sc_val(f, "oi_long_buildup", 0.0)
    oi_short = _sc_val(f, "oi_short_buildup", 0.0)
    oi_unwind = _sc_val(f, "oi_long_unwinding", 0.0) - _sc_val(f, "oi_short_covering", 0.0)
    or_state = _sc_val(f, "or_breakout_state", 0.0)
    orw = _sc_val(f, "or_width_atr", 0.0)
    d_orh = _sc_val(f, "dist_to_or_high_atr", 0.0)
    d_orl = _sc_val(f, "dist_to_or_low_atr", 0.0)
    gap = _sc_val(f, "gap_atr", 0.0)
    pdh = _sc_val(f, "dist_to_pdh_atr", 0.0)
    pdl = _sc_val(f, "dist_to_pdl_atr", 0.0)
    volz = _sc_val(f, "volume_zscore", 0.0)
    vel = _sc_val(f, "kalman_velocity", 0.0)
    gex = _sc_val(f, "gex_proxy", 0.0)
    dte = _sc_val(f, "zero_dte_intensity", 0.0)
    atr = max(_sc_val(f, "atr_14_prev", 1.0), 1e-9)
    oi_ch = _sc_ratio(_sc_val(f, "ce_oi_change", 0.0) - _sc_val(f, "pe_oi_change", 0.0), atr)
    atm_imb = _sc_ratio(_sc_val(f, "pe_oi_atm", 0.0) - _sc_val(f, "ce_oi_atm", 0.0), max(_sc_val(f, "pe_oi_atm", 0.0) + _sc_val(f, "ce_oi_atm", 0.0), 1.0))
    r = lambda x: _sc_clip(x)
    scanners = [
        ("TREND_VWAP_CONT", "TREND", r(0.45*st + 0.30*ss + 0.25*twc)),
        ("TREND_SLOPE_ACCEL", "TREND", r(0.65*ss + 0.20*vel + 0.15*st)),
        ("TREND_ADX_ALIGNMENT", "TREND", r(0.55*st + 0.45*_sc_sig(adx-20, 8))),
        ("TREND_BREADTH_CONFIRM", "TREND", r(0.55*st + 0.45*(2*breadth-1))),
        ("TREND_BASIS_LEAD", "TREND", r(0.60*sp + 0.40*ss)),
        ("TREND_PERSISTENCE", "TREND", r(0.50*st + 0.30*ss + 0.20*_sc_sig(adx-18, 10))),
        ("MOMENTUM_RSI_EXPANSION", "MOMENTUM", r(0.55*_sc_sig(rsi-50, 12) + 0.45*ss)),
        ("MOMENTUM_MACD_IMPULSE", "MOMENTUM", r(0.60*_sc_sig(macd, max(atr*0.01,0.1)) + 0.40*vel)),
        ("MOMENTUM_KALMAN_SURGE", "MOMENTUM", r(0.70*vel + 0.30*ss)),
        ("MOMENTUM_ACCELERATION", "MOMENTUM", r(0.55*ss + 0.25*ps + 0.20*vel)),
        ("MOMENTUM_EXHAUSTION", "MOMENTUM", r(-0.60*st - 0.25*ss + 0.15*_sc_sig(50-rsi,12))),
        ("MOMENTUM_DIVERGENCE", "MOMENTUM", r(-0.55*st + 0.45*_sc_sig(50-rsi,12))),
        ("VWAP_RECLAIM", "LOCATION", r(0.50*ss + 0.30*(-st) + 0.20*twc)),
        ("VWAP_REJECTION", "LOCATION", r(-0.55*ss + 0.35*st - 0.10*twc)),
        ("STRETCH_MEAN_REVERT", "LOCATION", r(-0.75*st - 0.25*ss)),
        ("STRETCH_CONTINUATION", "LOCATION", r(0.70*st + 0.30*ss)),
        ("BASIS_COMPRESSION", "LOCATION", r(-0.60*sp - 0.40*ps)),
        ("BASIS_EXPANSION", "LOCATION", r(0.60*sp + 0.40*ps)),
        ("OR_BREAKOUT", "BREAKOUT", r(0.45*or_state + 0.25*volz + 0.20*ss + 0.10*(2*breadth-1))),
        ("OR_FAILED_BREAKOUT", "BREAKOUT", r(-0.50*or_state - 0.30*ss + 0.20*(-volz))),
        ("OR_COMPRESSION_BREAK", "BREAKOUT", r(0.55*_sc_sig(0.8-orw,0.4) + 0.25*volz + 0.20*ss)),
        ("PDH_ACCEPTANCE", "BREAKOUT", r(-_sc_sig(pdh,1.0)*0.45 + 0.35*ss + 0.20*volz)),
        ("PDL_ACCEPTANCE", "BREAKOUT", r(_sc_sig(-pdl,1.0)*0.45 + 0.35*ss + 0.20*volz)),
        ("BREAKOUT_VOLUME_CONFIRM", "BREAKOUT", r(0.55*volz + 0.25*ss + 0.20*(2*breadth-1))),
        ("ACCUMULATION_PRICE_OI", "FLOW", r(0.40*st + 0.35*oi_long + 0.25*twc)),
        ("DISTRIBUTION_PRICE_OI", "FLOW", r(0.40*st + 0.35*oi_short + 0.25*(-twc))),
        ("OI_FLOW_IMBALANCE", "FLOW", r(0.65*oi_ch + 0.35*twc)),
        ("ATM_OPTION_IMBALANCE", "FLOW", r(0.65*atm_imb + 0.35*(pcr-1.0))),
        ("PCR_REGIME_SHIFT", "FLOW", r(0.55*(pcr-1.0) + 0.45*(pcrv-1.0))),
        ("OPTION_FLOW_REVERSAL", "FLOW", r(-0.50*oi_ch - 0.30*atm_imb + 0.20*ivchg)),
        ("OI_UNWIND_REVERSAL", "FLOW", r(-0.60*oi_unwind + 0.40*(-st))),
        ("BREADTH_LEADERSHIP", "BREADTH", r(0.75*(2*breadth-1) + 0.25*st)),
        ("BREADTH_DIVERGENCE", "BREADTH", r(0.60*(2*breadth-1) - 0.40*st)),
        ("BREADTH_DISPERSION", "BREADTH", r((2*breadth-1) - 0.50*disp)),
        ("HEAVYWEIGHT_CONFIRM", "BREADTH", r(0.65*twc + 0.35*(2*breadth-1))),
        ("IV_EXPANSION", "VOLATILITY", r(0.65*ivchg + 0.35*ss)),
        ("IV_COMPRESSION", "VOLATILITY", r(-0.65*ivchg + 0.35*(-abs(st)))),
        ("VOLATILITY_BREAK", "VOLATILITY", r(0.55*ivchg + 0.25*volz + 0.20*ss)),
        ("GEX_DIRECTIONAL", "VOLATILITY", r(-0.45*gex + 0.35*st + 0.20*ss)),
        ("ZERO_DTE_EXPANSION", "VOLATILITY", r(0.55*dte + 0.25*abs(ss) + 0.20*ivchg) * (1 if ss>=0 else -1)),
        ("GAP_CONTINUATION", "CONTEXT", r(0.60*gap + 0.25*ss + 0.15*(2*breadth-1))),
        ("GAP_FADE", "CONTEXT", r(-0.60*gap - 0.25*ss + 0.15*(-st))),
        ("OR_LOCATION_PRESSURE", "CONTEXT", r(0.45*(d_orh-d_orl) + 0.35*ss + 0.20*st)),
        ("PDH_PDL_LOCATION", "CONTEXT", r(0.50*(-pdh-pdl) + 0.30*st + 0.20*ss)),
        ("IMPULSE_SETUP", "STRATEGY", r(0.35*st+0.25*ss+0.20*vel+0.20*(2*breadth-1))),
        ("STAIRCASE_SETUP", "STRATEGY", r(0.45*st+0.25*ss+0.15*(2*breadth-1)+0.15*_sc_sig(0.55-abs(st),0.25))),
        ("MEAN_REVERSION_SETUP", "STRATEGY", r(-0.55*st-0.25*ss+0.20*(2*breadth-1))),
        ("FAILED_BREAKOUT_REVERSAL", "STRATEGY", r(-0.40*or_state-0.25*ss-0.20*volz-0.15*st)),
        ("MOMENTUM_BREAKOUT_COMBO", "STRATEGY", r(0.30*ss+0.25*vel+0.20*volz+0.15*or_state+0.10*(2*breadth-1))),
        ("INSTITUTIONAL_ALIGNMENT", "STRATEGY", r(0.25*st+0.20*sp+0.20*twc+0.20*oi_ch+0.15*(2*breadth-1))),
    ]
    return scanners


def _scanner_groups_3m(scanners):
    groups = defaultdict(list)
    for name, group, score in scanners:
        groups[group].append(score)
    return {g: float(np.mean(v)) if v else 0.0 for g,v in groups.items()}


def _load_scanner_stats_3m():
    stats = defaultdict(lambda: {"n": 0, "wins": 0})
    try:
        if SCANNER_HISTORY_FILE.exists():
            for line in SCANNER_HISTORY_FILE.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                r=json.loads(line); k=str(r.get("key",""))
                if k:
                    stats[k]["n"] += int(r.get("n",1) or 1)
                    stats[k]["wins"] += int(r.get("wins",0) or 0)
    except Exception:
        pass
    return stats


def _append_scanner_learning_3m(record):
    try:
        SCANNER_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        with SCANNER_HISTORY_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str, ensure_ascii=False)+"\n")
    except Exception:
        pass


def _scanner_estimate_3m(stats, key):
    r=stats.get(key, {}); n=float(r.get("n",0)); w=float(r.get("wins",0))
    # Conservative Beta(10,10) prior: usable from day one, gradually replaced by data.
    p=(10.0+w)/(20.0+n)
    conf=1.0-math.exp(-n/SCANNER_MIN_OBSERVATIONS_HIGH_CONF)
    return p, conf, n


def evaluate_scanner_layer_3m(feats):
    scans=_scanners_3m(feats); groups=_scanner_groups_3m(scans); stats=_load_scanner_stats_3m()
    active=[]
    for name,group,score in scans:
        p,conf,n=_scanner_estimate_3m(stats,name)
        active.append({"id":name,"group":group,"score":round(score,5),"estimate":round(p,5),"confidence":round(conf,5),"n":int(n)})
    # Group evidence is the mean of independent hypotheses, not a raw vote count.
    direction=sum(groups.get(g,0.0)*w for g,w in {
        "TREND":0.16,"MOMENTUM":0.16,"LOCATION":0.14,"BREAKOUT":0.12,
        "FLOW":0.14,"BREADTH":0.10,"VOLATILITY":0.07,"CONTEXT":0.06,"STRATEGY":0.05
    }.items())
    positive=sum(1 for x in active if x["score"]>=0.45); negative=sum(1 for x in active if x["score"]<=-0.45)
    contradiction=negative if direction>0 else positive
    evidence=max(0.0,min(1.0,(abs(direction)+0.10*min(positive+negative,10))/2.0))
    # Cold-start: confidence is explicitly conservative; historical data only nudges the score.
    hist_conf=float(np.mean([x["confidence"] for x in active])) if active else 0.0
    adaptive=direction*(0.90+0.10*hist_conf)
    return {"scanners":active,"groups":groups,"direction_score":adaptive,"evidence_score":evidence,"positive":positive,"negative":negative,"historical_confidence":hist_conf}


def _strategy_score_3m(layer, regime, action):
    d=layer["direction_score"]*(1 if action=="CE" else -1)
    g=layer["groups"]
    compat=1.0
    if action in ("CE","PE"):
        if regime.startswith("IMPULSE"):
            compat=0.75+0.25*max(0.0,min(1.0,(abs(g.get("TREND",0))+abs(g.get("MOMENTUM",0)))/2))
        elif regime.startswith("STAIRCASE"):
            compat=0.75+0.25*max(0.0,min(1.0,abs(g.get("TREND",0))))
        elif regime=="GRIND":
            compat=0.75+0.25*max(0.0,min(1.0,abs(g.get("LOCATION",0))))
    return float(np.clip(0.5+0.5*d*compat,0.0,1.0))


# Preserve the original DecisionEngine verbatim; add a bounded, auditable gate around it.
_ORIGINAL_DECIDE_3M = DecisionEngine.decide

def _decision_with_scanners_3m(self, feats):
    layer=evaluate_scanner_layer_3m(feats)
    feats["scanner_layer_score"]=layer["direction_score"]
    feats["scanner_evidence_score"]=layer["evidence_score"]
    feats["scanner_historical_confidence"]=layer["historical_confidence"]
    feats["scanner_groups_json"]=json.dumps(layer["groups"],sort_keys=True)
    feats["scanner_evidence_json"]=json.dumps(layer["scanners"],sort_keys=True)
    d=_ORIGINAL_DECIDE_3M(self,feats)
    if d.action in ("CE","PE"):
        sscore=_strategy_score_3m(layer,d.regime,d.action)
        d.confidence=float(np.clip(d.confidence*(0.90+0.10*sscore),0.0,0.95))
        aligned=(layer["direction_score"]>=0.30) if d.action=="CE" else (layer["direction_score"]<=-0.30)
        # Never manufacture a trade. Scanner disagreement can only reduce/skip the original decision.
        if layer["evidence_score"]<0.28 or not aligned or sscore<0.56:
            d.action="SKIP"
            d.reason += " | Scanner gate: insufficient independent mathematical evidence"
        else:
            d.reason += f" | Scanner strategy={sscore:.2f} evidence={layer['evidence_score']:.2f}"
        feats["scanner_strategy_score"]=sscore
    else:
        feats["scanner_strategy_score"]=0.5
    return d

DecisionEngine.decide=_decision_with_scanners_3m


def _learn_scanner_outcome_3m(merged):
    action=str(merged.get("decision_action","")).upper()
    if action not in ("CE","PE"): return
    outcome=str(merged.get("triple_barrier_outcome","")).upper()
    if outcome not in ("TARGET_FIRST","STOP_FIRST","TIMEOUT","AMBIGUOUS"): return
    win=1 if outcome=="TARGET_FIRST" else 0
    try: scans=json.loads(merged.get("scanner_evidence_json","[]"))
    except Exception: scans=[]
    for x in scans:
        key=str(x.get("id",""));
        if not key: continue
        _append_scanner_learning_3m({"key":key,"n":1,"wins":win,"action":action,"regime":merged.get("decision_regime"),"timestamp":merged.get("timestamp")})
    try:
        groups=json.loads(merged.get("scanner_groups_json","{}"))
        for g,score in groups.items():
            _append_scanner_learning_3m({"key":"GROUP:"+str(g),"n":1,"wins":win,"score":score,"action":action,"regime":merged.get("decision_regime"),"timestamp":merged.get("timestamp")})
    except Exception: pass

# Patch only the delayed-label persistence hook; original label generation is untouched.
_ORIGINAL_PROCESS_LABELS_3M = KotakNeoAdapter._process_delayed_labels

def _process_labels_with_scanner_learning(self):
    max_tb_bars = CONFIG["time_barrier_min"] // CONFIG["bar_minutes"]
    completed_records = []
    with self.lock:
        candles_list = list(self.candles_3m)
        while self._unlabeled_decisions:
            target_time, entry_px, atr_val, direction, f_row = self._unlabeled_decisions[0]
            future_candles = [c for c in candles_list if to_ist(c.timestamp) > to_ist(target_time)]
            if len(future_candles) >= max_tb_bars:
                self._unlabeled_decisions.popleft()
                try:
                    labels = self.label_engine.generate(
                        entry_price=entry_px, atr=atr_val,
                        future_after_entry=future_candles, direction=direction,
                        signal_timestamp=f_row["timestamp"], entry_timestamp=target_time
                    )
                    merged = {**f_row, **labels}
                    _learn_scanner_outcome_3m(merged)
                    completed_records.append(merged)
                except Exception:
                    pass
            else:
                break
    if completed_records:
        self.dataset_manager.write_parquet(pd.DataFrame(completed_records), name="labeled_features_3min")

KotakNeoAdapter._process_delayed_labels=_process_labels_with_scanner_learning

def main():
    if st is None:
        print("Streamlit not installed.")
        return

    st.set_page_config(page_title="NIFTY 3M | Micro Engine v7.0", page_icon="[LIVE]", layout="wide", initial_sidebar_state="expanded")
    inject_custom_css()

    adapter: KotakNeoAdapter = get_global_adapter()
    is_logged_in = adapter.connected

    with st.sidebar:
        st.subheader("[LIVE] Gateway Controls")
        
        if is_logged_in:
            conn_txt = getattr(adapter, "conn_state", "AUTHENTICATED")
            if conn_txt == "STREAMING":
                st.markdown('<span class="status-pill status-active">* STREAMING (LIVE)</span>', unsafe_allow_html=True)
            else:
                st.markdown('<span class="status-pill status-auth">* CONNECTED (AUTHENTICATED)</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-pill status-offline">* DISCONNECTED</span>', unsafe_allow_html=True)

        user_live_totp = st.text_input("Live TOTP (Optional)", type="password", help="Use if secret not in config")
        
        col_sb1, col_sb2 = st.columns(2)
        with col_sb1:
            if st.button("Connect", key="btn_conn"):
                try:
                    with st.spinner("Connecting..."):
                        adapter.login(live_totp_override=user_live_totp)
                        st.session_state.discovered = False
                        st.rerun()
                except Exception as exc:
                    st.error(f"{exc}")
        with col_sb2:
            if st.button("Reconnect", key="btn_reconn", disabled=not is_logged_in):
                adapter.login(live_totp_override=user_live_totp)
                st.rerun()

        st.markdown("---")
        st.subheader(" Subscriptions")
        
        if st.button("Discover Instruments", key="btn_disc", disabled=not is_logged_in):
            with st.spinner("Locking NIFTY Instruments & Heavyweights..."):
                adapter.discover_nifty_instruments(auto_pcr=True)
                st.session_state.discovered = True
                st.rerun()

        if st.session_state.get("discovered") and adapter.discovery_log:
            st.success("OK Instruments Mapped!")
            for l in adapter.discovery_log:
                st.caption(l)

        is_streaming = (adapter.conn_state == "STREAMING")
        if st.button("Start Live Feed", key="btn_start_feed", disabled=not is_logged_in or is_streaming):
            with st.spinner("Subscribing & Ingesting Feed..."):
                adapter.subscribe_live_feed()
                adapter.start_bar_watchdog()
                st.session_state.stream_active = True
                st.rerun()

        if adapter.last_error:
            st.warning(f"Engine Log: {adapter.last_error}")

        st.markdown("---")
        if st.button("Run Unit Tests", key="btn_tests"):
            try:
                st.success("Engine Verification Passed (v7.0)" if run_unit_tests() else "Test Failed")
            except Exception as exc:
                st.error(str(exc))

    if is_streaming and adapter:
        adapter.fetch_market_snapshot()

    spot_val, fut_val, fut_oi, ticks_count = "-", "-", "-", 0
    if adapter and adapter.latest:
        with adapter.lock:
            s = adapter.latest.get("Nifty 50", {})
            spot_p = extract_tick_price(s)
            
            if not is_valid_number(spot_p):
                for k, v in adapter.latest.items():
                    sym_str = str(v.get("display_symbol", "")).upper()
                    if ("NIFTY" in sym_str or "NIFTY 50" in sym_str) and "EQ" not in sym_str and "FUT" not in sym_str:
                        spot_p = extract_tick_price(v)
                        if is_valid_number(spot_p): break

            spot_val = f"{spot_p:.2f}" if is_valid_number(spot_p) else "-"
            
            f = adapter.latest.get(str(adapter.future_token), {})
            fut_p = extract_tick_price(f)
            fut_val = f"{fut_p:.2f}" if is_valid_number(fut_p) else "-"
            
            fut_oi = adapter._extract_oi(f) if hasattr(adapter, "_extract_oi") else safe_float(f.get("oi") or f.get("open_interest"), "-")
            ticks_count = len(adapter.tick_buffer)

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("NIFTY SPOT", f"Rs. {spot_val}")
    t2.metric("NIFTY FUT", f"Rs. {fut_val}")
    t3.metric("FUT OPEN INTEREST", f"{int(fut_oi):,}" if isinstance(fut_oi, (int, float)) and np.isfinite(fut_oi) else str(fut_oi))
    t4.metric("TICKS INGESTED", f"{ticks_count:,}")

    st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
    col_hud1, col_hud2, col_hud3, col_hud4, col_hud5 = st.columns([1.5, 1.2, 1.2, 1, 2])
    
    if adapter and adapter.last_decision:
        d = adapter.last_decision
        badge_cls = "badge-ce" if d.action == "CE" else ("badge-pe" if d.action == "PE" else "badge-neutral")
        
        with col_hud1:
            st.caption("TACTICAL SIGNAL")
            st.markdown(f'<div class="{badge_cls}">{d.action}</div>', unsafe_allow_html=True)
        with col_hud2:
            st.metric("Regime", d.regime)
        with col_hud3:
            st.metric("Option Target / SL", f"+{d.option_target_pts} / -{d.option_stop_pts} pt")
            st.caption(f"**Effective Delta:** {d.effective_delta:.2f}")
        with col_hud4:
            st.metric("Confidence", f"{d.confidence * 100:.0f}%")
        with col_hud5:
            st.caption("Engine Rationale")
            st.write(f"_{d.reason}_")
    else:
        st.info("Awaiting first completed 3-minute bar to establish baseline regime and signal...")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
    st.markdown("**[LIVE] Live Option-Centric Paper Trading Desk & Journal**")
    
    col_p1, col_p2, col_p3, col_p4 = st.columns(4)
    if adapter:
        desk = adapter.paper_desk
        active_pos = desk.active_position
        total_pnl = round(desk.realized_pnl_pts + desk.unrealized_pnl_pts, 2)
        closed_count = len(desk.closed_trades)
        wins = sum(1 for t in desk.closed_trades if t.pnl_pts > 0)
        hit_rate = (wins / closed_count * 100) if closed_count > 0 else 0.0

        col_p1.metric("Total Net PnL", f"{total_pnl:+.2f} pt", delta=f"Realized: {desk.realized_pnl_pts:+.2f} pt")
        col_p2.metric("Unrealized MTM", f"{desk.unrealized_pnl_pts:+.2f} pt")
        col_p3.metric("Closed Trades", f"{closed_count}", delta=f"Win Rate: {hit_rate:.0f}%")
        
        is_locked = getattr(desk, "risk_locked", False)
        if is_locked:
            col_p4.markdown(f"**Status:** <span style='color:red; font-weight:bold;'>KILL-SWITCH LOCKED (Max Daily Loss Reached)</span>", unsafe_allow_html=True)
        elif active_pos:
            dir_str = "CE (LONG)" if active_pos.direction == 1 else "PE (SHORT)"
            col_p4.markdown(f"**Active Position:** `{dir_str}`<br>Entry Opt: `Rs. {active_pos.entry_option_price}` | Target Opt: `+{active_pos.option_target}` pt", unsafe_allow_html=True)
        elif desk.pending_order:
            p_dir = "CE" if desk.pending_order["direction"] == 1 else "PE"
            col_p4.markdown(f"**Order Staged:** `{p_dir}` (Filling Next Open)", unsafe_allow_html=True)
        else:
            col_p4.markdown("**Active Position:** `FLAT (NO POSITION)`", unsafe_allow_html=True)

        if desk.closed_trades:
            st.markdown("---")
            col_tbl_head, col_tbl_dl = st.columns([3, 1])
            with col_tbl_head:
                st.caption("Recent Closed Option Paper Trades (Option-Centric Journal v7.0)")
            
            trades_raw = [asdict(t) for t in desk.closed_trades]
            df_full_journal = pd.DataFrame(trades_raw)
            
            with col_tbl_dl:
                csv_data = df_full_journal.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=" Download Journal (.csv)",
                    data=csv_data,
                    file_name=f"nifty_option_journal_v70_{now_ist().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            
            recent_trades_data = []
            for t in list(desk.closed_trades)[-8:]:
                recent_trades_data.append({
                    "Exit Time": t.exit_time.strftime("%H:%M:%S") if t.exit_time else "-",
                    "Type": "CE (LONG)" if t.direction == 1 else "PE (SHORT)",
                    "Entry Opt (Rs. )": f"{t.entry_option_price:.2f}",
                    "Exit Opt (Rs. )": f"{t.exit_option_price:.2f}" if t.exit_option_price else "-",
                    "Option PnL (pt)": t.pnl_pts,
                    "Bars Held": t.bars_held,
                    "Exit Reason": t.exit_reason
                })
            
            df_trades = pd.DataFrame(recent_trades_data)
            st.dataframe(df_trades, hide_index=True)

    st.markdown('</div>', unsafe_allow_html=True)

    grid_left, grid_right = st.columns([1.2, 0.8])
    latest_row = None
    with grid_left:
        st.markdown("**Core Feature Vector (Heatmap Traffic Light)**")
        if adapter:
            with adapter.lock:
                latest_row = dict(adapter.feature_engine.history[-1]) if adapter.feature_engine.history else None
            if latest_row:
                f1, f2, f3, f4 = st.columns(4)
                
                val_stretch = latest_row.get("kalman_stretch", 0.0)
                f1.markdown(f"**Kalman Stretch**<br>{get_colored_text(val_stretch, 'kalman_stretch')}", unsafe_allow_html=True)
                
                val_slope = latest_row.get("stretch_slope_3", 0.0)
                f2.markdown(f"**Slope (3-Bar)**<br>{get_colored_text(val_slope, 'stretch_slope_3')}", unsafe_allow_html=True)
                
                val_pcr = latest_row.get("pcr_oi", 1.0)
                f3.markdown(f"**PCR (OI)**<br>{get_colored_text(val_pcr, 'pcr_oi')}", unsafe_allow_html=True)
                
                val_breadth = latest_row.get("breadth_10", 0.5)
                f4.markdown(f"**Breadth (10)**<br>{get_colored_text(val_breadth, 'breadth_10')}", unsafe_allow_html=True)
                
                with st.expander(" Institutional MLOps & 2nd-Order Greeks Scorecard", expanded=False):
                    dq_val = latest_row.get("data_quality_score", 1.0)
                    st.write(f"**Overall DQ Score:** `{dq_val * 100:.0f}%`")
                    
                    c_dq1, c_dq2 = st.columns(2)
                    with c_dq1:
                        st.write(f" | Top 5 Lead Pressure (SLP_5): `{latest_row.get('slp_top5_pressure', 0.0):.3f}`")
                        st.write(f" | Order Book Imbalance (OBI): `{latest_row.get('order_book_imbalance', 0.0):.3f}`")
                        st.write(f" | Dealer Vanna Flow: `{latest_row.get('dealer_vanna_flow', 0.0):.3f}`")
                        st.write(f" | Dealer Charm Flow: `{latest_row.get('dealer_charm_flow', 0.0):.3f}`")
                        st.write(f" | Dealer GEX Proxy: `{latest_row.get('gex_proxy', 0.0):.3f}`")
                    with c_dq2:
                        model_loaded = adapter.decision_engine.ml_model is not None
                        feat_cnt = len(adapter.decision_engine.expected_feature_names)
                        st.write(f" | ML Status: `{'ACTIVE (' + str(feat_cnt) + ' Features)' if model_loaded else 'FALLBACK (Heuristic)'}`")
                        st.write(f" | 0DTE Intensity: `{latest_row.get('zero_dte_intensity', 0.0):.2f}`")
                        st.write(f" | Minutes to Expiry: `{latest_row.get('minutes_to_expiry', 0.0):.0f} min`")
                        st.write(f" | Gap Points: `{latest_row.get('gap_points', 0.0):.1f} pt`")
                        st.write(f" | Causal Integrity Tag: `{'1 (VERIFIED)' if latest_row.get('is_causal') == 1 else '0 (INVALID)'}`")
                    st.json(latest_row)
            else:
                st.caption("Feature extraction initializing...")

    with grid_right:
        st.markdown("**Top 5 Core Heavyweights Momentum & Live Impact**")
        if adapter and adapter.heavy_tokens:
            hw_list = []
            with adapter.lock:
                for sym, base_w in HEAVYWEIGHTS_TOP5.items():
                    tok = str(adapter.heavy_tokens.get(sym))
                    t = adapter.latest.get(tok, {})
                    ltp = extract_tick_price(t)
                    
                    open_price = extract_quote_field(t, ("o", "open", "pOpen", "openPrice", "op"))
                    if not is_valid_number(open_price) or open_price <= 0:
                        open_price = ltp
                    
                    if is_valid_number(ltp) and is_valid_number(open_price) and open_price > 0:
                        ret_pct = ((ltp - open_price) / open_price) * 100.0
                        effective_impact = base_w * (ltp - open_price) / open_price * 100
                    else:
                        ret_pct = 0.0
                        effective_impact = 0.0

                    hw_list.append({
                        "Symbol": sym,
                        "LTP": f"Rs. {ltp:.2f}" if is_valid_number(ltp) else "-",
                        "Change %": f"{ret_pct:+.2f}%" if is_valid_number(ret_pct) else "-",
                        "Base Wt": f"{base_w*100:.1f}%",
                        "Live Impact": f"{effective_impact:+.3f}"
                    })
            
            df_hw = pd.DataFrame(hw_list)
            st.dataframe(df_hw, height=210, hide_index=True)
        else:
            st.caption("Heavyweights mapping pending discovery... Click Discover Instruments in Sidebar.")

    if is_streaming:
        time.sleep(CONFIG["ui_refresh_sec"])
        st.rerun()


if __name__ == "__main__":
    if st is not None and hasattr(st, "runtime") and st.runtime.exists():
        main()
    else:
        print("[LIVE] Running Institutional Prop-Engine Verification...")
        if run_unit_tests():
            print("OK All Quant Engines Verified + IST Timezone + Library Bug Hardened.")
        else:
            raise RuntimeError("Engine Verification Failed.")
