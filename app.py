#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine | v7.1 Final Complete - PART 1 of 3
Kuch bhi short nahi kiya. Saari core concepts + bug fixes included.
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
from datetime import datetime, timedelta, date
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
# TIMEZONE
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
# CONFIG
# =========================================================
CONFIG = {
    "app_version": "v7.1_final_complete_3parts",
    "feature_version": "v6.2_full_ui_stats",
    "label_version": "TB_v3.0_clean",
    "schema_version": "4.3",
    "weight_version": "NIFTY_STATIC_2025Q1",
    "atr_period": 14,
    "sma_period": 20,
    "triple_upper_atr": 1.5,
    "triple_lower_atr": 0.75,
    "time_barrier_min": 30,
    "mfe_horizons_min": [15, 30, 45],
    "opening_range_minutes": 15,
    "execution_model": "next_bar_open",
    "session_start": "09:15",
    "session_end": "15:30",
    "bar_minutes": 3,
    "dataset_path": "./nifty_3min_dataset",
    "model_path": "./model/nifty_lgbm_latest.joblib",
    "neo_environment": "prod",
    "nifty_spot_token": "Nifty 50",
    "nifty_future_token": os.getenv("NIFTY_FUT_TOKEN", "").strip(),
    "pcr_strike_count": int(os.getenv("PCR_STRIKE_COUNT", "5")),
    "pcr_strike_step": float(os.getenv("PCR_STRIKE_STEP", "50")),
    "min_data_quality_to_trade": 0.45,
    "ui_refresh_sec": 3,
    "session_end_flush": True,
    "base_delta": 0.52,
    "base_slippage_pts": 0.35,
    "option_exit_spread_penalty": 0.65,
    "max_daily_loss_pts": 120.0,
    "risk_free_rate": 0.065,
    "default_atm_iv": 0.135,
    "vp_value_area_pct": 0.70,
    "vp_tick_size": 1.0,
    "bos_lookback": 8,
    "fvg_min_gap_atr": 0.25,
    "min_aligned_for_entry": 3,
    "time_guard_non_expiry": (15, 20),
    "time_guard_expiry": (15, 25),
    "stats_min_trades_for_strength": 30,
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
    "TCS": 0.040,
    "LT": 0.038,
    "AXISBANK": 0.033,
    "KOTAKBANK": 0.029,
    "SBIN": 0.028,
}

NSE_CASH_TOKENS = {
    "HDFCBANK": "1333",
    "RELIANCE": "2885",
    "ICICIBANK": "4963",
    "INFY": "1594",
    "ITC": "1660",
    "TCS": "11536",
    "LT": "11483",
    "AXISBANK": "5900",
    "KOTAKBANK": "1922",
    "SBIN": "3045",
}

# =========================================================
# UTILITIES
# =========================================================
def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

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
    for fmt in ["%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d%b%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d"]:
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
# ENGINES START
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
    def compute_second_order_greeks(spot: float, strike: float, minutes_to_exp: float, iv: float = 0.135, r: float = 0.065) -> Dict[str, float]:
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
        return {"vanna": vanna, "charm_ce": charm_ce, "charm_pe": charm_pe, "d1": d1, "d2": d2}

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
        self.prev_ha_open = ha_open
        self.prev_ha_close = ha_close
        color = 1 if ha_close >= ha_open else -1
        strong = 1 if (color == 1 and l >= ha_open - 1e-6) or (color == -1 and h <= ha_open + 1e-6) else 0
        return {"ha_open": ha_open, "ha_close": ha_close, "ha_color": color, "ha_strong": strong}

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
        return {"supertrend": supertrend, "st_direction": direction, "st_flip": flip}

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
        return {"hm_rsi": rsi, "hm_ema": ema3, "hm_wma": wma21, "hm_signal": signal}

class CumulativeDeltaEngine:
    def __init__(self):
        self.cum_delta = 0.0
        self.prev_ltp = None
        self.bar_delta = 0.0
        self.history = deque(maxlen=100)

    def reset(self):
        self.cum_delta = 0.0
        self.prev_ltp = None
        self.bar_delta = 0.0
        self.history.clear()

    def on_tick(self, ltp: float, volume: float = 1.0):
        if not is_valid_number(ltp) or ltp <= 0:
            return
        vol = max(0.5, safe_float(volume, 1.0))
        if self.prev_ltp is not None:
            if ltp > self.prev_ltp + 0.05:
                delta = vol
            elif ltp < self.prev_ltp - 0.05:
                delta = -vol
            else:
                delta = 0.0
            self.cum_delta += delta
            self.bar_delta += delta
        self.prev_ltp = ltp

    def on_bar_close(self):
        self.history.append(self.bar_delta)
        self.bar_delta = 0.0

    def features(self) -> Dict[str, float]:
        recent = list(self.history)[-5:] if self.history else [0.0]
        slope = calc_3bar_slope(recent) if len(recent) >= 3 else 0.0
        return {
            "cum_delta": self.cum_delta,
            "bar_delta": self.bar_delta,
            "delta_slope_3": slope,
            "delta_sign": 1 if self.cum_delta > 30 else (-1 if self.cum_delta < -30 else 0),
        }

class VolumeProfileEngine:
    def __init__(self, tick_size=1.0, value_area_pct=0.70):
        self.tick_size = tick_size
        self.value_area_pct = value_area_pct
        self.volume_at_price: Dict[float, float] = defaultdict(float)
        self.total_volume = 0.0
        self.poc = np.nan
        self.vah = np.nan
        self.val = np.nan

    def reset(self):
        self.volume_at_price.clear()
        self.total_volume = 0.0
        self.poc = self.vah = self.val = np.nan

    def update(self, high: float, low: float, close: float, volume: float):
        if not all(is_valid_number(x) for x in [high, low, close]) or volume <= 0:
            return
        price_levels = np.arange(
            math.floor(low / self.tick_size) * self.tick_size,
            math.ceil(high / self.tick_size) * self.tick_size + self.tick_size,
            self.tick_size
        )
        if len(price_levels) == 0:
            price_levels = [round(close / self.tick_size) * self.tick_size]
        vol_per_level = volume / max(len(price_levels), 1)
        for p in price_levels:
            self.volume_at_price[round(p, 2)] += vol_per_level
        self.total_volume += volume
        self._recompute()

    def _recompute(self):
        if not self.volume_at_price or self.total_volume <= 0:
            return
        self.poc = max(self.volume_at_price.items(), key=lambda x: x[1])[0]
        sorted_prices = sorted(self.volume_at_price.items(), key=lambda x: x[1], reverse=True)
        target = self.total_volume * self.value_area_pct
        cum = 0.0
        va_prices = []
        for p, v in sorted_prices:
            cum += v
            va_prices.append(p)
            if cum >= target:
                break
        if va_prices:
            self.vah = max(va_prices)
            self.val = min(va_prices)

    def features(self, current_price: float, atr: float = 15.0) -> Dict[str, float]:
        if not is_valid_number(self.poc):
            return {
                "vp_poc": np.nan, "vp_vah": np.nan, "vp_val": np.nan,
                "dist_to_poc_atr": np.nan, "above_poc": 0, "inside_va": 0,
                "vah_break": 0, "val_break": 0
            }
        dist_poc = (current_price - self.poc) / atr if atr > 0 else 0.0
        above_poc = 1 if current_price > self.poc else -1
        inside_va = 1 if (is_valid_number(self.val) and is_valid_number(self.vah) and self.val <= current_price <= self.vah) else 0
        vah_break = 1 if is_valid_number(self.vah) and current_price > self.vah else 0
        val_break = -1 if is_valid_number(self.val) and current_price < self.val else 0
        return {
            "vp_poc": self.poc,
            "vp_vah": self.vah,
            "vp_val": self.val,
            "dist_to_poc_atr": dist_poc,
            "above_poc": above_poc,
            "inside_va": inside_va,
            "vah_break": vah_break,
            "val_break": val_break,
        }

class SMCEngine:
    def __init__(self, lookback=8, fvg_min_gap_atr=0.25):
        self.lookback = lookback
        self.fvg_min_gap_atr = fvg_min_gap_atr
        self.swing_highs = deque(maxlen=30)
        self.swing_lows = deque(maxlen=30)
        self.last_bos = 0
        self.active_fvgs: List[Dict] = []
        self.demand_zones: List[Dict] = []
        self.supply_zones: List[Dict] = []

    def reset(self):
        self.swing_highs.clear()
        self.swing_lows.clear()
        self.last_bos = 0
        self.active_fvgs.clear()
        self.demand_zones.clear()
        self.supply_zones.clear()

    def update(self, candle: "Candle3Min", atr: float):
        if not is_valid_number(atr) or atr <= 0:
            atr = 15.0
        if len(self.swing_highs) >= 2:
            prev_h = self.swing_highs[-1]
            if candle.fut_c > prev_h["price"]:
                self.last_bos = 1
        if len(self.swing_lows) >= 2:
            prev_l = self.swing_lows[-1]
            if candle.fut_c < prev_l["price"]:
                self.last_bos = -1
        self.swing_highs.append({"price": candle.fut_h, "ts": candle.timestamp})
        self.swing_lows.append({"price": candle.fut_l, "ts": candle.timestamp})

    def detect_fvg(self, c1: "Candle3Min", c2: "Candle3Min", c3: "Candle3Min", atr: float):
        if not all(is_valid_number(x) for x in [c1.fut_h, c1.fut_l, c3.fut_h, c3.fut_l]):
            return
        gap_up = c1.fut_h < c3.fut_l and (c3.fut_l - c1.fut_h) >= self.fvg_min_gap_atr * atr
        gap_down = c1.fut_l > c3.fut_h and (c1.fut_l - c3.fut_h) >= self.fvg_min_gap_atr * atr
        if gap_up:
            self.active_fvgs.append({"type": "bullish", "top": c3.fut_l, "bottom": c1.fut_h, "ts": c3.timestamp})
            self.demand_zones.append({"top": c3.fut_l, "bottom": c1.fut_h, "ts": c3.timestamp})
        if gap_down:
            self.active_fvgs.append({"type": "bearish", "top": c1.fut_l, "bottom": c3.fut_h, "ts": c3.timestamp})
            self.supply_zones.append({"top": c1.fut_l, "bottom": c3.fut_h, "ts": c3.timestamp})
        self.active_fvgs = self.active_fvgs[-8:]
        self.demand_zones = self.demand_zones[-6:]
        self.supply_zones = self.supply_zones[-6:]

    def features(self, current_price: float, atr: float = 15.0) -> Dict[str, Any]:
        near_demand = 0
        near_supply = 0
        for z in self.demand_zones[-3:]:
            if z["bottom"] - 0.3 * atr <= current_price <= z["top"] + 0.3 * atr:
                near_demand = 1
                break
        for z in self.supply_zones[-3:]:
            if z["bottom"] - 0.3 * atr <= current_price <= z["top"] + 0.3 * atr:
                near_supply = 1
                break
        bullish_fvg = any(f["type"] == "bullish" for f in self.active_fvgs[-3:])
        bearish_fvg = any(f["type"] == "bearish" for f in self.active_fvgs[-3:])
        return {
            "bos_signal": self.last_bos,
            "near_demand_zone": near_demand,
            "near_supply_zone": near_supply,
            "bullish_fvg_active": int(bullish_fvg),
            "bearish_fvg_active": int(bearish_fvg),
            "smc_bias": 1 if (self.last_bos == 1 or bullish_fvg or near_demand) else (-1 if (self.last_bos == -1 or bearish_fvg or near_supply) else 0),
        }

class StatsCollector:
    def __init__(self):
        self.trades: List[Dict] = []
        self.strength_cache = {
            "above_poc": {"win": 0, "total": 0, "sum_r": 0.0},
            "below_poc": {"win": 0, "total": 0, "sum_r": 0.0},
            "smc_bull": {"win": 0, "total": 0, "sum_r": 0.0},
            "smc_bear": {"win": 0, "total": 0, "sum_r": 0.0},
        }

    def record(self, trade: Dict):
        self.trades.append(trade)
        r = safe_float(trade.get("pnl_pts"), 0.0)
        won = 1 if r > 0 else 0
        above = trade.get("above_poc", 0)
        smc = trade.get("smc_bias", 0)
        key = "above_poc" if above == 1 else "below_poc"
        self.strength_cache[key]["total"] += 1
        self.strength_cache[key]["win"] += won
        self.strength_cache[key]["sum_r"] += r
        key = "smc_bull" if smc == 1 else "smc_bear"
        self.strength_cache[key]["total"] += 1
        self.strength_cache[key]["win"] += won
        self.strength_cache[key]["sum_r"] += r

    def get_level_strength(self, above_poc: int, smc_bias: int, regime: str = "") -> float:
        if len(self.trades) < CONFIG["stats_min_trades_for_strength"]:
            return 1.0
        scores = []
        for key in ["above_poc" if above_poc == 1 else "below_poc", "smc_bull" if smc_bias == 1 else "smc_bear"]:
            d = self.strength_cache[key]
            if d["total"] >= 8:
                wr = d["win"] / d["total"]
                avg_r = d["sum_r"] / d["total"]
                score = 0.7 + (wr - 0.45) * 1.5 + max(-0.3, min(0.4, avg_r / 20))
                scores.append(max(0.6, min(1.4, score)))
        if not scores:
            return 1.0
        return float(np.mean(scores))

print("PART 1 LOADED SUCCESSFULLY")
print("Ab 'Part 2 de do' likho")
# =========================================================
# FEATURE ENGINE + REGIME + DECISION + PAPER TRADING
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
            "or_high": self.or_high,
            "or_low": self.or_low,
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

        zero_dte_intensity = 0.0
        if exp_flag and is_valid_number(mins_to_exp) and mins_to_exp <= 375:
            zero_dte_intensity = max(0.0, 1.0 - (mins_to_exp / 375.0))
            if mins_to_exp <= 90:
                zero_dte_intensity = min(1.0, zero_dte_intensity + 0.35)

        atm_diff = atm_ce - atm_pe
        total_diff = total_ce - total_pe
        tot_oi_baseline = (tot_sum + 1e-5)
        gex_proxy = float(((atm_diff * 2.5) + (total_diff * 0.4)) / tot_oi_baseline)
        atm_gamma_imb = (atm_ce - atm_pe) / (atm_sum + 1e-5) if atm_sum > 0 else 0.0
        gex_x_0dte = float(gex_proxy * zero_dte_intensity)

        greeks = GreeksEngine.compute_second_order_greeks(
            spot=spot_price,
            strike=atm_strike,
            minutes_to_exp=mins_to_exp if is_valid_number(mins_to_exp) else 375.0,
            iv=CONFIG["default_atm_iv"],
            r=CONFIG["risk_free_rate"]
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
        self.cum_delta = CumulativeDeltaEngine()
        self.vol_profile = VolumeProfileEngine(tick_size=CONFIG["vp_tick_size"], value_area_pct=CONFIG["vp_value_area_pct"])
        self.smc = SMCEngine(lookback=CONFIG["bos_lookback"], fvg_min_gap_atr=CONFIG["fvg_min_gap_atr"])
        self._last_3_candles = deque(maxlen=3)

    def reset_session(self):
        self.vwap_pv = self.vwap_vol = 0.0
        self.tr_history.clear()
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
        self.cum_delta.reset()
        self.vol_profile.reset()
        self.smc.reset()
        self._last_3_candles.clear()

    def set_previous_day(self, close, high, low):
        self.sess.set_previous_day(close, high, low)

    def set_today_open(self, open_price):
        self.sess.set_today_open(open_price)

    def on_tick_for_delta(self, ltp: float, volume: float = 1.0):
        self.cum_delta.on_tick(ltp, volume)

    def compute(self, candle: Candle3Min, prev: deque):
        typical = (candle.fut_h + candle.fut_l + candle.fut_c) / 3.0
        volume = safe_float(candle.fut_volume, 1.0)
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
        atr = atr_prev if is_valid_number(atr_prev) else 15.0

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
            normalized_stretch = kalman_stretch = normalized_spread = 0.0

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
        micro_price_drift = (micro_price - candle.fut_c) / atr if atr > 0 else 0.0

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
        self.vol_profile.update(candle.fut_h, candle.fut_l, candle.fut_c, volume)
        self.cum_delta.on_bar_close()
        self.smc.update(candle, atr)

        self._last_3_candles.append(candle)
        if len(self._last_3_candles) == 3:
            c1, c2, c3 = self._last_3_candles
            self.smc.detect_fvg(c1, c2, c3, atr)

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
        ha_res = self.ha.update(candle.fut_o, candle.fut_h, candle.fut_l, candle.fut_c)
        st_res = self.st.update(candle.fut_h, candle.fut_l, candle.fut_c, atr=atr)
        hm_res = self.hm.update(candle.fut_c)
        delta_res = self.cum_delta.features()
        vp_res = self.vol_profile.features(candle.fut_c, atr)
        smc_res = self.smc.features(candle.fut_c, atr)

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
            "atr_mode": "session_local",
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
            "atr_14_prev": atr,
            "atr_warmup_flag": int(not is_valid_number(atr_prev)),
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
            **self.hw.compute(candle),
            **self.or_eng.features(candle, atr),
            **self.sess.features(candle, atr),
            **pcr_features,
            **ha_res,
            **st_res,
            **hm_res,
            **delta_res,
            **vp_res,
            **smc_res,
            "missing_spot": missing_spot,
            "missing_future": missing_future,
            "missing_oi": missing_oi,
            "missing_volume": missing_volume,
            "missing_heavyweight": missing_heavyweight,
            "missing_option_chain": missing_option,
            "bad_ohlc": bad_ohlc,
            "zero_volume": zero_volume,
            "zero_oi": zero_oi,
            "data_quality_score": float(max(0.0, 1.0 - penalty)),
            "bar_complete": 1,
        }
        self.history.append(features)
        return features

class RegimeEngine:
    def detect(self, feats: Dict[str, Any]) -> str:
        dq = safe_float(feats.get("data_quality_score"), 0.0)
        atr_warm = safe_int(feats.get("atr_warmup_flag"), 0)
        if dq < CONFIG["min_data_quality_to_trade"] or atr_warm == 1:
            return "DATA_BAD"
        k_stretch = safe_float(feats.get("kalman_stretch"), 0.0)
        slope = safe_float(feats.get("stretch_slope_3"), 0.0)
        or_state = safe_int(feats.get("or_breakout_state"), 0)
        oi_long = safe_int(feats.get("oi_long_buildup"), 0)
        oi_short = safe_int(feats.get("oi_short_buildup"), 0)
        twc = safe_float(feats.get("twc"), 0.0)
        breadth = safe_float(feats.get("breadth_10"), 0.5)
        gex_val = safe_float(feats.get("gex_proxy"), 0.0)
        z_dte = safe_float(feats.get("zero_dte_intensity"), 0.0)
        delta_sign = safe_int(feats.get("delta_sign"), 0)

        if z_dte > 0.5 and gex_val > 0.70 and abs(k_stretch) <= 0.65:
            return "GRIND"
        if z_dte > 0.4 and gex_val < -0.70 and abs(k_stretch) > 0.40:
            return "IMPULSE_UP" if k_stretch > 0 else "IMPULSE_DOWN"
        if (abs(k_stretch) > 0.85 and abs(slope) > 0.12) or (or_state != 0 and abs(k_stretch) > 0.45):
            if k_stretch > 0 and (oi_long or twc > 0 or breadth > 0.55 or delta_sign > 0):
                return "IMPULSE_UP"
            if k_stretch < 0 and (oi_short or twc < 0 or breadth < 0.45 or delta_sign < 0):
                return "IMPULSE_DOWN"
        if 0.35 < abs(k_stretch) <= 0.85:
            return "STAIRCASE_UP" if k_stretch > 0 else "STAIRCASE_DOWN"
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
    aligned_count: int = 0
    is_reversal: bool = False
    level_strength: float = 1.0

class DecisionEngine:
    def __init__(self):
        self.regime_engine = RegimeEngine()
        self.stats = StatsCollector()
        self.bar_counter = 0

    def _realistic_target(self, atr: float, regime: str, aligned_count: int = 3) -> Tuple[float, float, float]:
        if not is_valid_number(atr) or atr <= 0:
            atr = 15.0
        conviction = 1.0 if aligned_count <= 3 else (1.25 if aligned_count == 4 else 1.45)
        if regime in ("IMPULSE_UP", "IMPULSE_DOWN"):
            return round(2.0 * atr * conviction, 1), round(0.75 * atr, 1), 1.0
        if regime in ("STAIRCASE_UP", "STAIRCASE_DOWN"):
            return round(1.5 * atr * conviction, 1), round(0.70 * atr, 1), 0.85
        return round(0.70 * atr, 1), round(0.55 * atr, 1), 0.70

    def _get_high_priority_signals(self, regime: str, feats: Dict[str, Any]) -> List[int]:
        stretch = safe_float(feats.get("kalman_stretch"), 0.0)
        slope = safe_float(feats.get("stretch_slope_3"), 0.0)
        st_dir = safe_int(feats.get("st_direction"), 0)
        hm_sig = safe_int(feats.get("hm_signal"), 0)
        ha_color = safe_int(feats.get("ha_color"), 0)
        delta_sign = safe_int(feats.get("delta_sign"), 0)
        above_poc = safe_int(feats.get("above_poc"), 0)
        smc_bias = safe_int(feats.get("smc_bias"), 0)

        def sign(val, thresh=0.0):
            if val > thresh: return 1
            if val < -thresh: return -1
            return 0

        return [sign(stretch, 0.25), sign(slope, 0.03), st_dir, hm_sig, ha_color, delta_sign, above_poc, smc_bias]

    def decide(self, feats: Dict[str, Any], current_position_direction: int = 0) -> TradeDecision:
        self.bar_counter += 1
        now_ts = now_ist()
        expiry_flag = safe_int(feats.get("expiry_day_flag"), 0)
        cutoff = CONFIG["time_guard_expiry"] if expiry_flag == 1 else CONFIG["time_guard_non_expiry"]

        if now_ts.hour > cutoff[0] or (now_ts.hour == cutoff[0] and now_ts.minute >= cutoff[1]):
            return TradeDecision(
                action="SKIP",
                regime="TIME_GUARD_ACTIVE",
                reason=f"Time guard ({cutoff[0]}:{cutoff[1]:02d})",
                timestamp=feats.get("timestamp"),
                decision_timestamp=now_ts
            )

        regime = self.regime_engine.detect(feats)
        dq = safe_float(feats.get("data_quality_score"), 0.0)

        if regime == "DATA_BAD" or dq < CONFIG["min_data_quality_to_trade"]:
            return TradeDecision(
                action="SKIP",
                regime=regime,
                reason="Data quality / warmup",
                timestamp=feats.get("timestamp"),
                decision_timestamp=now_ts
            )

        atr = safe_float(feats.get("atr_14_prev"), 15.0)
        signals = self._get_high_priority_signals(regime, feats)
        aligned_buy = sum(1 for s in signals if s == 1)
        aligned_sell = sum(1 for s in signals if s == -1)
        aligned_count = max(aligned_buy, aligned_sell)

        above_poc = safe_int(feats.get("above_poc"), 0)
        smc_bias = safe_int(feats.get("smc_bias"), 0)
        level_strength = self.stats.get_level_strength(above_poc, smc_bias, regime)

        action = "SKIP"
        size_mult = 1.0
        reason_parts = [f"Regime={regime}", f"B{aligned_buy}/S{aligned_sell}", f"LS={level_strength:.2f}"]
        is_reversal = False
        min_req = CONFIG["min_aligned_for_entry"]

        if current_position_direction == 1 and aligned_sell >= min_req and aligned_sell > aligned_buy:
            action = "PE"
            is_reversal = True
            reason_parts.append("REVERSAL CE→PE")
        elif current_position_direction == -1 and aligned_buy >= min_req and aligned_buy > aligned_sell:
            action = "CE"
            is_reversal = True
            reason_parts.append("REVERSAL PE→CE")
        elif aligned_buy >= min_req and aligned_buy > aligned_sell:
            action = "CE"
            size_mult = 1.0 + (aligned_buy - 3) * 0.12
            reason_parts.append(f"Buy {aligned_buy}")
        elif aligned_sell >= min_req and aligned_sell > aligned_buy:
            action = "PE"
            size_mult = 1.0 + (aligned_sell - 3) * 0.12
            reason_parts.append(f"Sell {aligned_sell}")
        else:
            reason_parts.append("Low alignment")

        target, stop, base_size = self._realistic_target(atr, regime, aligned_count)
        size = base_size * size_mult * level_strength
        conf = float(np.clip(0.48 + (aligned_count - 3) * 0.07, 0.30, 0.85))

        opt_target = round(target * CONFIG["base_delta"], 1)
        opt_stop = round(stop * 0.75, 1)

        return TradeDecision(
            action=action,
            regime=regime,
            target_points=round(target, 1),
            stop_points=round(stop, 1),
            option_target_pts=opt_target,
            option_stop_pts=opt_stop,
            effective_delta=CONFIG["base_delta"],
            size_factor=round(size, 2),
            confidence=round(conf, 3),
            reason=" | ".join(reason_parts),
            timestamp=feats.get("timestamp"),
            decision_timestamp=now_ts,
            aligned_count=aligned_count,
            is_reversal=is_reversal,
            level_strength=level_strength
        )

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
            table,
            root_path=str(self.base / name),
            partition_cols=["date"] if "date" in data.columns else None,
            existing_data_behavior="overwrite_or_ignore"
        )

@dataclass
class PaperPosition:
    entry_time: datetime
    direction: int
    entry_future_price: float
    entry_option_price: float
    option_target: float
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
    exit_reason: str = ""
    max_favorable_pts: float = 0.0
    initial_aligned_count: int = 0
    peak_aligned_count: int = 0
    exit_aligned_count: int = 0
    alignment_path: str = "0"
    above_poc: int = 0
    smc_bias: int = 0
    delta_sign: int = 0
    near_demand: int = 0
    near_supply: int = 0
    level_strength: float = 1.0
    vp_poc: float = 0.0
    failure_reason: str = ""

class PaperTradingDesk:
    def __init__(self, dataset_manager: DatasetManager, stats_collector: StatsCollector):
        self.dataset_manager = dataset_manager
        self.stats = stats_collector
        self.active_position: Optional[PaperPosition] = None
        self.pending_order: Optional[Dict[str, Any]] = None
        self.closed_trades: deque = deque(maxlen=300)
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
        if self.realized_pnl_pts + self.unrealized_pnl_pts <= -CONFIG["max_daily_loss_pts"]:
            self.risk_locked = True
            self.pending_order = None
            return True
        return False

    def stage_signal(self, decision: TradeDecision, atr: float, next_bar_time: datetime, feats: Dict):
        if self.risk_locked or self.check_total_risk_limit():
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
                "aligned_count": decision.aligned_count,
                "is_reversal": decision.is_reversal,
                "above_poc": safe_int(feats.get("above_poc"), 0),
                "smc_bias": safe_int(feats.get("smc_bias"), 0),
                "delta_sign": safe_int(feats.get("delta_sign"), 0),
                "near_demand": safe_int(feats.get("near_demand_zone"), 0),
                "near_supply": safe_int(feats.get("near_supply_zone"), 0),
                "level_strength": decision.level_strength,
                "vp_poc": safe_float(feats.get("vp_poc"), 0.0),
            }

    def on_bar_open_fill(self, candle: Candle3Min, atr: float):
        self.check_and_reset_new_day(candle.timestamp)
        if self.pending_order and to_ist(candle.timestamp) >= to_ist(self.pending_order["target_fill_time"]):
            if self.check_total_risk_limit():
                self.pending_order = None
                return
            order = self.pending_order
            direction = order["direction"]
            vol_factor = max(0.5, min(2.0, (atr / 15.0))) if is_valid_number(atr) else 1.0
            slippage = CONFIG["base_slippage_pts"] * vol_factor * direction
            fill_price = candle.fut_o + slippage
            baseline = round(max(80.0, (fill_price * CONFIG["default_atm_iv"] * math.sqrt(1.0 / 252.0)) * 2.2), 2)

            self.active_position = PaperPosition(
                entry_time=candle.timestamp,
                direction=direction,
                entry_future_price=round(fill_price, 2),
                entry_option_price=baseline,
                option_target=order["option_target"],
                option_stop=order["option_stop"],
                effective_delta=order["effective_delta"],
                size=order["size"],
                regime=order["regime"],
                initial_aligned_count=order.get("aligned_count", 0),
                peak_aligned_count=order.get("aligned_count", 0),
                exit_aligned_count=order.get("aligned_count", 0),
                alignment_path=str(order.get("aligned_count", 0)),
                above_poc=order.get("above_poc", 0),
                smc_bias=order.get("smc_bias", 0),
                delta_sign=order.get("delta_sign", 0),
                near_demand=order.get("near_demand", 0),
                near_supply=order.get("near_supply", 0),
                level_strength=order.get("level_strength", 1.0),
                vp_poc=order.get("vp_poc", 0.0),
            )
            self.pending_order = None

    def force_close_and_reverse(self, candle: Candle3Min, decision: TradeDecision, atr: float, feats: Dict):
        if self.active_position is None:
            return
        pos = self.active_position
        fut_close_move = (candle.fut_c - pos.entry_future_price) if pos.direction == 1 else (pos.entry_future_price - candle.fut_c)
        option_close_pnl = fut_close_move * pos.effective_delta
        penalty = min(1.40, CONFIG["option_exit_spread_penalty"] * max(1.0, abs(option_close_pnl) / 10.0))
        net = (option_close_pnl - penalty) * pos.size

        pos.exit_time = candle.timestamp
        pos.exit_future_price = round(candle.fut_c, 2)
        pos.exit_option_price = round(max(5.0, pos.entry_option_price + option_close_pnl), 2)
        pos.pnl_pts = round(net, 2)
        pos.status = "CLOSED"
        pos.exit_reason = f"TWO-WAY REVERSAL (Spread -{penalty:.2f})"
        pos.exit_aligned_count = decision.aligned_count
        pos.failure_reason = "Reversal" if net < 0 else ""

        self.realized_pnl_pts = round(self.realized_pnl_pts + pos.pnl_pts, 2)
        self.closed_trades.append(pos)
        self.stats.record({
            "pnl_pts": pos.pnl_pts,
            "above_poc": pos.above_poc,
            "smc_bias": pos.smc_bias,
            "regime": pos.regime
        })
        self.dataset_manager.write_parquet(pd.DataFrame([asdict(pos)]), name="paper_trades_log")
        self.active_position = None
        self.unrealized_pnl_pts = 0.0

        next_t = candle.timestamp + timedelta(minutes=CONFIG["bar_minutes"])
        self.stage_signal(decision, atr, next_t, feats)

    def on_bar_update_and_exit_eval(self, candle: Candle3Min, is_session_end: bool = False,
                                    current_aligned_count: int = 0, decision: Optional[TradeDecision] = None,
                                    feats: Optional[Dict] = None):
        if self.active_position is None:
            self.unrealized_pnl_pts = 0.0
            self.check_total_risk_limit()
            return

        pos = self.active_position
        pos.bars_held += 1

        last = int(pos.alignment_path.split(" -> ")[-1])
        if current_aligned_count != last:
            pos.alignment_path += f" -> {current_aligned_count}"

        if current_aligned_count > pos.peak_aligned_count:
            pos.peak_aligned_count = current_aligned_count
            if current_aligned_count >= 5:
                pos.option_target = round(pos.option_target * 1.25, 1)

        if pos.direction == 1:
            fut_high_move = candle.fut_h - pos.entry_future_price
            fut_low_move = pos.entry_future_price - candle.fut_l
            fut_close_move = candle.fut_c - pos.entry_future_price
        else:
            fut_high_move = pos.entry_future_price - candle.fut_l
            fut_low_move = candle.fut_h - pos.entry_future_price
            fut_close_move = pos.entry_future_price - candle.fut_c

        option_high_pnl = fut_high_move * pos.effective_delta
        option_low_pnl = -(fut_low_move * pos.effective_delta)
        option_close_pnl = fut_close_move * pos.effective_delta

        pos.max_favorable_pts = max(pos.max_favorable_pts, option_high_pnl)
        if pos.max_favorable_pts >= 10.0 and pos.option_stop > 0.5:
            pos.option_stop = 0.5

        self.unrealized_pnl_pts = round(option_close_pnl * pos.size, 2)

        hit_target = option_high_pnl >= pos.option_target
        hit_stop = option_low_pnl <= -pos.option_stop
        momentum_fade = (pos.max_favorable_pts >= 8.0 and current_aligned_count <= 1)

        if decision and decision.is_reversal and decision.action in ("CE", "PE"):
            new_dir = 1 if decision.action == "CE" else -1
            if new_dir != pos.direction:
                self.force_close_and_reverse(candle, decision, 15.0, feats or {})
                return

        self.check_total_risk_limit()
        timeout = pos.bars_held >= (CONFIG["time_barrier_min"] // CONFIG["bar_minutes"])

        if is_session_end or hit_target or hit_stop or momentum_fade or timeout or self.risk_locked:
            if hit_target:
                exit_pnl = pos.option_target
                reason = "TARGET HIT"
                fail = ""
            elif hit_stop:
                exit_pnl = -pos.option_stop
                reason = "STOP LOSS"
                fail = "Stop"
            elif momentum_fade:
                exit_pnl = option_close_pnl
                reason = "MOMENTUM FADE"
                fail = "Fade"
            elif is_session_end:
                exit_pnl = option_close_pnl
                reason = "SESSION END"
                fail = "Session" if exit_pnl < 0 else ""
            else:
                exit_pnl = option_close_pnl
                reason = "TIME / KILL"
                fail = "Timeout"

            pos.exit_time = candle.timestamp
            pos.exit_future_price = round(candle.fut_c, 2)
            pos.exit_option_price = round(max(5.0, pos.entry_option_price + exit_pnl), 2)
            pos.exit_aligned_count = current_aligned_count
            penalty = min(1.40, CONFIG["option_exit_spread_penalty"] * max(1.0, abs(exit_pnl) / 10.0))
            pos.pnl_pts = round((exit_pnl - penalty) * pos.size, 2)
            pos.status = "CLOSED"
            pos.exit_reason = reason + f" (Spread -{penalty:.2f})"
            pos.failure_reason = fail

            self.realized_pnl_pts = round(self.realized_pnl_pts + pos.pnl_pts, 2)
            self.closed_trades.append(pos)
            self.stats.record({
                "pnl_pts": pos.pnl_pts,
                "above_poc": pos.above_poc,
                "smc_bias": pos.smc_bias,
                "regime": pos.regime
            })
            self.dataset_manager.write_parquet(pd.DataFrame([asdict(pos)]), name="paper_trades_log")
            self.active_position = None
            self.unrealized_pnl_pts = 0.0

print("PART 2 LOADED SUCCESSFULLY")
print("Ab 'Part 3 de do' likho")
# =========================================================
# KOTAK NEO ADAPTER + FULL UI
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
        self.spot_token = "Nifty 50"
        self.future_token = CONFIG.get("nifty_future_token", "53000")
        self.pcr_tokens: List[str] = []
        self.pcr_records: Dict[str, Dict[str, Any]] = {}
        self.active_pcr_expiry = None
        self.heavy_tokens = dict(NSE_CASH_TOKENS)
        self.token_to_symbol = {v: k for k, v in NSE_CASH_TOKENS.items()}
        self.discovery_log: List[str] = []
        self.last_error = ""
        self.dataset_manager = DatasetManager()
        self.feature_engine = FeatureEngine()
        self.decision_engine = DecisionEngine()
        self.paper_desk = PaperTradingDesk(self.dataset_manager, self.decision_engine.stats)
        self.candles_3m = deque(maxlen=150)
        self.current_bar_ticks: List[Dict] = []
        self.current_bar_time = None
        self._bar_deadline = None
        self.last_decision = None
        self._prev_ce_oi = np.nan
        self._prev_pe_oi = np.nan
        self._last_cum_volume = None
        self._watchdog_stop = threading.Event()
        self._watchdog_thread = None

    def _extract_oi(self, record: dict) -> float:
        if not isinstance(record, dict):
            return np.nan
        for key in ("oi", "open_interest", "openInterest", "OpenInterest", "oI", "OI"):
            val = safe_float(record.get(key))
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
                        self.latest[token] = item
                        self.tick_buffer.append(item)
                        self.current_bar_ticks.append(item)
                        if str(token) == str(self.future_token):
                            ltp = extract_tick_price(item)
                            vol = safe_float(item.get("v") or item.get("vol") or item.get("volume") or item.get("last_volume") or item.get("ltq"), 1.0)
                            if is_valid_number(ltp):
                                self.feature_engine.on_tick_for_delta(ltp, max(vol, 1.0))
        except Exception as e:
            self.last_error = f"on_message: {e}"

    def on_error(self, error):
        self.last_error = str(error) if error else ""

    def on_close(self, message=None):
        pass

    def on_open(self, message=None):
        self.connected = True

    def login(self, live_totp_override=""):
        if NeoAPI is None:
            raise RuntimeError("neo_api_client missing")
        totp = (live_totp_override or "").strip() or self.totp
        required = {
            "KOTAK_CONSUMER_KEY": self.consumer_key,
            "KOTAK_MOBILE": self.mobile,
            "KOTAK_UCC": self.ucc,
            "TOTP": totp,
            "KOTAK_MPIN": self.mpin
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError("Missing: " + ", ".join(missing))
        self.client = NeoAPI(
            environment=CONFIG["neo_environment"],
            access_token=None,
            neo_fin_key=None,
            consumer_key=self.consumer_key
        )
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
                sym = str(r.get("pTrdSymbol", r.get("ts", ""))).upper().strip()
                if sym.startswith("NIFTY") and ("FUT" in sym) and not any(x in sym for x in ["BANK", "FIN", "MID", "IT"]):
                    exp = expiry_from_record(r)
                    tok = token_from_record(r)
                    if exp and exp.date() >= now_d and tok:
                        futures.append((exp, tok, sym))
            if futures:
                futures.sort(key=lambda x: x[0])
                self.discovery_log.append(f"✓ Future: {futures[0][2]}")
                return futures[0][1]
        except Exception as e:
            self.last_error = str(e)
        return CONFIG.get("nifty_future_token", "53000")

    def discover_nifty_instruments(self, auto_pcr=True):
        if not self.connected:
            raise RuntimeError("Not authenticated")
        self.discovery_log.clear()
        self.heavy_tokens = dict(NSE_CASH_TOKENS)
        self.token_to_symbol = {v: k for k, v in NSE_CASH_TOKENS.items()}
        self.discovery_log.append(f"✓ {len(self.heavy_tokens)} Heavyweights")
        self.future_token = self.resolve_current_nifty_future_token()
        self.token_to_symbol[self.future_token] = "NIFTY_FUT"
        self.discovery_log.append(f"✓ Future Token: {self.future_token}")
        if auto_pcr:
            self.discover_pcr_chain()
        return True

    def discover_pcr_chain(self, center_strike=None):
        if not self.connected:
            return 0
        try:
            if not center_strike or not is_valid_number(center_strike):
                with self.lock:
                    center_strike = extract_tick_price(self.latest.get("Nifty 50", {})) or 24300.0
            step = CONFIG["pcr_strike_step"]
            atm = round(center_strike / step) * step
            count = CONFIG["pcr_strike_count"]
            targets = [atm + i * step for i in range(-count, count + 1)]
            res = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
            records = record_list(res)
            now_d = now_ist().date()
            valid_exps = []
            for r in records:
                sym = str(r.get("pTrdSymbol", "")).upper()
                if "NIFTY" in sym and (sym.endswith("CE") or sym.endswith("PE")) and not any(x in sym for x in ["BANK", "FIN"]):
                    exp = expiry_from_record(r)
                    if exp and exp.date() >= now_d:
                        valid_exps.append(exp)
            if not valid_exps:
                return 0
            self.active_pcr_expiry = min(valid_exps, key=lambda x: x.date())
            discovered = []
            for r in records:
                sym = str(r.get("pTrdSymbol", "")).upper()
                if "NIFTY" in sym and (sym.endswith("CE") or sym.endswith("PE")):
                    exp = expiry_from_record(r)
                    strike = strike_from_record(r)
                    op = option_type_from_record(r)
                    tok = token_from_record(r)
                    if tok and strike in targets and op in ("CE", "PE") and exp and exp.date() == self.active_pcr_expiry.date():
                        discovered.append(tok)
                        self.pcr_records[tok] = {"strike": strike, "option_type": op, "expiry": exp}
            self.pcr_tokens = list(set(discovered))
            self.discovery_log.append(f"✓ PCR: {len(self.pcr_tokens)} strikes")
            return len(self.pcr_tokens)
        except Exception as e:
            self.last_error = str(e)
            return 0

    def fetch_real_option_oi(self):
        if not self.connected or not self.pcr_tokens:
            return
        try:
            tokens = [{"instrument_token": str(t), "exchange_segment": "nse_fo"} for t in self.pcr_tokens[:25]]
            res = self.client.quotes(instrument_tokens=tokens, quote_type="all")
            for r in record_list(res):
                tok = token_from_record(r)
                if tok:
                    oi = self._extract_oi(r)
                    if is_valid_number(oi):
                        if tok not in self.latest:
                            self.latest[tok] = {}
                        self.latest[tok]["oi"] = oi
        except Exception:
            pass

    def fetch_market_snapshot(self):
        if not self.connected:
            return
        now_ts = now_ist()
        tokens = [
            {"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"},
            {"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"},
        ]
        for tok in self.heavy_tokens.values():
            tokens.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        for tok in self.pcr_tokens[:20]:
            tokens.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})
        try:
            res = self.client.quotes(instrument_tokens=tokens, quote_type="all")
            with self.lock:
                for r in record_list(res):
                    tok = token_from_record(r)
                    if not tok and "NIFTY" in str(r.get("display_symbol", "")).upper():
                        tok = "Nifty 50"
                    if tok:
                        r["_parsed_ts"] = now_ts
                        oi = self._extract_oi(r)
                        if is_valid_number(oi):
                            r["oi"] = oi
                        self.latest[tok] = r
                        self.tick_buffer.append(r)
                        self.current_bar_ticks.append(r)
                        if tok == str(self.future_token):
                            pdc = safe_float(r.get("c") or r.get("close"))
                            if is_valid_number(pdc) and self.feature_engine.sess.prev_close is None:
                                self.feature_engine.set_previous_day(pdc, safe_float(r.get("h")), safe_float(r.get("l")))
                            open_p = safe_float(r.get("o") or r.get("open"))
                            if is_valid_number(open_p):
                                self.feature_engine.set_today_open(open_p)
            self.fetch_real_option_oi()
        except Exception as e:
            self.last_error = f"Poll: {e}"

    def subscribe_live_feed(self):
        if not self.connected:
            raise RuntimeError("Not authenticated")
        self.fetch_market_snapshot()
        sub = [
            {"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"},
            {"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"},
        ]
        for tok in self.heavy_tokens.values():
            sub.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        for tok in self.pcr_tokens:
            sub.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})
        try:
            self.client.subscribe(instrument_tokens=[{"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"}], isIndex=True)
            self.client.subscribe(instrument_tokens=sub[1:], isIndex=False)
        except Exception as e:
            self.last_error = str(e)
        self.conn_state = "STREAMING"
        return len(sub)

    def maybe_flush_bars(self):
        now = now_ist()
        with self.lock:
            if self.current_bar_time is None:
                self.current_bar_time = floor_bar_timestamp(now, CONFIG["bar_minutes"])
                self._bar_deadline = self.current_bar_time + timedelta(minutes=CONFIG["bar_minutes"])
            if now >= self._bar_deadline:
                self._close_bar(self.current_bar_time)
                self.current_bar_time = self._bar_deadline
                self._bar_deadline = self.current_bar_time + timedelta(minutes=CONFIG["bar_minutes"])
                self.current_bar_ticks.clear()
            if CONFIG["session_end_flush"]:
                if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
                    if self.current_bar_ticks:
                        self._close_bar(self.current_bar_time or floor_bar_timestamp(now), is_session_end=True)
                        self.current_bar_ticks.clear()

    def _resolve_volume_clean(self, fut_ticks):
        if not fut_ticks:
            return 1.0
        last_cum = None
        for t in reversed(fut_ticks):
            c = safe_float(t.get("v") or t.get("vol") or t.get("volume") or t.get("last_volume"))
            if is_valid_number(c) and c > 0:
                last_cum = c
                break
        if last_cum is None:
            return 1.0
        if self._last_cum_volume is None:
            self._last_cum_volume = last_cum
            return 1.0
        delta = last_cum - self._last_cum_volume
        if delta < 0:
            delta = last_cum
        self._last_cum_volume = last_cum
        return max(1.0, float(delta))

    def _close_bar(self, bar_time: datetime, is_session_end=False):
        with self.lock:
            ticks_source = self.current_bar_ticks or list(self.tick_buffer)
            if not ticks_source:
                return

            def _prices(token):
                ticks = [t for t in ticks_source if str(token_from_record(t)) == str(token)]
                vals = [extract_tick_price(t) for t in ticks if is_valid_number(extract_tick_price(t))]
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
                fut_oi = self._extract_oi(self.latest.get(str(self.future_token), {})) or 1e7
                l2 = {}
            else:
                fut_o, fut_h, fut_l, fut_c = fut_prices[0], max(fut_prices), min(fut_prices), fut_prices[-1]
                fut_vol = self._resolve_volume_clean(fut_ticks)
                last_t = fut_ticks[-1] if fut_ticks else {}
                fut_oi = self._extract_oi(last_t) or self._extract_oi(self.latest.get(str(self.future_token), {})) or 1e7
                l2 = {
                    "best_bid": safe_float(last_t.get("bp"), fut_c),
                    "best_ask": safe_float(last_t.get("ap"), fut_c),
                    "bid_qty": safe_float(last_t.get("bq"), 1),
                    "ask_qty": safe_float(last_t.get("aq"), 1)
                }

            hw_snap = {}
            for sym, tok in self.heavy_tokens.items():
                t = self.latest.get(str(tok), {})
                c_val = extract_tick_price(t)
                o_val = extract_quote_field(t, ("o", "open")) or c_val
                vwap = extract_quote_field(t, ("vwap", "avp")) or o_val
                if is_valid_number(c_val):
                    hw_snap[sym] = {"o": o_val, "c": c_val, "vwap": vwap}

            total_ce = total_pe = 0.0
            atm_ce = atm_pe = np.nan
            atm = round((spot_c or 24300) / CONFIG["pcr_strike_step"]) * CONFIG["pcr_strike_step"]
            for tok in self.pcr_tokens:
                info = self.pcr_records.get(str(tok), {})
                t = self.latest.get(str(tok), {})
                oi = self._extract_oi(t)
                if is_valid_number(oi):
                    if info.get("option_type") == "CE":
                        total_ce += oi
                        if info.get("strike") == atm:
                            atm_ce = oi
                    elif info.get("option_type") == "PE":
                        total_pe += oi
                        if info.get("strike") == atm:
                            atm_pe = oi

            ce_chg = total_ce - self._prev_ce_oi if is_valid_number(self._prev_ce_oi) else 0.0
            pe_chg = total_pe - self._prev_pe_oi if is_valid_number(self._prev_pe_oi) else 0.0
            self._prev_ce_oi = total_ce if total_ce > 0 else self._prev_ce_oi
            self._prev_pe_oi = total_pe if total_pe > 0 else self._prev_pe_oi

            pcr_chain = {
                "pcr_oi": total_pe / max(total_ce, 1),
                "pcr_volume": np.nan,
                "ce_oi_change": ce_chg,
                "pe_oi_change": pe_chg,
                "ce_oi_atm": atm_ce,
                "pe_oi_atm": atm_pe,
                "atm_strike": atm,
                "total_ce_oi": total_ce,
                "total_pe_oi": total_pe,
                "active_expiry": self.active_pcr_expiry,
                "ce_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "CE"),
                "pe_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "PE"),
            }

            candle = Candle3Min(
                timestamp=bar_time,
                spot_o=spot_o, spot_h=spot_h, spot_l=spot_l, spot_c=spot_c,
                fut_o=fut_o, fut_h=fut_h, fut_l=fut_l, fut_c=fut_c,
                fut_volume=fut_vol, fut_oi=fut_oi,
                heavy=hw_snap, option_chain=pcr_chain, l2_depth=l2
            )

            feats = self.feature_engine.compute(candle, self.candles_3m)
            atr_v = safe_float(feats.get("atr_14_prev"), 15.0)

            self.paper_desk.on_bar_open_fill(candle, atr_v)

            curr_dir = self.paper_desk.active_position.direction if self.paper_desk.active_position else 0
            decision = self.decision_engine.decide(feats, current_position_direction=curr_dir)
            self.last_decision = decision

            signals = self.decision_engine._get_high_priority_signals(decision.regime, feats)
            curr_align = max(sum(1 for s in signals if s == 1), sum(1 for s in signals if s == -1))

            self.paper_desk.on_bar_update_and_exit_eval(
                candle,
                is_session_end=is_session_end,
                current_aligned_count=curr_align,
                decision=decision,
                feats=feats
            )

            self.candles_3m.append(candle)

            if not is_session_end:
                next_t = bar_time + timedelta(minutes=CONFIG["bar_minutes"])
                self.paper_desk.stage_signal(decision, atr_v, next_t, feats)

            feats["decision_action"] = decision.action
            feats["decision_regime"] = decision.regime
            feats["aligned_count"] = decision.aligned_count
            feats["level_strength"] = decision.level_strength
            self.dataset_manager.write_parquet(pd.DataFrame([feats]), name="features_3min")

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
                except Exception as e:
                    self.last_error = f"watchdog: {e}"
                self._watchdog_stop.wait(3.0)
        self._watchdog_thread = threading.Thread(target=_loop, daemon=True)
        self._watchdog_thread.start()

    def stop_bar_watchdog(self):
        self._watchdog_stop.set()

# =========================================================
# STREAMLIT UI
# =========================================================
def inject_custom_css():
    st.markdown("""
        <style>
            .stApp { background-color: #0b0e14; color: #e1e7ec; }
            .terminal-card { background: #151a23; border: 1px solid #232b38; border-radius: 8px; padding: 16px; margin-bottom: 12px; }
            .badge-ce { background:#064e3b; color:#34d399; padding:6px 14px; border-radius:6px; font-weight:700; }
            .badge-pe { background:#7f1d1d; color:#f87171; padding:6px 14px; border-radius:6px; font-weight:700; }
            .badge-neutral { background:#374151; color:#9ca3af; padding:6px 14px; border-radius:6px; font-weight:700; }
            .color-green { color:#34d399 !important; font-weight:bold; }
            .color-red { color:#f87171 !important; font-weight:bold; }
            .color-brown { color:#d97706 !important; font-weight:bold; }
            .status-pill { padding:3px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }
            .status-active { background:#064e3b; color:#10b981; }
            .status-auth { background:#1e3a5f; color:#60a5fa; }
            .status-offline { background:#451a1a; color:#ef4444; }
        </style>
    """, unsafe_allow_html=True)

def get_colored_text(value, name):
    if not is_valid_number(value):
        return f'<span style="color:#6b7280;">{value}</span>'
    color = "brown"
    if name in ["kalman_stretch", "normalized_stretch"]:
        if value > 0.3: color = "green"
        elif value < -0.3: color = "red"
    elif name == "stretch_slope_3":
        if value > 0.02: color = "green"
        elif value < -0.02: color = "red"
    elif name == "pcr_oi":
        if value > 1.05: color = "green"
        elif value < 0.95: color = "red"
    elif name == "breadth_10":
        if value > 0.55: color = "green"
        elif value < 0.45: color = "red"
    return f'<span class="color-{color}">{value:.2f}</span>'

if st is not None:
    @st.cache_resource
    def get_global_adapter():
        return KotakNeoAdapter()

def main():
    if st is None:
        print("Streamlit not installed")
        return

    st.set_page_config(page_title="NIFTY 3M v7.1 Final Complete", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")
    inject_custom_css()
    adapter = get_global_adapter()
    is_logged_in = adapter.connected

    with st.sidebar:
        st.subheader("⚡ Gateway")
        if is_logged_in:
            st.markdown(f'<span class="status-pill status-{"active" if adapter.conn_state=="STREAMING" else "auth"}">● {adapter.conn_state}</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-pill status-offline">● DISCONNECTED</span>', unsafe_allow_html=True)

        totp = st.text_input("Live TOTP", type="password")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Connect"):
                try:
                    adapter.login(live_totp_override=totp)
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        with c2:
            if st.button("Reconnect", disabled=not is_logged_in):
                adapter.login(live_totp_override=totp)
                st.rerun()

        if st.button("Discover", disabled=not is_logged_in):
            try:
                with st.spinner("Discovering instruments..."):
                    adapter.discover_nifty_instruments()
                    st.session_state.discovered = True
                    st.success("Discover complete!")
                    for log in adapter.discovery_log:
                        st.caption(log)
                    if adapter.last_error:
                        st.error(adapter.last_error)
            except Exception as e:
                st.error(f"Discover failed: {e}")
                adapter.last_error = str(e)
            st.rerun()

        if st.session_state.get("discovered"):
            st.caption("--- Discovery Log ---")
            for l in adapter.discovery_log:
                st.caption(l)
            if not adapter.discovery_log:
                st.warning("Discovery log empty (possible off-market)")

        if st.button("Start Live Feed", disabled=not is_logged_in or adapter.conn_state == "STREAMING"):
            adapter.subscribe_live_feed()
            adapter.start_bar_watchdog()
            st.rerun()

        if adapter.last_error:
            st.warning(adapter.last_error)

    if adapter.conn_state == "STREAMING":
        adapter.fetch_market_snapshot()

    # Top metrics
    spot_val = fut_val = "-"
    ticks = 0
    if adapter.latest:
        with adapter.lock:
            sp = extract_tick_price(adapter.latest.get("Nifty 50", {}))
            if is_valid_number(sp):
                spot_val = f"{sp:.2f}"
            fp = extract_tick_price(adapter.latest.get(str(adapter.future_token), {}))
            if is_valid_number(fp):
                fut_val = f"{fp:.2f}"
            ticks = len(adapter.tick_buffer)

    t1, t2, t3 = st.columns(3)
    t1.metric("NIFTY SPOT", f"₹{spot_val}")
    t2.metric("NIFTY FUT", f"₹{fut_val}")
    t3.metric("TICKS", f"{ticks}")

    # Decision
    st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
    if adapter.last_decision:
        d = adapter.last_decision
        badge = "badge-ce" if d.action == "CE" else ("badge-pe" if d.action == "PE" else "badge-neutral")
        c1, c2, c3, c4 = st.columns([1.4, 1.2, 1.2, 2])
        with c1:
            st.caption("SIGNAL")
            st.markdown(f'<div class="{badge}">{d.action}</div>', unsafe_allow_html=True)
        with c2:
            st.metric("Regime", d.regime)
        with c3:
            st.metric("Align / LS", f"{d.aligned_count}/8 | {d.level_strength:.2f}")
        with c4:
            st.caption("Reason")
            st.write(d.reason)
    else:
        st.info("Waiting for first bar...")
    st.markdown('</div>', unsafe_allow_html=True)

    # Paper Desk
    st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
    st.markdown("**Paper Trading + Enhanced Journal**")
    desk = adapter.paper_desk
    total = round(desk.realized_pnl_pts + desk.unrealized_pnl_pts, 2)
    closed = len(desk.closed_trades)
    wins = sum(1 for t in desk.closed_trades if t.pnl_pts > 0)
    wr = (wins / closed * 100) if closed else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Net PnL", f"{total:+.2f}")
    c2.metric("Unrealized", f"{desk.unrealized_pnl_pts:+.2f}")
    c3.metric("Closed", f"{closed}", delta=f"WR {wr:.0f}%")
    if desk.active_position:
        c4.markdown(f"**Active:** `{'CE' if desk.active_position.direction==1 else 'PE'}`")
    else:
        c4.markdown("**Active:** `FLAT`")

    if desk.closed_trades:
        recent = []
        for t in list(desk.closed_trades)[-8:]:
            recent.append({
                "Time": t.exit_time.strftime("%H:%M") if t.exit_time else "-",
                "Type": "CE" if t.direction == 1 else "PE",
                "PnL": t.pnl_pts,
                "Regime": t.regime,
                "Align": t.alignment_path,
                "Delta": t.delta_sign,
                "POC": t.above_poc,
                "SMC": t.smc_bias,
                "LS": round(t.level_strength, 2),
                "Fail": t.failure_reason,
                "Reason": t.exit_reason[:35]
            })
        st.dataframe(pd.DataFrame(recent), hide_index=True)
        csv = pd.DataFrame([asdict(t) for t in desk.closed_trades]).to_csv(index=False).encode()
        st.download_button("Download Journal", csv, f"journal_v71_{now_ist().strftime('%Y%m%d')}.csv")
    st.markdown('</div>', unsafe_allow_html=True)

    # Features + Full Scorecard
    if adapter.feature_engine.history:
        row = dict(adapter.feature_engine.history[-1])
        st.markdown("**Core Features + New Pillars**")
        f1, f2, f3, f4 = st.columns(4)
        f1.markdown(f"**Kalman Stretch**<br>{get_colored_text(row.get('kalman_stretch', 0), 'kalman_stretch')}", unsafe_allow_html=True)
        f2.markdown(f"**Slope**<br>{get_colored_text(row.get('stretch_slope_3', 0), 'stretch_slope_3')}", unsafe_allow_html=True)
        f3.markdown(f"**PCR**<br>{get_colored_text(row.get('pcr_oi', 1), 'pcr_oi')}", unsafe_allow_html=True)
        f4.markdown(f"**Breadth**<br>{get_colored_text(row.get('breadth_10', 0.5), 'breadth_10')}", unsafe_allow_html=True)

        with st.expander("🛡️ Full Institutional Scorecard (Vanna/Charm + All Items)", expanded=True):
            st.write(f"**Overall DQ Score:** `{row.get('data_quality_score', 0)*100:.0f}%`")
            st.write(f"• Cum Delta: `{row.get('cum_delta', 0):.0f}` | Sign: `{row.get('delta_sign', 0)}`")
            st.write(f"• VP POC: `{row.get('vp_poc', 0):.1f}` | Above POC: `{row.get('above_poc', 0)}`")
            st.write(f"• VAH/VAL Break: `{row.get('vah_break', 0)}` / `{row.get('val_break', 0)}`")
            st.write(f"• SMC Bias: `{row.get('smc_bias', 0)}` | BoS: `{row.get('bos_signal', 0)}`")
            st.write(f"• Near Demand/Supply: `{row.get('near_demand_zone', 0)}` / `{row.get('near_supply_zone', 0)}`")
            st.write(f"• ML Status: `FALLBACK (Heuristic)`")
            st.write(f"• 0DTE Intensity: `{row.get('zero_dte_intensity', 0):.2f}`")
            st.write(f"• **Dealer Vanna/Charm:** `{row.get('dealer_vanna_flow', 0):.3f}` / `{row.get('dealer_charm_flow', 0):.3f}`")
            st.write(f"• Causal Integrity: `{'1 (VERIFIED)' if row.get('is_causal')==1 else '0'}`")

            # FIXED Level Strength
            _ls = adapter.decision_engine.stats.get_level_strength(
                safe_int(row.get("above_poc", 0)),
                safe_int(row.get("smc_bias", 0))
            )
            st.write(f"• Level Strength (live): `{_ls:.2f}`")
            st.write(f"• Trades recorded for stats: `{len(adapter.decision_engine.stats.trades)}`")
            st.json(row)

    if adapter.conn_state == "STREAMING":
        time.sleep(CONFIG["ui_refresh_sec"])
        st.rerun()

if __name__ == "__main__":
    if st is not None and hasattr(st, "runtime") and st.runtime.exists():
        main()
    else:
        print("v7.1 Final Complete Engine - All 3 Parts Ready")
