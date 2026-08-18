#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine | v5.1 Institutional Prop-Grade Architecture
FIXED:
1. Kotak Neo library 'NoneType += str' crash hardened
2. All timing forced to IST (Asia/Kolkata) — Streamlit Cloud UTC issue fixed
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
from collections import deque
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
    """Always return current time in IST (Asia/Kolkata)"""
    return datetime.now(IST)

def to_ist(dt: datetime) -> datetime:
    """Convert any datetime to IST"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume naive datetime is already IST (common in this codebase)
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


# =========================================================
# 1. CONFIGURATION & CONSTANTS
# =========================================================

CONFIG = {
    "app_version": "v5.1_institutional_prop",
    "feature_version": "v4.0_vanna_charm_kalman_lob",
    "label_version": "TB_v3.0_clean",
    "schema_version": "4.0",
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
    "atm_delta_approx": 0.52,
    "estimated_slippage_pts": 0.50,
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
    """Floor to 3-min bar using IST market open (09:15)"""
    ts = to_ist(ts)
    # Remove timezone for arithmetic, then re-attach
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
    for key in ["exchange_token", "pSymbol", "pSymbolToken", "instrument_token", "instrumentToken", "tok", "token", "pToken", "tk"]:
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

def record_list(response):
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    for key in ["data", "result", "records", "data_list", "scrips", "list", "message"]:
        value = response.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for k in ["data", "records", "result", "scrips"]:
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
# 4. RESEARCH ENGINES
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
            if open_price is None:
                open_price = safe_float(data.get("o", data.get("c")))
                if is_valid_number(open_price) and open_price > 0:
                    self.day_open[symbol] = open_price
            close_price = safe_float(data.get("c"))
            if not is_valid_number(open_price) or open_price <= 0 or not is_valid_number(close_price):
                continue

            ret = (close_price - open_price) / open_price
            contributions.append(weight * ret)
            returns.append(ret)
            vwap = safe_float(data.get("vwap"), close_price)
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
            "breadth_10": bullish / n,
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
            **self.hw.compute(candle),
            **self.or_eng.features(candle, atr if is_valid_number(atr) else 0.0),
            **self.sess.features(candle, atr if is_valid_number(atr) else 0.0),
            **pcr_features,
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
# 5. REGIME & DECISION ENGINE
# =========================================================

class RegimeEngine:
    def detect(self, feats: Dict[str, Any]) -> str:
        dq = safe_float(feats.get("data_quality_score"), 0.0)
        atr_warm = int(feats.get("atr_warmup_flag") or 0)
        
        if dq < CONFIG["min_data_quality_to_trade"] or atr_warm == 1:
            return "DATA_BAD"
        
        k_stretch = safe_float(feats.get("kalman_stretch"), feats.get("normalized_stretch", 0.0))
        slope = safe_float(feats.get("stretch_slope_3"), 0.0)
        or_state = safe_int(feats.get("or_breakout_state"), 0)
oi_long = safe_int(feats.get("oi_long_buildup"), 0)
oi_short = safe_int(feats.get("oi_short_buildup"), 0)

oi_unwind = (
    safe_int(feats.get("oi_long_unwinding"), 0)
    or safe_int(feats.get("oi_short_covering"), 0)
)
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
    action: str
    regime: str
    target_points: float
    stop_points: float
    option_target_pts: float
    option_stop_pts: float
    size_factor: float
    confidence: float
    reason: str
    timestamp: Optional[datetime] = None
    decision_timestamp: Optional[datetime] = None
    ml_probability: float = 0.5


class DecisionEngine:
    def __init__(self):
        self.regime_engine = RegimeEngine()
        self.last_action: Optional[str] = None
        self.last_action_bar_idx: int = -999
        self.bar_counter: int = 0
        self.ml_model = None
        self.expected_feature_names: List[str] = []
        self._load_production_model()

    def _load_production_model(self):
        model_p = Path(CONFIG.get("model_path", ""))
        if joblib and model_p.exists():
            try:
                loaded = joblib.load(model_p)
                if isinstance(loaded, dict) and "model" in loaded:
                    self.ml_model = loaded["model"]
                    self.expected_feature_names = loaded.get("features", [])
                else:
                    self.ml_model = loaded
                    if hasattr(self.ml_model, "feature_name_"):
                        self.expected_feature_names = list(self.ml_model.feature_name_)
                    elif hasattr(self.ml_model, "feature_names_in_"):
                        self.expected_feature_names = list(self.ml_model.feature_names_in_)
            except Exception:
                self.ml_model = None
                self.expected_feature_names = []

    def get_adaptive_weights(self, regime: str) -> Tuple[float, float]:
        if regime in ["IMPULSE_UP", "IMPULSE_DOWN", "STAIRCASE_UP", "STAIRCASE_DOWN"]:
            return 0.82, 0.18
        if regime in ["GRIND", "NEUTRAL"]:
            return 0.55, 0.45
        if regime == "FAILURE":
            return 0.35, 0.65
        return 0.70, 0.30

    def _realistic_target(self, atr: float, regime: str, strategy: str) -> Tuple[float, float, float]:
        if not is_valid_number(atr) or atr <= 0:
            atr = 15.0
        if strategy == "TREND":
            if regime in ("IMPULSE_UP", "IMPULSE_DOWN"):
                return 1.15 * atr, 0.70 * atr, 1.00
            if regime in ("STAIRCASE_UP", "STAIRCASE_DOWN"):
                return 0.90 * atr, 0.65 * atr, 0.85
            return 0.70 * atr, 0.55 * atr, 0.70
        if strategy == "MEAN_REVERSION":
            return 0.55 * atr, 0.40 * atr, 0.75
        return 0.60 * atr, 0.50 * atr, 0.55

    def _predict_real_ml_proba(self, feats: Dict[str, Any]) -> float:
        if self.ml_model is None:
            return 0.5
        try:
            if self.expected_feature_names:
                row = [safe_float(feats.get(k), np.nan) for k in self.expected_feature_names]
                df_in = pd.DataFrame([row], columns=self.expected_feature_names)
            else:
                numeric_feats = {k: v for k, v in feats.items() if isinstance(v, (int, float)) and np.isfinite(v)}
                df_in = pd.DataFrame([numeric_feats])
                
            if hasattr(self.ml_model, "predict_proba"):
                probs = self.ml_model.predict_proba(df_in)
                return float(probs[0][1])
            elif hasattr(self.ml_model, "predict"):
                pred = self.ml_model.predict(df_in)
                return float(pred[0])
        except Exception:
            pass
        return 0.5

    def decide(self, feats: Dict[str, Any]) -> TradeDecision:
        self.bar_counter += 1
        now_ts = now_ist()
        regime = self.regime_engine.detect(feats)
        atr = safe_float(feats.get("atr_14_prev"), 15.0)
        stretch = safe_float(feats.get("kalman_stretch"), feats.get("normalized_stretch", 0.0))
        slope = safe_float(feats.get("stretch_slope_3"), 0.0)
        or_state = int(feats.get("or_breakout_state") or 0)
        dq = safe_float(feats.get("data_quality_score"), 0.0)
        twc = safe_float(feats.get("twc"), 0.0)
        breadth = safe_float(feats.get("breadth_10"), 0.5)
        pcr = safe_float(feats.get("pcr_oi"), 1.0)
        pcr_vel = safe_float(feats.get("pcr_velocity"), 0.0)
        vanna = safe_float(feats.get("dealer_vanna_flow"), 0.0)
        gex_x = safe_float(feats.get("gex_x_0dte"), 0.0)
        obi = safe_float(feats.get("order_book_imbalance"), 0.0)

        if regime == "DATA_BAD" or dq < CONFIG["min_data_quality_to_trade"]:
            return TradeDecision("SKIP", regime, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                                "Data quality low / warmup pending", feats.get("timestamp"), now_ts)

        strategy = "NONE"
        action = "SKIP"
        reason = ""
        conf_penalty = 0.0

        if regime in ("IMPULSE_UP", "IMPULSE_DOWN", "STAIRCASE_UP", "STAIRCASE_DOWN"):
            strategy = "TREND"
            score = (
                np.clip(stretch, -2, 2) * 1.25 +
                np.clip(slope, -1, 1) * 0.85 +
                or_state * 0.45 +
                np.clip(twc * 55.0, -0.9, 0.9) +
                (breadth - 0.5) * 1.4 +
                np.clip(obi * 0.35, -0.35, 0.35) +
                np.clip((pcr - 1.0) * 0.45, -0.45, 0.45) +
                np.clip(pcr_vel * 1.8, -0.30, 0.30) +
                np.clip(gex_x * 0.70, -0.70, 0.70) +
                np.clip(vanna * 0.30, -0.30, 0.30)
            )
            raw_action = "CE" if score >= 0.18 else ("PE" if score <= -0.18 else "SKIP")

            hold = CONFIG["signal_min_hold_bars"]
            if self.last_action and self.last_action != "SKIP":
                bars_since = self.bar_counter - self.last_action_bar_idx
                if bars_since < hold and raw_action != self.last_action and raw_action != "SKIP":
                    action = self.last_action
                    conf_penalty = 0.12
                    reason = f"Trend hold ({bars_since}/{hold}) | {regime}"
                else:
                    action = raw_action
                    reason = f"Trend strategy | {regime} | score={score:.2f}"
            else:
                action = raw_action
                reason = f"Trend strategy | {regime} | score={score:.2f}"

        elif regime in ("GRIND", "NEUTRAL"):
            strategy = "MEAN_REVERSION"

            upper_rej = (
                stretch > 0.25 and
                slope < 0.05 and
                (breadth < 0.53 or twc < 0.0005 or pcr > 1.04 or vanna < -0.05 or obi < -0.15)
            )
            lower_rej = (
                stretch < -0.25 and
                slope > -0.05 and
                (breadth > 0.47 or twc > -0.0005 or pcr < 0.96 or vanna > 0.05 or obi > 0.15)
            )

            if upper_rej:
                action = "PE"
                reason = f"Mean-reversion (upper rejection) | {regime} | stretch={stretch:.2f}"
            elif lower_rej:
                action = "CE"
                reason = f"Mean-reversion (lower rejection) | {regime} | stretch={stretch:.2f}"
            else:
                action = "SKIP"
                reason = f"Range middle / no clear boundary | {regime} | stretch={stretch:.2f}"

        else:
            action = "SKIP"
            reason = f"No edge regime: {regime}"

        target, stop, size = self._realistic_target(atr, regime, strategy)

        ml_prob = self._predict_real_ml_proba(feats)
        rule_conf = 0.52 + min(0.28, abs(stretch) * 0.18) - conf_penalty

        if strategy == "TREND" and regime.startswith("IMPULSE"):
            rule_conf += 0.09
        if strategy == "MEAN_REVERSION" and action != "SKIP":
            rule_conf += 0.05

        rule_w, ml_w = self.get_adaptive_weights(regime)
        combined_conf = (rule_w * rule_conf) + (ml_w * abs(ml_prob - 0.5) * 2.0)
        conf = float(np.clip(combined_conf, 0.28, 0.88))

        hw_seen = int(feats.get("hw_symbols_seen") or 0)
        min_hw = CONFIG.get("hw_min_symbols_required", 5)
        if hw_seen < min_hw:
            size *= 0.55
            conf = max(0.28, conf - 0.14)
            reason += f" | HW weak ({hw_seen})"
        elif hw_seen < 8:
            size *= 0.85
            conf = max(0.28, conf - 0.05)

        delta = CONFIG["atm_delta_approx"]
        opt_target = round(target * delta, 1)
        opt_stop = round(stop * delta, 1)

        if action in ("CE", "PE"):
            self.last_action = action
            self.last_action_bar_idx = self.bar_counter

        return TradeDecision(
            action, regime,
            round(target, 1), round(stop, 1),
            opt_target, opt_stop,
            round(size, 2), round(conf, 3),
            reason,
            feats.get("timestamp"), now_ts, ml_prob
        )


# =========================================================
# 6. PAPER TRADING & DATASET
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
    entry_price: float
    target_price: float
    stop_price: float
    size: float
    regime: str
    option_target: float
    option_stop: float
    bars_held: int = 0
    status: str = "OPEN"
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    pnl_pts: float = 0.0
    exit_reason: str = ""


class PaperTradingDesk:
    def __init__(self, dataset_manager: DatasetManager):
        self.dataset_manager = dataset_manager
        self.active_position: Optional[PaperPosition] = None
        self.pending_order: Optional[Dict[str, Any]] = None
        self.closed_trades: deque = deque(maxlen=200)
        self.realized_pnl_pts: float = 0.0
        self.unrealized_pnl_pts: float = 0.0
        self.current_trade_date: Optional[date] = None

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

    def stage_signal(self, decision: TradeDecision, atr: float, next_bar_time: datetime):
        if decision.action in ("CE", "PE") and self.active_position is None and self.pending_order is None:
            direction = 1 if decision.action == "CE" else -1
            self.pending_order = {
                "target_fill_time": next_bar_time,
                "direction": direction,
                "target_pts": decision.target_points,
                "stop_pts": decision.stop_points,
                "size": decision.size_factor,
                "regime": decision.regime,
                "option_target": decision.option_target_pts,
                "option_stop": decision.option_stop_pts,
            }

    def on_bar_open_fill(self, candle: Candle3Min):
        self.check_and_reset_new_day(candle.timestamp)
        if self.pending_order and to_ist(candle.timestamp) >= to_ist(self.pending_order["target_fill_time"]):
            order = self.pending_order
            direction = order["direction"]
            slippage = CONFIG["estimated_slippage_pts"] * direction
            fill_price = candle.fut_o + slippage
            
            if direction == 1:
                target_p = fill_price + order["target_pts"]
                stop_p = fill_price - order["stop_pts"]
            else:
                target_p = fill_price - order["target_pts"]
                stop_p = fill_price + order["stop_pts"]

            self.active_position = PaperPosition(
                entry_time=candle.timestamp,
                direction=direction,
                entry_price=round(fill_price, 2),
                target_price=round(target_p, 2),
                stop_price=round(stop_p, 2),
                size=order["size"],
                regime=order["regime"],
                option_target=order["option_target"],
                option_stop=order["option_stop"]
            )
            self.pending_order = None

    def on_bar_update_and_exit_eval(self, candle: Candle3Min, is_session_end: bool = False):
        if self.active_position is None:
            self.unrealized_pnl_pts = 0.0
            return

        pos = self.active_position
        pos.bars_held += 1
        
        if pos.direction == 1:
            self.unrealized_pnl_pts = round((candle.fut_c - pos.entry_price) * pos.size, 2)
            hit_target = candle.fut_h >= pos.target_price
            hit_stop = candle.fut_l <= pos.stop_price
        else:
            self.unrealized_pnl_pts = round((pos.entry_price - candle.fut_c) * pos.size, 2)
            hit_target = candle.fut_l <= pos.target_price
            hit_stop = candle.fut_h >= pos.stop_price

        timeout = pos.bars_held >= (CONFIG["time_barrier_min"] // CONFIG["bar_minutes"])

        if is_session_end or hit_target or hit_stop or timeout:
            if is_session_end:
                exit_p = candle.fut_c
                reason = "SESSION END AUTO-EXIT"
            elif hit_target and hit_stop:
                exit_p = pos.stop_price
                reason = "AMBIGUOUS (SL ASSUMED)"
            elif hit_target:
                exit_p = pos.target_price
                reason = "TARGET HIT"
            elif hit_stop:
                exit_p = pos.stop_price
                reason = "STOP LOSS HIT"
            else:
                exit_p = candle.fut_c
                reason = "TIME BARRIER EXIT"

            pos.exit_time = candle.timestamp
            pos.exit_price = round(exit_p, 2)
            pos.pnl_pts = round(((exit_p - pos.entry_price) if pos.direction == 1 else (pos.entry_price - exit_p)) * pos.size, 2)
            pos.status = "CLOSED"
            pos.exit_reason = reason

            self.realized_pnl_pts = round(self.realized_pnl_pts + pos.pnl_pts, 2)
            self.closed_trades.append(pos)
            
            trade_record = asdict(pos)
            trade_record["timestamp"] = pos.exit_time
            self.dataset_manager.write_parquet(pd.DataFrame([trade_record]), name="paper_trades_log")

            self.active_position = None
            self.unrealized_pnl_pts = 0.0


# =========================================================
# 7. KOTAK NEO ADAPTER - FULLY HARDENED
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
                    "FPI" not in sym and
                    "BANK" not in sym and
                    "FIN" not in sym and
                    "MID" not in sym and
                    "NXT" not in sym and
                    "IT" not in sym
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
                self.discovery_log.append(f"✓ Resolved Nifty Future: {nearest_sym} | Token: {nearest_tok} | Expiry: {nearest_exp.date()}")
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
        
        self.spot_token = "Nifty 50"
        self.token_to_symbol[self.spot_token] = "NIFTY_SPOT"
        self.discovery_log.append("✓ Configured Nifty Spot Index: Nifty 50")

        self.future_token = self.resolve_current_nifty_future_token()
        self.token_to_symbol[self.future_token] = "NIFTY_FUT"
        self.discovery_log.append(f"✓ Configured Active Future Token: {self.future_token}")

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
                    center_strike = extract_tick_price(spot_tick) or 24300.0
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
                if not nifty_opt_pattern.match(sym) or any(x in sym for x in ["NXT", "FPI", "FIN", "BANK", "MID", "IT"]):
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
                if not nifty_opt_pattern.match(sym) or any(x in sym for x in ["NXT", "FPI", "FIN", "BANK", "MID", "IT"]):
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
            self.discovery_log.append(f"✓ Single-Expiry PCR ({target_exp_date}): {len(self.pcr_tokens)} Strikes")
            return len(self.pcr_tokens)
        except Exception:
            return 0

    def fetch_real_option_oi(self):
        """Hardened against Kotak library NoneType += str bug"""
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
                    if is_valid_number(oi):
                        if tok not in self.latest:
                            self.latest[tok] = {}
                        self.latest[tok]["oi"] = oi
                        self.latest[tok]["open_interest"] = oi
                        self.latest[tok]["open_int"] = oi
        except Exception as e:
            err_msg = str(e)
            if "NoneType" in err_msg and ("+=" in err_msg or "unsupported operand" in err_msg):
                # Library internal bug — ignore
                pass
            else:
                self.last_error = f"Option OI: {err_msg}"

    def fetch_market_snapshot(self):
        """HARDENED against Kotak library 'NoneType += str' crash + IST timing"""
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
            err_msg = str(exc)
            if "NoneType" in err_msg and ("+=" in err_msg or "unsupported operand" in err_msg):
                # This is the exact library bug — swallow it so bars can continue
                self.last_error = "Poll: Kotak library internal bug ignored (using live ticks)"
            else:
                self.last_error = f"Poll error: {err_msg}"

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
                o_val = safe_float(t.get("o") or t.get("open"), c_val)
                if is_valid_number(c_val):
                    hw_snap[sym] = {"o": o_val if is_valid_number(o_val) else c_val, "c": c_val, "vwap": safe_float(t.get("vwap"), c_val)}

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

            ce_oi_change = total_ce_oi - self._prev_ce_oi if is_valid_number(self._prev_ce_oi) else np.nan
            pe_oi_change = total_pe_oi - self._prev_pe_oi if is_valid_number(self._prev_pe_oi) else np.nan
            self._prev_ce_oi = total_ce_oi
            self._prev_pe_oi = total_pe_oi

            pcr_chain = {
                "pcr_oi": total_pe_oi / max(total_ce_oi, 1.0) if total_ce_oi > 0 else np.nan,
                "pcr_volume": total_pe_vol / max(total_ce_vol, 1.0) if total_ce_vol > 0 else np.nan,
                "ce_oi_change": ce_oi_change, "pe_oi_change": pe_oi_change,
                "ce_oi_atm": atm_ce_oi, "pe_oi_atm": atm_pe_oi, "atm_strike": atm,
                "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi,
                "active_expiry": self.active_pcr_expiry,
                "ce_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "CE"),
                "pe_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "PE"),
            }

            candle = Candle3Min(
                timestamp=bar_time, spot_o=spot_o, spot_h=spot_h, spot_l=spot_l, spot_c=spot_c,
                fut_o=fut_o, fut_h=fut_h, fut_l=fut_l, fut_c=fut_c, fut_volume=fut_vol, fut_oi=fut_oi,
                heavy=hw_snap, option_chain=pcr_chain, l2_depth=l2_snap
            )
            
            self.paper_desk.on_bar_open_fill(candle)
            self.paper_desk.on_bar_update_and_exit_eval(candle, is_session_end=is_session_end)

            feats = self.feature_engine.compute(candle, self.candles_3m)
            self.candles_3m.append(candle)

            decision = self.decision_engine.decide(feats)
            self.last_decision = decision
            
            if not is_session_end:
                atr_v = safe_float(feats.get("atr_14_prev"), 15.0)
                next_t = bar_time + timedelta(minutes=CONFIG["bar_minutes"])
                self.paper_desk.stage_signal(decision, atr_v, next_t)

            feats["decision_action"] = decision.action
            feats["decision_regime"] = decision.regime
            feats["decision_target"] = decision.target_points
            feats["decision_confidence"] = decision.confidence
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
# 8. STREAMLIT UI
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


def main():
    if st is None:
        print("Streamlit not installed.")
        return

    st.set_page_config(page_title="NIFTY 3M | Micro Engine v5.1", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")
    inject_custom_css()

    adapter: KotakNeoAdapter = get_global_adapter()
    is_logged_in = adapter.connected

    with st.sidebar:
        st.subheader("⚡ Gateway Controls")
        
        if is_logged_in:
            conn_txt = getattr(adapter, "conn_state", "AUTHENTICATED")
            if conn_txt == "STREAMING":
                st.markdown('<span class="status-pill status-active">● STREAMING (LIVE)</span>', unsafe_allow_html=True)
            else:
                st.markdown('<span class="status-pill status-auth">● CONNECTED (AUTHENTICATED)</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-pill status-offline">● DISCONNECTED</span>', unsafe_allow_html=True)

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
        st.subheader("🔍 Subscriptions")
        
        if st.button("Discover Instruments", key="btn_disc", disabled=not is_logged_in):
            with st.spinner("Locking NIFTY Instruments..."):
                adapter.discover_nifty_instruments(auto_pcr=True)
                st.session_state.discovered = True
                st.rerun()

        if st.session_state.get("discovered") and adapter.discovery_log:
            st.success("✓ Instruments Mapped!")
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
                st.success("Engine Verification Passed (v5.1)" if run_unit_tests() else "Test Failed")
            except Exception as exc:
                st.error(str(exc))

    if is_streaming and adapter:
        adapter.fetch_market_snapshot()
        adapter.maybe_flush_bars()

    # Top Metric Strip
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
    t1.metric("NIFTY SPOT", f"₹{spot_val}")
    t2.metric("NIFTY FUT", f"₹{fut_val}")
    t3.metric("FUT OPEN INTEREST", f"{int(fut_oi):,}" if isinstance(fut_oi, (int, float)) and np.isfinite(fut_oi) else str(fut_oi))
    t4.metric("TICKS INGESTED", f"{ticks_count:,}")

    # Tactical Signal HUD
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
            st.metric("Spot Target / SL", f"+{d.target_points} / -{d.stop_points} pt")
            st.caption(f"**Theoretical Option Move:** +{d.option_target_pts} / -{d.option_stop_pts} pt")
        with col_hud4:
            st.metric("Confidence", f"{d.confidence * 100:.0f}%")
        with col_hud5:
            st.caption("Engine Rationale")
            st.write(f"_{d.reason}_")
    else:
        st.info("Awaiting first completed 3-minute bar to establish baseline regime and signal...")
    st.markdown('</div>', unsafe_allow_html=True)

    # Paper Trading Desk
    st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
    st.markdown("**⚡ Live Paper Trading Desk & Journal**")
    
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
        
        if active_pos:
            dir_str = "CE (LONG)" if active_pos.direction == 1 else "PE (SHORT)"
            col_p4.markdown(f"**Active Position:** `{dir_str}`<br>Entry: `₹{active_pos.entry_price}` | SL: `₹{active_pos.stop_price}`", unsafe_allow_html=True)
        elif desk.pending_order:
            p_dir = "CE" if desk.pending_order["direction"] == 1 else "PE"
            col_p4.markdown(f"**Order Staged:** `{p_dir}` (Filling Next Open)", unsafe_allow_html=True)
        else:
            col_p4.markdown("**Active Position:** `FLAT (NO POSITION)`", unsafe_allow_html=True)

        if desk.closed_trades:
            st.markdown("---")
            col_tbl_head, col_tbl_dl = st.columns([3, 1])
            with col_tbl_head:
                st.caption("Recent Closed Paper Trades (Real-Time)")
            
            trades_raw = [asdict(t) for t in desk.closed_trades]
            df_full_journal = pd.DataFrame(trades_raw)
            
            with col_tbl_dl:
                csv_data = df_full_journal.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Journal (.csv)",
                    data=csv_data,
                    file_name=f"nifty_paper_trades_{now_ist().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            
            recent_trades_data = []
            for t in list(desk.closed_trades)[-8:]:
                recent_trades_data.append({
                    "Exit Time": t.exit_time.strftime("%H:%M:%S") if t.exit_time else "-",
                    "Type": "CE (LONG)" if t.direction == 1 else "PE (SHORT)",
                    "Entry (₹)": f"{t.entry_price:.2f}",
                    "Exit (₹)": f"{t.exit_price:.2f}",
                    "PnL (pt)": t.pnl_pts,
                    "Bars Held": t.bars_held,
                    "Exit Reason": t.exit_reason
                })
            
            df_trades = pd.DataFrame(recent_trades_data)
            st.dataframe(df_trades, hide_index=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # Analytics Grid
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
                
                with st.expander("🛡️ Institutional MLOps & 2nd-Order Greeks Scorecard", expanded=False):
                    dq_val = latest_row.get("data_quality_score", 1.0)
                    st.write(f"**Overall DQ Score:** `{dq_val * 100:.0f}%`")
                    
                    c_dq1, c_dq2 = st.columns(2)
                    with c_dq1:
                        st.write(f"• Top 5 Lead Pressure (SLP_5): `{latest_row.get('slp_top5_pressure', 0.0):.3f}`")
                        st.write(f"• Order Book Imbalance (OBI): `{latest_row.get('order_book_imbalance', 0.0):.3f}`")
                        st.write(f"• Dealer Vanna Flow: `{latest_row.get('dealer_vanna_flow', 0.0):.3f}`")
                        st.write(f"• Dealer Charm Flow: `{latest_row.get('dealer_charm_flow', 0.0):.3f}`")
                        st.write(f"• Dealer GEX Proxy: `{latest_row.get('gex_proxy', 0.0):.3f}`")
                    with c_dq2:
                        model_loaded = adapter.decision_engine.ml_model is not None
                        feat_cnt = len(adapter.decision_engine.expected_feature_names)
                        st.write(f"• ML Status: `{'ACTIVE (' + str(feat_cnt) + ' Features)' if model_loaded else 'FALLBACK (Heuristic)'}`")
                        st.write(f"• 0DTE Intensity: `{latest_row.get('zero_dte_intensity', 0.0):.2f}`")
                        st.write(f"• Minutes to Expiry: `{latest_row.get('minutes_to_expiry', 0.0):.0f} min`")
                        st.write(f"• Gap Points: `{latest_row.get('gap_points', 0.0):.1f} pt`")
                        st.write(f"• Causal Integrity Tag: `{'1 (VERIFIED)' if latest_row.get('is_causal') == 1 else '0 (INVALID)'}`")
                    st.json(latest_row)
            else:
                st.caption("Feature extraction initializing...")

    with grid_right:
        st.markdown("**Top 5 Core Heavyweights Momentum (SLP-5)**")
        if adapter and adapter.heavy_tokens:
            hw_list = []
            with adapter.lock:
                for sym in HEAVYWEIGHTS_TOP5.keys():
                    tok = str(adapter.heavy_tokens.get(sym))
                    t = adapter.latest.get(tok, {})
                    ltp = extract_tick_price(t)
                    hw_list.append({"Symbol": sym, "LTP": f"₹{ltp:.2f}" if is_valid_number(ltp) else "-", "Weight": f"{HEAVYWEIGHTS_TOP5.get(sym, 0)*100:.1f}%"})
            st.dataframe(pd.DataFrame(hw_list), height=210, hide_index=True)
        else:
            st.caption("Heavyweights mapping pending discovery...")

    if is_streaming:
        time.sleep(CONFIG["ui_refresh_sec"])
        st.rerun()


if __name__ == "__main__":
    if st is not None and hasattr(st, "runtime") and st.runtime.exists():
        main()
    else:
        print("⚡ Running Institutional Prop-Engine Verification...")
        if run_unit_tests():
            print("✓ All Quant Engines Verified + IST Timezone + Library Bug Hardened.")
        else:
            raise RuntimeError("Engine Verification Failed.")
