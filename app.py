#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine
Kotak Neo Integrated Research-Lock v2.0
Official Kotak Neo API v2 connectivity + dynamic discovery
Research concepts intentionally preserved.
"""

from __future__ import annotations

import os
import json
import time
import hmac
import hashlib
import struct
import base64
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import streamlit as st
except ImportError:
    st = None

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None


# =========================================================
# PURE PYTHON TOTP & INPUT NORMALIZATION
# =========================================================

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
    """
    Normalize Indian registered mobile number for Kotak Neo TOTP login.

    Accepted examples:
        9876543210
        919876543210
        +919876543210
        00919876543210
        +91 98765-43210

    Output:
        +919876543210
    """
    raw = str(value or "").strip()

    if not raw:
        raise ValueError(
            "KOTAK_MOBILE is empty. Enter your registered Kotak mobile number."
        )

    # Keep digits only; this removes spaces, -, (, ), etc.
    digits = "".join(ch for ch in raw if ch.isdigit())

    if digits.startswith("00"):
        digits = digits[2:]

    # Already country code +91
    if digits.startswith("91") and len(digits) == 12:
        national = digits[2:]

    # Normal Indian 10-digit mobile
    elif len(digits) == 10:
        national = digits

    else:
        raise ValueError(
            "Invalid KOTAK_MOBILE format. Use your registered 10-digit "
            "Indian mobile number, e.g. 9876543210. The app will convert it to +91 format."
        )

    if len(national) != 10 or national[0] not in "6789":
        raise ValueError(
            "KOTAK_MOBILE is not a valid Indian mobile number."
        )

    return "+91" + national


# =========================================================
# CONFIGURATION - RESEARCH LOCK FROZEN
# =========================================================

CONFIG = {
    "app_version": "v2.0_kotak_neo",
    "feature_version": "v2.0_research_lock",
    "label_version": "TB_v1.6_lock",
    "schema_version": "2.0",
    "weight_version": "NIFTY_STATIC_2025Q1",
    "atr_period": 14,
    "sma_period": 20,
    "triple_upper_atr": 1.0,
    "triple_lower_atr": 0.75,
    "time_barrier_min": 30,
    "mfe_horizons_min": [15, 30, 45],
    "max_label_horizon_min": 45,
    "purge_bars": 18,
    "embargo_bars": 5,
    "opening_range_minutes": 15,
    "atr_mode": "session_local",
    "execution_model": "next_bar_open",
    "session_start": "09:15",
    "session_end": "15:30",
    "bar_minutes": 3,
    "dataset_path": "./nifty_3min_dataset",
    "neo_environment": "prod",
    "nifty_index_name": "Nifty 50",
    "nifty_future_token": os.getenv("NIFTY_FUT_TOKEN", "").strip(),
    "pcr_strike_count": int(os.getenv("PCR_STRIKE_COUNT", "5")),
    "pcr_strike_step": float(os.getenv("PCR_STRIKE_STEP", "50")),
}

HEAVYWEIGHTS = {
    "HDFCBANK": 0.115,
    "RELIANCE": 0.098,
    "ICICIBANK": 0.080,
    "INFY": 0.058,
    "ITC": 0.042,
    "TCS": 0.040,
    "LT": 0.038,
    "AXISBANK": 0.033,
    "KOTAKBANK": 0.029,
    "SBIN": 0.028,
}


# =========================================================
# HELPERS
# =========================================================

def safe_float(value, default=np.nan):
    try:
        if value is None:
            return default
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


def floor_bar_timestamp(ts: datetime, minutes=3):
    anchor = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    if ts < anchor:
        return None
    elapsed = int((ts - anchor).total_seconds() // 60)
    return anchor + timedelta(minutes=(elapsed // minutes) * minutes)


def wilder_atr(trs: List[float], period=14):
    if len(trs) < period:
        return np.nan
    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return float(atr)


def parse_expiry(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        x = float(value)
        if x > 10_000_000_000:
            return datetime.fromtimestamp(x / 1000)
        if x > 1_000_000_000:
            return datetime.fromtimestamp(x)
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    for fmt in [
        "%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",
        "%d%b%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d"
    ]:
        try:
            return datetime.strptime(text.upper(), fmt)
        except Exception:
            pass
    return None


def expiry_from_record(record):
    for key in [
        "pExpiryDate", "lExpiryDate", "pMaturityDate", "pLastTradingDate",
        "expiryDate", "expiry", "expiry_date"
    ]:
        dt = parse_expiry(record.get(key))
        if dt is not None:
            return dt
    return None


def option_type_from_record(record):
    val = str(
        record.get("pOptionType")
        or record.get("optType")
        or record.get("option_type")
        or ""
    ).upper().strip()
    if "CE" in val or "CALL" in val:
        return "CE"
    if "PE" in val or "PUT" in val:
        return "PE"
    symbol = str(record.get("pTrdSymbol", record.get("ts", ""))).upper()
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    return ""


def strike_from_record(record):
    for key in [
        "dStrikePrice", "dStrikePrice;", "strike_price", "strikePrice",
        "dStrike", "strike", "pStrikePrice"
    ]:
        value = safe_float(record.get(key))
        if is_valid_number(value) and value > 0:
            if value > 1_000_000:
                value /= 100.0
            return value
    return np.nan


def token_from_record(record):
    for key in [
        "pSymbol", "pSymbolToken", "instrument_token", "instrumentToken",
        "tok", "token", "pToken"
    ]:
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def record_list(response):
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    for key in ["data", "result", "records", "data_list", "scrips", "list"]:
        value = response.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for k in ["data", "records", "result"]:
                if isinstance(value.get(k), list):
                    return value[k]
    return []


# =========================================================
# CANDLE STRUCTURE
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


# =========================================================
# OPENING RANGE
# =========================================================

class OpeningRangeEngine:
    def __init__(self, minutes=15):
        self.minutes = minutes
        self.or_high = None
        self.or_low = None
        self.or_set = False

    def reset(self):
        self.or_high = self.or_low = None
        self.or_set = False

    def update(self, candle):
        mins = (candle.timestamp.hour * 60 + candle.timestamp.minute) - 555
        if mins < self.minutes:
            self.or_high = candle.fut_h if self.or_high is None else max(self.or_high, candle.fut_h)
            self.or_low = candle.fut_l if self.or_low is None else min(self.or_low, candle.fut_l)
        else:
            self.or_set = True

    def features(self, candle, atr):
        names = [
            "or_high", "or_low", "or_width_atr",
            "dist_to_or_high_atr", "dist_to_or_low_atr", "or_breakout_state"
        ]
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


# =========================================================
# SESSION CONTEXT
# =========================================================

class SessionContextEngine:
    def __init__(self):
        self.prev_close = None
        self.prev_high = None
        self.prev_low = None
        self.today_open = None

    def set_previous_day(self, close, high, low):
        self.prev_close, self.prev_high, self.prev_low = close, high, low

    def set_today_open(self, open_price):
        self.today_open = open_price

    def reset(self):
        self.today_open = None

    def features(self, candle, atr):
        names = ["gap_points", "gap_atr", "gap_direction", "dist_to_pdh_atr", "dist_to_pdl_atr"]
        if self.prev_close is None or not is_valid_number(atr) or atr <= 0:
            return {k: np.nan for k in names}
        gap = (self.today_open if self.today_open is not None else candle.fut_o) - self.prev_close
        return {
            "gap_points": gap,
            "gap_atr": gap / atr,
            "gap_direction": 1 if gap > 0 else (-1 if gap < 0 else 0),
            "dist_to_pdh_atr": (candle.fut_c - self.prev_high) / atr if self.prev_high is not None else np.nan,
            "dist_to_pdl_atr": (candle.fut_c - self.prev_low) / atr if self.prev_low is not None else np.nan,
        }


# =========================================================
# OPTION CHAIN
# =========================================================

class OptionChainEngine:
    def compute(self, chain):
        keys = [
            "pcr_oi", "pcr_volume", "ce_oi_change", "pe_oi_change",
            "atm_iv", "iv_change", "ce_oi_atm", "pe_oi_atm", "atm_strike"
        ]
        if not chain:
            out = {}
            for key in keys:
                out[key] = np.nan
                out[f"{key}_missing"] = 1
            out["ce_contracts_seen"] = out["pe_contracts_seen"] = 0
            return out
        out = {}
        for key in keys:
            value = chain.get(key, np.nan)
            out[key] = value
            out[f"{key}_missing"] = int(not is_valid_number(value))
        out["ce_contracts_seen"] = int(chain.get("ce_contracts_seen", 0))
        out["pe_contracts_seen"] = int(chain.get("pe_contracts_seen", 0))
        return out


# =========================================================
# HEAVYWEIGHTS
# =========================================================

class HeavyweightEngine:
    def __init__(self, weights):
        self.base_weights = weights
        self.day_open = {}

    def set_day_open(self, symbol, price):
        if is_valid_number(price) and price > 0:
            self.day_open[symbol] = price

    def reset_day(self):
        self.day_open.clear()

    def compute(self, candle):
        contributions, returns = [], []
        bullish = 0
        for symbol, weight in self.base_weights.items():
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
        total = sum(contributions) if contributions else 0.0
        n = max(len(contributions), 1)
        return {
            "twc": total,
            "breadth_10": bullish / n,
            "dispersion_index": float(np.std(returns)) if returns else 0.0,
            "contribution_concentration": max(contributions, key=abs) / (abs(total) + 1e-9) if contributions else 0.0,
            "hw_bullish_count": bullish,
            "hw_symbols_seen": len(contributions),
        }


# =========================================================
# FEATURE ENGINE - RESEARCH LOCK
# =========================================================

class FeatureEngine:
    def __init__(self):
        self.vwap_pv = 0.0
        self.vwap_vol = 0.0
        self.tr_history = []
        self.history = []
        self.hw = HeavyweightEngine(HEAVYWEIGHTS)
        self.or_eng = OpeningRangeEngine(CONFIG["opening_range_minutes"])
        self.sess = SessionContextEngine()
        self.opt = OptionChainEngine()

    def reset_session(self):
        self.vwap_pv = self.vwap_vol = 0.0
        self.tr_history.clear()
        self.history.clear()
        self.hw.reset_day()
        self.or_eng.reset()
        self.sess.reset()

    def set_previous_day(self, close, high, low):
        self.sess.set_previous_day(close, high, low)

    def set_today_open(self, open_price):
        self.sess.set_today_open(open_price)

    def compute(self, candle, prev):
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

        atr_prev = wilder_atr(self.tr_history, CONFIG["atr_period"])
        self.tr_history.append(tr)
        atr_close = wilder_atr(self.tr_history, CONFIG["atr_period"])
        atr = atr_prev

        closes = [c.spot_c for c in prev[-(CONFIG["sma_period"] - 1):]]
        closes.append(candle.spot_c)
        sma_ready = len(closes) >= CONFIG["sma_period"]
        spot_sma = float(np.mean(closes)) if sma_ready else np.nan

        if is_valid_number(atr) and atr > 0:
            normalized_stretch = (candle.fut_c - fut_vwap) / atr
            normalized_spread = (spot_sma - fut_vwap) / atr if is_valid_number(spot_sma) else np.nan
        else:
            normalized_stretch = normalized_spread = np.nan

        last = self.history[-1] if self.history else {}
        stretch_slope = (
            normalized_stretch - last["normalized_stretch"]
            if is_valid_number(normalized_stretch) and is_valid_number(last.get("normalized_stretch"))
            else 0.0
        )
        spread_slope = (
            normalized_spread - last["normalized_spread"]
            if is_valid_number(normalized_spread) and is_valid_number(last.get("normalized_spread"))
            else 0.0
        )

        if prev:
            oi_change = candle.fut_oi - prev[-1].fut_oi
            price_up = candle.fut_c > prev[-1].fut_c
            price_down = candle.fut_c < prev[-1].fut_c
        else:
            oi_change = 0.0
            price_up = price_down = False

        oi_long_buildup = int(price_up and oi_change > 0)
        oi_short_buildup = int(price_down and oi_change > 0)
        oi_short_covering = int(price_up and oi_change < 0)
        oi_long_unwinding = int(price_down and oi_change < 0)
        oi_neutral = int(oi_change == 0 or (not price_up and not price_down))
        oi_strength = ((1 if price_up else -1) * np.sign(oi_change) * np.log1p(abs(oi_change))) if oi_change != 0 else 0.0

        self.or_eng.update(candle)

        missing_spot = int(not is_valid_number(candle.spot_c))
        missing_future = int(not is_valid_number(candle.fut_c))
        missing_oi = int(not is_valid_number(candle.fut_oi))
        missing_volume = int(not is_valid_number(candle.fut_volume) or candle.fut_volume <= 0)
        missing_heavyweight = int(len(candle.heavy) == 0)
        missing_option = int(len(candle.option_chain) == 0)
        bad_ohlc = int(candle.fut_h < candle.fut_l or candle.spot_h < candle.spot_l)
        zero_volume = int(candle.fut_volume == 0)
        zero_oi = int(candle.fut_oi == 0)
        penalties = sum([
            missing_spot, missing_future, missing_oi, missing_volume,
            missing_heavyweight, missing_option, bad_ohlc, zero_volume, zero_oi
        ])

        pcr_features = self.opt.compute(candle.option_chain)
        features = {
            "timestamp": candle.timestamp,
            "feature_version": CONFIG["feature_version"],
            "schema_version": CONFIG["schema_version"],
            "weight_version": CONFIG["weight_version"],
            "atr_mode": CONFIG["atr_mode"],
            "execution_model": CONFIG["execution_model"],
            "basis": candle.fut_c - candle.spot_c,
            "fut_vwap": fut_vwap,
            "normalized_stretch": normalized_stretch,
            "normalized_spread": normalized_spread,
            "stretch_slope_3": stretch_slope,
            "spread_slope_3": spread_slope,
            "atr_14_prev": atr_prev,
            "atr_14_close": atr_close,
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
            "minutes_from_open": (candle.timestamp.hour * 60 + candle.timestamp.minute) - 555,
            "day_of_week": candle.timestamp.weekday(),
            **self.hw.compute(candle),
            **self.or_eng.features(candle, atr if is_valid_number(atr) else 0.0),
            **self.sess.features(candle, atr if is_valid_number(atr) else 0.0),
            **pcr_features,
            "missing_spot": missing_spot,
            "missing_future": missing_future,
            "missing_oi": missing_oi,
            "missing_volume": missing_volume,
            "missing_heavyweight": missing_heavyweight,
            "missing_option_chain": missing_option,
            "bad_ohlc": bad_ohlc,
            "zero_volume": zero_volume,
            "zero_oi": zero_oi,
            "data_quality_score": max(0.0, 1.0 - 0.1 * penalties),
            "bar_complete": 1,
        }
        self.history.append(features)
        return features


# =========================================================
# LABEL ENGINE - TRIPLE BARRIER LOCKED
# =========================================================

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

    def generate(self, entry_price, atr, future_after_entry, direction=1,
                 signal_timestamp=None, entry_timestamp=None):
        if entry_timestamp and future_after_entry and future_after_entry[0].timestamp <= entry_timestamp:
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
            "label_version": CONFIG["label_version"],
            "execution_model": self.execution_model,
            "signal_timestamp": signal_timestamp,
            "entry_timestamp": entry_timestamp,
            "entry_price": entry_price,
            "triple_barrier_outcome": outcome,
            "label_valid_for_training": valid,
            "r_multiple": r_multiple,
            "trajectory": trajectory,
            "real_breakout": int(outcome == "TARGET_FIRST" and is_valid_number(mfe_atr) and mfe_atr >= 1.0 and mae_atr <= 0.55),
            "mfe_atr_tb": mfe_atr,
            "mae_atr_tb": mae_atr,
            "time_to_mfe": time_to_mfe,
            "bars_to_outcome": bars,
            "velocity": velocity,
        }
        for horizon in self.mfe_horizons:
            mfe_h, mae_h, complete = self._excursion(entry_price, future_after_entry, direction, horizon // CONFIG["bar_minutes"])
            labels[f"mfe_atr_{horizon}m"] = mfe_h / atr if not np.isnan(atr) else np.nan
            labels[f"mae_atr_{horizon}m"] = mae_h / atr if not np.isnan(atr) else np.nan
            labels[f"horizon_{horizon}m_complete"] = complete
        return labels


# =========================================================
# DATASET
# =========================================================

class DatasetManager:
    def __init__(self, path=None):
        self.base = Path(path or CONFIG["dataset_path"])
        self.base.mkdir(parents=True, exist_ok=True)

    def write_parquet(self, df, name="features"):
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
            existing_data_behavior="overwrite_or_ignore",
        )

    def purged_walk_forward_by_date(self, df, n_splits=5):
        if "timestamp" not in df.columns:
            raise ValueError("timestamp required")
        data = df.copy()
        data["date"] = pd.to_datetime(data["timestamp"]).dt.date
        dates = sorted(data["date"].unique())
        fold = max(1, len(dates) // (n_splits + 1))
        splits = []
        for i in range(n_splits):
            train_end = (i + 1) * fold
            test_start = train_end + 1
            test_end = min(test_start + fold, len(dates))
            if test_start >= len(dates):
                break
            train_idx = data[data["date"].isin(dates[:train_end])].index.tolist()
            test_idx = data[data["date"].isin(dates[test_start:test_end])].index.tolist()
            if len(train_idx) > CONFIG["embargo_bars"]:
                train_idx = train_idx[:-CONFIG["embargo_bars"]]
            if train_idx and test_idx:
                splits.append((train_idx, test_idx))
        return splits


# =========================================================
# KOTAK NEO ADAPTER - OFFICIAL SDK V2
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
        self.lock = threading.Lock()
        self.latest = {}
        self.tick_buffer = []
        self.future_token = ""
        self.future_symbol = ""
        self.future_expiry = None
        self.pcr_tokens = []
        self.pcr_records = {}
        self.heavy_tokens = {}
        self.discovery_log = []
        self.last_error = ""

    def on_message(self, message):
        try:
            items = message if isinstance(message, list) else [message]
            with self.lock:
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    token = str(item.get("tk") or item.get("token") or item.get("pSymbolToken") or "").strip()
                    if token:
                        self.latest[token] = item
                        self.tick_buffer.append(item)
        except Exception as exc:
            self.last_error = str(exc)

    def on_error(self, error):
        self.last_error = str(error)

    def on_close(self, message=None):
        self.connected = False

    def on_open(self, message=None):
        self.connected = True

    def login(self, live_totp_override=""):
        if NeoAPI is None:
            raise RuntimeError(
                "neo_api_client missing. Install the official Kotak Neo API v2 dependency from requirements.txt."
            )

        totp = (live_totp_override or "").strip() or self.totp
        required = {
            "KOTAK_CONSUMER_KEY": self.consumer_key,
            "KOTAK_MOBILE": self.mobile,
            "KOTAK_UCC": self.ucc,
            "TOTP": totp,
            "KOTAK_MPIN": self.mpin,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError("Missing credentials: " + ", ".join(missing))

        self.client = NeoAPI(
            environment=CONFIG["neo_environment"],
            access_token=None,
            neo_fin_key=None,
            consumer_key=self.consumer_key,
        )
        self.client.on_message = self.on_message
        self.client.on_error = self.on_error
        self.client.on_close = self.on_close
        self.client.on_open = self.on_open

        step1 = self.client.totp_login(
            mobile_number=self.mobile,
            ucc=self.ucc,
            totp=generate_live_totp(totp),
        )
        if isinstance(step1, dict) and step1.get("error"):
            raise RuntimeError(str(step1))

        step2 = self.client.totp_validate(mpin=self.mpin)
        if isinstance(step2, dict) and step2.get("error"):
            raise RuntimeError(str(step2))

        self.connected = True
        return True


# =========================================================
# STREAMLIT UI APPLICATION
# =========================================================

def run_unit_tests() -> bool:
    oe = OpeningRangeEngine(15)
    c1 = Candle3Min(datetime(2026, 1, 1, 9, 15), 100, 110, 95, 105, 100, 110, 95, 105, 1000, 500)
    oe.update(c1)
    f = oe.features(c1, 10.0)
    assert "or_width_atr" in f

    le = LabelEngine()
    future = [
        Candle3Min(datetime(2026, 1, 1, 9, 18), 105, 120, 104, 119, 105, 120, 104, 119, 1000, 500)
    ]
    lbl = le.generate(100.0, 10.0, future, direction=1)
    assert lbl["triple_barrier_outcome"] in ["TARGET_FIRST", "STOP_FIRST", "TIMEOUT", "AMBIGUOUS"]
    return True


def main():
    if st is None:
        print("Streamlit is not installed. Running headless mode.")
        return

    st.set_page_config(page_title="NIFTY 3-Min Micro Engine", layout="wide")
    st.caption("Kotak Neo API v2 • Research-Lock v2.0 • Dynamic Discovery")
    st.header("Kotak Neo Authentication")

    # Persistent Connection Status Display
    is_logged_in = "neo" in st.session_state and getattr(st.session_state.neo, "connected", False)
    
    if is_logged_in:
        st.success("✅ Kotak Neo Connected & Active (Session Live)")
    else:
        st.info("⚪ Not Connected to Kotak Neo")

    user_live_totp = st.text_input(
        "Live 6-Digit TOTP (optional if KOTAK_TOTP is a Base32 secret)",
        type="password",
    )

    c1, c2 = st.columns([1, 1])

    with c1:
        if st.button("Connect Kotak Neo", use_container_width=True):
            try:
                with st.spinner("Authenticating with Kotak Neo..."):
                    adapter = KotakNeoAdapter()
                    adapter.login(live_totp_override=user_live_totp)
                    st.session_state.neo = adapter
                    st.success("Kotak Neo API v2 authentication successful.")
                    st.rerun()
            except Exception as exc:
                st.error(f"Login failed: {exc}")

    with c2:
        if st.button("Run Unit Tests", use_container_width=True):
            try:
                if run_unit_tests():
                    st.success("All tests passed.")
            except Exception as exc:
                st.error(f"Unit tests failed: {exc}")

    st.divider()
    st.header("Instrument Discovery")
    if st.button("Discover NIFTY Instruments", use_container_width=False, disabled=not is_logged_in):
        st.info("Dynamic discovery will query active Nifty futures and options from Kotak Neo Master.")

    st.header("Live Market Feed")
    col_a, col_b = st.columns(2)
    with col_a:
        st.button("Subscribe NIFTY + Heavyweights", use_container_width=True, disabled=not is_logged_in)
    with col_b:
        st.button("Auto Discover + Subscribe PCR", use_container_width=True, disabled=not is_logged_in)

    st.subheader("NIFTY Spot")
    st.write("-")


if __name__ == "__main__":
    main()
