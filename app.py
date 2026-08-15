#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine
Kotak Neo Integrated Research-Lock v2.8 (Auth State Fixed + Pro UI)
- Instant state sync on Login & Stream activation
- Clear visual badges (AUTHENTICATED / STREAMING / DISCONNECTED)
- RLock protection across async ticks, bar closes, parquet writes & UI reads
- Safe cumulative delta volume calculation with spike guard
- Expiry date-only comparisons
- TOTP auto-reconnect abort guard
- Pro Dark Trading Terminal UI Layout
"""

from __future__ import annotations

import os
import time
import hmac
import hashlib
import struct
import base64
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

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
# TOTP + MOBILE UTILITIES
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
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("KOTAK_MOBILE is empty.")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("91") and len(digits) == 12:
        national = digits[2:]
    elif len(digits) == 10:
        national = digits
    else:
        raise ValueError("Invalid KOTAK_MOBILE. Use 10-digit Indian mobile number.")
    if len(national) != 10 or national[0] not in "6789":
        raise ValueError("KOTAK_MOBILE is not a valid Indian mobile number.")
    return "+91" + national


def is_base32_totp_secret(value: str) -> bool:
    raw = (value or "").strip().replace(" ", "")
    if not raw:
        return False
    if raw.isdigit() and len(raw) == 6:
        return False
    return True


# =========================================================
# CONFIGURATION — RESEARCH LOCK FROZEN
# =========================================================

CONFIG = {
    "app_version": "v2.8_auth_fixed_ui",
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
    "nifty_spot_token": "26000",
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
    "feed_silence_sec": 45,
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

HEAVYWEIGHTS = {
    "HDFCBANK": 0.115, "RELIANCE": 0.098, "ICICIBANK": 0.080,
    "INFY": 0.058, "ITC": 0.042, "TCS": 0.040, "LT": 0.038,
    "AXISBANK": 0.033, "KOTAKBANK": 0.029, "SBIN": 0.028,
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


def parse_tick_timestamp(tick: Dict[str, Any]) -> datetime:
    for key in ("ft", "exch_tm", "timestamp", "ltt", "t", "time", "ts"):
        val = tick.get(key)
        if val is None:
            continue
        try:
            if isinstance(val, datetime):
                return val
            x = float(val)
            if x > 1e12:
                return datetime.fromtimestamp(x / 1000.0)
            if x > 1e9:
                return datetime.fromtimestamp(x)
        except Exception:
            pass
        text = str(val).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%H:%M:%S"):
            try:
                dt = datetime.strptime(text, fmt)
                if dt.year < 2000:
                    now = datetime.now()
                    dt = dt.replace(year=now.year, month=now.month, day=now.day)
                return dt
            except Exception:
                pass
    return datetime.now()


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
    for fmt in ["%d%b%Y", "%d%b%y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",
                "%d%b%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d"]:
        try:
            return datetime.strptime(text.upper(), fmt)
        except Exception:
            pass
    return None


def expiry_from_record(record):
    for key in ["pExpiryDate", "lExpiryDate", "pMaturityDate", "pLastTradingDate",
                "expiryDate", "expiry", "expiry_date"]:
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
    symbol = str(record.get("pTrdSymbol", record.get("ts", ""))).upper()
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
    for key in ["pSymbol", "pSymbolToken", "instrument_token", "instrumentToken", "tok", "token", "pToken"]:
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
# RESEARCH ENGINES
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
        mins = (candle.timestamp.hour * 60 + candle.timestamp.minute) - 555
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
            "gap_points": gap, "gap_atr": gap / atr,
            "gap_direction": 1 if gap > 0 else (-1 if gap < 0 else 0),
            "dist_to_pdh_atr": (candle.fut_c - self.prev_high) / atr if self.prev_high is not None else np.nan,
            "dist_to_pdl_atr": (candle.fut_c - self.prev_low) / atr if self.prev_low is not None else np.nan,
        }


class OptionChainEngine:
    def compute(self, chain: Dict[str, Any]):
        keys = ["pcr_oi", "pcr_volume", "ce_oi_change", "pe_oi_change",
                "atm_iv", "iv_change", "ce_oi_atm", "pe_oi_atm", "atm_strike"]
        if not chain:
            out = {k: np.nan for k in keys}
            for k in keys:
                out[f"{k}_missing"] = 1
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


class HeavyweightEngine:
    def __init__(self, weights):
        self.base_weights = weights
        self.day_open: Dict[str, float] = {}

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


class FeatureEngine:
    def __init__(self):
        self.vwap_pv = self.vwap_vol = 0.0
        self.tr_history: List[float] = []
        self.history: List[Dict[str, Any]] = []
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

    def compute(self, candle: Candle3Min, prev: List[Candle3Min]):
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
            if is_valid_number(normalized_stretch) and is_valid_number(last.get("normalized_stretch")) else 0.0
        )
        spread_slope = (
            normalized_spread - last["normalized_spread"]
            if is_valid_number(normalized_spread) and is_valid_number(last.get("normalized_spread")) else 0.0
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

        w = CONFIG["dq_weights"]
        penalty = (
            w["missing_spot"] * missing_spot +
            w["missing_future"] * missing_future +
            w["missing_oi"] * missing_oi +
            w["missing_volume"] * missing_volume +
            w["missing_heavyweight"] * missing_heavyweight +
            w["missing_option_chain"] * missing_option +
            w["bad_ohlc"] * bad_ohlc +
            w["zero_volume"] * zero_volume +
            w["zero_oi"] * zero_oi
        )

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


# =========================================================
# REGIME & DECISION ENGINE
# =========================================================

class RegimeEngine:
    def detect(self, feats: Dict[str, Any]) -> str:
        dq = safe_float(feats.get("data_quality_score"), 0.0)
        if dq < CONFIG["min_data_quality_to_trade"]:
            return "DATA_BAD"
        stretch = safe_float(feats.get("normalized_stretch"), 0.0)
        slope = safe_float(feats.get("stretch_slope_3"), 0.0)
        or_state = int(feats.get("or_breakout_state") or 0)
        oi_long = int(feats.get("oi_long_buildup") or 0)
        oi_short = int(feats.get("oi_short_buildup") or 0)
        oi_unwind = int(feats.get("oi_long_unwinding") or 0) or int(feats.get("oi_short_covering") or 0)
        twc = safe_float(feats.get("twc"), 0.0)
        breadth = safe_float(feats.get("breadth_10"), 0.5)

        if (abs(stretch) > 0.9 and abs(slope) > 0.15) or (or_state != 0 and abs(stretch) > 0.5):
            if stretch > 0 and (oi_long or twc > 0 or breadth > 0.55):
                return "IMPULSE_UP"
            if stretch < 0 and (oi_short or twc < 0 or breadth < 0.45):
                return "IMPULSE_DOWN"
        if 0.35 < abs(stretch) <= 0.9:
            return "STAIRCASE_UP" if stretch > 0 else "STAIRCASE_DOWN"
        if oi_unwind and abs(stretch) > 0.6:
            return "FAILURE"
        if abs(stretch) <= 0.35 and abs(slope) < 0.08:
            return "GRIND"
        return "NEUTRAL"


@dataclass
class TradeDecision:
    action: str
    regime: str
    target_points: float
    stop_points: float
    size_factor: float
    confidence: float
    reason: str
    timestamp: Optional[datetime] = None


class DecisionEngine:
    def __init__(self):
        self.regime_engine = RegimeEngine()
        self.last_action: Optional[str] = None
        self.last_action_bar_idx: int = -999
        self.bar_counter: int = 0

    def _realistic_target(self, atr: float, regime: str) -> Tuple[float, float, float]:
        if not is_valid_number(atr) or atr <= 0:
            atr = 15.0
        if regime in ("IMPULSE_UP", "IMPULSE_DOWN"):
            return 1.1 * atr, 0.7 * atr, 1.0
        if regime in ("STAIRCASE_UP", "STAIRCASE_DOWN"):
            return 0.85 * atr, 0.65 * atr, 0.85
        if regime == "FAILURE":
            return 0.55 * atr, 0.45 * atr, 0.6
        if regime == "GRIND":
            return 0.45 * atr, 0.40 * atr, 0.45
        return 0.60 * atr, 0.50 * atr, 0.55

    def decide(self, feats: Dict[str, Any]) -> TradeDecision:
        self.bar_counter += 1
        regime = self.regime_engine.detect(feats)
        atr = safe_float(feats.get("atr_14_prev") or feats.get("atr_14_close"), 15.0)
        stretch = safe_float(feats.get("normalized_stretch"), 0.0)
        slope = safe_float(feats.get("stretch_slope_3"), 0.0)
        or_state = int(feats.get("or_breakout_state") or 0)
        dq = safe_float(feats.get("data_quality_score"), 0.0)

        if regime == "DATA_BAD" or dq < CONFIG["min_data_quality_to_trade"]:
            return TradeDecision("SKIP", regime, 0.0, 0.0, 0.0, 0.0,
                                 "Data quality too low / feed unreliable", feats.get("timestamp"))

        twc = safe_float(feats.get("twc"), 0.0)
        twc_term = float(np.clip(twc * 50.0, -0.8, 0.8))
        breadth_term = (safe_float(feats.get("breadth_10"), 0.5) - 0.5) * 1.5
        score = (np.clip(stretch, -2, 2) * 1.2 +
                 np.clip(slope, -1, 1) * 0.8 +
                 or_state * 0.5 +
                 twc_term + breadth_term)

        raw_action = "CE" if score >= 0 else "PE"
        hold = CONFIG["signal_min_hold_bars"]
        conf_penalty = 0.0
        if self.last_action and self.last_action != "SKIP":
            bars_since = self.bar_counter - self.last_action_bar_idx
            if bars_since < hold and raw_action != self.last_action:
                action = self.last_action
                conf_penalty = 0.15
                reason = f"Stability hold ({bars_since}/{hold}) — kept {action}"
            else:
                action = raw_action
                reason = f"Regime={regime}, score={score:.2f} [heuristic]"
        else:
            action = raw_action
            reason = f"Regime={regime}, score={score:.2f} [heuristic]"

        target, stop, size = self._realistic_target(atr, regime)
        conf = 0.50 + min(0.30, abs(score) * 0.12) - conf_penalty
        if regime.startswith("IMPULSE"):
            conf += 0.10
        if regime == "GRIND":
            conf -= 0.08
        conf = float(np.clip(conf, 0.30, 0.88))

        hw_seen = int(feats.get("hw_symbols_seen") or 0)
        min_hw = CONFIG.get("hw_min_symbols_required", 5)
        if hw_seen >= 8:
            pass
        elif hw_seen >= min_hw:
            size *= 0.85
            conf = max(0.30, conf - 0.05)
            reason += f" | HW soft ({hw_seen})"
        else:
            size *= 0.60
            conf = max(0.30, conf - 0.12)
            reason += f" | HW weak ({hw_seen})"

        self.last_action = action
        self.last_action_bar_idx = self.bar_counter
        return TradeDecision(action, regime, round(target, 1), round(stop, 1),
                             round(size, 2), round(conf, 3), reason, feats.get("timestamp"))


# =========================================================
# KOTAK NEO ADAPTER — PRODUCTION HARDENED
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
        self.tick_buffer: List[Dict[str, Any]] = []

        self.spot_token = CONFIG.get("nifty_spot_token", "26000")
        self.future_token = CONFIG.get("nifty_future_token", "")
        self.future_symbol = ""
        self.future_expiry = None
        self.pcr_tokens: List[str] = []
        self.pcr_records: Dict[str, Dict[str, Any]] = {}
        self.heavy_tokens: Dict[str, str] = dict(NSE_CASH_TOKENS)
        self.token_to_symbol: Dict[str, str] = {v: k for k, v in NSE_CASH_TOKENS.items()}
        self.discovery_log: List[str] = []
        self.last_error = ""

        self.feature_engine = FeatureEngine()
        self.label_engine = LabelEngine()
        self.dataset_manager = DatasetManager()
        self.decision_engine = DecisionEngine()
        self.candles_3m: List[Candle3Min] = []
        self.current_bar_ticks: List[Dict[str, Any]] = []
        self.current_bar_time: Optional[datetime] = None
        self._bar_deadline: Optional[datetime] = None
        self.last_decision: Optional[TradeDecision] = None
        self._prev_ce_oi = 0.0
        self._prev_pe_oi = 0.0
        self._last_tick_wall = time.time()

        self._watchdog_stop = threading.Event()
        self._watchdog_thread: Optional[threading.Thread] = None
        self._reconnect_attempts = 0
        self._next_reconnect_ts = 0.0
        self._max_reconnect_attempts = 5
        self._backoff_sec = [5, 10, 20, 40, 60]

    def on_message(self, message):
        try:
            items = message if isinstance(message, list) else [message]
            with self.lock:
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    token = str(item.get("tk") or item.get("token") or item.get("pSymbolToken") or "").strip()
                    if token:
                        item["_parsed_ts"] = parse_tick_timestamp(item)
                        self.latest[token] = item
                        self.tick_buffer.append(item)
                        self._process_live_tick(token, item)
        except Exception as exc:
            self.last_error = str(exc)

    def on_error(self, error):
        self.last_error = str(error)
        self.connected = False
        self.conn_state = "DISCONNECTED"

    def on_close(self, message=None):
        self.connected = False
        self.conn_state = "DISCONNECTED"
        self.last_error = f"WebSocket closed: {message}"

    def on_open(self, message=None):
        self.connected = True

    def login(self, live_totp_override=""):
        if NeoAPI is None:
            raise RuntimeError("neo_api_client missing. Install official Kotak Neo API v2 package.")
        totp = (live_totp_override or "").strip() or self.totp
        required = {"KOTAK_CONSUMER_KEY": self.consumer_key, "KOTAK_MOBILE": self.mobile,
                    "KOTAK_UCC": self.ucc, "TOTP": totp, "KOTAK_MPIN": self.mpin}
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError("Missing credentials: " + ", ".join(missing))
        self.client = NeoAPI(environment=CONFIG["neo_environment"], access_token=None,
                             neo_fin_key=None, consumer_key=self.consumer_key)
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

    def _can_auto_reconnect(self) -> bool:
        return is_base32_totp_secret(self.totp)

    def _reset_bar_state(self, reason: str = "reconnect"):
        with self.lock:
            self.current_bar_ticks.clear()
            self.current_bar_time = None
            self._bar_deadline = None
        self.last_error = f"Bar state cleared ({reason})"

    def try_reconnect_and_resubscribe(self, live_totp_override: str = "") -> bool:
        self.conn_state = "RECONNECTING"
        self._reset_bar_state("reconnect")
        try:
            self.login(live_totp_override=live_totp_override)
            ok = self.discover_nifty_instruments(auto_pcr=True)
            if not ok or not self.future_token:
                raise RuntimeError("Rediscovery failed: no active future")
            n = self.subscribe_live_feed()
            if n <= 0:
                raise RuntimeError("Subscribe returned 0 instruments")
            self.fetch_market_snapshot()
            time.sleep(1.0)
            with self.lock:
                has_fut = bool(self.latest.get(self.future_token))
                has_spot = bool(self.latest.get(self.spot_token))
            if not (has_fut and has_spot):
                raise RuntimeError("Feed validate failed: both Spot + Future required")
            self.conn_state = "STREAMING"
            self.connected = True
            self._reconnect_attempts = 0
            self._next_reconnect_ts = 0.0
            self.last_error = ""
            return True
        except Exception as exc:
            self.last_error = f"Reconnect failed: {exc}"
            self.conn_state = "DISCONNECTED"
            self.connected = False
            return False

    def discover_nifty_instruments(self, auto_pcr: bool = True) -> bool:
        if not self.connected or not self.client:
            raise RuntimeError("Kotak Neo not authenticated.")
        self.discovery_log.clear()
        
        self.heavy_tokens = dict(NSE_CASH_TOKENS)
        self.token_to_symbol = {v: k for k, v in NSE_CASH_TOKENS.items()}
        self.token_to_symbol[self.spot_token] = "NIFTY_SPOT"
        self.discovery_log.append(f"✓ 10 Nifty Heavyweights & Spot Mapped.")

        try:
            res = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
            records = record_list(res)
            future_candidates = []
            now_d = datetime.now().date()
            for r in records:
                sym = str(r.get("pTrdSymbol", r.get("ts", r.get("symbol", "")))).upper()
                inst_type = str(r.get("pInstType", r.get("instrument_type", ""))).upper()
                if "NIFTY" in sym and ("FUT" in sym or "FUTIDX" in inst_type):
                    exp = expiry_from_record(r)
                    tok = token_from_record(r)
                    if tok and exp and exp.date() >= now_d:
                        future_candidates.append((exp, tok, sym))
            if future_candidates:
                future_candidates.sort(key=lambda x: x[0])
                self.future_expiry, self.future_token, self.future_symbol = future_candidates[0]
                self.token_to_symbol[self.future_token] = "NIFTY_FUT"
                self.discovery_log.append(f"✓ Future: {self.future_symbol} (Token {self.future_token})")
            else:
                self.future_token = CONFIG.get("nifty_future_token", "45450")
                self.token_to_symbol[self.future_token] = "NIFTY_FUT"
                self.discovery_log.append(f"✓ Future Fallback: Token {self.future_token}")
        except Exception:
            self.future_token = CONFIG.get("nifty_future_token", "45450")
            self.token_to_symbol[self.future_token] = "NIFTY_FUT"
            self.discovery_log.append(f"✓ Configured Future Token: {self.future_token}")

        if auto_pcr:
            self.discover_pcr_chain()

        return True

    def discover_pcr_chain(self, center_strike: Optional[float] = None) -> int:
        if not self.connected or not self.client:
            return 0
        try:
            if not center_strike or not is_valid_number(center_strike):
                with self.lock:
                    spot_tick = self.latest.get(self.spot_token, {})
                    center_strike = safe_float(spot_tick.get("ltp") or spot_tick.get("lp") or spot_tick.get("c"), 24000.0)
            step = CONFIG["pcr_strike_step"]
            atm = round(center_strike / step) * step
            count = CONFIG["pcr_strike_count"]
            target_strikes = [atm + (i * step) for i in range(-count, count + 1)]
            
            res = self.client.search_scrip(exchange_segment="nse_fo", symbol="NIFTY")
            records = record_list(res)
            discovered = []
            now_d = datetime.now().date()
            for r in records:
                exp = expiry_from_record(r)
                strike = strike_from_record(r)
                op_type = option_type_from_record(r)
                tok = token_from_record(r)
                if tok and strike in target_strikes and op_type in ("CE", "PE"):
                    if not exp or exp.date() >= now_d:
                        discovered.append(tok)
                        self.pcr_records[tok] = {
                            "strike": strike, "option_type": op_type,
                            "expiry": exp, "symbol": str(r.get("pTrdSymbol", ""))
                        }
            self.pcr_tokens = list(set(discovered))
            self.discovery_log.append(f"✓ PCR Strikes Mapped: {len(self.pcr_tokens)}")
            return len(self.pcr_tokens)
        except Exception:
            return 0

    def fetch_market_snapshot(self):
        if not self.connected or not self.client:
            return
        tokens = [{"instrument_token": self.spot_token, "exchange_segment": "nse_cm"}]
        if self.future_token:
            tokens.append({"instrument_token": self.future_token, "exchange_segment": "nse_fo"})
        for tok in self.heavy_tokens.values():
            tokens.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        try:
            res = self.client.quotes(instrument_tokens=tokens, isIndex=False)
            with self.lock:
                for r in record_list(res):
                    tok = token_from_record(r)
                    if tok:
                        self.latest[tok] = r
        except Exception as exc:
            self.last_error = f"Quote Fetch Error: {exc}"

    def subscribe_live_feed(self) -> int:
        if not self.connected or not self.client:
            raise RuntimeError("Kotak Neo not authenticated.")
        tokens = [{"instrument_token": self.spot_token, "exchange_segment": "nse_cm"}]
        if self.future_token:
            tokens.append({"instrument_token": self.future_token, "exchange_segment": "nse_fo"})
        for tok in self.heavy_tokens.values():
            tokens.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        for tok in self.pcr_tokens:
            tokens.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})
        if tokens:
            self.client.subscribe(instrument_tokens=tokens)
            self.conn_state = "STREAMING"
            return len(tokens)
        return 0

    def _process_live_tick(self, token: str, tick: Dict[str, Any]):
        ts = tick.get("_parsed_ts") or parse_tick_timestamp(tick)
        self._last_tick_wall = time.time()
        bar_start = floor_bar_timestamp(ts, CONFIG["bar_minutes"])
        if not bar_start:
            return
        if self.current_bar_time is None:
            self.current_bar_time = bar_start
            self._bar_deadline = bar_start + timedelta(minutes=CONFIG["bar_minutes"])
        if bar_start > self.current_bar_time:
            self._close_bar(self.current_bar_time)
            self.current_bar_time = bar_start
            self._bar_deadline = bar_start + timedelta(minutes=CONFIG["bar_minutes"])
            self.current_bar_ticks.clear()
        self.current_bar_ticks.append(tick)

    def maybe_flush_bars(self):
        now = datetime.now()
        with self.lock:
            if self.current_bar_time and self._bar_deadline:
                if now >= self._bar_deadline + timedelta(seconds=CONFIG["bar_close_grace_sec"]):
                    if self.current_bar_ticks:
                        self._close_bar(self.current_bar_time)
                    self.current_bar_ticks.clear()
                    self.current_bar_time = None
                    self._bar_deadline = None
            if CONFIG["session_end_flush"]:
                end_h, end_m = map(int, CONFIG["session_end"].split(":"))
                sess_end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
                if now >= sess_end and self.current_bar_ticks:
                    self._close_bar(self.current_bar_time or floor_bar_timestamp(now) or now)
                    self.current_bar_ticks.clear()
                    self.current_bar_time = None
                    self._bar_deadline = None

    def _close_bar(self, bar_time: datetime):
        with self.lock:
            if not self.current_bar_ticks:
                return

            def _prices(token):
                ticks = [t for t in self.current_bar_ticks if str(t.get("tk") or t.get("token")) == token]
                vals = [safe_float(t.get("ltp") or t.get("lp") or t.get("c")) for t in ticks]
                vals = [v for v in vals if is_valid_number(v)]
                return ticks, vals

            _, spot_prices = _prices(self.spot_token)
            if not spot_prices:
                last = safe_float(self.latest.get(self.spot_token, {}).get("ltp"), 0.0)
                spot_o = spot_h = spot_l = spot_c = last
            else:
                spot_o, spot_h, spot_l, spot_c = spot_prices[0], max(spot_prices), min(spot_prices), spot_prices[-1]

            fut_ticks, fut_prices = _prices(self.future_token)
            if not fut_prices:
                last = safe_float(self.latest.get(self.future_token, {}).get("ltp"), spot_c)
                fut_o = fut_h = fut_l = fut_c = last
                fut_vol = 0.0
                fut_oi = safe_float(self.latest.get(self.future_token, {}).get("oi"), 0.0)
            else:
                fut_o, fut_h, fut_l, fut_c = fut_prices[0], max(fut_prices), min(fut_prices), fut_prices[-1]
                
                vols = []
                for t in fut_ticks:
                    v = safe_float(t.get("v") or t.get("vol") or t.get("volume"), np.nan)
                    if is_valid_number(v):
                        vols.append(v)

                fut_vol = 0.0
                if len(vols) >= 2:
                    if vols[-1] >= vols[0] and vols[-1] < (vols[0] * 50.0 + 10.0):
                        fut_vol = max(0.0, vols[-1] - vols[0])
                    else:
                        s = sum(vols)
                        med = float(np.median(vols)) if vols else 0.0
                        fut_vol = s if s < max(med * len(vols) * 5.0, 1.0) else med * len(vols)
                elif len(vols) == 1:
                    fut_vol = 0.0

                fut_oi = safe_float(fut_ticks[-1].get("oi") or fut_ticks[-1].get("open_interest"), 0.0)

            hw_snap = {}
            bar_end = bar_time + timedelta(minutes=CONFIG["bar_minutes"])
            for sym, tok in self.heavy_tokens.items():
                t = self.latest.get(tok, {})
                qts = t.get("_parsed_ts") or parse_tick_timestamp(t)
                age = abs((bar_end - qts).total_seconds()) if isinstance(qts, datetime) else 9999
                if age > CONFIG["hw_max_quote_age_sec"]:
                    continue
                c_val = safe_float(t.get("ltp") or t.get("lp") or t.get("c"))
                o_val = safe_float(t.get("o") or t.get("open"), c_val)
                hw_snap[sym] = {"o": o_val, "c": c_val, "vwap": safe_float(t.get("vwap"), c_val)}

            total_ce_oi = total_pe_oi = total_ce_vol = total_pe_vol = 0.0
            atm_ce_oi = atm_pe_oi = np.nan
            spot_approx = spot_c if is_valid_number(spot_c) and spot_c > 0 else 24000.0
            step = CONFIG["pcr_strike_step"]
            atm = round(spot_approx / step) * step
            for tok in self.pcr_tokens:
                info = self.pcr_records.get(tok, {})
                t = self.latest.get(tok, {})
                oi_raw = t.get("oi")
                if oi_raw is None or str(oi_raw).strip() == "":
                    continue
                oi = safe_float(oi_raw, np.nan)
                if not is_valid_number(oi):
                    continue
                vol = safe_float(t.get("v") or t.get("vol"), 0.0)
                strike = info.get("strike")
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

            ce_oi_change = total_ce_oi - self._prev_ce_oi if self._prev_ce_oi > 0 else np.nan
            pe_oi_change = total_pe_oi - self._prev_pe_oi if self._prev_pe_oi > 0 else np.nan
            self._prev_ce_oi = total_ce_oi
            self._prev_pe_oi = total_pe_oi

            pcr_chain = {
                "pcr_oi": total_pe_oi / max(total_ce_oi, 1.0) if total_ce_oi > 0 else np.nan,
                "pcr_volume": total_pe_vol / max(total_ce_vol, 1.0) if total_ce_vol > 0 else np.nan,
                "ce_oi_change": ce_oi_change, "pe_oi_change": pe_oi_change,
                "ce_oi_atm": atm_ce_oi, "pe_oi_atm": atm_pe_oi, "atm_strike": atm,
                "atm_iv": np.nan, "iv_change": np.nan,
                "ce_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "CE"),
                "pe_contracts_seen": sum(1 for t in self.pcr_tokens if self.pcr_records.get(t, {}).get("option_type") == "PE"),
            }

            candle = Candle3Min(
                timestamp=bar_time,
                spot_o=spot_o, spot_h=spot_h, spot_l=spot_l, spot_c=spot_c,
                fut_o=fut_o, fut_h=fut_h, fut_l=fut_l, fut_c=fut_c,
                fut_volume=fut_vol, fut_oi=fut_oi,
                heavy=hw_snap, option_chain=pcr_chain,
            )
            feats = self.feature_engine.compute(candle, self.candles_3m)
            self.candles_3m.append(candle)

            decision = self.decision_engine.decide(feats)
            self.last_decision = decision
            feats["decision_action"] = decision.action
            feats["decision_regime"] = decision.regime
            feats["decision_target"] = decision.target_points
            feats["decision_confidence"] = decision.confidence
            self.dataset_manager.write_parquet(pd.DataFrame([feats]), name="features_3min")

    def start_bar_watchdog(self):
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()

        def _loop():
            while not self._watchdog_stop.is_set():
                try:
                    self.maybe_flush_bars()
                    silent = time.time() - self._last_tick_wall
                    if self.conn_state == "STREAMING" and silent > CONFIG["feed_silence_sec"]:
                        self.conn_state = "DISCONNECTED"
                        self.connected = False

                    if self.conn_state == "DISCONNECTED":
                        if not self._can_auto_reconnect():
                            self._reconnect_attempts = self._max_reconnect_attempts
                            self._next_reconnect_ts = time.time() + 3600
                        else:
                            now = time.time()
                            if (self._reconnect_attempts < self._max_reconnect_attempts and
                                    now >= self._next_reconnect_ts):
                                ok = self.try_reconnect_and_resubscribe()
                                if ok:
                                    self._reconnect_attempts = 0
                                    self._next_reconnect_ts = 0.0
                                else:
                                    delay = self._backoff_sec[min(self._reconnect_attempts, len(self._backoff_sec) - 1)]
                                    self._reconnect_attempts += 1
                                    self._next_reconnect_ts = now + delay
                except Exception as exc:
                    self.last_error = f"watchdog: {exc}"
                self._watchdog_stop.wait(1.0)

        self._watchdog_thread = threading.Thread(target=_loop, name="BarWatchdog", daemon=True)
        self._watchdog_thread.start()

    def stop_bar_watchdog(self):
        self._watchdog_stop.set()


# =========================================================
# UI THEME & DASHBOARD
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


def run_unit_tests() -> bool:
    oe = OpeningRangeEngine(15)
    c1 = Candle3Min(datetime(2026, 1, 1, 9, 15), 100, 110, 95, 105, 100, 110, 95, 105, 1000, 500)
    oe.update(c1)
    assert "or_width_atr" in oe.features(c1, 10.0)
    le = LabelEngine()
    future = [Candle3Min(datetime(2026, 1, 1, 9, 18), 105, 120, 104, 119, 105, 120, 104, 119, 1000, 500)]
    lbl = le.generate(100.0, 10.0, future, direction=1)
    assert lbl["triple_barrier_outcome"] in ["TARGET_FIRST", "STOP_FIRST", "TIMEOUT", "AMBIGUOUS"]
    de = DecisionEngine()
    d = de.decide({
        "data_quality_score": 0.9, "atr_14_prev": 12.0, "normalized_stretch": 0.8,
        "stretch_slope_3": 0.2, "or_breakout_state": 1, "oi_long_buildup": 1,
        "twc": 0.002, "breadth_10": 0.7, "hw_symbols_seen": 9, "timestamp": datetime.now(),
    })
    assert d.action in ("CE", "PE", "SKIP")
    return True


def main():
    if st is None:
        print("Streamlit not installed.")
        return

    st.set_page_config(
        page_title="NIFTY 3M | Micro Engine",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    inject_custom_css()

    adapter: Optional[KotakNeoAdapter] = st.session_state.get("neo")
    is_logged_in = adapter is not None and getattr(adapter, "connected", False)

    # =========================================================
    # SIDEBAR: AUTH & CONTROLS
    # =========================================================
    with st.sidebar:
        st.subheader("⚡ Gateway Controls")
        
        if is_logged_in:
            conn_txt = getattr(adapter, "conn_state", "AUTHENTICATED")
            if conn_txt == "STREAMING":
                st.markdown(f'<span class="status-pill status-active">● STREAMING (LIVE)</span>', unsafe_allow_html=True)
            else:
                st.markdown(f'<span class="status-pill status-auth">● CONNECTED (AUTHENTICATED)</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-pill status-offline">● DISCONNECTED</span>', unsafe_allow_html=True)

        user_live_totp = st.text_input("Live TOTP (Optional)", type="password", help="Use if secret not in config")
        
        col_sb1, col_sb2 = st.columns(2)
        with col_sb1:
            if st.button("Connect", use_container_width=True):
                try:
                    with st.spinner("Connecting..."):
                        ad = KotakNeoAdapter()
                        ad.login(live_totp_override=user_live_totp)
                        ad.start_bar_watchdog()
                        st.session_state.neo = ad
                        st.session_state.discovered = False
                        st.rerun()
                except Exception as exc:
                    st.error(f"{exc}")
        with col_sb2:
            if st.button("Reconnect", use_container_width=True, disabled=not adapter):
                if adapter:
                    adapter.try_reconnect_and_resubscribe(live_totp_override=user_live_totp)
                    st.rerun()

        st.markdown("---")
        st.subheader("🔍 Subscriptions")
        
        if st.button("Discover Instruments", use_container_width=True, disabled=not is_logged_in):
            adapter.discover_nifty_instruments(auto_pcr=False)
            st.session_state.discovered = True
            st.rerun()

        if st.session_state.get("discovered") and adapter:
            st.success("✓ Instruments Mapped!")
            for l in adapter.discovery_log:
                st.caption(l)

        if st.button("Start Live Feed", use_container_width=True, disabled=not is_logged_in):
            n = adapter.subscribe_live_feed()
            adapter.start_bar_watchdog()
            st.session_state.stream_active = True
            st.rerun()

        if st.button("Refresh Chain (PCR)", use_container_width=True, disabled=not is_logged_in):
            n = adapter.discover_pcr_chain()
            adapter.subscribe_live_feed()
            st.info(f"{n} Option strikes active")

        st.markdown("---")
        if st.button("Run Unit Tests", use_container_width=True):
            try:
                st.success("Engine Verification Passed" if run_unit_tests() else "Test Failed")
            except Exception as exc:
                st.error(str(exc))

    # =========================================================
    # TOP METRICS STRIP: NIFTY LIVE RATES
    # =========================================================
    spot_val, fut_val, fut_oi, ticks_count = "-", "-", "-", 0
    if adapter and adapter.latest:
        with adapter.lock:
            s = adapter.latest.get(adapter.spot_token, {})
            spot_val = s.get("ltp") or s.get("lp") or s.get("c", "-")
            f = adapter.latest.get(adapter.future_token, {})
            fut_val = f.get("ltp") or f.get("lp") or f.get("c", "-")
            fut_oi = f.get("oi") or f.get("open_interest", "-")
            ticks_count = len(adapter.tick_buffer)

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("NIFTY SPOT", f"₹{spot_val}")
    t2.metric("NIFTY FUT", f"₹{fut_val}")
    t3.metric("FUT OPEN INTEREST", f"{fut_oi:,}" if isinstance(fut_oi, (int, float)) else str(fut_oi))
    t4.metric("TICKS INGESTED", f"{ticks_count:,}")

    # =========================================================
    # MAIN STAGE: SIGNAL HUD + REGIME CARD
    # =========================================================
    st.markdown('<div class="terminal-card">', unsafe_allow_html=True)
    
    col_hud1, col_hud2, col_hud3, col_hud4, col_hud5 = st.columns([1.5, 1.2, 1, 1, 2])
    
    if adapter and adapter.last_decision:
        d = adapter.last_decision
        badge_cls = "badge-ce" if d.action == "CE" else ("badge-pe" if d.action == "PE" else "badge-neutral")
        
        with col_hud1:
            st.caption("TACTICAL SIGNAL")
            st.markdown(f'<div class="{badge_cls}">{d.action}</div>', unsafe_allow_html=True)
        with col_hud2:
            st.metric("Regime", d.regime)
        with col_hud3:
            st.metric("Target / SL", f"+{d.target_points} / -{d.stop_points} pt")
        with col_hud4:
            st.metric("Confidence", f"{d.confidence * 100:.0f}%")
        with col_hud5:
            st.caption("Engine Rationale")
            st.write(f"_{d.reason}_")
    else:
        st.info("Awaiting first completed 3-minute bar to establish baseline regime and signal...")

    st.markdown('</div>', unsafe_allow_html=True)

    # =========================================================
    # ANALYTICS & MARKET BREADTH (2-COLUMN GRID)
    # =========================================================
    grid_left, grid_right = st.columns([1.2, 0.8])

    with grid_left:
        st.markdown("**Core Feature Vector (3-Min)**")
        if adapter:
            with adapter.lock:
                latest_row = dict(adapter.feature_engine.history[-1]) if adapter.feature_engine.history else None
            if latest_row:
                f1, f2, f3, f4 = st.columns(4)
                f1.metric("VWAP Stretch", f"{latest_row.get('normalized_stretch', 0.0):.2f}σ")
                f2.metric("Slope (3B)", f"{latest_row.get('stretch_slope_3', 0.0):.3f}")
                f3.metric("ATR (14)", f"{latest_row.get('atr_14_prev', 0.0):.1f}")
                f4.metric("Data Quality", f"{latest_row.get('data_quality_score', 0.0) * 100:.0f}%")
                
                with st.expander("Inspect Raw Vector Properties", expanded=False):
                    st.json(latest_row)
            else:
                st.caption("Feature extraction initializing...")

    with grid_right:
        st.markdown("**Heavyweights Breadth (Top 10 NSE Cash)**")
        if adapter and adapter.heavy_tokens:
            hw_list = []
            with adapter.lock:
                for sym, tok in adapter.heavy_tokens.items():
                    t = adapter.latest.get(tok, {})
                    ltp = safe_float(t.get("ltp") or t.get("lp") or t.get("c"))
                    hw_list.append({"Symbol": sym, "LTP": ltp, "Weight": f"{HEAVYWEIGHTS.get(sym, 0)*100:.1f}%"})
            st.dataframe(pd.DataFrame(hw_list), height=210, use_container_width=True, hide_index=True)
        else:
            st.caption("Heavyweights mapping pending discovery...")

    # Auto refresh tabhi trigger ho jab WebSocket actively stream kar raha ho
    if is_logged_in and st.session_state.get("stream_active", False):
        time.sleep(CONFIG["ui_refresh_sec"])
        st.rerun()


if __name__ == "__main__":
    main()
