#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine
Kotak Neo Integrated Research-Lock v1.4

CORE LOGIC PRESERVED:
- session-local ATR
- VWAP
- SMA20
- OI classification
- heavyweight contribution
- opening range
- PCR feature
- triple barrier
- MFE/MAE
- next_bar_open research convention
- date-aware walk-forward

OPTION DATA DESIGN:
- No unsupported full Option Chain API dependency.
- PCR remains part of the feature set.
- PCR is calculated from live CE/PE option quotes supplied through
  Kotak Neo quote/feed data.
- Full option-chain endpoint is NOT fabricated.

DATA SOURCE:
- Kotak Neo API
- Live WebSocket feed
- 3-minute candle aggregation
- Optional live option quotes for PCR

STREAMLIT:
streamlit run app.py

Required Streamlit Secrets:
KOTAK_CONSUMER_KEY
KOTAK_MOBILE
KOTAK_UCC
KOTAK_TOTP
KOTAK_MPIN

Optional:
NIFTY_FUT_TOKEN

PCR configuration:
PCR is calculated from CE/PE contracts listed in:
PCR_CE_TOKENS
PCR_PE_TOKENS

These may be supplied as comma-separated token lists in Streamlit
Secrets, for example:

PCR_CE_TOKENS = "12345,12346,12347"
PCR_PE_TOKENS = "22345,22346,22347"

If these are empty, PCR remains NaN rather than inventing data.
"""

from __future__ import annotations

import os
import json
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
# CONFIG
# =========================================================

CONFIG = {
    "app_version": "v1.4_kotak_neo_pcr",
    "feature_version": "v1.7_research_lock",
    "label_version": "TB_v1.6_lock",
    "schema_version": "1.4",
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

    "nifty_future_token":
        os.getenv("NIFTY_FUT_TOKEN", ""),

    # PCR option tokens can be supplied as
    # comma separated values.
    "pcr_ce_tokens":
        os.getenv("PCR_CE_TOKENS", ""),

    "pcr_pe_tokens":
        os.getenv("PCR_PE_TOKENS", ""),

    "pcr_exchange_segment": "nse_fo",
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

        return float(value)

    except Exception:

        return default


def is_valid_number(value):

    try:

        return (
            value is not None
            and np.isfinite(float(value))
        )

    except Exception:

        return False


def parse_tokens(raw):

    if not raw:
        return []

    if isinstance(raw, list):
        return [
            str(x).strip()
            for x in raw
            if str(x).strip()
        ]

    return [
        x.strip()
        for x in str(raw).split(",")
        if x.strip()
    ]


def wilder_atr(
    trs: List[float],
    period: int = 14
) -> float:

    if len(trs) < period:
        return np.nan

    atr = np.mean(trs[:period])

    for tr in trs[period:]:

        atr = (
            (atr * (period - 1)) + tr
        ) / period

    return float(atr)


def floor_bar_timestamp(
    ts: datetime,
    minutes: int = 3
):

    session_anchor = ts.replace(
        hour=9,
        minute=15,
        second=0,
        microsecond=0,
    )

    if ts < session_anchor:
        return None

    elapsed = int(
        (ts - session_anchor).total_seconds()
        // 60
    )

    bucket = (
        elapsed // minutes
    ) * minutes

    return (
        session_anchor
        + timedelta(minutes=bucket)
    )


# =========================================================
# DATA CLASS
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

    heavy: Dict[str, Dict[str, float]] = field(
        default_factory=dict
    )

    option_chain: Dict[str, Any] = field(
        default_factory=dict
    )


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

        self.or_high = None
        self.or_low = None
        self.or_set = False

    def update(self, candle):

        mins = (
            candle.timestamp.hour * 60
            + candle.timestamp.minute
        ) - (
            9 * 60 + 15
        )

        if mins < self.minutes:

            if self.or_high is None:

                self.or_high = candle.fut_h
                self.or_low = candle.fut_l

            else:

                self.or_high = max(
                    self.or_high,
                    candle.fut_h
                )

                self.or_low = min(
                    self.or_low,
                    candle.fut_l
                )

        else:

            self.or_set = True

    def features(self, candle, atr):

        names = [
            "or_high",
            "or_low",
            "or_width_atr",
            "dist_to_or_high_atr",
            "dist_to_or_low_atr",
            "or_breakout_state",
        ]

        if (
            not self.or_set
            or self.or_high is None
            or not is_valid_number(atr)
            or atr <= 0
        ):

            return {
                k: np.nan
                for k in names
            }

        return {

            "or_high":
                self.or_high,

            "or_low":
                self.or_low,

            "or_width_atr":
                (
                    self.or_high
                    - self.or_low
                ) / atr,

            "dist_to_or_high_atr":
                (
                    candle.fut_c
                    - self.or_high
                ) / atr,

            "dist_to_or_low_atr":
                (
                    candle.fut_c
                    - self.or_low
                ) / atr,

            "or_breakout_state":
                1
                if candle.fut_c > self.or_high
                else (
                    -1
                    if candle.fut_c < self.or_low
                    else 0
                ),
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

    def set_previous_day(
        self,
        close,
        high,
        low
    ):

        self.prev_close = close
        self.prev_high = high
        self.prev_low = low

    def set_today_open(self, open_price):

        self.today_open = open_price

    def reset(self):

        self.today_open = None

    def features(self, candle, atr):

        names = [
            "gap_points",
            "gap_atr",
            "gap_direction",
            "dist_to_pdh_atr",
            "dist_to_pdl_atr",
        ]

        if (
            self.prev_close is None
            or not is_valid_number(atr)
            or atr <= 0
        ):

            return {
                k: np.nan
                for k in names
            }

        gap = (
            self.today_open
            if self.today_open is not None
            else candle.fut_o
        ) - self.prev_close

        return {

            "gap_points":
                gap,

            "gap_atr":
                gap / atr,

            "gap_direction":
                1 if gap > 0
                else (
                    -1 if gap < 0
                    else 0
                ),

            "dist_to_pdh_atr":
                (
                    candle.fut_c
                    - self.prev_high
                ) / atr
                if self.prev_high is not None
                else np.nan,

            "dist_to_pdl_atr":
                (
                    candle.fut_c
                    - self.prev_low
                ) / atr
                if self.prev_low is not None
                else np.nan,
        }


# =========================================================
# PCR ENGINE
# =========================================================

class OptionChainEngine:

    """
    PCR is retained.

    No full option-chain endpoint is called here.

    Instead, the engine consumes live option quote data
    collected by KotakNeoAdapter.

    PCR formula:

        PCR OI =
            total PE OI / total CE OI

        PCR Volume =
            total PE volume / total CE volume

    Missing data produces NaN + missing flag.
    """

    def compute(self, chain):

        if not chain:
            return {
                "pcr_oi": np.nan,
                "pcr_oi_missing": 1,
                "pcr_volume": np.nan,
                "pcr_volume_missing": 1,
                "ce_oi_change": np.nan,
                "ce_oi_change_missing": 1,
                "pe_oi_change": np.nan,
                "pe_oi_change_missing": 1,
                "atm_iv": np.nan,
                "atm_iv_missing": 1,
                "iv_change": np.nan,
                "iv_change_missing": 1,
                "ce_oi_atm": np.nan,
                "ce_oi_atm_missing": 1,
                "pe_oi_atm": np.nan,
                "pe_oi_atm_missing": 1,
                "atm_strike": np.nan,
                "atm_strike_missing": 1,
            }

        mapping = {

            "pcr_oi":
                "pcr_oi",

            "pcr_volume":
                "pcr_volume",

            "delta_ce_oi":
                "ce_oi_change",

            "delta_pe_oi":
                "pe_oi_change",

            "atm_iv":
                "atm_iv",

            "delta_atm_iv":
                "iv_change",

            "ce_oi_atm":
                "ce_oi_atm",

            "pe_oi_atm":
                "pe_oi_atm",

            "atm_strike":
                "atm_strike",
        }

        out = {}

        for feat, key in mapping.items():

            val = chain.get(
                key,
                np.nan
            )

            out[feat] = val

            missing = (
                val is None
                or (
                    isinstance(val, float)
                    and np.isnan(val)
                )
            )

            out[
                f"{feat}_missing"
            ] = int(missing)

        return out


# =========================================================
# HEAVYWEIGHT ENGINE
# =========================================================

class HeavyweightEngine:

    def __init__(self, weights):

        self.base_weights = weights
        self.day_open = {}

    def set_day_open(self, symbol, price):

        if price and price > 0:

            self.day_open[symbol] = price

    def reset_day(self):

        self.day_open.clear()

    def compute(self, candle):

        ics = []
        rets = []
        bullish = 0

        for sym, w in self.base_weights.items():

            if sym not in candle.heavy:
                continue

            d = candle.heavy[sym]

            open_p = self.day_open.get(sym)

            if open_p is None:

                open_p = d.get(
                    "o",
                    d.get("c", np.nan)
                )

                if (
                    is_valid_number(open_p)
                    and open_p > 0
                ):

                    self.day_open[sym] = open_p

            if (
                not is_valid_number(open_p)
                or open_p <= 0
            ):
                continue

            close_p = safe_float(
                d.get("c")
            )

            if not is_valid_number(close_p):
                continue

            ret = (
                close_p - open_p
            ) / open_p

            rel_vol = safe_float(
                d.get(
                    "rel_volume",
                    1.0
                ),
                1.0
            )

            ics.append(
                w * ret * rel_vol
            )

            rets.append(ret)

            vwap = safe_float(
                d.get(
                    "vwap",
                    close_p
                ),
                close_p
            )

            if close_p >= vwap:
                bullish += 1

        twc = sum(ics) if ics else 0.0

        n = max(
            len(ics),
            1
        )

        return {

            "twc":
                twc,

            "breadth_10":
                bullish / n,

            "dispersion_index":
                float(
                    np.std(rets)
                )
                if rets
                else 0.0,

            "contribution_concentration":
                (
                    max(ics)
                    / (
                        abs(twc)
                        + 1e-9
                    )
                )
                if ics
                else 0.0,

            "hw_bullish_count":
                bullish,
        }


# =========================================================
# FEATURE ENGINE
# =========================================================

class FeatureEngine:

    def __init__(self):

        self.vwap_pv = 0.0
        self.vwap_vol = 0.0

        self.tr_history = []

        self.history = []

        self.hw = HeavyweightEngine(
            HEAVYWEIGHTS
        )

        self.or_eng = OpeningRangeEngine(
            CONFIG[
                "opening_range_minutes"
            ]
        )

        self.sess = SessionContextEngine()

        self.opt = OptionChainEngine()

    def reset_session(self):

        self.vwap_pv = 0.0
        self.vwap_vol = 0.0

        self.tr_history.clear()
        self.history.clear()

        self.hw.reset_day()
        self.or_eng.reset()
        self.sess.reset()

    def set_previous_day(self, c, h, l):

        self.sess.set_previous_day(
            c,
            h,
            l
        )

    def set_today_open(self, o):

        self.sess.set_today_open(o)

    def compute(self, candle, prev):

        # -------------------------
        # VWAP
        # -------------------------

        typical = (
            candle.fut_h
            + candle.fut_l
            + candle.fut_c
        ) / 3.0

        self.vwap_pv += (
            typical
            * candle.fut_volume
        )

        self.vwap_vol += (
            candle.fut_volume
        )

        fut_vwap = (
            self.vwap_pv
            / self.vwap_vol
            if self.vwap_vol > 0
            else typical
        )

        # -------------------------
        # TR / ATR
        # -------------------------

        if prev:

            tr = max(

                candle.fut_h
                - candle.fut_l,

                abs(
                    candle.fut_h
                    - prev[-1].fut_c
                ),

                abs(
                    candle.fut_l
                    - prev[-1].fut_c
                ),
            )

        else:

            tr = (
                candle.fut_h
                - candle.fut_l
            )

        atr_prev = wilder_atr(
            self.tr_history,
            CONFIG["atr_period"]
        )

        self.tr_history.append(tr)

        atr_close = wilder_atr(
            self.tr_history,
            CONFIG["atr_period"]
        )

        atr = atr_prev

        atr_warmup = int(
            np.isnan(atr)
        )

        # -------------------------
        # SMA20
        # -------------------------

        closes = (
            [
                c.spot_c
                for c in prev[
                    -(CONFIG["sma_period"] - 1):
                ]
            ]
            + [candle.spot_c]
        )

        sma_ready = int(
            len(closes)
            >= CONFIG["sma_period"]
        )

        spot_sma = (
            float(np.mean(closes))
            if sma_ready
            else np.nan
        )

        # -------------------------
        # Normalized features
        # -------------------------

        if (
            is_valid_number(atr)
            and atr > 0
        ):

            norm_stretch = (
                candle.fut_c
                - fut_vwap
            ) / atr

            norm_spread = (
                (
                    spot_sma
                    - fut_vwap
                ) / atr
                if not np.isnan(
                    spot_sma
                )
                else np.nan
            )

        else:

            norm_stretch = np.nan
            norm_spread = np.nan

        if (
            self.history
            and not np.isnan(norm_stretch)
            and not np.isnan(
                self.history[-1][
                    "normalized_stretch"
                ]
            )
        ):

            stretch_slope = (
                norm_stretch
                - self.history[-1][
                    "normalized_stretch"
                ]
            )

        else:

            stretch_slope = 0.0

        if (
            self.history
            and not np.isnan(norm_spread)
            and not np.isnan(
                self.history[-1][
                    "normalized_spread"
                ]
            )
        ):

            spread_slope = (
                norm_spread
                - self.history[-1][
                    "normalized_spread"
                ]
            )

        else:

            spread_slope = 0.0

        # -------------------------
        # OI
        # -------------------------

        oi_chg = (
            candle.fut_oi
            - prev[-1].fut_oi
            if prev
            else 0.0
        )

        price_up = (
            candle.fut_c
            > prev[-1].fut_c
            if prev
            else False
        )

        price_dn = (
            candle.fut_c
            < prev[-1].fut_c
            if prev
            else False
        )

        oi_long_buildup = int(
            price_up
            and oi_chg > 0
        )

        oi_short_buildup = int(
            price_dn
            and oi_chg > 0
        )

        oi_short_covering = int(
            price_up
            and oi_chg < 0
        )

        oi_long_unwinding = int(
            price_dn
            and oi_chg < 0
        )

        oi_neutral = int(
            oi_chg == 0
            or (
                not price_up
                and not price_dn
            )
        )

        oi_strength = 0.0

        if oi_chg != 0:

            oi_strength = (
                (
                    1
                    if price_up
                    else -1
                )
                * np.sign(oi_chg)
                * np.log1p(
                    abs(oi_chg)
                )
            )

        # -------------------------
        # OR
        # -------------------------

        self.or_eng.update(candle)

        # -------------------------
        # DATA QUALITY
        # -------------------------

        missing_spot = int(
            not is_valid_number(
                candle.spot_c
            )
        )

        missing_fut = int(
            not is_valid_number(
                candle.fut_c
            )
        )

        missing_oi = int(
            not is_valid_number(
                candle.fut_oi
            )
        )

        missing_volume = int(
            not is_valid_number(
                candle.fut_volume
            )
            or candle.fut_volume <= 0
        )

        missing_heavy = int(
            len(candle.heavy) == 0
        )

        missing_option = int(
            len(candle.option_chain) == 0
        )

        bad_ohlc = int(
            candle.fut_h < candle.fut_l
            or candle.spot_h < candle.spot_l
        )

        zero_volume = int(
            candle.fut_volume == 0
        )

        zero_oi = int(
            candle.fut_oi == 0
        )

        data_quality_score = (
            1.0
            - 0.1
            * sum([
                missing_spot,
                missing_fut,
                missing_oi,
                missing_volume,
                missing_heavy,
                missing_option,
                bad_ohlc,
                zero_volume,
                zero_oi,
            ])
        )

        basis = (
            candle.fut_c
            - candle.spot_c
        )

        # -------------------------
        # FINAL FEATURE DICT
        # -------------------------

        feats = {

            "timestamp":
                candle.timestamp,

            "feature_version":
                CONFIG["feature_version"],

            "schema_version":
                CONFIG["schema_version"],

            "weight_version":
                CONFIG["weight_version"],

            "atr_mode":
                CONFIG["atr_mode"],

            "execution_model":
                CONFIG["execution_model"],

            "basis":
                basis,

            "fut_vwap":
                fut_vwap,

            "normalized_stretch":
                norm_stretch,

            "normalized_spread":
                norm_spread,

            "stretch_slope_3":
                stretch_slope,

            "spread_slope_3":
                spread_slope,

            "atr_14_prev":
                atr_prev,

            "atr_14_close":
                atr_close,

            "atr_warmup_flag":
                atr_warmup,

            "spot_sma_20":
                spot_sma,

            "sma20_warmup_flag":
                1 - sma_ready,

            "oi_long_buildup":
                oi_long_buildup,

            "oi_short_buildup":
                oi_short_buildup,

            "oi_short_covering":
                oi_short_covering,

            "oi_long_unwinding":
                oi_long_unwinding,

            "oi_neutral":
                oi_neutral,

            "oi_strength":
                oi_strength,

            "minutes_from_open":
                (
                    candle.timestamp.hour * 60
                    + candle.timestamp.minute
                ) - (
                    9 * 60 + 15
                ),

            "day_of_week":
                candle.timestamp.weekday(),

            **self.hw.compute(candle),

            **self.or_eng.features(
                candle,
                atr
                if is_valid_number(atr)
                else 0.0
            ),

            **self.sess.features(
                candle,
                atr
                if is_valid_number(atr)
                else 0.0
            ),

            # PCR retained.
            **self.opt.compute(
                candle.option_chain
            ),

            "missing_spot":
                missing_spot,

            "missing_future":
                missing_fut,

            "missing_oi":
                missing_oi,

            "missing_volume":
                missing_volume,

            "missing_heavyweight":
                missing_heavy,

            "missing_option_chain":
                missing_option,

            "bad_ohlc":
                bad_ohlc,

            "zero_volume":
                zero_volume,

            "zero_oi":
                zero_oi,

            "data_quality_score":
                max(
                    0.0,
                    data_quality_score
                ),

            "bar_complete":
                1,
        }

        self.history.append(feats)

        return feats


# =========================================================
# LABEL ENGINE
# =========================================================

class LabelEngine:

    def __init__(self):

        self.upper = CONFIG[
            "triple_upper_atr"
        ]

        self.lower = CONFIG[
            "triple_lower_atr"
        ]

        self.tb_horizon = CONFIG[
            "time_barrier_min"
        ]

        self.mfe_horizons = CONFIG[
            "mfe_horizons_min"
        ]

        self.execution_model = CONFIG[
            "execution_model"
        ]

        if (
            self.execution_model
            != "next_bar_open"
        ):

            raise ValueError(
                "Research-Lock requires "
                "next_bar_open"
            )

    def _excursion(
        self,
        entry,
        future,
        direction,
        max_bars
    ):

        mfe = 0.0
        mae = 0.0

        available = min(
            len(future),
            max_bars
        )

        for c in future[:available]:

            if direction == 1:

                mfe = max(
                    mfe,
                    c.fut_h - entry
                )

                mae = max(
                    mae,
                    entry - c.fut_l
                )

            else:

                mfe = max(
                    mfe,
                    entry - c.fut_l
                )

                mae = max(
                    mae,
                    c.fut_h - entry
                )

        complete = int(
            available >= max_bars
        )

        return (
            mfe,
            mae,
            complete
        )

    def generate(
        self,
        entry_price,
        atr,
        future_after_entry,
        direction=1,
        signal_timestamp=None,
        entry_timestamp=None,
    ):

        if (
            entry_timestamp
            and future_after_entry
        ):

            first_future_ts = (
                future_after_entry[
                    0
                ].timestamp
            )

            if (
                first_future_ts
                <= entry_timestamp
            ):

                raise ValueError(
                    "FUTURE ALIGNMENT "
                    "VIOLATION: future candle "
                    "must be strictly AFTER "
                    "entry bar."
                )

        if (
            atr is None
            or not is_valid_number(atr)
            or atr <= 0
        ):

            atr = np.nan

        upper = (
            entry_price
            + direction
            * self.upper
            * atr
            if not np.isnan(atr)
            else np.nan
        )

        lower = (
            entry_price
            - direction
            * self.lower
            * atr
            if not np.isnan(atr)
            else np.nan
        )

        outcome = "TIMEOUT"

        bars = 0

        mfe_tb = 0.0
        mae_tb = 0.0

        time_to_mfe = 0

        max_tb_bars = (
            self.tb_horizon
            // CONFIG["bar_minutes"]
        )

        for i, c in enumerate(
            future_after_entry[:max_tb_bars]
        ):

            bars = i + 1

            if direction == 1:

                mfe_tb = max(
                    mfe_tb,
                    c.fut_h
                    - entry_price
                )

                mae_tb = max(
                    mae_tb,
                    entry_price
                    - c.fut_l
                )

                hit_t = (
                    not np.isnan(upper)
                    and c.fut_h >= upper
                )

                hit_s = (
                    not np.isnan(lower)
                    and c.fut_l <= lower
                )

            else:

                mfe_tb = max(
                    mfe_tb,
                    entry_price
                    - c.fut_l
                )

                mae_tb = max(
                    mae_tb,
                    c.fut_h
                    - entry_price
                )

                hit_t = (
                    not np.isnan(upper)
                    and c.fut_l <= upper
                )

                hit_s = (
                    not np.isnan(lower)
                    and c.fut_h >= lower
                )

            if (
                mfe_tb > 0
                and time_to_mfe == 0
            ):

                time_to_mfe = bars

            if hit_t and hit_s:

                outcome = "AMBIGUOUS"
                break

            if hit_t:

                outcome = "TARGET_FIRST"
                break

            if hit_s:

                outcome = "STOP_FIRST"
                break

        if outcome == "TARGET_FIRST":

            r_multiple = (
                self.upper
                / self.lower
            )

        elif outcome == "STOP_FIRST":

            r_multiple = -1.0

        else:

            r_multiple = np.nan

        valid = int(
            outcome != "AMBIGUOUS"
            and not np.isnan(atr)
        )

        mfe_atr = (
            mfe_tb / atr
            if not np.isnan(atr)
            and atr > 0
            else np.nan
        )

        mae_atr = (
            mae_tb / atr
            if not np.isnan(atr)
            and atr > 0
            else np.nan
        )

        velocity = (
            mfe_atr
            / max(bars, 1)
            if not np.isnan(mfe_atr)
            else np.nan
        )

        if not np.isnan(mfe_atr):

            if (
                mfe_atr >= 1.2
                and mae_atr <= 0.45
                and velocity > 0.25
            ):

                traj = "IMPULSE"

            elif (
                mfe_atr >= 0.8
                and mae_atr <= 0.70
                and 0.08
                < velocity
                <= 0.25
            ):

                traj = "STAIRCASE"

            elif (
                mfe_atr >= 0.5
                and velocity <= 0.08
            ):

                traj = "GRIND"

            else:

                traj = "FAILURE"

        else:

            traj = "UNKNOWN"

        real_breakout = int(
            outcome == "TARGET_FIRST"
            and not np.isnan(mfe_atr)
            and mfe_atr >= 1.0
            and mae_atr <= 0.55
        )

        labels = {

            "label_version":
                CONFIG["label_version"],

            "execution_model":
                self.execution_model,

            "signal_timestamp":
                signal_timestamp,

            "entry_timestamp":
                entry_timestamp,

            "entry_price":
                entry_price,

            "triple_barrier_outcome":
                outcome,

            "label_valid_for_training":
                valid,

            "r_multiple":
                r_multiple,

            "trajectory":
                traj,

            "real_breakout":
                real_breakout,

            "mfe_atr_tb":
                mfe_atr,

            "mae_atr_tb":
                mae_atr,

            "time_to_mfe":
                time_to_mfe,

            "bars_to_outcome":
                bars,

            "velocity":
                velocity,
        }

        for h in self.mfe_horizons:

            max_bars = (
                h
                // CONFIG["bar_minutes"]
            )

            mfe_h, mae_h, complete = (
                self._excursion(
                    entry_price,
                    future_after_entry,
                    direction,
                    max_bars
                )
            )

            labels[
                f"mfe_atr_{h}m"
            ] = (
                mfe_h / atr
                if not np.isnan(atr)
                and atr > 0
                else np.nan
            )

            labels[
                f"mae_atr_{h}m"
            ] = (
                mae_h / atr
                if not np.isnan(atr)
                and atr > 0
                else np.nan
            )

            labels[
                f"horizon_{h}m_complete"
            ] = complete

        return labels


# =========================================================
# DATASET MANAGER
# =========================================================

class DatasetManager:

    def __init__(self, path=None):

        self.base = Path(
            path
            or CONFIG["dataset_path"]
        )

        self.base.mkdir(
            parents=True,
            exist_ok=True
        )

    def write_parquet(
        self,
        df,
        name="features"
    ):

        if df.empty:
            return

        df = df.copy()

        if "timestamp" in df.columns:

            df["date"] = (
                pd.to_datetime(
                    df["timestamp"]
                )
                .dt.date
                .astype(str)
            )

        table = pa.Table.from_pandas(
            df,
            preserve_index=False
        )

        pq.write_to_dataset(
            table,
            root_path=str(
                self.base / name
            ),
            partition_cols=(
                ["date"]
                if "date" in df.columns
                else None
            ),
            existing_data_behavior=
                "overwrite_or_ignore",
        )

    def purged_walk_forward_by_date(
        self,
        df,
        n_splits=5
    ):

        if "timestamp" not in df.columns:

            raise ValueError(
                "timestamp required"
            )

        df = df.copy()

        df["date"] = (
            pd.to_datetime(
                df["timestamp"]
            ).dt.date
        )

        unique_dates = sorted(
            df["date"].unique()
        )

        n_dates = len(unique_dates)

        fold = max(
            1,
            n_dates
            // (n_splits + 1)
        )

        purge_days = 1

        splits = []

        for i in range(n_splits):

            train_end = (
                (i + 1) * fold
            )

            test_start = (
                train_end
                + purge_days
            )

            test_end = min(
                test_start + fold,
                n_dates
            )

            if test_start >= n_dates:
                break

            train_dates = (
                unique_dates[:train_end]
            )

            test_dates = (
                unique_dates[
                    test_start:test_end
                ]
            )

            train_idx = (
                df[
                    df["date"].isin(
                        train_dates
                    )
                ]
                .index
                .tolist()
            )

            test_idx = (
                df[
                    df["date"].isin(
                        test_dates
                    )
                ]
                .index
                .tolist()
            )

            if (
                train_idx
                and CONFIG["embargo_bars"] > 0
            ):

                train_idx = train_idx[
                    :-CONFIG["embargo_bars"]
                ]

            splits.append(
                (
                    train_idx,
                    test_idx
                )
            )

        return splits


# =========================================================
# KOTAK NEO DATA ADAPTER
# =========================================================

class KotakNeoAdapter:

    """
    Live Kotak Neo adapter.

    No unsupported historical candle endpoint.

    NIFTY spot/future candles are built from live ticks.

    PCR:
        live CE/PE option quote data can be subscribed
        and aggregated into PCR.
    """

    def __init__(self):

        if NeoAPI is None:

            raise ImportError(
                "neo_api_client missing. "
                "Install Kotak Neo SDK."
            )

        self.consumer_key = os.getenv(
            "KOTAK_CONSUMER_KEY",
            ""
        )

        self.mobile = os.getenv(
            "KOTAK_MOBILE",
            ""
        )

        self.ucc = os.getenv(
            "KOTAK_UCC",
            ""
        )

        self.totp = os.getenv(
            "KOTAK_TOTP",
            ""
        )

        self.mpin = os.getenv(
            "KOTAK_MPIN",
            ""
        )

        self.client = NeoAPI(
            environment=CONFIG[
                "neo_environment"
            ],
            access_token=None,
            neo_fin_key=None,
            consumer_key=(
                self.consumer_key
                or None
            ),
        )

        self.connected = False

        self.lock = threading.Lock()

        self.latest = {}

        self.tick_buffer = []

    # ---------------------------------
    # Authentication
    # ---------------------------------

    def login(self):

        required = {

            "KOTAK_MOBILE":
                self.mobile,

            "KOTAK_UCC":
                self.ucc,

            "KOTAK_TOTP":
                self.totp,

            "KOTAK_MPIN":
                self.mpin,
        }

        missing = [
            k
            for k, v in required.items()
            if not v
        ]

        if missing:

            raise RuntimeError(
                "Missing credentials: "
                + ", ".join(missing)
            )

        self.client.totp_login(
            mobile_number=self.mobile,
            ucc=self.ucc,
            totp=self.totp,
        )

        self.client.totp_validate(
            mpin=self.mpin
        )

        self.connected = True

        return True

    # ---------------------------------
    # callbacks
    # ---------------------------------

    def on_open(self, message):

        print(
            "[Kotak Neo] WebSocket opened:",
            message
        )

    def on_error(self, message):

        print(
            "[Kotak Neo] ERROR:",
            message
        )

    def on_close(self, message):

        self.connected = False

        print(
            "[Kotak Neo] WebSocket closed:",
            message
        )

    def on_message(self, message):

        try:

            data = self._decode_message(
                message
            )

            if data is None:
                return

            self._process_tick(data)

        except Exception as exc:

            print(
                "[Kotak Neo] parse error:",
                repr(exc)
            )

    def _decode_message(self, message):

        if isinstance(message, dict):
            return message

        if isinstance(message, str):

            try:
                return json.loads(message)

            except Exception:
                return None

        return None

    # ---------------------------------
    # timestamp
    # ---------------------------------

    def _tick_time(self, data):

        raw = (
            data.get("ltt")
            or data.get("ftdm")
            or data.get("tvalue")
        )

        if raw is None:
            return datetime.now()

        try:

            if isinstance(
                raw,
                (int, float)
            ):

                if raw > 10_000_000_000:

                    return datetime.fromtimestamp(
                        raw / 1000
                    )

                if raw > 1_000_000_000:

                    return datetime.fromtimestamp(
                        raw
                    )

            text = str(raw)

            formats = (
                "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%d-%m-%Y %H:%M:%S",
            )

            for fmt in formats:

                try:

                    return datetime.strptime(
                        text,
                        fmt
                    )

                except ValueError:
                    pass

        except Exception:
            pass

        return datetime.now()

    # ---------------------------------
    # tick processing
    # ---------------------------------

    def _process_tick(self, data):

        if isinstance(
            data.get("data"),
            dict
        ):

            data = data["data"]

        if not isinstance(data, dict):
            return

        token = str(
            data.get("tk", "")
        )

        exchange = str(
            data.get("e", "")
        )

        ltp = safe_float(
            data.get("ltp")
        )

        if not is_valid_number(ltp):
            return

        timestamp = self._tick_time(data)

        item = {

            "timestamp":
                timestamp,

            "token":
                token,

            "exchange":
                exchange,

            "symbol":
                data.get("ts", ""),

            "ltp":
                ltp,

            "volume":
                safe_float(
                    data.get("v"),
                    0.0
                ),

            "oi":
                safe_float(
                    data.get("oi"),
                    0.0
                ),

            "open":
                safe_float(
                    data.get("op")
                ),

            "high":
                safe_float(
                    data.get("h")
                ),

            "low":
                safe_float(
                    data.get("lo")
                ),

            "vwap":
                safe_float(
                    data.get("ap")
                ),

            # Optional option fields.
            "iv":
                safe_float(
                    data.get("iv")
                ),

            "strike":
                safe_float(
                    data.get("strike")
                ),

            "option_type":
                str(
                    data.get(
                        "option_type",
                        data.get(
                            "optType",
                            ""
                        )
                    )
                ).upper(),
        }

        with self.lock:

            self.latest[token] = item

            self.tick_buffer.append(item)

    # ---------------------------------
    # subscribe
    # ---------------------------------

    def subscribe(
        self,
        instrument_tokens,
        is_index=False,
        is_depth=False
    ):

        if not self.connected:

            raise RuntimeError(
                "Login first."
            )

        self.client.on_message = (
            self.on_message
        )

        self.client.on_error = (
            self.on_error
        )

        self.client.on_close = (
            self.on_close
        )

        self.client.on_open = (
            self.on_open
        )

        return self.client.subscribe(
            instrument_tokens=
                instrument_tokens,
            isIndex=is_index,
            isDepth=is_depth,
        )

    # ---------------------------------
    # PCR subscription
    # ---------------------------------

    def subscribe_pcr_tokens(self):

        ce_tokens = parse_tokens(
            CONFIG["pcr_ce_tokens"]
        )

        pe_tokens = parse_tokens(
            CONFIG["pcr_pe_tokens"]
        )

        tokens = []

        for token in ce_tokens + pe_tokens:

            tokens.append({

                "instrument_token":
                    token,

                "exchange_segment":
                    CONFIG[
                        "pcr_exchange_segment"
                    ],
            })

        if not tokens:

            return False

        self.subscribe(
            tokens,
            is_index=False,
            is_depth=False
        )

        return True

    # ---------------------------------
    # PCR calculation
    # ---------------------------------

    def calculate_pcr(self):

        ce_oi = 0.0
        pe_oi = 0.0

        ce_volume = 0.0
        pe_volume = 0.0

        ce_oi_prev = 0.0
        pe_oi_prev = 0.0

        ce_count = 0
        pe_count = 0

        ce_ivs = []
        pe_ivs = []

        ce_atm = []
        pe_atm = []

        ce_tokens = parse_tokens(
            CONFIG["pcr_ce_tokens"]
        )

        pe_tokens = parse_tokens(
            CONFIG["pcr_pe_tokens"]
        )

        with self.lock:

            latest = dict(
                self.latest
            )

        for token in ce_tokens:

            d = latest.get(token)

            if not d:
                continue

            oi = safe_float(
                d.get("oi"),
                0.0
            )

            vol = safe_float(
                d.get("volume"),
                0.0
            )

            ce_oi += max(oi, 0.0)
            ce_volume += max(vol, 0.0)

            ce_count += 1

            iv = safe_float(
                d.get("iv")
            )

            if is_valid_number(iv):
                ce_ivs.append(iv)

            ce_atm.append(d)

        for token in pe_tokens:

            d = latest.get(token)

            if not d:
                continue

            oi = safe_float(
                d.get("oi"),
                0.0
            )

            vol = safe_float(
                d.get("volume"),
                0.0
            )

            pe_oi += max(oi, 0.0)
            pe_volume += max(vol, 0.0)

            pe_count += 1

            iv = safe_float(
                d.get("iv")
            )

            if is_valid_number(iv):
                pe_ivs.append(iv)

            pe_atm.append(d)

        pcr_oi = (
            pe_oi / ce_oi
            if ce_oi > 0
            else np.nan
        )

        pcr_volume = (
            pe_volume / ce_volume
            if ce_volume > 0
            else np.nan
        )

        return {

            "pcr_oi":
                pcr_oi,

            "pcr_volume":
                pcr_volume,

            "ce_oi_change":
                ce_oi_prev,

            "pe_oi_change":
                pe_oi_prev,

            "atm_iv":
                (
                    float(
                        np.mean(
                            ce_ivs
                            + pe_ivs
                        )
                    )
                    if (
                        ce_ivs
                        or pe_ivs
                    )
                    else np.nan
                ),

            "iv_change":
                np.nan,

            "ce_oi_atm":
                (
                    ce_oi / ce_count
                    if ce_count
                    else np.nan
                ),

            "pe_oi_atm":
                (
                    pe_oi / pe_count
                    if pe_count
                    else np.nan
                ),

            "atm_strike":
                np.nan,

            "ce_contracts_seen":
                ce_count,

            "pe_contracts_seen":
                pe_count,
        }

    # ---------------------------------
    # 3-minute candle builder
    # ---------------------------------

    def build_3min_candles(self):

        with self.lock:

            ticks = list(
                self.tick_buffer
            )

            self.tick_buffer.clear()

            latest = dict(
                self.latest
            )

        if not ticks:
            return []

        # Only NIFTY spot/future tokens
        # should become candles.

        allowed_tokens = set()

        if CONFIG[
            "nifty_future_token"
        ]:

            allowed_tokens.add(
                str(
                    CONFIG[
                        "nifty_future_token"
                    ]
                )
            )

        # NIFTY index is identified separately
        # through its configured name.

        buckets = {}

        for tick in ticks:

            token = str(
                tick["token"]
            )

            symbol = str(
                tick.get("symbol", "")
            )

            is_index = (
                symbol.lower()
                == CONFIG[
                    "nifty_index_name"
                ].lower()
            )

            is_future = (
                token in allowed_tokens
            )

            if not (
                is_index
                or is_future
            ):
                continue

            ts = floor_bar_timestamp(
                tick["timestamp"],
                CONFIG["bar_minutes"]
            )

            if ts is None:
                continue

            key = (
                token,
                ts
            )

            buckets.setdefault(
                key,
                []
            ).append(tick)

        output = []

        for (
            token,
            ts
        ), rows in sorted(
            buckets.items()
        ):

            rows = sorted(
                rows,
                key=lambda x:
                    x["timestamp"]
            )

            prices = [
                x["ltp"]
                for x in rows
                if is_valid_number(
                    x["ltp"]
                )
            ]

            if not prices:
                continue

            volumes = [
                x["volume"]
                for x in rows
                if is_valid_number(
                    x["volume"]
                )
            ]

            ois = [
                x["oi"]
                for x in rows
                if is_valid_number(
                    x["oi"]
                )
            ]

            output.append({

                "token":
                    token,

                "symbol":
                    rows[-1].get(
                        "symbol",
                        ""
                    ),

                "timestamp":
                    ts,

                "open":
                    prices[0],

                "high":
                    max(prices),

                "low":
                    min(prices),

                "close":
                    prices[-1],

                "volume":
                    max(volumes)
                    if volumes
                    else 0.0,

                "oi":
                    ois[-1]
                    if ois
                    else 0.0,
            })

        # Merge spot + future by timestamp.
        merged = {}

        for bar in output:

            ts = bar["timestamp"]

            merged.setdefault(
                ts,
                {}
            )

            symbol = str(
                bar.get("symbol", "")
            ).lower()

            if (
                symbol
                == CONFIG[
                    "nifty_index_name"
                ].lower()
            ):

                merged[ts]["spot"] = bar

            elif (
                str(bar["token"])
                == str(
                    CONFIG[
                        "nifty_future_token"
                    ]
                )
            ):

                merged[ts]["future"] = bar

        final = []

        for ts, d in sorted(
            merged.items()
        ):

            spot = d.get("spot")
            future = d.get("future")

            if spot is None:
                continue

            if future is None:
                # Preserve functionality but do not
                # fabricate future prices.
                continue

            final.append({

                "timestamp":
                    ts,

                "spot":
                    spot,

                "future":
                    future,
            })

        return final


# =========================================================
# NIFTY ENGINE CONTROLLER
# =========================================================

class NiftyMicroEngine:

    def __init__(self):

        self.features = FeatureEngine()

        self.labels = LabelEngine()

        self.dataset = DatasetManager()

        self.prev_candles = []

        self.feature_rows = []

        self.signal_rows = []

        self.current_date = None

        self.last_bar_timestamp = None

    def reset_if_new_day(self, timestamp):

        current_date = timestamp.date()

        if (
            self.current_date
            != current_date
        ):

            self.features.reset_session()

            self.prev_candles.clear()

            self.current_date = current_date

            self.last_bar_timestamp = None

    def process_candle(self, candle):

        self.reset_if_new_day(
            candle.timestamp
        )

        if (
            self.last_bar_timestamp
            is not None
            and candle.timestamp
            <= self.last_bar_timestamp
        ):

            return None

        feat = self.features.compute(
            candle,
            self.prev_candles
        )

        self.prev_candles.append(
            candle
        )

        if len(
            self.prev_candles
        ) > 500:

            self.prev_candles = (
                self.prev_candles[-500:]
            )

        self.last_bar_timestamp = (
            candle.timestamp
        )

        self.feature_rows.append(
            feat
        )

        return feat

    def dataframe(self):

        if not self.feature_rows:
            return pd.DataFrame()

        return pd.DataFrame(
            self.feature_rows
        )

    def save(self):

        df = self.dataframe()

        if df.empty:
            return

        self.dataset.write_parquet(
            df,
            name="features"
        )


# =========================================================
# UNIT TESTS
# =========================================================

def run_unit_tests():

    print(
        "\n===== "
        "KOTAK NEO RESEARCH LOCK "
        "UNIT TESTS ====="
    )

    assert np.isnan(
        wilder_atr(
            [10, 12],
            14
        )
    )

    print("✓ ATR warm-up")

    ts = datetime(
        2025,
        1,
        2,
        9,
        18
    )

    candle = Candle3Min(

        ts,

        24000,
        24050,
        23980,
        24020,

        24010,
        24060,
        23990,
        24030,

        100000,
        5_000_000,
    )

    engine = NiftyMicroEngine()

    f = engine.process_candle(
        candle
    )

    assert f is not None

    assert (
        f["execution_model"]
        == "next_bar_open"
    )

    assert (
        "data_quality_score"
        in f
    )

    assert (
        "pcr_oi"
        in f
    )

    print("✓ FeatureEngine + PCR")

    label = LabelEngine()

    future = [
        Candle3Min(

            datetime(
                2025,
                1,
                2,
                9,
                24
            ),

            0,
            0,
            0,
            0,

            24100,
            24200,
            23900,
            24050,

            0,
            0,
        )
    ]

    result = label.generate(

        entry_price=24040,

        atr=20.0,

        future_after_entry=future,

        direction=1,

        signal_timestamp=
            datetime(
                2025,
                1,
                2,
                9,
                18
            ),

        entry_timestamp=
            datetime(
                2025,
                1,
                2,
                9,
                21
            ),
    )

    assert (
        result[
            "execution_model"
        ]
        == "next_bar_open"
    )

    assert (
        "horizon_45m_complete"
        in result
    )

    print(
        "✓ next_bar_open alignment"
    )

    try:

        label.generate(

            entry_price=24040,

            atr=20.0,

            future_after_entry=[
                Candle3Min(
                    datetime(
                        2025,
                        1,
                        2,
                        9,
                        21
                    ),
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0
                )
            ],

            direction=1,

            entry_timestamp=
                datetime(
                    2025,
                    1,
                    2,
                    9,
                    21
                )
        )

    except ValueError:

        print(
            "✓ Future alignment lock"
        )

    else:

        raise AssertionError(
            "Alignment lock failed"
        )

    print(
        "===== ALL TESTS PASSED =====\n"
    )


# =========================================================
# STREAMLIT
# =========================================================

def run_streamlit_app():

    if st is None:

        raise RuntimeError(
            "Streamlit is not installed."
        )

    st.set_page_config(
        page_title=
            "NIFTY 3-Min Micro Engine",
        layout="wide"
    )

    st.title(
        "NIFTY 3-Min Micro Engine"
    )

    st.caption(
        "Kotak Neo • Research-Lock v1.4 • PCR retained"
    )

    st.info(
        "Full Option Chain API dependency removed. "
        "PCR is retained and calculated from the "
        "live CE/PE option quotes configured below."
    )

    # ---------------------------------
    # session state
    # ---------------------------------

    if "neo" not in st.session_state:

        st.session_state.neo = None

    if "engine" not in st.session_state:

        st.session_state.engine = (
            NiftyMicroEngine()
        )

    # ---------------------------------
    # sidebar
    # ---------------------------------

    st.sidebar.header(
        "Kotak Neo"
    )

    credentials = {

        "Consumer Key":
            bool(
                os.getenv(
                    "KOTAK_CONSUMER_KEY"
                )
            ),

        "Mobile":
            bool(
                os.getenv(
                    "KOTAK_MOBILE"
                )
            ),

        "UCC":
            bool(
                os.getenv(
                    "KOTAK_UCC"
                )
            ),

        "TOTP":
            bool(
                os.getenv(
                    "KOTAK_TOTP"
                )
            ),

        "MPIN":
            bool(
                os.getenv(
                    "KOTAK_MPIN"
                )
            ),
    }

    for name, present in credentials.items():

        st.sidebar.write(
            name + ":",
            "✓" if present else "✗"
        )

    st.sidebar.divider()

    st.sidebar.subheader(
        "PCR"
    )

    ce_tokens = parse_tokens(
        CONFIG["pcr_ce_tokens"]
    )

    pe_tokens = parse_tokens(
        CONFIG["pcr_pe_tokens"]
    )

    st.sidebar.write(
        "CE tokens:",
        len(ce_tokens)
    )

    st.sidebar.write(
        "PE tokens:",
        len(pe_tokens)
    )

    st.sidebar.caption(
        "PCR is calculated from subscribed "
        "CE/PE live quotes."
    )

    # ---------------------------------
    # buttons
    # ---------------------------------

    col1, col2 = st.columns(2)

    with col1:

        if st.button(
            "Connect Kotak Neo"
        ):

            try:

                neo = KotakNeoAdapter()

                neo.login()

                st.session_state.neo = neo

                st.success(
                    "Kotak Neo login successful."
                )

            except Exception as exc:

                st.error(
                    f"Login failed: {exc}"
                )

    with col2:

        if st.button(
            "Run Unit Tests"
        ):

            try:

                run_unit_tests()

                st.success(
                    "All tests passed."
                )

            except Exception as exc:

                st.error(
                    str(exc)
                )

    neo = st.session_state.neo

    # ---------------------------------
    # live feed
    # ---------------------------------

    if neo is not None:

        st.subheader(
            "Live Feed"
        )

        nifty_token = {

            "instrument_token":
                CONFIG[
                    "nifty_index_name"
                ],

            "exchange_segment":
                "nse_cm",
        }

        fut_token = (
            CONFIG[
                "nifty_future_token"
            ]
        )

        subscribe_list = [
            nifty_token
        ]

        if fut_token:

            subscribe_list.append({

                "instrument_token":
                    fut_token,

                "exchange_segment":
                    "nse_fo",
            })

        if st.button(
            "Subscribe NIFTY Spot + Future"
        ):

            try:

                neo.subscribe(
                    subscribe_list,
                    is_index=True,
                    is_depth=False
                )

                st.success(
                    "NIFTY subscription request sent."
                )

            except Exception as exc:

                st.error(
                    f"Subscription failed: {exc}"
                )

        if st.button(
            "Subscribe PCR Options"
        ):

            try:

                ok = (
                    neo.subscribe_pcr_tokens()
                )

                if ok:

                    st.success(
                        "PCR CE/PE subscriptions sent."
                    )

                else:

                    st.warning(
                        "PCR_CE_TOKENS / "
                        "PCR_PE_TOKENS are empty."
                    )

            except Exception as exc:

                st.error(
                    f"PCR subscription failed: {exc}"
                )

        # ---------------------------------
        # live PCR
        # ---------------------------------

        pcr = neo.calculate_pcr()

        p1, p2, p3, p4 = st.columns(4)

        with p1:

            st.metric(
                "PCR OI",
                (
                    round(
                        pcr["pcr_oi"],
                        3
                    )
                    if is_valid_number(
                        pcr["pcr_oi"]
                    )
                    else "-"
                )
            )

        with p2:

            st.metric(
                "PCR Volume",
                (
                    round(
                        pcr["pcr_volume"],
                        3
                    )
                    if is_valid_number(
                        pcr["pcr_volume"]
                    )
                    else "-"
                )
            )

        with p3:

            st.metric(
                "CE Contracts",
                pcr[
                    "ce_contracts_seen"
                ]
            )

        with p4:

            st.metric(
                "PE Contracts",
                pcr[
                    "pe_contracts_seen"
                ]
            )

        if st.button(
            "Build Latest 3-Min Bars"
        ):

            bars = (
                neo.build_3min_candles()
            )

            if not bars:

                st.info(
                    "No complete Spot + Future "
                    "3-minute bar available yet."
                )

            else:

                processed = 0

                for item in bars:

                    spot = item["spot"]
                    fut = item["future"]

                    option_data = (
                        neo.calculate_pcr()
                    )

                    candle = Candle3Min(

                        timestamp=
                            item[
                                "timestamp"
                            ],

                        spot_o=
                            spot["open"],

                        spot_h=
                            spot["high"],

                        spot_l=
                            spot["low"],

                        spot_c=
                            spot["close"],

                        fut_o=
                            fut["open"],

                        fut_h=
                            fut["high"],

                        fut_l=
                            fut["low"],

                        fut_c=
                            fut["close"],

                        fut_volume=
                            fut["volume"],

                        fut_oi=
                            fut["oi"],

                        option_chain=
                            option_data,
                    )

                    result = (
                        st.session_state.engine
                        .process_candle(
                            candle
                        )
                    )

                    if result is not None:

                        processed += 1

                st.success(
                    f"Processed {processed} "
                    "3-minute candle(s)."
                )

    # ---------------------------------
    # dataset
    # ---------------------------------

    df = (
        st.session_state.engine
        .dataframe()
    )

    if not df.empty:

        st.subheader(
            "Latest Features"
        )

        display_cols = [
            "timestamp",
            "basis",
            "fut_vwap",
            "normalized_stretch",
            "normalized_spread",
            "atr_14_prev",
            "spot_sma_20",
            "oi_strength",
            "twc",
            "breadth_10",
            "pcr_oi",
            "pcr_volume",
            "data_quality_score",
        ]

        display_cols = [
            c
            for c in display_cols
            if c in df.columns
        ]

        st.dataframe(
            df[
                display_cols
            ].tail(20),
            use_container_width=True
        )

        c1, c2, c3, c4 = st.columns(4)

        with c1:

            st.metric(
                "3-Min Bars",
                len(df)
            )

        with c2:

            st.metric(
                "Latest Future",
                round(
                    float(
                        st.session_state
                        .engine
                        .prev_candles[-1]
                        .fut_c
                    ),
                    2
                )
                if (
                    st.session_state
                    .engine
                    .prev_candles
                )
                else "-"
            )

        with c3:

            st.metric(
                "PCR OI",
                (
                    round(
                        float(
                            df.iloc[-1][
                                "pcr_oi"
                            ]
                        ),
                        3
                    )
                    if is_valid_number(
                        df.iloc[-1][
                            "pcr_oi"
                        ]
                    )
                    else "-"
                )
            )

        with c4:

            st.metric(
                "Data Quality",
                round(
                    float(
                        df.iloc[-1][
                            "data_quality_score"
                        ]
                    ),
                    3
                )
            )

        if st.button(
            "Save Dataset"
        ):

            try:

                st.session_state.engine.save()

                st.success(
                    "Dataset saved to "
                    "./nifty_3min_dataset"
                )

            except Exception as exc:

                st.error(
                    str(exc)
                )

    else:

        st.info(
            "No 3-minute candles yet. "
            "Connect → Subscribe Spot + Future → "
            "Subscribe PCR Options → "
            "wait for live ticks → "
            "Build Latest 3-Min Bars."
        )

    st.divider()

    st.caption(
        "Execution model: next_bar_open | "
        "ATR: session_local | "
        "Label horizon: 45m | "
        "Full Option Chain dependency: OFF | "
        "PCR feature: ON"
    )


# =========================================================
# CLI
# =========================================================

if __name__ == "__main__":

    if os.getenv(
        "RUN_TESTS",
        "0"
    ) == "1":

        run_unit_tests()

    elif st is not None:

        run_streamlit_app()

    else:

        print(
            "Streamlit not installed."
        )

        print(
            "Run: streamlit run app.py"
        )
