#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine
Kotak Neo Integrated Research-Lock v1.4

PCR-PRESERVED / OPTION-CHAIN-API-INDEPENDENT VERSION

CORE LOGIC:
- session-local ATR
- VWAP
- SMA20
- Futures OI classification
- heavyweight contribution
- opening range
- PCR / option OI feature interface
- local PCR calculation from CE/PE quotes
- triple barrier
- MFE / MAE
- next_bar_open research convention
- date-aware walk-forward
- parquet dataset

DATA:
- Kotak Neo live WebSocket
- Kotak Neo quotes
- 3-minute candle aggregation

IMPORTANT:
Kotak Neo ka dedicated Option Chain endpoint available
na hone ki situation mein yah code koi fake endpoint call
NAHI karta.

PCR:
PCR = Total PE OI / Total CE OI

Option data ke liye KOTAK_OPTION_TOKENS_JSON configure karo.

Example:

export KOTAK_OPTION_TOKENS_JSON='[
  {"token":"123456","exchange_segment":"nse_fo","type":"CE","strike":25000},
  {"token":"123457","exchange_segment":"nse_fo","type":"PE","strike":25000}
]'

Production mein apne actual current NIFTY option tokens
use karna zaroori hai.

Install:

pip install -U \
"git+https://github.com/Kotak-Neo/Kotak-neo-api-v2.git@v2.0.2#egg=neo_api_client" \
pandas numpy pyarrow streamlit

Run:

streamlit run app.py

Environment:

KOTAK_CONSUMER_KEY
KOTAK_MOBILE
KOTAK_UCC
KOTAK_TOTP
KOTAK_MPIN

Optional:

NIFTY_FUT_TOKEN
NIFTY_SPOT_TOKEN
NIFTY_VIX_TOKEN
KOTAK_OPTION_TOKENS_JSON
"""

from __future__ import annotations

import os
import json
import time
import threading
import warnings

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# =========================================================
# OPTIONAL IMPORTS
# =========================================================

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

    "app_version":
        "v1.4_kotak_neo_pcr_preserved",

    "feature_version":
        "v1.7_research_lock",

    "label_version":
        "TB_v1.6_lock",

    "schema_version":
        "1.4",

    "weight_version":
        "NIFTY_STATIC_2025Q1",

    # Indicators
    "atr_period":
        14,

    "sma_period":
        20,

    # Triple barrier
    "triple_upper_atr":
        1.0,

    "triple_lower_atr":
        0.75,

    "time_barrier_min":
        30,

    # MFE / MAE
    "mfe_horizons_min":
        [15, 30, 45],

    "max_label_horizon_min":
        45,

    # Walk forward
    "purge_bars":
        18,

    "embargo_bars":
        5,

    # Opening range
    "opening_range_minutes":
        15,

    # Research conventions
    "atr_mode":
        "session_local",

    "execution_model":
        "next_bar_open",

    # NSE
    "session_start":
        "09:15",

    "session_end":
        "15:30",

    # Candle
    "bar_minutes":
        3,

    # Dataset
    "dataset_path":
        "./nifty_3min_dataset",

    # Neo
    "neo_environment":
        "prod",

    # Instrument names
    "nifty_index_name":
        "Nifty 50",

    # Tokens
    "nifty_spot_token":
        os.getenv(
            "NIFTY_SPOT_TOKEN",
            "26000"
        ),

    "nifty_vix_token":
        os.getenv(
            "NIFTY_VIX_TOKEN",
            "26001"
        ),

    "nifty_future_token":
        os.getenv(
            "NIFTY_FUT_TOKEN",
            ""
        ),

    # Poll interval
    "quote_poll_seconds":
        5,
}


# =========================================================
# HEAVYWEIGHTS
# =========================================================

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

def safe_float(
    value,
    default=np.nan
):

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
            and np.isfinite(
                float(value)
            )
        )

    except Exception:

        return False


def json_load_env(
    name,
    default
):

    raw = os.getenv(
        name,
        ""
    ).strip()

    if not raw:
        return default

    try:

        return json.loads(raw)

    except Exception as exc:

        warnings.warn(
            f"{name} invalid JSON: {exc}"
        )

        return default


# =========================================================
# WILDER ATR
# =========================================================

def wilder_atr(
    trs: List[float],
    period: int = 14
) -> float:

    if len(trs) < period:
        return np.nan

    atr = np.mean(
        trs[:period]
    )

    for tr in trs[period:]:

        atr = (
            (
                atr * (
                    period - 1
                )
            )
            + tr
        ) / period

    return float(atr)


# =========================================================
# 3-MIN BUCKET
# =========================================================

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
        (
            ts
            - session_anchor
        ).total_seconds()
        // 60
    )

    bucket = (
        elapsed // minutes
    ) * minutes

    return (
        session_anchor
        + timedelta(
            minutes=bucket
        )
    )


# =========================================================
# CANDLE
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

    heavy: Dict[
        str,
        Dict[str, float]
    ] = field(
        default_factory=dict
    )

    option_chain: Dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )


# =========================================================
# OPENING RANGE
# =========================================================

class OpeningRangeEngine:

    def __init__(
        self,
        minutes=15
    ):

        self.minutes = minutes

        self.or_high = None
        self.or_low = None

        self.or_set = False

    def reset(self):

        self.or_high = None
        self.or_low = None

        self.or_set = False

    def update(
        self,
        candle
    ):

        mins = (
            candle.timestamp.hour
            * 60
            + candle.timestamp.minute
        ) - (
            9 * 60 + 15
        )

        if mins < self.minutes:

            if self.or_high is None:

                self.or_high = (
                    candle.fut_h
                )

                self.or_low = (
                    candle.fut_l
                )

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

    def features(
        self,
        candle,
        atr
    ):

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
            or not is_valid_number(
                atr
            )
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
                if candle.fut_c
                > self.or_high
                else (
                    -1
                    if candle.fut_c
                    < self.or_low
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

    def set_today_open(
        self,
        open_price
    ):

        self.today_open = (
            open_price
        )

    def reset(self):

        self.today_open = None

    def features(
        self,
        candle,
        atr
    ):

        names = [

            "gap_points",

            "gap_atr",

            "gap_direction",

            "dist_to_pdh_atr",

            "dist_to_pdl_atr",
        ]

        if (
            self.prev_close is None
            or not is_valid_number(
                atr
            )
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
                1
                if gap > 0
                else (
                    -1
                    if gap < 0
                    else 0
                ),

            "dist_to_pdh_atr":
                (
                    candle.fut_c
                    - self.prev_high
                ) / atr
                if self.prev_high
                is not None
                else np.nan,

            "dist_to_pdl_atr":
                (
                    candle.fut_c
                    - self.prev_low
                ) / atr
                if self.prev_low
                is not None
                else np.nan,
        }


# =========================================================
# PCR ENGINE
# =========================================================

class OptionChainEngine:

    """
    PCR engine.

    IMPORTANT:
    No dedicated Option Chain API is called.

    PCR is calculated locally:

        PCR_OI =
            Total PE OI /
            Total CE OI

    The input may come from:
        - Neo quotes
        - WebSocket quote snapshots
        - any supported quote source

    Expected normalized option record:

    {
        "token": "...",
        "type": "CE" / "PE",
        "strike": 25000,
        "oi": 123456,
        "volume": 100000,
        "ltp": 100.0,
        "iv": 15.2
    }
    """

    def compute(
        self,
        chain
    ):

        if not chain:

            return self._missing()

        rows = []

        # -------------------------------------------------
        # Already-normalized list
        # -------------------------------------------------

        if isinstance(
            chain,
            list
        ):

            rows = chain

        # -------------------------------------------------
        # Dict wrapper
        # -------------------------------------------------

        elif isinstance(
            chain,
            dict
        ):

            if isinstance(
                chain.get("rows"),
                list
            ):

                rows = chain[
                    "rows"
                ]

            elif isinstance(
                chain.get("options"),
                list
            ):

                rows = chain[
                    "options"
                ]

        if not rows:

            return self._missing()

        ce_oi = 0.0
        pe_oi = 0.0

        ce_vol = 0.0
        pe_vol = 0.0

        valid_ce = 0
        valid_pe = 0

        strikes = []

        atm_iv = np.nan

        # ---------------------------------------------
        # First pass
        # ---------------------------------------------

        for row in rows:

            if not isinstance(
                row,
                dict
            ):
                continue

            typ = str(
                row.get(
                    "type",
                    row.get(
                        "option_type",
                        ""
                    )
                )
            ).upper()

            oi = safe_float(
                row.get(
                    "oi",
                    row.get(
                        "open_interest",
                        np.nan
                    )
                )
            )

            volume = safe_float(
                row.get(
                    "volume",
                    row.get(
                        "v",
                        np.nan
                    )
                )
            )

            strike = safe_float(
                row.get(
                    "strike",
                    np.nan
                )
            )

            iv = safe_float(
                row.get(
                    "iv",
                    row.get(
                        "implied_volatility",
                        np.nan
                    )
                )
            )

            if is_valid_number(
                strike
            ):

                strikes.append(
                    strike
                )

            if typ == "CE":

                valid_ce += 1

                if is_valid_number(
                    oi
                ):

                    ce_oi += oi

                if is_valid_number(
                    volume
                ):

                    ce_vol += volume

            elif typ == "PE":

                valid_pe += 1

                if is_valid_number(
                    oi
                ):

                    pe_oi += oi

                if is_valid_number(
                    volume
                ):

                    pe_vol += volume

        # ---------------------------------------------
        # PCR
        # ---------------------------------------------

        pcr_oi = (
            pe_oi / ce_oi
            if ce_oi > 0
            else np.nan
        )

        pcr_volume = (
            pe_vol / ce_vol
            if ce_vol > 0
            else np.nan
        )

        # ---------------------------------------------
        # ATM
        # ---------------------------------------------

        if strikes:

            atm_strike = (
                min(strikes)
            )

        else:

            atm_strike = np.nan

        # ---------------------------------------------
        # ATM IV
        # ---------------------------------------------

        if is_valid_number(
            atm_strike
        ):

            candidates = []

            for row in rows:

                if not isinstance(
                    row,
                    dict
                ):
                    continue

                strike = safe_float(
                    row.get(
                        "strike"
                    )
                )

                iv = safe_float(
                    row.get(
                        "iv",
                        row.get(
                            "implied_volatility",
                            np.nan
                        )
                    )
                )

                if (
                    is_valid_number(
                        strike
                    )
                    and is_valid_number(
                        iv
                    )
                ):

                    candidates.append(
                        (
                            abs(
                                strike
                                - atm_strike
                            ),
                            iv
                        )
                    )

            if candidates:

                candidates.sort(
                    key=lambda x:
                        x[0]
                )

                atm_iv = (
                    candidates[0][1]
                )

        return {

            "pcr_oi":
                pcr_oi,

            "pcr_volume":
                pcr_volume,

            "ce_oi_change":
                np.nan,

            "pe_oi_change":
                np.nan,

            "atm_iv":
                atm_iv,

            "iv_change":
                np.nan,

            "ce_oi_atm":
                np.nan,

            "pe_oi_atm":
                np.nan,

            "atm_strike":
                atm_strike,

            "pcr_ce_oi_total":
                ce_oi,

            "pcr_pe_oi_total":
                pe_oi,

            "pcr_ce_volume_total":
                ce_vol,

            "pcr_pe_volume_total":
                pe_vol,

            "option_contract_count":
                valid_ce
                + valid_pe,

            "pcr_oi_missing":
                int(
                    not is_valid_number(
                        pcr_oi
                    )
                ),

            "pcr_volume_missing":
                int(
                    not is_valid_number(
                        pcr_volume
                    )
                ),

            "ce_oi_change_missing":
                1,

            "pe_oi_change_missing":
                1,

            "atm_iv_missing":
                int(
                    not is_valid_number(
                        atm_iv
                    )
                ),

            "iv_change_missing":
                1,

            "ce_oi_atm_missing":
                1,

            "pe_oi_atm_missing":
                1,

            "atm_strike_missing":
                int(
                    not is_valid_number(
                        atm_strike
                    )
                ),
        }

    def _missing(self):

        return {

            "pcr_oi":
                np.nan,

            "pcr_volume":
                np.nan,

            "ce_oi_change":
                np.nan,

            "pe_oi_change":
                np.nan,

            "atm_iv":
                np.nan,

            "iv_change":
                np.nan,

            "ce_oi_atm":
                np.nan,

            "pe_oi_atm":
                np.nan,

            "atm_strike":
                np.nan,

            "pcr_ce_oi_total":
                np.nan,

            "pcr_pe_oi_total":
                np.nan,

            "pcr_ce_volume_total":
                np.nan,

            "pcr_pe_volume_total":
                np.nan,

            "option_contract_count":
                0,

            "pcr_oi_missing":
                1,

            "pcr_volume_missing":
                1,

            "ce_oi_change_missing":
                1,

            "pe_oi_change_missing":
                1,

            "atm_iv_missing":
                1,

            "iv_change_missing":
                1,

            "ce_oi_atm_missing":
                1,

            "pe_oi_atm_missing":
                1,

            "atm_strike_missing":
                1,
        }


# =========================================================
# HEAVYWEIGHT ENGINE
# =========================================================

class HeavyweightEngine:

    def __init__(
        self,
        weights
    ):

        self.base_weights = weights

        self.day_open = {}

    def set_day_open(
        self,
        symbol,
        price
    ):

        if (
            is_valid_number(price)
            and price > 0
        ):

            self.day_open[
                symbol
            ] = price

    def reset_day(self):

        self.day_open.clear()

    def compute(
        self,
        candle
    ):

        ics = []
        rets = []

        bullish = 0

        for sym, w in (
            self.base_weights.items()
        ):

            if sym not in candle.heavy:
                continue

            d = candle.heavy[
                sym
            ]

            open_p = (
                self.day_open.get(
                    sym
                )
            )

            if open_p is None:

                open_p = d.get(
                    "o",
                    d.get(
                        "c",
                        np.nan
                    )
                )

                if (
                    is_valid_number(
                        open_p
                    )
                    and open_p > 0
                ):

                    self.day_open[
                        sym
                    ] = open_p

            if (
                not is_valid_number(
                    open_p
                )
                or open_p <= 0
            ):

                continue

            close_p = safe_float(
                d.get("c")
            )

            if not is_valid_number(
                close_p
            ):

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
                w
                * ret
                * rel_vol
            )

            rets.append(
                ret
            )

            vwap = safe_float(
                d.get(
                    "vwap",
                    close_p
                ),
                close_p
            )

            if close_p >= vwap:

                bullish += 1

        twc = (
            sum(ics)
            if ics
            else 0.0
        )

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

        self.sess = (
            SessionContextEngine()
        )

        self.opt = (
            OptionChainEngine()
        )

    def reset_session(self):

        self.vwap_pv = 0.0
        self.vwap_vol = 0.0

        self.tr_history.clear()
        self.history.clear()

        self.hw.reset_day()
        self.or_eng.reset()
        self.sess.reset()

    def set_previous_day(
        self,
        c,
        h,
        l
    ):

        self.sess.set_previous_day(
            c,
            h,
            l
        )

    def set_today_open(
        self,
        o
    ):

        self.sess.set_today_open(
            o
        )

    def compute(
        self,
        candle,
        prev
    ):

        # ---------------------------------------------
        # VWAP
        # ---------------------------------------------

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

        # ---------------------------------------------
        # TR / ATR
        # ---------------------------------------------

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

            CONFIG[
                "atr_period"
            ]
        )

        self.tr_history.append(
            tr
        )

        atr_close = wilder_atr(

            self.tr_history,

            CONFIG[
                "atr_period"
            ]
        )

        # Research lock:
        # current bar features use
        # ATR before current TR.

        atr = atr_prev

        atr_warmup = int(
            np.isnan(atr)
        )

        # ---------------------------------------------
        # SMA20
        # ---------------------------------------------

        closes = (

            [
                c.spot_c
                for c in prev[
                    -(
                        CONFIG[
                            "sma_period"
                        ] - 1
                    ):
                ]
            ]

            + [
                candle.spot_c
            ]
        )

        sma_ready = int(

            len(closes)
            >= CONFIG[
                "sma_period"
            ]
        )

        spot_sma = (

            float(
                np.mean(closes)
            )

            if sma_ready

            else np.nan
        )

        # ---------------------------------------------
        # Normalized features
        # ---------------------------------------------

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

        # ---------------------------------------------
        # Slopes
        # ---------------------------------------------

        if (

            self.history

            and not np.isnan(
                norm_stretch
            )

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

            and not np.isnan(
                norm_spread
            )

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

        # ---------------------------------------------
        # Futures OI
        # ---------------------------------------------

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

                * np.sign(
                    oi_chg
                )

                * np.log1p(
                    abs(oi_chg)
                )
            )

        # ---------------------------------------------
        # Opening Range
        # ---------------------------------------------

        self.or_eng.update(
            candle
        )

        # ---------------------------------------------
        # Data quality
        # ---------------------------------------------

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

            len(candle.heavy)
            == 0
        )

        missing_option = int(

            len(
                candle.option_chain
            )
            == 0
        )

        bad_ohlc = int(

            candle.fut_h
            < candle.fut_l

            or candle.spot_h
            < candle.spot_l
        )

        zero_volume = int(

            candle.fut_volume
            == 0
        )

        zero_oi = int(

            candle.fut_oi
            == 0
        )

        # PCR features first
        option_features = (
            self.opt.compute(
                candle.option_chain
            )
        )

        # ---------------------------------------------
        # Quality score
        # ---------------------------------------------

        quality_flags = [

            missing_spot,

            missing_fut,

            missing_oi,

            missing_volume,

            missing_heavy,

            missing_option,

            bad_ohlc,

            zero_volume,

            zero_oi,
        ]

        data_quality_score = (

            1.0
            - 0.1
            * sum(
                quality_flags
            )
        )

        # ---------------------------------------------
        # Basis
        # ---------------------------------------------

        basis = (

            candle.fut_c
            - candle.spot_c
        )

        # ---------------------------------------------
        # FINAL FEATURES
        # ---------------------------------------------

        feats = {

            "timestamp":
                candle.timestamp,

            "feature_version":
                CONFIG[
                    "feature_version"
                ],

            "schema_version":
                CONFIG[
                    "schema_version"
                ],

            "weight_version":
                CONFIG[
                    "weight_version"
                ],

            "atr_mode":
                CONFIG[
                    "atr_mode"
                ],

            "execution_model":
                CONFIG[
                    "execution_model"
                ],

            # -----------------------------------------
            # Price / VWAP
            # -----------------------------------------

            "spot_close":
                candle.spot_c,

            "future_close":
                candle.fut_c,

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

            # -----------------------------------------
            # ATR
            # -----------------------------------------

            "atr_14_prev":
                atr_prev,

            "atr_14_close":
                atr_close,

            "atr_warmup_flag":
                atr_warmup,

            # -----------------------------------------
            # SMA
            # -----------------------------------------

            "spot_sma_20":
                spot_sma,

            "sma20_warmup_flag":
                1 - sma_ready,

            # -----------------------------------------
            # OI
            # -----------------------------------------

            "oi_change":
                oi_chg,

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

            # -----------------------------------------
            # Time
            # -----------------------------------------

            "minutes_from_open":
                (
                    candle.timestamp.hour
                    * 60
                    + candle.timestamp.minute
                )
                - (
                    9 * 60 + 15
                ),

            "day_of_week":
                candle.timestamp.weekday(),

            # -----------------------------------------
            # Heavyweights
            # -----------------------------------------

            **self.hw.compute(
                candle
            ),

            # -----------------------------------------
            # Opening Range
            # -----------------------------------------

            **self.or_eng.features(

                candle,

                atr
                if is_valid_number(
                    atr
                )
                else 0.0
            ),

            # -----------------------------------------
            # Session
            # -----------------------------------------

            **self.sess.features(

                candle,

                atr
                if is_valid_number(
                    atr
                )
                else 0.0
            ),

            # -----------------------------------------
            # PCR / OPTIONS
            # -----------------------------------------

            **option_features,

            # -----------------------------------------
            # Data quality
            # -----------------------------------------

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

        self.history.append(
            feats
        )

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

        for c in future[
            :available
        ]:

            if direction == 1:

                mfe = max(

                    mfe,

                    c.fut_h
                    - entry
                )

                mae = max(

                    mae,

                    entry
                    - c.fut_l
                )

            else:

                mfe = max(

                    mfe,

                    entry
                    - c.fut_l
                )

                mae = max(

                    mae,

                    c.fut_h
                    - entry
                )

        complete = int(

            available
            >= max_bars
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

        # Alignment lock
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
                    "VIOLATION"
                )

        if (
            atr is None
            or not is_valid_number(
                atr
            )
            or atr <= 0
        ):

            atr = np.nan

        upper = (

            entry_price
            + direction
            * self.upper
            * atr

            if not np.isnan(
                atr
            )

            else np.nan
        )

        lower = (

            entry_price
            - direction
            * self.lower
            * atr

            if not np.isnan(
                atr
            )

            else np.nan
        )

        outcome = "TIMEOUT"

        bars = 0

        mfe_tb = 0.0
        mae_tb = 0.0

        time_to_mfe = 0

        max_tb_bars = (

            self.tb_horizon
            // CONFIG[
                "bar_minutes"
            ]
        )

        for i, c in enumerate(

            future_after_entry[
                :max_tb_bars
            ]
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

                    not np.isnan(
                        upper
                    )

                    and c.fut_h
                    >= upper
                )

                hit_s = (

                    not np.isnan(
                        lower
                    )

                    and c.fut_l
                    <= lower
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

                    not np.isnan(
                        upper
                    )

                    and c.fut_l
                    <= upper
                )

                hit_s = (

                    not np.isnan(
                        lower
                    )

                    and c.fut_h
                    >= lower
                )

            if (
                mfe_tb > 0
                and time_to_mfe == 0
            ):

                time_to_mfe = bars

            if hit_t and hit_s:

                outcome = (
                    "AMBIGUOUS"
                )

                break

            if hit_t:

                outcome = (
                    "TARGET_FIRST"
                )

                break

            if hit_s:

                outcome = (
                    "STOP_FIRST"
                )

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

            outcome
            != "AMBIGUOUS"

            and not np.isnan(
                atr
            )
        )

        mfe_atr = (

            mfe_tb / atr

            if (
                not np.isnan(atr)
                and atr > 0
            )

            else np.nan
        )

        mae_atr = (

            mae_tb / atr

            if (
                not np.isnan(atr)
                and atr > 0
            )

            else np.nan
        )

        velocity = (

            mfe_atr
            / max(
                bars,
                1
            )

            if not np.isnan(
                mfe_atr
            )

            else np.nan
        )

        if not np.isnan(
            mfe_atr
        ):

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

            outcome
            == "TARGET_FIRST"

            and not np.isnan(
                mfe_atr
            )

            and mfe_atr >= 1.0

            and mae_atr <= 0.55
        )

        labels = {

            "label_version":
                CONFIG[
                    "label_version"
                ],

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

        for h in (
            self.mfe_horizons
        ):

            max_bars = (

                h
                // CONFIG[
                    "bar_minutes"
                ]
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

                if (
                    not np.isnan(atr)
                    and atr > 0
                )

                else np.nan
            )

            labels[
                f"mae_atr_{h}m"
            ] = (

                mae_h / atr

                if (
                    not np.isnan(atr)
                    and atr > 0
                )

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

    def __init__(
        self,
        path=None
    ):

        self.base = Path(

            path
            or CONFIG[
                "dataset_path"
            ]
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

        table = (
            pa.Table.from_pandas(
                df,
                preserve_index=False
            )
        )

        pq.write_to_dataset(

            table,

            root_path=str(
                self.base / name
            ),

            partition_cols=(

                ["date"]

                if "date"
                in df.columns

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
            )
            .dt.date
        )

        unique_dates = sorted(
            df["date"].unique()
        )

        n_dates = len(
            unique_dates
        )

        fold = max(

            1,

            n_dates
            // (
                n_splits + 1
            )
        )

        purge_days = 1

        splits = []

        for i in range(
            n_splits
        ):

            train_end = (
                (i + 1)
                * fold
            )

            test_start = (
                train_end
                + purge_days
            )

            test_end = min(

                test_start + fold,

                n_dates
            )

            if (
                test_start
                >= n_dates
            ):

                break

            train_dates = (
                unique_dates[
                    :train_end
                ]
            )

            test_dates = (
                unique_dates[
                    test_start:
                    test_end
                ]
            )

            train_idx = (

                df[
                    df["date"]
                    .isin(
                        train_dates
                    )
                ]

                .index
                .tolist()
            )

            test_idx = (

                df[
                    df["date"]
                    .isin(
                        test_dates
                    )
                ]

                .index
                .tolist()
            )

            if (
                train_idx

                and CONFIG[
                    "embargo_bars"
                ] > 0
            ):

                train_idx = (

                    train_idx[
                        :-
                        CONFIG[
                            "embargo_bars"
                        ]
                    ]
                )

            splits.append(
                (
                    train_idx,
                    test_idx
                )
            )

        return splits


# =========================================================
# KOTAK NEO ADAPTER
# =========================================================

class KotakNeoAdapter:

    """
    Kotak Neo live adapter.

    PCR is NOT requested through a fictional
    option-chain endpoint.

    Instead option contracts can be supplied
    through KOTAK_OPTION_TOKENS_JSON and their
    live OI is read from supported quote/feed
    responses.
    """

    def __init__(self):

        if NeoAPI is None:

            raise ImportError(

                "neo_api_client missing.\n"

                "Install:\n"

                'pip install -U '
                '"git+https://github.com/'
                'Kotak-Neo/'
                'Kotak-neo-api-v2.git@v2.0.2'
                '#egg=neo_api_client"'
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

            environment=
                CONFIG[
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

        self.lock = (
            threading.Lock()
        )

        self.latest = {}

        self.tick_buffer = []

        # Normalized option universe
        self.option_specs = (
            json_load_env(
                "KOTAK_OPTION_TOKENS_JSON",
                []
            )
        )

        self.option_latest = {}

        self.option_history = []

    # -----------------------------------------------------
    # LOGIN
    # -----------------------------------------------------

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

            for k, v
            in required.items()

            if not v
        ]

        if missing:

            raise RuntimeError(

                "Missing environment "
                "variables: "

                + ", ".join(
                    missing
                )
            )

        self.client.totp_login(

            mobilenumber=
                self.mobile,

            ucc=
                self.ucc,

            totp=
                self.totp,
        )

        self.client.totp_validate(

            mpin=
                self.mpin
        )

        self.connected = True

        return True

    # -----------------------------------------------------
    # CALLBACKS
    # -----------------------------------------------------

    def on_open(
        self,
        message
    ):

        print(
            "[Kotak Neo] WebSocket opened:",
            message
        )

    def on_error(
        self,
        message
    ):

        print(
            "[Kotak Neo] ERROR:",
            message
        )

    def on_close(
        self,
        message
    ):

        self.connected = False

        print(
            "[Kotak Neo] WebSocket closed:",
            message
        )

    def on_message(
        self,
        message
    ):

        try:

            data = (
                self._decode_message(
                    message
                )
            )

            if data is None:
                return

            self._process_tick(
                data
            )

        except Exception as exc:

            print(
                "[Kotak Neo] message parse error:",
                repr(exc)
            )

    # -----------------------------------------------------
    # MESSAGE DECODER
    # -----------------------------------------------------

    def _decode_message(
        self,
        message
    ):

        if isinstance(
            message,
            dict
        ):

            return message

        if isinstance(
            message,
            str
        ):

            try:

                return json.loads(
                    message
                )

            except Exception:

                return None

        return None

    # -----------------------------------------------------
    # TIMESTAMP
    # -----------------------------------------------------

    def _tick_time(
        self,
        data
    ):

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

    # -----------------------------------------------------
    # TICK PROCESSOR
    # -----------------------------------------------------

    def _process_tick(
        self,
        data
    ):

        # Wrapper handling
        if isinstance(
            data.get("data"),
            dict
        ):

            data = data[
                "data"
            ]

        if not isinstance(
            data,
            dict
        ):

            return

        token = str(

            data.get(
                "tk",
                ""
            )
        )

        exchange = str(

            data.get(
                "e",
                ""
            )
        )

        ltp = safe_float(

            data.get(
                "ltp"
            )
        )

        if not is_valid_number(
            ltp
        ):

            return

        timestamp = (
            self._tick_time(
                data
            )
        )

        item = {

            "timestamp":
                timestamp,

            "token":
                token,

            "exchange":
                exchange,

            "symbol":
                data.get(
                    "ts",
                    ""
                ),

            "ltp":
                ltp,

            "volume":
                safe_float(
                    data.get(
                        "v"
                    ),
                    0.0
                ),

            "oi":
                safe_float(
                    data.get(
                        "oi"
                    ),
                    0.0
                ),

            "open":
                safe_float(
                    data.get(
                        "op"
                    )
                ),

            "high":
                safe_float(
                    data.get(
                        "h"
                    )
                ),

            "low":
                safe_float(
                    data.get(
                        "lo"
                    )
                ),

            "vwap":
                safe_float(
                    data.get(
                        "ap"
                    )
                ),
        }

        with self.lock:

            self.latest[
                token
            ] = item

            self.tick_buffer.append(
                item
            )

        # Option snapshot
        self._update_option_snapshot(
            token,
            item
        )

    # -----------------------------------------------------
    # OPTION SNAPSHOT
    # -----------------------------------------------------

    def _update_option_snapshot(
        self,
        token,
        item
    ):

        for spec in (
            self.option_specs
        ):

            if str(
                spec.get(
                    "token",
                    ""
                )
            ) != str(token):

                continue

            row = {

                "token":
                    token,

                "type":
                    str(
                        spec.get(
                            "type",
                            ""
                        )
                    ).upper(),

                "strike":
                    safe_float(
                        spec.get(
                            "strike"
                        )
                    ),

                "expiry":
                    spec.get(
                        "expiry"
                    ),

                "oi":
                    item.get(
                        "oi",
                        np.nan
                    ),

                "volume":
                    item.get(
                        "volume",
                        np.nan
                    ),

                "ltp":
                    item.get(
                        "ltp",
                        np.nan
                    ),

                "iv":
                    safe_float(
                        item.get(
                            "iv"
                        )
                    ),

                "timestamp":
                    item.get(
                        "timestamp"
                    ),
            }

            self.option_latest[
                token
            ] = row

            break

    # -----------------------------------------------------
    # SUBSCRIBE
    # -----------------------------------------------------

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

            isIndex=
                is_index,

            isDepth=
                is_depth,
        )

    # -----------------------------------------------------
    # SUBSCRIBE EVERYTHING
    # -----------------------------------------------------

    def subscribe_core_and_options(
        self
    ):

        instruments = []

        # Spot
        if CONFIG[
            "nifty_spot_token"
        ]:

            instruments.append({

                "instrument_token":
                    CONFIG[
                        "nifty_spot_token"
                    ],

                "exchange_segment":
                    "nse_cm",
            })

        # Future
        if CONFIG[
            "nifty_future_token"
        ]:

            instruments.append({

                "instrument_token":
                    CONFIG[
                        "nifty_future_token"
                    ],

                "exchange_segment":
                    "nse_fo",
            })

        # VIX
        if CONFIG[
            "nifty_vix_token"
        ]:

            instruments.append({

                "instrument_token":
                    CONFIG[
                        "nifty_vix_token"
                    ],

                "exchange_segment":
                    "nse_cm",
            })

        # Options
        for spec in (
            self.option_specs
        ):

            token = str(

                spec.get(
                    "token",
                    ""
                )
            )

            segment = spec.get(

                "exchange_segment",
                "nse_fo"
            )

            if token:

                instruments.append({

                    "instrument_token":
                        token,

                    "exchange_segment":
                        segment,
                })

        if not instruments:

            raise RuntimeError(
                "No instruments configured."
            )

        return self.subscribe(

            instruments,

            is_index=False,

            is_depth=False
        )

    # -----------------------------------------------------
    # BUILD 3-MIN CANDLES
    # -----------------------------------------------------

    def build_3min_candles(
        self
    ):

        with self.lock:

            ticks = list(
                self.tick_buffer
            )

            self.tick_buffer.clear()

        if not ticks:

            return []

        buckets = {}

        for tick in ticks:

            ts = (
                floor_bar_timestamp(

                    tick[
                        "timestamp"
                    ],

                    CONFIG[
                        "bar_minutes"
                    ]
                )
            )

            if ts is None:
                continue

            token = tick[
                "token"
            ]

            key = (
                token,
                ts
            )

            buckets.setdefault(
                key,
                []
            ).append(
                tick
            )

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
                    x[
                        "timestamp"
                    ]
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

            candle = {

                "token":
                    token,

                "timestamp":
                    ts,

                "open":
                    prices[0],

                "high":
                    max(
                        prices
                    ),

                "low":
                    min(
                        prices
                    ),

                "close":
                    prices[-1],

                "volume":
                    max(
                        volumes
                    )
                    if volumes
                    else 0.0,

                "oi":
                    ois[-1]
                    if ois
                    else 0.0,
            }

            output.append(
                candle
            )

        return output

    # -----------------------------------------------------
    # CURRENT PCR SNAPSHOT
    # -----------------------------------------------------

    def get_option_chain_snapshot(
        self
    ):

        rows = list(
            self.option_latest.values()
        )

        if not rows:

            return {}

        return {
            "rows":
                rows
        }


# =========================================================
# NIFTY ENGINE
# =========================================================

class NiftyMicroEngine:

    def __init__(self):

        self.features = (
            FeatureEngine()
        )

        self.labels = (
            LabelEngine()
        )

        self.dataset = (
            DatasetManager()
        )

        self.prev_candles = []

        self.feature_rows = []

        self.signal_rows = []

        self.current_date = None

        self.last_bar_timestamp = None

    def reset_if_new_day(
        self,
        timestamp
    ):

        current_date = (
            timestamp.date()
        )

        if (
            self.current_date
            != current_date
        ):

            self.features.reset_session()

            self.prev_candles.clear()

            self.current_date = (
                current_date
            )

            self.last_bar_timestamp = (
                None
            )

    def process_candle(
        self,
        candle
    ):

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

        feat = (
            self.features.compute(

                candle,

                self.prev_candles
            )
        )

        self.prev_candles.append(
            candle
        )

        if len(
            self.prev_candles
        ) > 500:

            self.prev_candles = (

                self.prev_candles[
                    -500:
                ]
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
        "KOTAK NEO PCR RESEARCH LOCK "
        "UNIT TESTS ====="
    )

    # ATR
    assert np.isnan(

        wilder_atr(
            [10, 12],
            14
        )
    )

    print(
        "✓ ATR warm-up"
    )

    # PCR
    option_engine = (
        OptionChainEngine()
    )

    pcr = option_engine.compute({

        "rows": [

            {
                "token":
                    "1",

                "type":
                    "CE",

                "strike":
                    25000,

                "oi":
                    1000,

                "volume":
                    100,
            },

            {
                "token":
                    "2",

                "type":
                    "PE",

                "strike":
                    25000,

                "oi":
                    1500,

                "volume":
                    200,
            },
        ]
    })

    assert (
        pcr["pcr_oi"]
        == 1.5
    )

    assert (
        pcr["pcr_volume"]
        == 2.0
    )

    print(
        "✓ PCR calculation"
    )

    # Candle
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

        option_chain={

            "rows": [

                {
                    "type":
                        "CE",

                    "strike":
                        24000,

                    "oi":
                        1000,
                },

                {
                    "type":
                        "PE",

                    "strike":
                        24000,

                    "oi":
                        1200,
                },
            ]
        }
    )

    engine = (
        NiftyMicroEngine()
    )

    f = (
        engine.process_candle(
            candle
        )
    )

    assert f is not None

    assert (
        f[
            "execution_model"
        ]
        == "next_bar_open"
    )

    assert (
        "pcr_oi"
        in f
    )

    assert (
        f["pcr_oi"]
        == 1.2
    )

    assert (
        "data_quality_score"
        in f
    )

    print(
        "✓ FeatureEngine + PCR"
    )

    # Label
    label = (
        LabelEngine()
    )

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

        entry_price=
            24040,

        atr=
            20.0,

        future_after_entry=
            future,

        direction=
            1,

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

    # Alignment violation
    try:

        label.generate(

            entry_price=
                24040,

            atr=
                20.0,

            future_after_entry=[

                Candle3Min(

                    datetime(
                        2025,
                        1,
                        2,
                        9,
                        21
                    ),

                    0,
                    0,
                    0,
                    0,

                    0,
                    0,
                    0,
                    0,

                    0,
                    0
                )
            ],

            direction=
                1,

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

        layout=
            "wide"
    )

    st.title(
        "NIFTY 3-Min Micro Engine"
    )

    st.caption(
        "Kotak Neo • "
        "Research-Lock v1.4 • "
        "PCR Preserved"
    )

    st.info(

        "Dedicated Option Chain endpoint "
        "use nahi kiya ja raha. PCR ko "
        "CE/PE option quote-OI data se "
        "locally calculate kiya ja sakta hai."
    )

    # -----------------------------------------------------
    # SIDEBAR
    # -----------------------------------------------------

    st.sidebar.header(
        "Kotak Neo Credentials"
    )

    checks = {

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

    for name, ok in (
        checks.items()
    ):

        st.sidebar.write(

            name,

            "✓"
            if ok
            else "✗"
        )

    option_specs = (
        json_load_env(
            "KOTAK_OPTION_TOKENS_JSON",
            []
        )
    )

    st.sidebar.write(
        "Configured option contracts:",
        len(option_specs)
    )

    # -----------------------------------------------------
    # SESSION STATE
    # -----------------------------------------------------

    if (
        "neo"
        not in st.session_state
    ):

        st.session_state.neo = None

    if (
        "engine"
        not in st.session_state
    ):

        st.session_state.engine = (
            NiftyMicroEngine()
        )

    # -----------------------------------------------------
    # BUTTONS
    # -----------------------------------------------------

    col1, col2 = st.columns(
        2
    )

    with col1:

        if st.button(
            "Connect Kotak Neo"
        ):

            try:

                neo = (
                    KotakNeoAdapter()
                )

                neo.login()

                st.session_state.neo = (
                    neo
                )

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

    # -----------------------------------------------------
    # LIVE FEED
    # -----------------------------------------------------

    neo = (
        st.session_state.neo
    )

    if neo is not None:

        st.subheader(
            "Live Feed"
        )

        if st.button(
            "Subscribe NIFTY + Options"
        ):

            try:

                neo.subscribe_core_and_options()

                st.success(
                    "Subscription request sent."
                )

            except Exception as exc:

                st.error(
                    f"Subscription failed: {exc}"
                )

        # -------------------------------------------------
        # BUILD BAR
        # -------------------------------------------------

        if st.button(
            "Build Latest 3-Min Bars"
        ):

            try:

                bars = (
                    neo.build_3min_candles()
                )

                if not bars:

                    st.info(
                        "No new ticks received yet."
                    )

                else:

                    # -------------------------------------------------
                    # Separate tokens
                    # -------------------------------------------------

                    spot_token = str(
                        CONFIG[
                            "nifty_spot_token"
                        ]
                    )

                    fut_token = str(
                        CONFIG[
                            "nifty_future_token"
                        ]
                    )

                    # Group latest bars by timestamp
                    grouped = {}

                    for bar in bars:

                        grouped.setdefault(
                            bar[
                                "timestamp"
                            ],
                            {}
                        )[

                            str(
                                bar[
                                    "token"
                                ]
                            )
                        ] = bar

                    processed = 0

                    for ts, bucket in (
                        sorted(
                            grouped.items()
                        )
                    ):

                        spot_bar = (
                            bucket.get(
                                spot_token
                            )
                        )

                        fut_bar = (
                            bucket.get(
                                fut_token
                            )
                        )

                        # If no future token,
                        # fallback to spot for
                        # display/testing only.
                        if fut_bar is None:

                            if (
                                not fut_token
                                and spot_bar
                            ):

                                fut_bar = (
                                    spot_bar
                                )

                            else:

                                continue

                        if spot_bar is None:

                            spot_bar = fut_bar

                        # PCR snapshot
                        option_snapshot = (
                            neo.get_option_chain_snapshot()
                        )

                        candle = Candle3Min(

                            timestamp=ts,

                            spot_o=
                                spot_bar[
                                    "open"
                                ],

                            spot_h=
                                spot_bar[
                                    "high"
                                ],

                            spot_l=
                                spot_bar[
                                    "low"
                                ],

                            spot_c=
                                spot_bar[
                                    "close"
                                ],

                            fut_o=
                                fut_bar[
                                    "open"
                                ],

                            fut_h=
                                fut_bar[
                                    "high"
                                ],

                            fut_l=
                                fut_bar[
                                    "low"
                                ],

                            fut_c=
                                fut_bar[
                                    "close"
                                ],

                            fut_volume=
                                fut_bar[
                                    "volume"
                                ],

                            fut_oi=
                                fut_bar[
                                    "oi"
                                ],

                            option_chain=
                                option_snapshot,
                        )

                        feat = (
                            st.session_state.engine
                            .process_candle(
                                candle
                            )
                        )

                        if feat is not None:

                            processed += 1

                    st.success(

                        f"Processed "
                        f"{processed} "
                        f"3-minute candle(s)."
                    )

            except Exception as exc:

                st.error(
                    f"Bar processing failed: {exc}"
                )

        # -------------------------------------------------
        # PCR SNAPSHOT
        # -------------------------------------------------

        option_snapshot = (
            neo.get_option_chain_snapshot()
        )

        pcr_engine = (
            OptionChainEngine()
        )

        pcr_features = (
            pcr_engine.compute(
                option_snapshot
            )
        )

        st.subheader(
            "Live Option Features"
        )

        pc1, pc2, pc3, pc4 = (
            st.columns(4)
        )

        with pc1:

            value = (
                pcr_features[
                    "pcr_oi"
                ]
            )

            st.metric(

                "PCR OI",

                "-"
                if not is_valid_number(
                    value
                )
                else round(
                    float(value),
                    3
                )
            )

        with pc2:

            value = (
                pcr_features[
                    "pcr_volume"
                ]
            )

            st.metric(

                "PCR Volume",

                "-"
                if not is_valid_number(
                    value
                )
                else round(
                    float(value),
                    3
                )
            )

        with pc3:

            st.metric(

                "CE OI",

                "-"
                if not is_valid_number(
                    pcr_features[
                        "pcr_ce_oi_total"
                    ]
                )
                else int(
                    pcr_features[
                        "pcr_ce_oi_total"
                    ]
                )
            )

        with pc4:

            st.metric(

                "PE OI",

                "-"
                if not is_valid_number(
                    pcr_features[
                        "pcr_pe_oi_total"
                    ]
                )
                else int(
                    pcr_features[
                        "pcr_pe_oi_total"
                    ]
                )
            )

        if (
            pcr_features[
                "pcr_oi_missing"
            ]
            == 1
        ):

            st.warning(

                "PCR unavailable: "
                "current option token "
                "universe mein valid CE/PE OI "
                "snapshot nahi mila."
            )

    # -----------------------------------------------------
    # DATASET
    # -----------------------------------------------------

    df = (
        st.session_state.engine
        .dataframe()
    )

    if not df.empty:

        st.subheader(
            "Latest Features"
        )

        st.dataframe(

            df.tail(20),

            use_container_width=True
        )

        c1, c2, c3, c4 = (
            st.columns(4)
        )

        with c1:

            st.metric(

                "3-Min Bars",

                len(df)
            )

        with c2:

            st.metric(

                "Future Close",

                round(

                    float(
                        df.iloc[-1][
                            "future_close"
                        ]
                    ),

                    2
                )
            )

        with c3:

            pcr = df.iloc[-1].get(
                "pcr_oi",
                np.nan
            )

            st.metric(

                "PCR OI",

                "-"
                if not is_valid_number(
                    pcr
                )
                else round(
                    float(pcr),
                    3
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

            "No 3-minute candles yet.\n\n"

            "1. Connect Kotak Neo\n"
            "2. Subscribe NIFTY + Options\n"
            "3. Wait for live ticks\n"
            "4. Build Latest 3-Min Bars"
        )

    st.divider()

    st.caption(

        "Execution model: "
        "next_bar_open | "
        "ATR: session_local | "
        "PCR: CE/PE OI ratio | "
        "Label horizon: 45m"
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
            "Run:"
        )

        print(
            "streamlit run app.py"
        )
