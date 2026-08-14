#!/usr/bin/env python3
"""
NIFTY 3-Min Micro Engine
Kotak Neo Integrated Research-Lock v1.5

Research-only / data-engine only.

CORE LOGIC
----------
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
- Parquet dataset

DATA SOURCE
-----------
Kotak Neo API / WebSocket

IMPORTANT
---------
This application DOES NOT place orders.

OPTION DATA
-----------
No fabricated full option-chain endpoint is used.

PCR is calculated from live CE/PE option quotes supplied through
Kotak Neo WebSocket subscriptions.

Required Streamlit secrets:

KOTAK_CONSUMER_KEY
KOTAK_MOBILE
KOTAK_UCC
KOTAK_TOTP
KOTAK_MPIN

Required market tokens:

NIFTY_SPOT_TOKEN
NIFTY_FUT_TOKEN

Optional PCR:

PCR_CE_TOKENS = "token1,token2,token3"
PCR_PE_TOKENS = "token4,token5,token6"

Optional heavyweight mapping:

HEAVY_TOKENS = '{"HDFCBANK":"...", "RELIANCE":"..."}'

Run:

streamlit run app.py

CLI tests:

RUN_TESTS=1 python app.py
"""

from __future__ import annotations

import json
import math
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

try:
    import streamlit as st
except ImportError:
    st = None

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None


# ============================================================
# CONFIG
# ============================================================

CONFIG = {
    "app_version": "v1.5_kotak_neo_research_lock",

    "feature_version": "v1.7_research_lock",

    "label_version": "TB_v1.6_lock",

    "schema_version": "1.5",

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

    "nifty_spot_token": "",

    "nifty_future_token": "",

    "pcr_ce_tokens": "",

    "pcr_pe_tokens": "",

    "pcr_exchange_segment": "nse_fo",

    "spot_exchange_segment": "nse_cm",

    "future_exchange_segment": "nse_fo",

    "volume_mode": "cumulative",

    "max_tick_buffer": 100000,
}


DEFAULT_HEAVYWEIGHTS = {
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


# ============================================================
# CONFIG / SECRETS HELPERS
# ============================================================

def secret_or_env(name: str, default: str = "") -> str:
    """
    Streamlit secrets first, environment second.
    """

    if st is not None:
        try:
            value = st.secrets.get(name)
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass

    return str(
        os.getenv(name, default)
    ).strip()


def load_runtime_config() -> Dict[str, Any]:
    """
    Build runtime configuration without inventing tokens.
    """

    cfg = dict(CONFIG)

    cfg["nifty_spot_token"] = secret_or_env(
        "NIFTY_SPOT_TOKEN",
        CONFIG["nifty_spot_token"],
    )

    cfg["nifty_future_token"] = secret_or_env(
        "NIFTY_FUT_TOKEN",
        CONFIG["nifty_future_token"],
    )

    cfg["pcr_ce_tokens"] = secret_or_env(
        "PCR_CE_TOKENS",
        CONFIG["pcr_ce_tokens"],
    )

    cfg["pcr_pe_tokens"] = secret_or_env(
        "PCR_PE_TOKENS",
        CONFIG["pcr_pe_tokens"],
    )

    cfg["volume_mode"] = secret_or_env(
        "NEO_VOLUME_MODE",
        CONFIG["volume_mode"],
    )

    return cfg


def parse_tokens(raw: Any) -> List[str]:

    if raw is None:
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


def load_heavy_tokens() -> Dict[str, str]:

    raw = secret_or_env(
        "HEAVY_TOKENS",
        "",
    )

    if not raw:
        return {}

    try:
        obj = json.loads(raw)

        if not isinstance(obj, dict):
            return {}

        return {
            str(k): str(v)
            for k, v in obj.items()
            if str(v).strip()
        }

    except Exception:
        return {}


# ============================================================
# NUMERIC HELPERS
# ============================================================

def safe_float(
    value: Any,
    default: float = np.nan,
) -> float:

    try:

        if value is None:
            return default

        result = float(value)

        if not np.isfinite(result):
            return default

        return result

    except Exception:

        return default


def is_valid_number(value: Any) -> bool:

    try:
        return (
            value is not None
            and np.isfinite(float(value))
        )
    except Exception:
        return False


def safe_int(
    value: Any,
    default: int = 0,
) -> int:

    try:
        return int(float(value))
    except Exception:
        return default


# ============================================================
# ATR
# ============================================================

def wilder_atr(
    trs: List[float],
    period: int = 14,
) -> float:

    if len(trs) < period:
        return np.nan

    clean = [
        float(x)
        for x in trs
        if is_valid_number(x)
    ]

    if len(clean) < period:
        return np.nan

    atr = float(
        np.mean(
            clean[:period]
        )
    )

    for tr in clean[period:]:

        atr = (
            atr * (period - 1)
            + tr
        ) / period

    return float(atr)


# ============================================================
# BAR TIME
# ============================================================

def floor_bar_timestamp(
    ts: datetime,
    minutes: int = 3,
) -> Optional[datetime]:

    anchor = ts.replace(
        hour=9,
        minute=15,
        second=0,
        microsecond=0,
    )

    if ts < anchor:
        return None

    elapsed = int(
        (ts - anchor).total_seconds()
        // 60
    )

    bucket = (
        elapsed // minutes
    ) * minutes

    result = (
        anchor
        + timedelta(minutes=bucket)
    )

    if result.time() > datetime.strptime(
        CONFIG["session_end"],
        "%H:%M",
    ).time():
        return None

    return result


# ============================================================
# DATA CLASS
# ============================================================

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


# ============================================================
# OPENING RANGE
# ============================================================

class OpeningRangeEngine:

    def __init__(self, minutes: int = 15):

        self.minutes = minutes

        self.or_high = None

        self.or_low = None

        self.or_set = False

    def reset(self):

        self.or_high = None
        self.or_low = None
        self.or_set = False

    def update(self, candle: Candle3Min):

        elapsed = (
            candle.timestamp.hour * 60
            + candle.timestamp.minute
            - 9 * 60
            - 15
        )

        if elapsed < 0:
            return

        if elapsed < self.minutes:

            if self.or_high is None:

                self.or_high = candle.fut_h
                self.or_low = candle.fut_l

            else:

                self.or_high = max(
                    self.or_high,
                    candle.fut_h,
                )

                self.or_low = min(
                    self.or_low,
                    candle.fut_l,
                )

        else:

            self.or_set = (
                self.or_high is not None
                and self.or_low is not None
            )

    def features(
        self,
        candle: Candle3Min,
        atr: float,
    ) -> Dict[str, Any]:

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
            or self.or_low is None
            or not is_valid_number(atr)
            or atr <= 0
        ):
            return {
                key: np.nan
                for key in names
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
                (
                    1
                    if candle.fut_c > self.or_high
                    else (
                        -1
                        if candle.fut_c < self.or_low
                        else 0
                    )
                ),
        }


# ============================================================
# SESSION CONTEXT
# ============================================================

class SessionContextEngine:

    def __init__(self):

        self.prev_close = None
        self.prev_high = None
        self.prev_low = None
        self.today_open = None

    def set_previous_day(
        self,
        close: float,
        high: float,
        low: float,
    ):

        self.prev_close = close
        self.prev_high = high
        self.prev_low = low

    def set_today_open(
        self,
        open_price: float,
    ):

        self.today_open = open_price

    def reset(self):

        self.today_open = None

    def features(
        self,
        candle: Candle3Min,
        atr: float,
    ) -> Dict[str, Any]:

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
                key: np.nan
                for key in names
            }

        open_price = (
            self.today_open
            if self.today_open is not None
            else candle.fut_o
        )

        gap = (
            open_price
            - self.prev_close
        )

        return {

            "gap_points":
                gap,

            "gap_atr":
                gap / atr,

            "gap_direction":
                (
                    1
                    if gap > 0
                    else (
                        -1
                        if gap < 0
                        else 0
                    )
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


# ============================================================
# PCR ENGINE
# ============================================================

class OptionChainEngine:
    """
    PCR feature engine.

    Input is already-collected live quote data.

    No full-chain endpoint is called.
    """

    def compute(
        self,
        chain: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:

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

                "ce_contracts_seen": 0,
                "pe_contracts_seen": 0,
            }

        mapping = {
            "pcr_oi": "pcr_oi",
            "pcr_volume": "pcr_volume",
            "ce_oi_change": "ce_oi_change",
            "pe_oi_change": "pe_oi_change",
            "atm_iv": "atm_iv",
            "iv_change": "iv_change",
            "ce_oi_atm": "ce_oi_atm",
            "pe_oi_atm": "pe_oi_atm",
            "atm_strike": "atm_strike",
        }

        out = {}

        for feature, key in mapping.items():

            value = chain.get(
                key,
                np.nan,
            )

            out[feature] = value

            missing = (
                not is_valid_number(value)
            )

            out[
                f"{feature}_missing"
            ] = int(missing)

        out["ce_contracts_seen"] = int(
            chain.get(
                "ce_contracts_seen",
                0,
            )
        )

        out["pe_contracts_seen"] = int(
            chain.get(
                "pe_contracts_seen",
                0,
            )
        )

        return out


# ============================================================
# HEAVYWEIGHT ENGINE
# ============================================================

class HeavyweightEngine:

    def __init__(
        self,
        weights: Dict[str, float],
    ):

        self.base_weights = weights

        self.day_open: Dict[str, float] = {}

    def set_day_open(
        self,
        symbol: str,
        price: float,
    ):

        if (
            is_valid_number(price)
            and price > 0
        ):
            self.day_open[symbol] = price

    def reset_day(self):
        self.day_open.clear()

    def compute(
        self,
        candle: Candle3Min,
    ) -> Dict[str, float]:

        contributions = []

        returns = []

        bullish = 0

        for symbol, weight in self.base_weights.items():

            data = candle.heavy.get(symbol)

            if not data:
                continue

            open_price = self.day_open.get(
                symbol
            )

            if open_price is None:

                open_price = safe_float(
                    data.get(
                        "o",
                        data.get(
                            "c",
                            np.nan,
                        ),
                    )
                )

                if (
                    is_valid_number(open_price)
                    and open_price > 0
                ):
                    self.day_open[
                        symbol
                    ] = open_price

            if (
                not is_valid_number(open_price)
                or open_price <= 0
            ):
                continue

            close_price = safe_float(
                data.get("c")
            )

            if not is_valid_number(close_price):
                continue

            ret = (
                close_price
                - open_price
            ) / open_price

            relative_volume = safe_float(
                data.get(
                    "rel_volume",
                    1.0,
                ),
                1.0,
            )

            contribution = (
                weight
                * ret
                * relative_volume
            )

            contributions.append(
                contribution
            )

            returns.append(ret)

            vwap = safe_float(
                data.get(
                    "vwap",
                    close_price,
                ),
                close_price,
            )

            if close_price >= vwap:
                bullish += 1

        total = (
            sum(contributions)
            if contributions
            else 0.0
        )

        count = max(
            len(contributions),
            1,
        )

        return {

            "twc":
                total,

            "breadth_10":
                bullish / count,

            "dispersion_index":
                (
                    float(
                        np.std(returns)
                    )
                    if returns
                    else 0.0
                ),

            "contribution_concentration":
                (
                    max(contributions)
                    / (
                        abs(total)
                        + 1e-9
                    )
                )
                if contributions
                else 0.0,

            "hw_bullish_count":
                bullish,
        }


# ============================================================
# FEATURE ENGINE
# ============================================================

class FeatureEngine:

    def __init__(
        self,
        heavy_weights: Dict[str, float],
    ):

        self.vwap_pv = 0.0

        self.vwap_vol = 0.0

        self.tr_history: List[float] = []

        self.history: List[Dict[str, Any]] = []

        self.hw = HeavyweightEngine(
            heavy_weights
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

    def set_previous_day(
        self,
        close: float,
        high: float,
        low: float,
    ):

        self.sess.set_previous_day(
            close,
            high,
            low,
        )

    def set_today_open(
        self,
        open_price: float,
    ):

        self.sess.set_today_open(
            open_price
        )

    def compute(
        self,
        candle: Candle3Min,
        previous: List[Candle3Min],
    ) -> Dict[str, Any]:

        # ----------------------------------------------------
        # VWAP
        # ----------------------------------------------------

        typical = (
            candle.fut_h
            + candle.fut_l
            + candle.fut_c
        ) / 3.0

        volume = max(
            safe_float(
                candle.fut_volume,
                0.0,
            ),
            0.0,
        )

        self.vwap_pv += (
            typical * volume
        )

        self.vwap_vol += volume

        if self.vwap_vol > 0:

            fut_vwap = (
                self.vwap_pv
                / self.vwap_vol
            )

        else:

            fut_vwap = typical

        # ----------------------------------------------------
        # TRUE RANGE / ATR
        # ----------------------------------------------------

        if previous:

            previous_close = (
                previous[-1].fut_c
            )

            tr = max(
                candle.fut_h
                - candle.fut_l,

                abs(
                    candle.fut_h
                    - previous_close
                ),

                abs(
                    candle.fut_l
                    - previous_close
                ),
            )

        else:

            tr = (
                candle.fut_h
                - candle.fut_l
            )

        atr_prev = wilder_atr(
            self.tr_history,
            CONFIG["atr_period"],
        )

        self.tr_history.append(
            tr
        )

        atr_close = wilder_atr(
            self.tr_history,
            CONFIG["atr_period"],
        )

        # Research convention:
        # features use ATR available BEFORE this bar's
        # TR is added.
        atr = atr_prev

        atr_warmup = int(
            not is_valid_number(atr)
        )

        # ----------------------------------------------------
        # SMA20
        # ----------------------------------------------------

        required_previous = (
            CONFIG["sma_period"] - 1
        )

        closes = [
            c.spot_c
            for c in previous[
                -required_previous:
            ]
        ]

        closes.append(
            candle.spot_c
        )

        sma_ready = (
            len(closes)
            >= CONFIG["sma_period"]
        )

        spot_sma = (
            float(
                np.mean(closes)
            )
            if sma_ready
            else np.nan
        )

        # ----------------------------------------------------
        # NORMALIZED FEATURES
        # ----------------------------------------------------

        if (
            is_valid_number(atr)
            and atr > 0
        ):

            normalized_stretch = (
                candle.fut_c
                - fut_vwap
            ) / atr

            normalized_spread = (
                (
                    spot_sma
                    - fut_vwap
                ) / atr
                if is_valid_number(
                    spot_sma
                )
                else np.nan
            )

        else:

            normalized_stretch = np.nan
            normalized_spread = np.nan

        if (
            self.history
            and is_valid_number(
                normalized_stretch
            )
            and is_valid_number(
                self.history[-1].get(
                    "normalized_stretch"
                )
            )
        ):

            stretch_slope = (
                normalized_stretch
                - self.history[-1][
                    "normalized_stretch"
                ]
            )

        else:

            stretch_slope = 0.0

        if (
            self.history
            and is_valid_number(
                normalized_spread
            )
            and is_valid_number(
                self.history[-1].get(
                    "normalized_spread"
                )
            )
        ):

            spread_slope = (
                normalized_spread
                - self.history[-1][
                    "normalized_spread"
                ]
            )

        else:

            spread_slope = 0.0

        # ----------------------------------------------------
        # OI CLASSIFICATION
        # ----------------------------------------------------

        previous_oi = (
            previous[-1].fut_oi
            if previous
            else candle.fut_oi
        )

        oi_change = (
            candle.fut_oi
            - previous_oi
        )

        previous_close = (
            previous[-1].fut_c
            if previous
            else candle.fut_c
        )

        price_up = (
            candle.fut_c
            > previous_close
        )

        price_down = (
            candle.fut_c
            < previous_close
        )

        oi_long_buildup = int(
            price_up
            and oi_change > 0
        )

        oi_short_buildup = int(
            price_down
            and oi_change > 0
        )

        oi_short_covering = int(
            price_up
            and oi_change < 0
        )

        oi_long_unwinding = int(
            price_down
            and oi_change < 0
        )

        oi_neutral = int(
            oi_change == 0
            or (
                not price_up
                and not price_down
            )
        )

        oi_strength = 0.0

        if oi_change != 0:

            direction = (
                1
                if price_up
                else -1
            )

            oi_strength = (
                direction
                * np.sign(oi_change)
                * np.log1p(
                    abs(oi_change)
                )
            )

        # ----------------------------------------------------
        # OPENING RANGE
        # ----------------------------------------------------

        self.or_eng.update(
            candle
        )

        # ----------------------------------------------------
        # DATA QUALITY
        # ----------------------------------------------------

        missing_spot = int(
            not is_valid_number(
                candle.spot_c
            )
        )

        missing_future = int(
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

        quality_flags = [
            missing_spot,
            missing_future,
            missing_oi,
            missing_volume,
            missing_heavy,
            missing_option,
            bad_ohlc,
            zero_volume,
            zero_oi,
        ]

        data_quality_score = max(
            0.0,
            1.0
            - 0.1 * sum(
                quality_flags
            ),
        )

        # ----------------------------------------------------
        # CONTEXT
        # ----------------------------------------------------

        if (
            is_valid_number(
                candle.fut_c
            )
            and is_valid_number(
                candle.spot_c
            )
        ):

            basis = (
                candle.fut_c
                - candle.spot_c
            )

        else:

            basis = np.nan

        if (
            is_valid_number(atr)
            and atr > 0
        ):
            or_atr = atr
        else:
            or_atr = 0.0

        # ----------------------------------------------------
        # FINAL FEATURES
        # ----------------------------------------------------

        features = {

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

            "basis":
                basis,

            "fut_vwap":
                fut_vwap,

            "normalized_stretch":
                normalized_stretch,

            "normalized_spread":
                normalized_spread,

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
                int(
                    not sma_ready
                ),

            "oi_change":
                oi_change,

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
                    candle.timestamp.hour
                    * 60
                    + candle.timestamp.minute
                    - 9 * 60
                    - 15
                ),

            "day_of_week":
                candle.timestamp.weekday(),

            **self.hw.compute(
                candle
            ),

            **self.or_eng.features(
                candle,
                or_atr,
            ),

            **self.sess.features(
                candle,
                or_atr,
            ),

            **self.opt.compute(
                candle.option_chain
            ),

            "missing_spot":
                missing_spot,

            "missing_future":
                missing_future,

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
                data_quality_score,

            "bar_complete":
                1,
        }

        self.history.append(
            features
        )

        return features


# ============================================================
# LABEL ENGINE
# ============================================================

class LabelEngine:

    def __init__(self):

        self.upper = CONFIG[
            "triple_upper_atr"
        ]

        self.lower = CONFIG[
            "triple_lower_atr"
        ]

        self.time_barrier = CONFIG[
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
        entry: float,
        future: List[Candle3Min],
        direction: int,
        max_bars: int,
    ):

        mfe = 0.0
        mae = 0.0

        available = min(
            len(future),
            max_bars,
        )

        for candle in future[
            :available
        ]:

            if direction == 1:

                mfe = max(
                    mfe,
                    candle.fut_h
                    - entry,
                )

                mae = max(
                    mae,
                    entry
                    - candle.fut_l,
                )

            else:

                mfe = max(
                    mfe,
                    entry
                    - candle.fut_l,
                )

                mae = max(
                    mae,
                    candle.fut_h
                    - entry,
                )

        complete = int(
            available >= max_bars
        )

        return (
            mfe,
            mae,
            complete,
        )

    def generate(
        self,
        entry_price: float,
        atr: float,
        future_after_entry: List[Candle3Min],
        direction: int = 1,
        signal_timestamp: Optional[
            datetime
        ] = None,
        entry_timestamp: Optional[
            datetime
        ] = None,
    ) -> Dict[str, Any]:

        # ----------------------------------------------------
        # Alignment lock
        # ----------------------------------------------------

        if (
            entry_timestamp
            and future_after_entry
        ):

            first_future_timestamp = (
                future_after_entry[
                    0
                ].timestamp
            )

            if (
                first_future_timestamp
                <= entry_timestamp
            ):

                raise ValueError(
                    "FUTURE ALIGNMENT "
                    "VIOLATION: future candle "
                    "must be strictly AFTER "
                    "entry bar."
                )

        if (
            not is_valid_number(atr)
            or atr <= 0
        ):

            atr = np.nan

        # ----------------------------------------------------
        # Barriers
        # ----------------------------------------------------

        if is_valid_number(atr):

            target = (
                entry_price
                + direction
                * self.upper
                * atr
            )

            stop = (
                entry_price
                - direction
                * self.lower
                * atr
            )

        else:

            target = np.nan
            stop = np.nan

        outcome = "TIMEOUT"

        bars_to_outcome = 0

        mfe_tb = 0.0
        mae_tb = 0.0

        time_to_mfe = 0

        max_bars = (
            self.time_barrier
            // CONFIG["bar_minutes"]
        )

        for i, candle in enumerate(
            future_after_entry[
                :max_bars
            ]
        ):

            bars_to_outcome = i + 1

            if direction == 1:

                mfe_tb = max(
                    mfe_tb,
                    candle.fut_h
                    - entry_price,
                )

                mae_tb = max(
                    mae_tb,
                    entry_price
                    - candle.fut_l,
                )

                hit_target = (
                    is_valid_number(target)
                    and candle.fut_h
                    >= target
                )

                hit_stop = (
                    is_valid_number(stop)
                    and candle.fut_l
                    <= stop
                )

            else:

                mfe_tb = max(
                    mfe_tb,
                    entry_price
                    - candle.fut_l,
                )

                mae_tb = max(
                    mae_tb,
                    candle.fut_h
                    - entry_price,
                )

                hit_target = (
                    is_valid_number(target)
                    and candle.fut_l
                    <= target
                )

                hit_stop = (
                    is_valid_number(stop)
                    and candle.fut_h
                    >= stop
                )

            if (
                mfe_tb > 0
                and time_to_mfe == 0
            ):
                time_to_mfe = (
                    i + 1
                )

            if hit_target and hit_stop:

                outcome = "AMBIGUOUS"
                break

            if hit_target:

                outcome = "TARGET_FIRST"
                break

            if hit_stop:

                outcome = "STOP_FIRST"
                break

        # ----------------------------------------------------
        # R multiple
        # ----------------------------------------------------

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
            and is_valid_number(atr)
        )

        # ----------------------------------------------------
        # Normalized excursion
        # ----------------------------------------------------

        if (
            is_valid_number(atr)
            and atr > 0
        ):

            mfe_atr = (
                mfe_tb / atr
            )

            mae_atr = (
                mae_tb / atr
            )

        else:

            mfe_atr = np.nan
            mae_atr = np.nan

        velocity = (
            mfe_atr
            / max(
                bars_to_outcome,
                1,
            )
            if is_valid_number(
                mfe_atr
            )
            else np.nan
        )

        # ----------------------------------------------------
        # Trajectory
        # ----------------------------------------------------

        if is_valid_number(
            mfe_atr
        ):

            if (
                mfe_atr >= 1.2
                and mae_atr <= 0.45
                and velocity > 0.25
            ):

                trajectory = "IMPULSE"

            elif (
                mfe_atr >= 0.8
                and mae_atr <= 0.70
                and 0.08
                < velocity
                <= 0.25
            ):

                trajectory = "STAIRCASE"

            elif (
                mfe_atr >= 0.5
                and velocity <= 0.08
            ):

                trajectory = "GRIND"

            else:

                trajectory = "FAILURE"

        else:

            trajectory = "UNKNOWN"

        real_breakout = int(
            outcome == "TARGET_FIRST"
            and is_valid_number(
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

            "target_price":
                target,

            "stop_price":
                stop,

            "direction":
                direction,

            "triple_barrier_outcome":
                outcome,

            "label_valid_for_training":
                valid,

            "r_multiple":
                r_multiple,

            "trajectory":
                trajectory,

            "real_breakout":
                real_breakout,

            "mfe_atr_tb":
                mfe_atr,

            "mae_atr_tb":
                mae_atr,

            "time_to_mfe":
                time_to_mfe,

            "bars_to_outcome":
                bars_to_outcome,

            "velocity":
                velocity,
        }

        # ----------------------------------------------------
        # MFE / MAE horizons
        # ----------------------------------------------------

        for horizon in self.mfe_horizons:

            max_horizon_bars = (
                horizon
                // CONFIG["bar_minutes"]
            )

            mfe_h, mae_h, complete = (
                self._excursion(
                    entry_price,
                    future_after_entry,
                    direction,
                    max_horizon_bars,
                )
            )

            if (
                is_valid_number(atr)
                and atr > 0
            ):

                labels[
                    f"mfe_atr_{horizon}m"
                ] = (
                    mfe_h / atr
                )

                labels[
                    f"mae_atr_{horizon}m"
                ] = (
                    mae_h / atr
                )

            else:

                labels[
                    f"mfe_atr_{horizon}m"
                ] = np.nan

                labels[
                    f"mae_atr_{horizon}m"
                ] = np.nan

            labels[
                f"horizon_{horizon}m_complete"
            ] = complete

        return labels


# ============================================================
# DATASET MANAGER
# ============================================================

class DatasetManager:

    def __init__(
        self,
        path: Optional[str] = None,
    ):

        self.base = Path(
            path
            or CONFIG["dataset_path"]
        )

        self.base.mkdir(
            parents=True,
            exist_ok=True,
        )

    def write_parquet(
        self,
        df: pd.DataFrame,
        name: str = "features",
    ):

        if df.empty:
            return

        if pa is None or pq is None:

            raise ImportError(
                "pyarrow is required for "
                "Parquet dataset writing."
            )

        frame = df.copy()

        if "timestamp" in frame.columns:

            frame["date"] = (
                pd.to_datetime(
                    frame["timestamp"]
                )
                .dt.date
                .astype(str)
            )

        table = pa.Table.from_pandas(
            frame,
            preserve_index=False,
        )

        output = (
            self.base
            / name
        )

        output.mkdir(
            parents=True,
            exist_ok=True,
        )

        pq.write_to_dataset(
            table,
            root_path=str(output),
            partition_cols=(
                ["date"]
                if "date" in frame.columns
                else None
            ),
            existing_data_behavior=
                "overwrite_or_ignore",
        )

    def purged_walk_forward_by_date(
        self,
        df: pd.DataFrame,
        n_splits: int = 5,
    ):

        if "timestamp" not in df.columns:

            raise ValueError(
                "timestamp required"
            )

        frame = df.copy()

        frame["date"] = (
            pd.to_datetime(
                frame["timestamp"]
            ).dt.date
        )

        dates = sorted(
            frame["date"].unique()
        )

        if len(dates) < 3:
            return []

        fold_size = max(
            1,
            len(dates)
            // (n_splits + 1),
        )

        splits = []

        for i in range(n_splits):

            train_end = (
                (i + 1)
                * fold_size
            )

            test_start = (
                train_end + 1
            )

            test_end = min(
                test_start + fold_size,
                len(dates),
            )

            if test_start >= len(dates):
                break

            train_dates = dates[
                :train_end
            ]

            test_dates = dates[
                test_start:test_end
            ]

            train_idx = frame[
                frame["date"].isin(
                    train_dates
                )
            ].index.tolist()

            test_idx = frame[
                frame["date"].isin(
                    test_dates
                )
            ].index.tolist()

            if (
                train_idx
                and CONFIG[
                    "embargo_bars"
                ] > 0
            ):

                embargo = min(
                    len(train_idx),
                    CONFIG[
                        "embargo_bars"
                    ],
                )

                train_idx = (
                    train_idx[:-embargo]
                )

            if train_idx and test_idx:

                splits.append(
                    (
                        train_idx,
                        test_idx,
                    )
                )

        return splits


# ============================================================
# KOTAK NEO ADAPTER
# ============================================================

class KotakNeoAdapter:

    def __init__(
        self,
        runtime_config: Optional[
            Dict[str, Any]
        ] = None,
    ):

        if NeoAPI is None:

            raise ImportError(
                "neo_api_client is not installed. "
                "Install the Kotak Neo SDK."
            )

        self.cfg = (
            runtime_config
            or load_runtime_config()
        )

        self.consumer_key = secret_or_env(
            "KOTAK_CONSUMER_KEY"
        )

        self.mobile = secret_or_env(
            "KOTAK_MOBILE"
        )

        self.ucc = secret_or_env(
            "KOTAK_UCC"
        )

        self.totp = secret_or_env(
            "KOTAK_TOTP"
        )

        self.mpin = secret_or_env(
            "KOTAK_MPIN"
        )

        self.client = NeoAPI(
            environment=self.cfg[
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

        self.latest: Dict[
            str,
            Dict[str, Any]
        ] = {}

        self.tick_buffer: List[
            Dict[str, Any]
        ] = []

        self.previous_option_oi: Dict[
            str,
            float
        ] = {}

        self.previous_atm_iv = np.nan

        self.last_bar_volume: Dict[
            str,
            float
        ] = {}

    # --------------------------------------------------------
    # LOGIN
    # --------------------------------------------------------

    def login(self):

        required = {
            "KOTAK_CONSUMER_KEY":
                self.consumer_key,

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
            key
            for key, value
            in required.items()
            if not value
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

    # --------------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------------

    def on_open(self, message):

        print(
            "[Kotak Neo] WebSocket opened:",
            message,
        )

    def on_error(self, message):

        print(
            "[Kotak Neo] WebSocket error:",
            message,
        )

    def on_close(self, message):

        self.connected = False

        print(
            "[Kotak Neo] WebSocket closed:",
            message,
        )

    def on_message(self, message):

        try:

            data = self._decode_message(
                message
            )

            if data is None:
                return

            self._process_tick(
                data
            )

        except Exception as exc:

            print(
                "[Kotak Neo] message parse error:",
                repr(exc),
            )

    # --------------------------------------------------------
    # MESSAGE DECODING
    # --------------------------------------------------------

    def _decode_message(
        self,
        message: Any,
    ) -> Optional[Dict[str, Any]]:

        if isinstance(
            message,
            dict,
        ):
            return message

        if isinstance(
            message,
            str,
        ):

            try:
                decoded = json.loads(
                    message
                )

                if isinstance(
                    decoded,
                    dict,
                ):
                    return decoded

            except Exception:
                return None

        return None

    # --------------------------------------------------------
    # TIMESTAMP
    # --------------------------------------------------------

    def _tick_time(
        self,
        data: Dict[str, Any],
    ) -> datetime:

        raw = (
            data.get("ltt")
            or data.get("ftdm")
            or data.get("tvalue")
            or data.get("timestamp")
        )

        if raw is None:
            return datetime.now()

        try:

            if isinstance(
                raw,
                (int, float),
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
                        fmt,
                    )

                except ValueError:
                    continue

        except Exception:
            pass

        return datetime.now()

    # --------------------------------------------------------
    # TICK PROCESSOR
    # --------------------------------------------------------

    def _process_tick(
        self,
        payload: Dict[str, Any],
    ):

        # Some feed messages wrap the quote in "data".
        if isinstance(
            payload.get("data"),
            dict,
        ):

            data = payload["data"]

        else:

            data = payload

        if not isinstance(
            data,
            dict,
        ):
            return

        token = str(
            data.get(
                "tk",
                data.get(
                    "token",
                    ""
                ),
            )
        ).strip()

        exchange = str(
            data.get(
                "e",
                data.get(
                    "exchange_segment",
                    ""
                ),
            )
        ).strip()

        if not token:
            return

        ltp = safe_float(
            data.get("ltp")
        )

        if not is_valid_number(
            ltp
        ):
            return

        timestamp = self._tick_time(
            data
        )

        item = {

            "timestamp":
                timestamp,

            "token":
                token,

            "exchange":
                exchange,

            "symbol":
                str(
                    data.get(
                        "ts",
                        data.get(
                            "symbol",
                            ""
                        ),
                    )
                ),

            "ltp":
                ltp,

            "volume":
                safe_float(
                    data.get(
                        "v",
                        data.get(
                            "volume",
                            0,
                        ),
                    ),
                    0.0,
                ),

            "oi":
                safe_float(
                    data.get(
                        "oi",
                        0,
                    ),
                    0.0,
                ),

            "open":
                safe_float(
                    data.get(
                        "op",
                        data.get(
                            "open",
                            np.nan,
                        ),
                    )
                ),

            "high":
                safe_float(
                    data.get(
                        "h",
                        data.get(
                            "high",
                            np.nan,
                        ),
                    )
                ),

            "low":
                safe_float(
                    data.get(
                        "lo",
                        data.get(
                            "low",
                            np.nan,
                        ),
                    )
                ),

            "vwap":
                safe_float(
                    data.get(
                        "ap",
                        data.get(
                            "vwap",
                            np.nan,
                        ),
                    )
                ),

            "iv":
                safe_float(
                    data.get(
                        "iv",
                        np.nan,
                    )
                ),

            "strike":
                safe_float(
                    data.get(
                        "strike",
                        np.nan,
                    )
                ),

            "option_type":
                str(
                    data.get(
                        "option_type",
                        data.get(
                            "optType",
                            ""
                        ),
                    )
                ).upper(),
        }

        with self.lock:

            self.latest[
                token
            ] = item

            self.tick_buffer.append(
                item
            )

            max_buffer = int(
                self.cfg.get(
                    "max_tick_buffer",
                    100000,
                )
            )

            if len(
                self.tick_buffer
            ) > max_buffer:

                self.tick_buffer = (
                    self.tick_buffer[
                        -max_buffer:
                    ]
                )

    # --------------------------------------------------------
    # CALLBACK REGISTRATION
    # --------------------------------------------------------

    def _register_callbacks(self):

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

    # --------------------------------------------------------
    # GENERIC SUBSCRIBE
    # --------------------------------------------------------

    def subscribe(
        self,
        instrument_tokens: List[Dict[str, str]],
        is_index: bool = False,
        is_depth: bool = False,
    ):

        if not self.connected:

            raise RuntimeError(
                "Login first."
            )

        self._register_callbacks()

        if not instrument_tokens:

            raise ValueError(
                "No instrument tokens supplied."
            )

        return self.client.subscribe(
            instrument_tokens=
                instrument_tokens,
            isIndex=is_index,
            isDepth=is_depth,
        )

    # --------------------------------------------------------
    # SPOT + FUTURE
    # --------------------------------------------------------

    def subscribe_spot_future(self):

        spot_token = self.cfg[
            "nifty_spot_token"
        ]

        future_token = self.cfg[
            "nifty_future_token"
        ]

        if not spot_token:

            raise RuntimeError(
                "NIFTY_SPOT_TOKEN missing."
            )

        if not future_token:

            raise RuntimeError(
                "NIFTY_FUT_TOKEN missing."
            )

        tokens = [

            {
                "instrument_token":
                    str(spot_token),

                "exchange_segment":
                    self.cfg[
                        "spot_exchange_segment"
                    ],
            },

            {
                "instrument_token":
                    str(future_token),

                "exchange_segment":
                    self.cfg[
                        "future_exchange_segment"
                    ],
            },
        ]

        return self.subscribe(
            tokens,
            is_index=True,
            is_depth=False,
        )

    # --------------------------------------------------------
    # PCR SUBSCRIPTION
    # --------------------------------------------------------

    def subscribe_pcr_tokens(self):

        ce_tokens = parse_tokens(
            self.cfg[
                "pcr_ce_tokens"
            ]
        )

        pe_tokens = parse_tokens(
            self.cfg[
                "pcr_pe_tokens"
            ]
        )

        tokens = []

        for token in (
            ce_tokens
            + pe_tokens
        ):

            tokens.append({

                "instrument_token":
                    token,

                "exchange_segment":
                    self.cfg[
                        "pcr_exchange_segment"
                    ],
            })

        if not tokens:

            return False

        self.subscribe(
            tokens,
            is_index=False,
            is_depth=False,
        )

        return True

    # --------------------------------------------------------
    # HEAVYWEIGHT SUBSCRIPTION
    # --------------------------------------------------------

    def subscribe_heavyweights(self):

        mapping = load_heavy_tokens()

        if not mapping:
            return False

        tokens = []

        for symbol, token in mapping.items():

            tokens.append({

                "instrument_token":
                    str(token),

                "exchange_segment":
                    "nse_cm",

            })

        self.subscribe(
            tokens,
            is_index=False,
            is_depth=False,
        )

        return True

    # --------------------------------------------------------
    # PCR
    # --------------------------------------------------------

    def calculate_pcr(self):

        ce_tokens = parse_tokens(
            self.cfg[
                "pcr_ce_tokens"
            ]
        )

        pe_tokens = parse_tokens(
            self.cfg[
                "pcr_pe_tokens"
            ]
        )

        with self.lock:

            latest = dict(
                self.latest
            )

        ce_oi = 0.0
        pe_oi = 0.0

        ce_volume = 0.0
        pe_volume = 0.0

        ce_count = 0
        pe_count = 0

        ce_ivs = []
        pe_ivs = []

        ce_rows = []
        pe_rows = []

        for token in ce_tokens:

            row = latest.get(
                str(token)
            )

            if not row:
                continue

            oi = max(
                safe_float(
                    row.get(
                        "oi",
                        0,
                    ),
                    0.0,
                ),
                0.0,
            )

            volume = max(
                safe_float(
                    row.get(
                        "volume",
                        0,
                    ),
                    0.0,
                ),
                0.0,
            )

            ce_oi += oi

            ce_volume += volume

            ce_count += 1

            iv = safe_float(
                row.get("iv")
            )

            if is_valid_number(iv):
                ce_ivs.append(iv)

            ce_rows.append(row)

        for token in pe_tokens:

            row = latest.get(
                str(token)
            )

            if not row:
                continue

            oi = max(
                safe_float(
                    row.get(
                        "oi",
                        0,
                    ),
                    0.0,
                ),
                0.0,
            )

            volume = max(
                safe_float(
                    row.get(
                        "volume",
                        0,
                    ),
                    0.0,
                ),
                0.0,
            )

            pe_oi += oi

            pe_volume += volume

            pe_count += 1

            iv = safe_float(
                row.get("iv")
            )

            if is_valid_number(iv):
                pe_ivs.append(iv)

            pe_rows.append(row)

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

        # ----------------------------------------------------
        # OI change from previous snapshot
        # ----------------------------------------------------

        ce_previous = 0.0
        pe_previous = 0.0

        for token in ce_tokens:

            token = str(token)

            current = latest.get(
                token
            )

            if not current:
                continue

            current_oi = max(
                safe_float(
                    current.get(
                        "oi",
                        0,
                    ),
                    0.0,
                ),
                0.0,
            )

            ce_previous += (
                self.previous_option_oi.get(
                    token,
                    current_oi,
                )
            )

        for token in pe_tokens:

            token = str(token)

            current = latest.get(
                token
            )

            if not current:
                continue

            current_oi = max(
                safe_float(
                    current.get(
                        "oi",
                        0,
                    ),
                    0.0,
                ),
                0.0,
            )

            pe_previous += (
                self.previous_option_oi.get(
                    token,
                    current_oi,
                )
            )

        ce_oi_change = (
            ce_oi
            - ce_previous
            if ce_count
            else np.nan
        )

        pe_oi_change = (
            pe_oi
            - pe_previous
            if pe_count
            else np.nan
        )

        # Update snapshot.
        for token in (
            ce_tokens
            + pe_tokens
        ):

            token = str(token)

            row = latest.get(
                token
            )

            if row:

                self.previous_option_oi[
                    token
                ] = max(
                    safe_float(
                        row.get(
                            "oi",
                            0,
                        ),
                        0.0,
                    ),
                    0.0,
                )

        # ----------------------------------------------------
        # ATM approximation
        # ----------------------------------------------------
        #
        # We do NOT invent ATM strike.
        #
        # If strike exists in feed, use the strike whose
        # distance to NIFTY spot is smallest.
        # ----------------------------------------------------

        spot_price = self._latest_spot_price()

        all_option_rows = (
            ce_rows + pe_rows
        )

        strike_candidates = [
            row
            for row in all_option_rows
            if is_valid_number(
                row.get("strike")
            )
        ]

        atm_strike = np.nan

        if (
            is_valid_number(
                spot_price
            )
            and strike_candidates
        ):

            atm_row = min(
                strike_candidates,
                key=lambda x:
                    abs(
                        float(
                            x["strike"]
                        )
                        - spot_price
                    ),
            )

            atm_strike = safe_float(
                atm_row.get(
                    "strike"
                )
            )

        atm_rows = []

        if is_valid_number(
            atm_strike
        ):

            for row in (
                all_option_rows
            ):

                strike = safe_float(
                    row.get("strike")
                )

                if (
                    is_valid_number(strike)
                    and abs(
                        strike
                        - atm_strike
                    ) < 0.01
                ):

                    atm_rows.append(
                        row
                    )

        atm_ivs = [
            safe_float(
                row.get("iv")
            )
            for row in atm_rows
            if is_valid_number(
                row.get("iv")
            )
        ]

        atm_iv = (
            float(
                np.mean(atm_ivs)
            )
            if atm_ivs
            else (
                float(
                    np.mean(
                        ce_ivs + pe_ivs
                    )
                )
                if (
                    ce_ivs
                    or pe_ivs
                )
                else np.nan
            )
        )

        if (
            is_valid_number(atm_iv)
            and is_valid_number(
                self.previous_atm_iv
            )
        ):

            iv_change = (
                atm_iv
                - self.previous_atm_iv
            )

        else:

            iv_change = np.nan

        if is_valid_number(atm_iv):

            self.previous_atm_iv = (
                atm_iv
            )

        ce_atm_oi = 0.0
        pe_atm_oi = 0.0

        for row in atm_rows:

            option_type = str(
                row.get(
                    "option_type",
                    "",
                )
            ).upper()

            oi = max(
                safe_float(
                    row.get(
                        "oi",
                        0,
                    ),
                    0.0,
                ),
                0.0,
            )

            if option_type in (
                "CE",
                "CALL",
            ):

                ce_atm_oi += oi

            elif option_type in (
                "PE",
                "PUT",
            ):

                pe_atm_oi += oi

        return {

            "pcr_oi":
                pcr_oi,

            "pcr_volume":
                pcr_volume,

            "ce_oi_change":
                ce_oi_change,

            "pe_oi_change":
                pe_oi_change,

            "atm_iv":
                atm_iv,

            "iv_change":
                iv_change,

            "ce_oi_atm":
                (
                    ce_atm_oi
                    if atm_rows
                    else np.nan
                ),

            "pe_oi_atm":
                (
                    pe_atm_oi
                    if atm_rows
                    else np.nan
                ),

            "atm_strike":
                atm_strike,

            "ce_contracts_seen":
                ce_count,

            "pe_contracts_seen":
                pe_count,
        }

    # --------------------------------------------------------
    # LATEST SPOT
    # --------------------------------------------------------

    def _latest_spot_price(self):

        token = str(
            self.cfg[
                "nifty_spot_token"
            ]
        )

        with self.lock:

            row = self.latest.get(
                token
            )

        if not row:
            return np.nan

        return safe_float(
            row.get("ltp")
        )

    # --------------------------------------------------------
    # HEAVYWEIGHT SNAPSHOT
    # --------------------------------------------------------

    def latest_heavyweights(self):

        mapping = load_heavy_tokens()

        result = {}

        if not mapping:
            return result

        with self.lock:
            latest = dict(
                self.latest
            )

        for symbol, token in mapping.items():

            row = latest.get(
                str(token)
            )

            if not row:
                continue

            result[symbol] = {

                "o":
                    safe_float(
                        row.get(
                            "open"
                        )
                    ),

                "c":
                    safe_float(
                        row.get(
                            "ltp"
                        )
                    ),

                "v":
                    safe_float(
                        row.get(
                            "volume"
                        ),
                        0.0,
                    ),

                "vwap":
                    safe_float(
                        row.get(
                            "vwap"
                        )
                    ),

            }

        return result

    # --------------------------------------------------------
    # BUILD 3-MIN CANDLES
    # --------------------------------------------------------

    def build_3min_candles(self):

        with self.lock:

            ticks = list(
                self.tick_buffer
            )

            self.tick_buffer.clear()

        if not ticks:
            return []

        spot_token = str(
            self.cfg[
                "nifty_spot_token"
            ]
        )

        future_token = str(
            self.cfg[
                "nifty_future_token"
            ]
        )

        allowed = {
            spot_token,
            future_token,
        }

        buckets = {}

        for tick in ticks:

            token = str(
                tick.get(
                    "token",
                    ""
                )
            )

            if token not in allowed:
                continue

            ts = floor_bar_timestamp(
                tick["timestamp"],
                self.cfg[
                    "bar_minutes"
                ],
            )

            if ts is None:
                continue

            key = (
                token,
                ts,
            )

            buckets.setdefault(
                key,
                [],
            ).append(tick)

        bars = {}

        for (
            token,
            timestamp,
        ), rows in sorted(
            buckets.items()
        ):

            rows.sort(
                key=lambda x:
                    x["timestamp"]
            )

            prices = [
                safe_float(
                    row.get("ltp")
                )
                for row in rows
                if is_valid_number(
                    row.get("ltp")
                )
            ]

            if not prices:
                continue

            raw_volumes = [
                safe_float(
                    row.get(
                        "volume",
                        0,
                    ),
                    0.0,
                )
                for row in rows
            ]

            raw_volumes = [
                max(v, 0.0)
                for v in raw_volumes
            ]

            # For most exchange feeds volume is cumulative.
            # Bar volume = end cumulative - previous cumulative.
            if (
                self.cfg[
                    "volume_mode"
                ].lower()
                == "cumulative"
            ):

                end_volume = (
                    raw_volumes[-1]
                    if raw_volumes
                    else 0.0
                )

                previous_volume = (
                    self.last_bar_volume.get(
                        token,
                        end_volume,
                    )
                )

                bar_volume = max(
                    0.0,
                    end_volume
                    - previous_volume,
                )

                self.last_bar_volume[
                    token
                ] = end_volume

            else:

                bar_volume = sum(
                    raw_volumes
                )

            oi_values = [
                safe_float(
                    row.get(
                        "oi",
                        0,
                    ),
                    0.0,
                )
                for row in rows
            ]

            bar = {

                "token":
                    token,

                "timestamp":
                    timestamp,

                "open":
                    prices[0],

                "high":
                    max(prices),

                "low":
                    min(prices),

                "close":
                    prices[-1],

                "volume":
                    bar_volume,

                "oi":
                    (
                        oi_values[-1]
                        if oi_values
                        else 0.0
                    ),
            }

            bars.setdefault(
                timestamp,
                {}
            )

            if token == spot_token:

                bars[timestamp][
                    "spot"
                ] = bar

            elif token == future_token:

                bars[timestamp][
                    "future"
                ] = bar

        output = []

        for timestamp, pair in sorted(
            bars.items()
        ):

            spot = pair.get(
                "spot"
            )

            future = pair.get(
                "future"
            )

            # Never fabricate missing future/spot.
            if spot is None or future is None:
                continue

            output.append({

                "timestamp":
                    timestamp,

                "spot":
                    spot,

                "future":
                    future,
            })

        return output


# ============================================================
# NIFTY CONTROLLER
# ============================================================

class NiftyMicroEngine:

    def __init__(self):

        self.features = FeatureEngine(
            DEFAULT_HEAVYWEIGHTS
        )

        self.labels = LabelEngine()

        self.dataset = DatasetManager()

        self.previous_candles = []

        self.feature_rows = []

        self.label_rows = []

        self.current_date = None

        self.last_bar_timestamp = None

    def reset_if_new_day(
        self,
        timestamp: datetime,
    ):

        current_date = timestamp.date()

        if (
            self.current_date
            != current_date
        ):

            self.features.reset_session()

            self.previous_candles.clear()

            self.current_date = current_date

            self.last_bar_timestamp = None

    def process_candle(
        self,
        candle: Candle3Min,
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

        feature = self.features.compute(
            candle,
            self.previous_candles,
        )

        self.previous_candles.append(
            candle
        )

        if len(
            self.previous_candles
        ) > 500:

            self.previous_candles = (
                self.previous_candles[
                    -500:
                ]
            )

        self.last_bar_timestamp = (
            candle.timestamp
        )

        self.feature_rows.append(
            feature
        )

        return feature

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
            name="features",
        )


# ============================================================
# UNIT TESTS
# ============================================================

def run_unit_tests():

    print(
        "\n======================================"
    )

    print(
        "KOTAK NEO RESEARCH LOCK UNIT TESTS"
    )

    print(
        "======================================"
    )

    # --------------------------------------------------------
    # ATR warmup
    # --------------------------------------------------------

    assert np.isnan(
        wilder_atr(
            [10, 12],
            14,
        )
    )

    print(
        "PASS: ATR warm-up"
    )

    # --------------------------------------------------------
    # Bar flooring
    # --------------------------------------------------------

    ts = datetime(
        2025,
        1,
        2,
        9,
        18,
        20,
    )

    assert (
        floor_bar_timestamp(
            ts,
            3,
        )
        == datetime(
            2025,
            1,
            2,
            9,
            18,
        )
    )

    print(
        "PASS: 3-minute flooring"
    )

    # --------------------------------------------------------
    # Candle / Feature engine
    # --------------------------------------------------------

    candle = Candle3Min(

        timestamp=datetime(
            2025,
            1,
            2,
            9,
            18,
        ),

        spot_o=24000,
        spot_h=24050,
        spot_l=23980,
        spot_c=24020,

        fut_o=24010,
        fut_h=24060,
        fut_l=23990,
        fut_c=24030,

        fut_volume=100000,
        fut_oi=5_000_000,
    )

    engine = NiftyMicroEngine()

    feature = (
        engine.process_candle(
            candle
        )
    )

    assert feature is not None

    assert (
        feature[
            "execution_model"
        ]
        == "next_bar_open"
    )

    assert (
        "pcr_oi"
        in feature
    )

    assert (
        "data_quality_score"
        in feature
    )

    print(
        "PASS: FeatureEngine"
    )

    # --------------------------------------------------------
    # Label test
    # --------------------------------------------------------

    label = LabelEngine()

    future = [

        Candle3Min(

            timestamp=datetime(
                2025,
                1,
                2,
                9,
                24,
            ),

            spot_o=0,
            spot_h=0,
            spot_l=0,
            spot_c=0,

            fut_o=24100,
            fut_h=24200,
            fut_l=23900,
            fut_c=24050,

            fut_volume=0,
            fut_oi=0,
        )
    ]

    result = label.generate(

        entry_price=24040,

        atr=20.0,

        future_after_entry=future,

        direction=1,

        signal_timestamp=datetime(
            2025,
            1,
            2,
            9,
            18,
        ),

        entry_timestamp=datetime(
            2025,
            1,
            2,
            9,
            21,
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
        "PASS: LabelEngine"
    )

    # --------------------------------------------------------
    # Future alignment lock
    # --------------------------------------------------------

    try:

        label.generate(

            entry_price=24040,

            atr=20.0,

            future_after_entry=[

                Candle3Min(

                    timestamp=datetime(
                        2025,
                        1,
                        2,
                        9,
                        21,
                    ),

                    spot_o=0,
                    spot_h=0,
                    spot_l=0,
                    spot_c=0,

                    fut_o=0,
                    fut_h=0,
                    fut_l=0,
                    fut_c=0,

                    fut_volume=0,
                    fut_oi=0,
                )
            ],

            direction=1,

            entry_timestamp=datetime(
                2025,
                1,
                2,
                9,
                21,
            ),
        )

    except ValueError:

        print(
            "PASS: Future alignment lock"
        )

    else:

        raise AssertionError(
            "Future alignment lock failed"
        )

    # --------------------------------------------------------
    # Short-side label
    # --------------------------------------------------------

    short_future = [

        Candle3Min(

            timestamp=datetime(
                2025,
                1,
                2,
                9,
                24,
            ),

            spot_o=0,
            spot_h=0,
            spot_l=0,
            spot_c=0,

            fut_o=24000,
            fut_h=24020,
            fut_l=23960,
            fut_c=23970,

            fut_volume=0,
            fut_oi=0,
        )
    ]

    short_result = label.generate(

        entry_price=24000,

        atr=20,

        future_after_entry=
            short_future,

        direction=-1,

        entry_timestamp=datetime(
            2025,
            1,
            2,
            9,
            21,
        ),
    )

    assert (
        short_result["direction"]
        == -1
    )

    print(
        "PASS: Short barrier"
    )

    print(
        "======================================"
    )

    print(
        "ALL TESTS PASSED"
    )

    print(
        "======================================\n"
    )


# ============================================================
# STREAMLIT HELPERS
# ============================================================

def format_metric(
    value: Any,
    decimals: int = 3,
):

    if not is_valid_number(value):
        return "-"

    return round(
        float(value),
        decimals,
    )


def create_engine_if_missing():

    if (
        "engine"
        not in st.session_state
    ):

        st.session_state.engine = (
            NiftyMicroEngine()
        )


def create_neo_if_missing():

    if (
        "neo"
        not in st.session_state
    ):

        st.session_state.neo = None


# ============================================================
# STREAMLIT APP
# ============================================================

def run_streamlit_app():

    if st is None:

        raise RuntimeError(
            "Streamlit is not installed."
        )

    st.set_page_config(
        page_title=
            "NIFTY 3-Min Micro Engine",
        layout="wide",
    )

    st.title(
        "NIFTY 3-Min Micro Engine"
    )

    st.caption(
        "Kotak Neo • Research-Lock • "
        "3-Min NIFTY • PCR"
    )

    st.info(
        "Research/data engine only. "
        "No order placement is enabled."
    )

    runtime = (
        load_runtime_config()
    )

    create_engine_if_missing()
    create_neo_if_missing()

    # ========================================================
    # SIDEBAR
    # ========================================================

    st.sidebar.header(
        "Kotak Neo Credentials"
    )

    credential_status = {

        "Consumer Key":
            bool(
                secret_or_env(
                    "KOTAK_CONSUMER_KEY"
                )
            ),

        "Mobile":
            bool(
                secret_or_env(
                    "KOTAK_MOBILE"
                )
            ),

        "UCC":
            bool(
                secret_or_env(
                    "KOTAK_UCC"
                )
            ),

        "TOTP":
            bool(
                secret_or_env(
                    "KOTAK_TOTP"
                )
            ),

        "MPIN":
            bool(
                secret_or_env(
                    "KOTAK_MPIN"
                )
            ),
    }

    for name, present in (
        credential_status.items()
    ):

        st.sidebar.write(
            f"{name}: "
            + (
                "✓"
                if present
                else "✗"
            )
        )

    st.sidebar.divider()

    st.sidebar.header(
        "Market Tokens"
    )

    spot_token = runtime[
        "nifty_spot_token"
    ]

    future_token = runtime[
        "nifty_future_token"
    ]

    st.sidebar.write(
        "NIFTY Spot:",
        "✓" if spot_token else "✗",
    )

    st.sidebar.write(
        "NIFTY Future:",
        "✓" if future_token else "✗",
    )

    st.sidebar.divider()

    st.sidebar.header(
        "PCR"
    )

    ce_tokens = parse_tokens(
        runtime[
            "pcr_ce_tokens"
        ]
    )

    pe_tokens = parse_tokens(
        runtime[
            "pcr_pe_tokens"
        ]
    )

    st.sidebar.write(
        "CE contracts:",
        len(ce_tokens),
    )

    st.sidebar.write(
        "PE contracts:",
        len(pe_tokens),
    )

    st.sidebar.caption(
        "PCR uses only live subscribed CE/PE "
        "quotes. Missing data remains NaN."
    )

    st.sidebar.divider()

    st.sidebar.header(
        "Engine"
    )

    st.sidebar.write(
        "Bar:",
        "3 minutes",
    )

    st.sidebar.write(
        "ATR:",
        "Session-local",
    )

    st.sidebar.write(
        "Execution:",
        "next_bar_open",
    )

    st.sidebar.write(
        "TB horizon:",
        "30 minutes",
    )

    st.sidebar.write(
        "MFE horizons:",
        "15 / 30 / 45 minutes",
    )

    # ========================================================
    # TOP CONTROLS
    # ========================================================

    c1, c2, c3 = st.columns(3)

    with c1:

        if st.button(
            "Connect Kotak Neo",
            use_container_width=True,
        ):

            try:

                neo = KotakNeoAdapter(
                    runtime
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

    with c2:

        if st.button(
            "Run Unit Tests",
            use_container_width=True,
        ):

            try:

                run_unit_tests()

                st.success(
                    "All unit tests passed."
                )

            except Exception as exc:

                st.error(
                    f"Tests failed: {exc}"
                )

    with c3:

        if st.button(
            "Reset Engine",
            use_container_width=True,
        ):

            st.session_state.engine = (
                NiftyMicroEngine()
            )

            st.success(
                "Engine state reset."
            )

    neo = st.session_state.neo

    # ========================================================
    # CONNECTION STATUS
    # ========================================================

    if neo is not None:

        if neo.connected:

            st.success(
                "Kotak Neo: CONNECTED"
            )

        else:

            st.warning(
                "Kotak Neo object exists but "
                "WebSocket/login is not connected."
            )

    else:

        st.warning(
            "Kotak Neo not connected."
        )

    # ========================================================
    # SUBSCRIPTIONS
    # ========================================================

    if neo is not None:

        st.subheader(
            "Live Subscriptions"
        )

        s1, s2, s3 = st.columns(3)

        with s1:

            if st.button(
                "Subscribe NIFTY Spot + Future",
                use_container_width=True,
            ):

                try:

                    neo.subscribe_spot_future()

                    st.success(
                        "NIFTY Spot + Future "
                        "subscription request sent."
                    )

                except Exception as exc:

                    st.error(
                        f"Subscription failed: {exc}"
                    )

        with s2:

            if st.button(
                "Subscribe PCR Options",
                use_container_width=True,
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
                            "PCR CE/PE tokens are empty."
                        )

                except Exception as exc:

                    st.error(
                        f"PCR subscription failed: {exc}"
                    )

        with s3:

            if st.button(
                "Subscribe Heavyweights",
                use_container_width=True,
            ):

                try:

                    ok = (
                        neo.subscribe_heavyweights()
                    )

                    if ok:

                        st.success(
                            "Heavyweight subscriptions sent."
                        )

                    else:

                        st.warning(
                            "HEAVY_TOKENS is empty."
                        )

                except Exception as exc:

                    st.error(
                        f"Heavyweight subscription failed: {exc}"
                    )

        # ====================================================
        # PCR DASHBOARD
        # ====================================================

        st.subheader(
            "Live PCR"
        )

        pcr = neo.calculate_pcr()

        p1, p2, p3, p4, p5 = st.columns(5)

        with p1:

            st.metric(
                "PCR OI",
                format_metric(
                    pcr[
                        "pcr_oi"
                    ]
                ),
            )

        with p2:

            st.metric(
                "PCR Volume",
                format_metric(
                    pcr[
                        "pcr_volume"
                    ]
                ),
            )

        with p3:

            st.metric(
                "CE Contracts",
                pcr[
                    "ce_contracts_seen"
                ],
            )

        with p4:

            st.metric(
                "PE Contracts",
                pcr[
                    "pe_contracts_seen"
                ],
            )

        with p5:

            st.metric(
                "ATM IV",
                format_metric(
                    pcr[
                        "atm_iv"
                    ]
                ),
            )

        p6, p7, p8 = st.columns(3)

        with p6:

            st.metric(
                "CE OI Δ",
                format_metric(
                    pcr[
                        "ce_oi_change"
                    ]
                ),
            )

        with p7:

            st.metric(
                "PE OI Δ",
                format_metric(
                    pcr[
                        "pe_oi_change"
                    ]
                ),
            )

        with p8:

            st.metric(
                "ATM Strike",
                format_metric(
                    pcr[
                        "atm_strike"
                    ],
                    2,
                ),
            )

        # ====================================================
        # BUILD BAR
        # ====================================================

        st.subheader(
            "3-Minute Aggregation"
        )

        if st.button(
            "Build Latest 3-Min Bars",
            use_container_width=True,
        ):

            try:

                bars = (
                    neo.build_3min_candles()
                )

                if not bars:

                    st.info(
                        "No complete Spot + Future "
                        "3-minute bar available."
                    )

                else:

                    processed = 0

                    for item in bars:

                        spot = item[
                            "spot"
                        ]

                        future = item[
                            "future"
                        ]

                        option_data = (
                            neo.calculate_pcr()
                        )

                        heavy_data = (
                            neo.latest_heavyweights()
                        )

                        candle = Candle3Min(

                            timestamp=
                                item[
                                    "timestamp"
                                ],

                            spot_o=
                                spot[
                                    "open"
                                ],

                            spot_h=
                                spot[
                                    "high"
                                ],

                            spot_l=
                                spot[
                                    "low"
                                ],

                            spot_c=
                                spot[
                                    "close"
                                ],

                            fut_o=
                                future[
                                    "open"
                                ],

                            fut_h=
                                future[
                                    "high"
                                ],

                            fut_l=
                                future[
                                    "low"
                                ],

                            fut_c=
                                future[
                                    "close"
                                ],

                            fut_volume=
                                future[
                                    "volume"
                                ],

                            fut_oi=
                                future[
                                    "oi"
                                ],

                            heavy=
                                heavy_data,

                            option_chain=
                                option_data,
                        )

                        result = (
                            st.session_state
                            .engine
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

            except Exception as exc:

                st.error(
                    f"Bar processing failed: {exc}"
                )

    # ========================================================
    # FEATURE DATASET
    # ========================================================

    engine = (
        st.session_state.engine
    )

    df = engine.dataframe()

    if not df.empty:

        st.subheader(
            "Latest Research Features"
        )

        display_columns = [

            "timestamp",

            "basis",

            "fut_vwap",

            "normalized_stretch",

            "normalized_spread",

            "stretch_slope_3",

            "spread_slope_3",

            "atr_14_prev",

            "atr_14_close",

            "spot_sma_20",

            "oi_change",

            "oi_strength",

            "oi_long_buildup",

            "oi_short_buildup",

            "oi_short_covering",

            "oi_long_unwinding",

            "twc",

            "breadth_10",

            "dispersion_index",

            "or_breakout_state",

            "pcr_oi",

            "pcr_volume",

            "ce_oi_change",

            "pe_oi_change",

            "atm_iv",

            "iv_change",

            "atm_strike",

            "data_quality_score",
        ]

        display_columns = [
            column
            for column in display_columns
            if column in df.columns
        ]

        st.dataframe(
            df[
                display_columns
            ].tail(30),
            use_container_width=True,
        )

        # ----------------------------------------------------
        # Metrics
        # ----------------------------------------------------

        m1, m2, m3, m4 = st.columns(4)

        with m1:

            st.metric(
                "3-Min Bars",
                len(df),
            )

        with m2:

            if engine.previous_candles:

                latest_future = (
                    engine
                    .previous_candles[
                        -1
                    ]
                    .fut_c
                )

                st.metric(
                    "Latest Future",
                    format_metric(
                        latest_future,
                        2,
                    ),
                )

            else:

                st.metric(
                    "Latest Future",
                    "-",
                )

        with m3:

            st.metric(
                "Latest PCR OI",
                format_metric(
                    df.iloc[-1].get(
                        "pcr_oi"
                    )
                ),
            )

        with m4:

            st.metric(
                "Data Quality",
                format_metric(
                    df.iloc[-1].get(
                        "data_quality_score"
                    )
                ),
            )

        # ----------------------------------------------------
        # Save
        # ----------------------------------------------------

        if st.button(
            "Save Feature Dataset",
            use_container_width=True,
        ):

            try:

                engine.save()

                st.success(
                    "Feature dataset saved to "
                    f"{CONFIG['dataset_path']}/features"
                )

            except Exception as exc:

                st.error(
                    f"Dataset save failed: {exc}"
                )

        # ----------------------------------------------------
        # Walk-forward
        # ----------------------------------------------------

        st.subheader(
            "Date-Aware Walk-Forward"
        )

        if st.button(
            "Build Walk-Forward Splits",
            use_container_width=True,
        ):

            try:

                splits = (
                    engine.dataset
                    .purged_walk_forward_by_date(
                        df,
                        n_splits=5,
                    )
                )

                if not splits:

                    st.info(
                        "Not enough dates for "
                        "walk-forward splitting."
                    )

                else:

                    rows = []

                    for i, (
                        train_idx,
                        test_idx,
                    ) in enumerate(
                        splits,
                        start=1,
                    ):

                        rows.append({

                            "fold":
                                i,

                            "train_rows":
                                len(
                                    train_idx
                                ),

                            "test_rows":
                                len(
                                    test_idx
                                ),

                        })

                    st.dataframe(
                        pd.DataFrame(rows),
                        use_container_width=True,
                    )

            except Exception as exc:

                st.error(
                    f"Walk-forward failed: {exc}"
                )

    else:

        st.info(
            "No 3-minute bars yet.\n\n"
            "1. Connect Kotak Neo\n"
            "2. Subscribe NIFTY Spot + Future\n"
            "3. Subscribe PCR Options\n"
            "4. Optionally subscribe Heavyweights\n"
            "5. Wait for live ticks\n"
            "6. Build Latest 3-Min Bars"
        )

    # ========================================================
    # FOOTER
    # ========================================================

    st.divider()

    st.caption(
        "Research-Lock | "
        "Execution model: next_bar_open | "
        "ATR: session_local | "
        "Bar: 3m | "
        "TB: 30m | "
        "MFE/MAE: 15/30/45m | "
        "Full Option Chain API: OFF | "
        "PCR: LIVE QUOTE BASED | "
        "Orders: OFF"
    )


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":

    if os.getenv(
        "RUN_TESTS",
        "0",
    ) == "1":

        run_unit_tests()

    elif st is not None:

        run_streamlit_app()

    else:

        print(
            "Streamlit is not installed."
        )

        print(
            "Run:"
        )

        print(
            "streamlit run app.py"
        )
