#!/usr/bin/env python3
"""
NIFTY Next-Day Stock Alpha Engine
---------------------------------
Independent backend layer for the existing NIFTY 3-Min Micro Engine.

Pipeline:
    ~NIFTY-500 universe
      -> quality/liquidity filter -> TOP 100
      -> momentum/relative-strength filter -> TOP 30
      -> multi-factor ranking -> TOP 50 checkpoint
      -> 7-day volume-shock analysis -> TOP 5
      -> final risk-adjusted ranking -> TOP 2

The module deliberately does NOT import or modify the existing NIFTY engine classes.
It stores its own cache under ./next_day_alpha/.
"""
from __future__ import annotations

import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

IST = ZoneInfo("Asia/Kolkata")
ROOT = Path("./next_day_alpha")
ROOT.mkdir(parents=True, exist_ok=True)
CACHE_JSON = ROOT / "latest.json"
UNIVERSE_CACHE = ROOT / "nifty500_universe.csv"
LOCK = threading.Lock()

NSE_500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
YF_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
NIFTY_TICKER = "^NSEI"

# Conservative filters for short-horizon liquidity and tradability.
MIN_PRICE = 40.0
MIN_AVG_TURNOVER_CR = 20.0
MIN_HISTORY_DAYS = 60
QUALITY_POOL = 100
MOMENTUM_POOL = 30
CHECKPOINT_POOL = 50
VOLUME_POOL = 5
FINAL_POOL = 2


def now_ist() -> datetime:
    return datetime.now(IST)


def _num(x: Any, default=np.nan) -> float:
    try:
        v = float(x)
        return v if np.isfinite(v) else default
    except Exception:
        return default


def _clip(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False, min_periods=n).mean()


def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    prev = df["Close"].shift(1)
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev).abs(),
        (df["Low"] - prev).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()


def _safe_z(x: float, mean: float, std: float) -> float:
    if not np.isfinite(x) or not np.isfinite(mean) or not np.isfinite(std) or std <= 1e-12:
        return 0.0
    return float((x - mean) / std)


def _normalise_scores(values: pd.Series, low=0.0, high=100.0) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce")
    if values.notna().sum() <= 1:
        return pd.Series(high, index=values.index, dtype=float)
    lo, hi = values.min(), values.max()
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return pd.Series((low + high) / 2.0, index=values.index, dtype=float)
    return ((values - lo) / (hi - lo) * (high - low) + low).clip(low, high)


def load_universe() -> pd.DataFrame:
    """Load NIFTY 500 constituents. Cached copy is preferred when fresh enough."""
    if UNIVERSE_CACHE.exists():
        try:
            age = time.time() - UNIVERSE_CACHE.stat().st_mtime
            if age < 7 * 86400:
                df = pd.read_csv(UNIVERSE_CACHE)
                if "Symbol" in df.columns and len(df) >= 350:
                    return df
        except Exception:
            pass

    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*"}
    try:
        import requests
        r = requests.get(NSE_500_URL, headers=headers, timeout=20)
        r.raise_for_status()
        text = r.content.decode("utf-8-sig", errors="replace")
        from io import StringIO
        df = pd.read_csv(StringIO(text))
        df.columns = [str(c).strip() for c in df.columns]
        if "Symbol" not in df.columns or len(df) < 350:
            raise ValueError("NIFTY 500 universe response was incomplete")
        df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper()
        df = df[df["Symbol"].str.match(r"^[A-Z0-9&.-]+$")].copy()
        df.to_csv(UNIVERSE_CACHE, index=False)
        return df
    except Exception as exc:
        # A cached stale file is better than silently producing an empty engine.
        if UNIVERSE_CACHE.exists():
            try:
                df = pd.read_csv(UNIVERSE_CACHE)
                if "Symbol" in df.columns and len(df) >= 350:
                    return df
            except Exception:
                pass
        raise RuntimeError(f"Unable to load NIFTY-500 universe: {exc}")


def _fetch_yf_chart(ticker: str, days: int = 140) -> Optional[pd.DataFrame]:
    try:
        import requests
        end = int(time.time())
        start = end - days * 86400
        url = YF_CHART.format(ticker=ticker)
        params = {
            "period1": start,
            "period2": end,
            "interval": "1d",
            "events": "history",
        }
        r = requests.get(
            url,
            params=params,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        result = (payload.get("chart", {}).get("result") or [None])[0]
        if not result:
            return None

        ts = result.get("timestamp") or []
        q = result.get("indicators", {}).get("quote", [{}])[0]
        if not ts or not q:
            return None

        df = pd.DataFrame({
            "Date": pd.to_datetime(
                ts,
                unit="s",
                utc=True,
            ).tz_convert(IST).date,
            "Open": q.get("open", []),
            "High": q.get("high", []),
            "Low": q.get("low", []),
            "Close": q.get("close", []),
            "Volume": q.get("volume", []),
        })

        for c in ["Open", "High", "Low", "Close", "Volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["Close"]).reset_index(drop=True)
        return df

    except Exception:
        return None


def fetch_history_batch(
    symbols: List[str],
    days: int = 140,
) -> Dict[str, pd.DataFrame]:
    """Prefer yfinance batch download, then fall back to Yahoo chart endpoint."""
    out: Dict[str, pd.DataFrame] = {}
    tickers = [f"{s}.NS" for s in symbols]

    try:
        import yfinance as yf

        raw = yf.download(
            tickers=tickers,
            period=f"{days}d",
            interval="1d",
            group_by="column",
            auto_adjust=False,
            progress=False,
            threads=True,
            timeout=20,
        )

        if isinstance(raw, pd.DataFrame) and not raw.empty:
            for s in symbols:
                t = f"{s}.NS"

                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        if t not in raw.columns.get_level_values(-1):
                            continue
                        sub = raw.xs(
                            t,
                            axis=1,
                            level=-1,
                        ).copy()
                    else:
                        sub = raw.copy()

                    sub = sub.reset_index()
                    sub.columns = [str(c) for c in sub.columns]

                    rename = {
                        "Date": "Date",
                        "Adj Close": "Close",
                    }
                    sub = sub.rename(columns=rename)

                    need = [
                        "Open",
                        "High",
                        "Low",
                        "Close",
                        "Volume",
                    ]

                    if all(c in sub.columns for c in need):
                        sub = sub[
                            ["Date"] + need
                        ].dropna(subset=["Close"])

                        out[s] = sub.reset_index(drop=True)

                except Exception:
                    continue

            if len(out) >= max(
                20,
                int(len(symbols) * 0.5),
            ):
                return out

    except Exception:
        pass

    def one(s: str):
        return s, _fetch_yf_chart(f"{s}.NS", days)

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [
            ex.submit(one, s)
            for s in symbols
        ]

        for fut in as_completed(futures):
            s, df = fut.result()

            if (
                df is not None
                and len(df) >= MIN_HISTORY_DAYS
            ):
                out[s] = df

    return out


def _features(
    symbol: str,
    df: pd.DataFrame,
    benchmark: Optional[pd.DataFrame],
) -> Optional[Dict[str, Any]]:

    if df is None or len(df) < MIN_HISTORY_DAYS:
        return None

    d = (
        df.copy()
        .dropna(subset=["Close"])
        .reset_index(drop=True)
    )

    if len(d) < MIN_HISTORY_DAYS:
        return None

    d["EMA20"] = _ema(d["Close"], 20)
    d["EMA50"] = _ema(d["Close"], 50)
    d["ATR14"] = _atr(d, 14)
    d["Turnover"] = d["Close"] * d["Volume"]

    last = d.iloc[-1]
    close = _num(last["Close"])

    if not np.isfinite(close) or close < MIN_PRICE:
        return None

    avg_turnover = (
        d["Turnover"].tail(20).mean() / 1e7
    )

    avg_volume = d["Volume"].tail(20).mean()

    if (
        not np.isfinite(avg_turnover)
        or avg_turnover < MIN_AVG_TURNOVER_CR
    ):
        return None

    ret1 = (
        (close / d["Close"].iloc[-2] - 1) * 100
        if len(d) >= 2
        else 0
    )

    ret5 = (
        (close / d["Close"].iloc[-6] - 1) * 100
        if len(d) >= 6
        else 0
    )

    ret20 = (
        (close / d["Close"].iloc[-21] - 1) * 100
        if len(d) >= 21
        else 0
    )

    ema20 = _num(last["EMA20"])
    ema50 = _num(last["EMA50"])
    atr14 = _num(last["ATR14"])

    atr_pct = (
        atr14 / close * 100
        if np.isfinite(atr14)
        else np.nan
    )

    high20 = (
        d["High"].iloc[-21:-1].max()
        if len(d) >= 22
        else np.nan
    )

    breakout20 = int(
        np.isfinite(high20)
        and close > high20
    )

    vol_ratio_today = (
        _num(last["Volume"])
        / d["Volume"].iloc[-21:-1].mean()
        if len(d) >= 22
        else np.nan
    )

    rs5, rs20 = ret5, ret20

    if benchmark is not None and len(benchmark) >= 22:
        b = (
            benchmark["Close"]
            .dropna()
            .reset_index(drop=True)
        )

        if len(b) >= 21:
            b5 = (
                b.iloc[-1] / b.iloc[-6] - 1
            ) * 100

            b20 = (
                b.iloc[-1] / b.iloc[-21] - 1
            ) * 100

            rs5 = ret5 - b5
            rs20 = ret20 - b20

    return {
        "Symbol": symbol,
        "LTP": close,
        "AvgTurnoverCr": avg_turnover,
        "AvgVolume20": avg_volume,
        "Ret1D": ret1,
        "Ret5D": ret5,
        "Ret20D": ret20,
        "RS5D": rs5,
        "RS20D": rs20,
        "EMA20": ema20,
        "EMA50": ema50,
        "AboveEMA20": int(
            np.isfinite(ema20)
            and close > ema20
        ),
        "AboveEMA50": int(
            np.isfinite(ema50)
            and close > ema50
        ),
        "ATRpct": atr_pct,
        "Breakout20": breakout20,
        "TodayVolRatio": vol_ratio_today,
    }


def _base_score(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()

    # Cross-sectional percentile-like scores.
    # This is deliberately transparent.
    x["QualityScore"] = (
        _normalise_scores(
            np.log1p(x["AvgTurnoverCr"])
        ) * 0.70
        + x["AboveEMA20"] * 15
        + x["AboveEMA50"] * 15
    ).clip(0, 100)

    x["MomentumScore"] = (
        _normalise_scores(x["Ret5D"]) * 0.35
        + _normalise_scores(x["Ret20D"]) * 0.25
        + _normalise_scores(x["RS5D"]) * 0.20
        + _normalise_scores(x["RS20D"]) * 0.20
    ).clip(0, 100)

    x["StructureScore"] = (
        x["Breakout20"] * 35
        + x["AboveEMA20"] * 25
        + x["AboveEMA50"] * 25
        + _normalise_scores(x["Ret1D"]) * 0.15
    ).clip(0, 100)

    # Prefer expanding volatility but penalise
    # extreme one-day instability.
    vol_quality = (
        1.0
        - (x["ATRpct"] - 2.5).abs() / 8.0
    ).clip(0, 1) * 100

    x["VolatilityScore"] = vol_quality

    x["BaseScore"] = (
        x["QualityScore"] * 0.20
        + x["MomentumScore"] * 0.35
        + x["StructureScore"] * 0.30
        + x["VolatilityScore"] * 0.15
    ).clip(0, 100)

    return x.sort_values(
        "BaseScore",
        ascending=False,
    )


def _volume_shock(
    top50: pd.DataFrame,
    histories: Dict[str, pd.DataFrame],
) -> pd.DataFrame:

    rows = []

    for _, r in top50.iterrows():
        s = r["Symbol"]
        d = histories.get(s)

        if d is None or len(d) < 10:
            continue

        d = d.dropna(
            subset=["Close", "Volume"]
        ).copy()

        v = d["Volume"].tail(8).astype(float)

        if len(v) < 8:
            continue

        # Last day is compared against the
        # previous 7 trading sessions.
        baseline = v.iloc[:-1].tail(7).mean()
        latest = v.iloc[-1]

        ratio = (
            latest / baseline
            if baseline > 0
            else 0
        )

        z = _safe_z(
            latest,
            v.iloc[:-1].tail(7).mean(),
            v.iloc[:-1].tail(7).std(ddof=0),
        )

        px5 = (
            (
                d["Close"].iloc[-1]
                / d["Close"].iloc[-6]
                - 1
            ) * 100
            if len(d) >= 6
            else 0
        )

        px1 = (
            (
                d["Close"].iloc[-1]
                / d["Close"].iloc[-2]
                - 1
            ) * 100
            if len(d) >= 2
            else 0
        )

        shock = _clip(
            (ratio - 1) * 35
            + _clip(z, -1, 4) * 8,
            0,
            100,
        )

        price_confirm = _clip(
            50 + px5 * 8 + px1 * 4,
            0,
            100,
        )

        final = _clip(
            r["BaseScore"] * 0.70
            + shock * 0.20
            + price_confirm * 0.10,
            0,
            100,
        )

        rows.append({
            **r.to_dict(),
            "Volume7DShock": shock,
            "VolumeRatio7D": ratio,
            "VolumeZ7D": z,
            "VolumePriceConfirm": price_confirm,
            "FinalScore": final,
        })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values(
        "FinalScore",
        ascending=False,
    )


def _probability(score: float) -> float:
    # NOT a calibrated ML probability.
    # It is only a ranking confidence until
    # historical labels train a real model.
    return _clip(
        50.0 + (float(score) - 50.0) * 0.45,
        50.0,
        78.0,
    )


def run_scan(force: bool = False) -> Dict[str, Any]:
    """Run the full after-market scan and persist results."""

    ts = now_ist()

    if not force and ts.hour < 16:
        return load_latest() or {
            "status": "WAITING",
            "message": (
                "Next-day scan starts after 16:30 IST."
            ),
        }

    universe = load_universe()

    symbols = (
        universe["Symbol"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
        .tolist()
    )

    histories = fetch_history_batch(
        symbols,
        140,
    )

    bench = _fetch_yf_chart(
        NIFTY_TICKER,
        140,
    )

    rows = []
    industry_map = {}

    if "Industry" in universe.columns:
        industry_map = dict(
            zip(
                universe["Symbol"]
                .astype(str)
                .str.upper(),
                universe["Industry"]
                .astype(str),
            )
        )

    for s, d in histories.items():
        f = _features(
            s,
            d,
            bench,
        )

        if f:
            f["Industry"] = industry_map.get(
                s,
                "",
            )
            rows.append(f)

    all_df = pd.DataFrame(rows)

    if all_df.empty:
        raise RuntimeError(
            "No usable stock history was returned "
            "by the data source."
        )

    quality100 = (
        all_df.sort_values(
            (
                ["AvgTurnoverCr", "QualityScore"]
                if "QualityScore" in all_df
                else ["AvgTurnoverCr"]
            ),
            ascending=False,
        )
        .head(QUALITY_POOL)
    )

    scored = _base_score(
        quality100
    )

    momentum30 = (
        scored
        .head(MOMENTUM_POOL)
        .copy()
    )

    # Build a 50-stock checkpoint from the broad
    # scored universe. The requested 7-day volume
    # shock is intentionally NOT calculated until
    # this checkpoint exists.
    checkpoint50 = (
        _base_score(all_df)
        .head(CHECKPOINT_POOL)
        .copy()
    )

    top5 = (
        _volume_shock(
            checkpoint50,
            histories,
        )
        .head(VOLUME_POOL)
        .copy()
    )

    top2 = (
        top5
        .head(FINAL_POOL)
        .copy()
    )

    # Add heuristic confidence only.
    # Never label it as calibrated probability.
    for frame in [top5, top2]:
        if not frame.empty:
            frame["ConfidencePct"] = (
                frame["FinalScore"]
                .apply(_probability)
            )

            frame["ExpectedMoveLowPct"] = (
                0.55
                + frame["ATRpct"].clip(lower=0.5)
                * 0.35
            ).clip(
                0.8,
                4.5,
            )

            frame["ExpectedMoveHighPct"] = (
                frame["ExpectedMoveLowPct"]
                * 1.55
            ).clip(
                1.2,
                7.5,
            )

    def records(frame):
        if frame.empty:
            return []

        return (
            frame
            .replace({np.nan: None})
            .to_dict("records")
        )

    result = {
        "status": "READY",
        "generated_at": ts.isoformat(),
        "generated_after_market_close": (
            ts.strftime("%Y-%m-%d")
            + " 16:30 IST"
        ),
        "universe_count": len(symbols),
        "history_count": len(histories),
        "quality_count": len(quality100),
        "momentum_count": len(momentum30),
        "checkpoint50_count": len(checkpoint50),
        "top5_count": len(top5),
        "top2_count": len(top2),
        "methodology": {
            "pipeline": (
                "Universe~500 -> Quality100 -> "
                "Momentum30 -> Checkpoint50 -> "
                "7D VolumeShock -> Top5 -> Top2"
            ),
            "volume_shock_window_days": 7,
            "probability_note": (
                "Heuristic confidence, not "
                "calibrated ML probability."
            ),
            "existing_nifty_engine_modified": False,
        },
        "top5": records(top5),
        "top2": records(top2),
        "checkpoint50": records(checkpoint50),
        "momentum30": records(momentum30),
    }

    tmp = CACHE_JSON.with_suffix(".tmp")

    with LOCK:
        tmp.write_text(
            json.dumps(
                result,
                default=str,
                indent=2,
            ),
            encoding="utf-8",
        )
        tmp.replace(CACHE_JSON)

    return result


def load_latest() -> Optional[Dict[str, Any]]:
    try:
        if CACHE_JSON.exists():
            return json.loads(
                CACHE_JSON.read_text(
                    encoding="utf-8"
                )
            )
    except Exception:
        return None

    return None


def run_if_due(
    force: bool = False,
) -> Optional[Dict[str, Any]]:
    """Run once per trading date after 16:30 IST; otherwise return cached result."""

    ts = now_ist()
    cached = load_latest()

    if not force and ts.hour < 16:
        return cached

    if not force and cached:
        try:
            generated = datetime.fromisoformat(
                cached["generated_at"]
            )

            if (
                generated.astimezone(IST).date()
                == ts.date()
                and generated.astimezone(IST).hour
                >= 16
            ):
                return cached

        except Exception:
            pass

    try:
        return run_scan(
            force=True
        )

    except Exception as exc:
        if cached:
            cached["status"] = "STALE"
            cached["error"] = str(exc)
            return cached

        return {
            "status": "ERROR",
            "error": str(exc),
        }


def _fetch_live_1m(
    symbol: str,
) -> Tuple[float, float]:

    try:
        import requests

        url = YF_CHART.format(
            ticker=f"{symbol}.NS"
        )

        params = {
            "range": "1d",
            "interval": "1m",
            "events": "history",
        }

        r = requests.get(
            url,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0"
            },
            timeout=8,
        )

        r.raise_for_status()

        result = (
            r.json()
            .get("chart", {})
            .get("result")
            or [None]
        )[0]

        if not result:
            return np.nan, np.nan

        q = (
            result
            .get("indicators", {})
            .get("quote", [{}])[0]
        )

        closes = [
            x
            for x in (
                q.get("close") or []
            )
            if x is not None
        ]

        if not closes:
            return np.nan, np.nan

        ltp = _num(
            closes[-1]
        )

        prev = _num(
            closes[0]
        )

        return ltp, prev

    except Exception:
        return np.nan, np.nan


def update_live(
    top5: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Refresh only the selected Top-5 intraday; never polls the whole universe."""

    if not top5:
        return top5

    out = []

    with ThreadPoolExecutor(
        max_workers=min(
            5,
            len(top5),
        )
    ) as ex:

        futs = {
            ex.submit(
                _fetch_live_1m,
                str(
                    x.get(
                        "Symbol",
                        "",
                    )
                ).upper(),
            ): x
            for x in top5
        }

        for fut in as_completed(futs):
            row = futs[fut]
            x = dict(row)

            try:
                ltp, day_open = fut.result()

            except Exception:
                ltp, day_open = (
                    np.nan,
                    np.nan,
                )

            if np.isfinite(ltp):
                x["LiveLTP"] = ltp

                x["LiveChangePct"] = (
                    (
                        ltp / day_open
                    ) - 1
                ) * 100 if (
                    np.isfinite(day_open)
                    and day_open
                ) else 0.0

                x["LiveStatus"] = (
                    "ACTIVE"
                    if x["LiveChangePct"] >= -1.0
                    else "WEAK"
                )

            out.append(x)

    order = {
        str(
            x.get("Symbol")
        ): i
        for i, x in enumerate(top5)
    }

    out.sort(
        key=lambda x: order.get(
            str(x.get("Symbol")),
            999,
        )
    )

    return out


class NextDayAlphaEngine:
    """Small facade used by the existing Streamlit app. No NIFTY core dependency."""

    def __init__(self):
        self._last_live_update = 0.0
        self._latest = None

    def run_if_due(
        self,
        force: bool = False,
    ):
        self._latest = run_if_due(
            force=force
        )
        return self._latest

    def latest(self):
        if self._latest is None:
            self._latest = load_latest()

        return self._latest

    def live_top5(
        self,
        refresh_seconds: int = 60,
    ):
        data = self.latest()

        if not data or data.get(
            "status"
        ) not in (
            "READY",
            "STALE",
        ):
            return []

        if (
            time.time()
            - self._last_live_update
            < refresh_seconds
            and data.get("live_top5")
        ):
            return data["live_top5"]

        live = update_live(
            data.get(
                "top5",
                [],
            )
        )

        data["live_top5"] = live

        self._last_live_update = time.time()
        self._latest = data

        return live

    def start_if_due_background(
        self,
        force: bool = False,
    ) -> bool:
        """Start the after-market scan in a daemon thread so the main NIFTY UI never blocks."""

        ts = now_ist()

        if not force and ts.hour < 16:
            return False

        if (
            getattr(
                self,
                "_scan_thread",
                None,
            )
            is not None
            and self._scan_thread.is_alive()
        ):
            return True

        cached = self.latest()

        if not force and cached:
            try:
                generated = (
                    datetime.fromisoformat(
                        cached["generated_at"]
                    )
                    .astimezone(IST)
                )

                if (
                    generated.date()
                    == ts.date()
                    and generated.hour >= 16
                ):
                    return False

            except Exception:
                pass

        def worker():
            self._latest = run_if_due(
                force=True
            )

        self._scan_thread = threading.Thread(
            target=worker,
            name="next-day-alpha-scan",
            daemon=True,
        )

        self._scan_thread.start()

        return True

    def scan_running(self) -> bool:
        t = getattr(
            self,
            "_scan_thread",
            None,
        )

        return bool(
            t is not None
            and t.is_alive()
        )
