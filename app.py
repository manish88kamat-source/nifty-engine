import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import sqlite3
import requests
import time
from datetime import datetime, timedelta
from ta.trend import ADXIndicator, SMAIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from ta.momentum import RSIIndicator
import warnings
warnings.filterwarnings("ignore")

# ============================================================
# CONFIG
# ============================================================
SYMBOLS = {
    "NIFTY": "^NSEI",
    "BANKNIFTY": "^NSEBANK",
    "INDIA_VIX": "^INDIAVIX"
}

TIMEFRAMES = {
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1H": "60m",
    "Daily": "1d"
}

DB_PATH = "market_journal.db"

# ============================================================
# FREE NSE SCRAPER (Improved)
# ============================================================
class FreeNSEScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive"
        }
        self._init_session()

    def _init_session(self):
        try:
            self.session.get("https://www.nseindia.com", headers=self.headers, timeout=8)
            time.sleep(0.4)
        except Exception:
            pass

    def get_live_index_and_vix(self):
        try:
            url = "https://www.nseindia.com/api/allIndices"
            r = self.session.get(url, headers=self.headers, timeout=8)
            if r.status_code != 200:
                raise Exception("Status not 200")
            data = r.json()
            result = {"NIFTY": 0.0, "BANKNIFTY": 0.0, "INDIA_VIX": 14.5}
            for idx in data.get("data", []):
                name = idx.get("index", "")
                if name == "NIFTY 50":
                    result["NIFTY"] = float(idx.get("last", 0))
                elif name == "NIFTY BANK":
                    result["BANKNIFTY"] = float(idx.get("last", 0))
                elif name == "INDIA VIX":
                    result["INDIA_VIX"] = float(idx.get("last", 14.5))
            return result
        except Exception:
            # Fallback to yfinance
            try:
                vix = yf.download("^INDIAVIX", period="1d", interval="1m", progress=False)
                vix_val = float(vix["Close"].iloc[-1]) if not vix.empty else 14.5
            except:
                vix_val = 14.5
            return {"NIFTY": 0.0, "BANKNIFTY": 0.0, "INDIA_VIX": vix_val}

    def get_realtime_pcr_oi(self, symbol="NIFTY"):
        try:
            sym = "NIFTY" if symbol == "NIFTY" else "BANKNIFTY"
            url = f"https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
            r = self.session.get(url, headers=self.headers, timeout=8)
            if r.status_code != 200:
                raise Exception("OI fetch failed")
            data = r.json()
            tot_ce = data.get("filtered", {}).get("CE", {}).get("totOI", 1)
            tot_pe = data.get("filtered", {}).get("PE", {}).get("totOI", 1)
            pcr = tot_pe / (tot_ce + 1e-9)
            strength = 1.18 if pcr > 1.15 else (0.82 if pcr < 0.88 else 1.0)
            return {"pcr": round(pcr, 2), "strength_multiplier": strength}
        except Exception:
            return {"pcr": 1.05, "strength_multiplier": 1.0}

nse_scraper = FreeNSEScraper()

# ============================================================
# JOURNAL
# ============================================================
def init_journal():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS candle_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, timeframe TEXT,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            adx REAL, adx_slope REAL, atr REAL, atr_ratio REAL,
            bb_bandwidth REAL, bb_bw_percentile REAL,
            rsi REAL, rsi_slope REAL, vwap REAL, regime INTEGER,
            early_signal TEXT, confirm_signal TEXT, final_decision TEXT,
            structure_note TEXT, india_vix REAL, gap_pct REAL, oi_pcr REAL, extra_json TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_to_journal(row: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT INTO candle_journal (
            timestamp, symbol, timeframe, open, high, low, close, volume,
            adx, adx_slope, atr, atr_ratio, bb_bandwidth, bb_bw_percentile,
            rsi, rsi_slope, vwap, regime, early_signal, confirm_signal,
            final_decision, structure_note, india_vix, gap_pct, oi_pcr, extra_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        row.get("timestamp"), row.get("symbol"), row.get("timeframe"),
        row.get("open"), row.get("high"), row.get("low"), row.get("close"), row.get("volume"),
        row.get("adx"), row.get("adx_slope"), row.get("atr"), row.get("atr_ratio"),
        row.get("bb_bandwidth"), row.get("bb_bw_percentile"),
        row.get("rsi"), row.get("rsi_slope"), row.get("vwap"),
        row.get("regime"), row.get("early_signal"), row.get("confirm_signal"),
        row.get("final_decision"), row.get("structure_note"),
        row.get("india_vix"), row.get("gap_pct"), row.get("oi_pcr"),
        str(row.get("extra_json", {}))
    ))
    conn.commit()
    conn.close()

# ============================================================
# DATA + INDICATORS
# ============================================================
def fetch_ohlcv(symbol_key: str, interval: str, period: str = "60d") -> pd.DataFrame:
    ticker = SYMBOLS.get(symbol_key, "^NSEI")
    df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
    if df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    df.columns = [c[0].lower() if isinstance(c, tuple) else str(c).lower() for c in df.columns]

    if "datetime" in df.columns:
        df.rename(columns={"datetime": "timestamp"}, inplace=True)
    elif "date" in df.columns:
        df.rename(columns={"date": "timestamp"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df[["timestamp", "open", "high", "low", "close", "volume"]].dropna().reset_index(drop=True)

    # Live price injection (only last candle)
    live = nse_scraper.get_live_index_and_vix()
    live_price = live.get(symbol_key, 0.0)
    if live_price > 100 and len(df) > 0:
        idx = df.index[-1]
        df.loc[idx, "close"] = live_price
        df.loc[idx, "high"] = max(df.loc[idx, "high"], live_price)
        df.loc[idx, "low"]  = min(df.loc[idx, "low"], live_price)

    return df

def get_vwap(df: pd.DataFrame) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3
    vol = df["volume"].replace(0, np.nan)
    return (tp * vol).cumsum() / vol.cumsum()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) < 40:
        return df

    df = df.copy()

    # ADX
    adx_ind = ADXIndicator(df["high"], df["low"], df["close"], window=14)
    df["adx"] = adx_ind.adx()
    df["adx_slope"] = df["adx"].diff(3)

    # ATR
    atr_ind = AverageTrueRange(df["high"], df["low"], df["close"], window=14)
    df["atr"] = atr_ind.average_true_range()
    df["atr_ma"] = df["atr"].rolling(20).mean()
    df["atr_ratio"] = df["atr"] / df["atr_ma"]

    # Bollinger
    bb = BollingerBands(df["close"], window=20, window_dev=2)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_lower"] = bb.bollinger_lband()
    df["bb_mid"] = bb.bollinger_mavg()
    df["bb_bandwidth"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
    df["bb_bw_percentile"] = df["bb_bandwidth"].rolling(80).rank(pct=True)

    # RSI
    rsi_ind = RSIIndicator(df["close"], window=14)
    df["rsi"] = rsi_ind.rsi()
    df["rsi_slope"] = df["rsi"].diff(2)

    df["sma_21"] = SMAIndicator(df["close"], window=21).sma_indicator()
    df["vwap"] = get_vwap(df)

    # Candlestick features (no future leak)
    df["body"] = (df["close"] - df["open"]).abs()
    df["lower_wick"] = np.minimum(df["open"], df["close"]) - df["low"]
    df["upper_wick"] = df["high"] - np.maximum(df["open"], df["close"])
    df["pinbar_bull"] = (df["lower_wick"] > 1.6 * df["body"]) & (df["close"] > df["open"])
    df["pinbar_bear"] = (df["upper_wick"] > 1.6 * df["body"]) & (df["close"] < df["open"])

    # Simple FVG (past only)
    df["fvg_bull"] = (df["low"] > df["high"].shift(2)) & (df["close"] > df["open"])
    df["fvg_bear"] = (df["high"] < df["low"].shift(2)) & (df["close"] < df["open"])

    df = df.fillna(method="ffill").fillna(0)
    return df

# ============================================================
# REGIME + SIGNALS
# ============================================================
def determine_regime(row) -> int:
    adx = float(row.get("adx", 20))
    adx_slope = float(row.get("adx_slope", 0))
    atr_ratio = float(row.get("atr_ratio", 1.0))
    bb_pct = float(row.get("bb_bw_percentile", 0.5))

    if atr_ratio >= 1.40 or bb_pct >= 0.80:
        return 3
    if adx > 37 and adx_slope < -0.6:
        return 4
    if adx > 22 and adx_slope >= 0:
        return 1
    return 2

def generate_signals(df: pd.DataFrame, regime: int, oi_data: dict, idx: int = -1) -> dict:
    if len(df) < 5:
        return {"early": "NONE", "confirm": "NONE", "final": "FLAT", "note": ""}

    last = df.iloc[idx]
    prev = df.iloc[idx-1]

    early = "NONE"
    confirm = "NONE"
    final = "FLAT"
    note = ""

    # Early Trigger
    if (last["rsi"] <= 41 and last["rsi_slope"] > 0.7) or last.get("pinbar_bull", False) or last.get("fvg_bull", False):
        early = "LONG_TRIGGER"
    elif (last["rsi"] >= 59 and last["rsi_slope"] < -0.7) or last.get("pinbar_bear", False) or last.get("fvg_bear", False):
        early = "SHORT_TRIGGER"

    # Confirmation (Trap filter)
    if early == "LONG_TRIGGER":
        confirm = "LONG_CONFIRM" if (last["low"] >= prev["low"] and last["close"] > prev["close"]) else "TRAP_AVOID"
    elif early == "SHORT_TRIGGER":
        confirm = "SHORT_CONFIRM" if (last["high"] <= prev["high"] and last["close"] < prev["close"]) else "TRAP_AVOID"

    strength = oi_data.get("strength_multiplier", 1.0)

    if regime == 3:
        final = "FLAT"
        note = "Shock Regime → No Trade"
    elif regime == 4:
        if early == "LONG_TRIGGER" and confirm == "LONG_CONFIRM":
            final = "LONG"
            note = "Exhaustion Long"
        elif early == "SHORT_TRIGGER" and confirm == "SHORT_CONFIRM":
            final = "SHORT"
            note = "Exhaustion Short"
    elif regime == 1:
        if last["close"] > last["vwap"] and early == "LONG_TRIGGER" and confirm == "LONG_CONFIRM":
            final = "LONG"
            note = "Trend Long"
        elif last["close"] < last["vwap"] and early == "SHORT_TRIGGER" and confirm == "SHORT_CONFIRM":
            final = "SHORT"
            note = "Trend Short"
    elif regime == 2:
        if last["close"] < last.get("bb_lower", last["close"]) and early == "LONG_TRIGGER" and confirm == "LONG_CONFIRM":
            final = "LONG"
            note = "Mean-Rev Long"
        elif last["close"] > last.get("bb_upper", last["close"]) and early == "SHORT_TRIGGER" and confirm == "SHORT_CONFIRM":
            final = "SHORT"
            note = "Mean-Rev Short"

    # OI filter
    if final == "LONG" and strength < 0.88:
        final = "FLAT"
        note += " | OI Filter"
    if final == "SHORT" and strength > 1.12:
        final = "FLAT"
        note += " | OI Filter"

    return {"early": early, "confirm": confirm, "final": final, "note": note}

# ============================================================
# BACKTEST
# ============================================================
def run_vectorized_backtest(df: pd.DataFrame):
    journal = []
    position = None
    entry_price = 0.0
    sl = tp = 0.0

    for i in range(40, len(df)):
        row = df.iloc[i]
        curr_close = float(row["close"])
        curr_high = float(row["high"])
        curr_low = float(row["low"])
        curr_atr = float(row["atr"]) if row["atr"] > 0 else 20
        t_stamp = row["timestamp"]

        regime = determine_regime(row)
        sig = generate_signals(df, regime, {"strength_multiplier": 1.0}, idx=i)

        # Manage open position
        if position == "LONG":
            if curr_low <= sl:
                journal.append({"Time": t_stamp, "Regime": regime, "Type": "LONG", "Entry": entry_price, "Exit": sl, "PnL": sl - entry_price, "Result": "LOSS"})
                position = None
            elif curr_high >= tp:
                journal.append({"Time": t_stamp, "Regime": regime, "Type": "LONG", "Entry": entry_price, "Exit": tp, "PnL": tp - entry_price, "Result": "WIN"})
                position = None

        elif position == "SHORT":
            if curr_high >= sl:
                journal.append({"Time": t_stamp, "Regime": regime, "Type": "SHORT", "Entry": entry_price, "Exit": sl, "PnL": entry_price - sl, "Result": "LOSS"})
                position = None
            elif curr_low <= tp:
                journal.append({"Time": t_stamp, "Regime": regime, "Type": "SHORT", "Entry": entry_price, "Exit": tp, "PnL": entry_price - tp, "Result": "WIN"})
                position = None

        # New entry
        if position is None:
            if sig["final"] == "LONG":
                position = "LONG"
                entry_price = curr_close
                sl = entry_price - 1.3 * curr_atr
                tp = entry_price + 2.1 * curr_atr
            elif sig["final"] == "SHORT":
                position = "SHORT"
                entry_price = curr_close
                sl = entry_price + 1.3 * curr_atr
                tp = entry_price - 2.1 * curr_atr

    df_res = pd.DataFrame(journal)
    if df_res.empty:
        return 0.0, 0, 0.0, df_res

    total = len(df_res)
    wins = len(df_res[df_res["PnL"] > 0])
    win_rate = (wins / total) * 100
    total_pnl = df_res["PnL"].sum()
    return round(win_rate, 2), total, round(total_pnl, 2), df_res

# ============================================================
# STREAMLIT UI
# ============================================================
def main():
    st.set_page_config(page_title="Regime Intelligence Engine", layout="wide", page_icon="📈")
    st.title("Market Regime Intelligence Engine")
    st.caption("Free NSE Live Data + Hierarchical Regime + Early/Confirm Signals + Backtest")

    init_journal()

    c1, c2, c3 = st.columns(3)
    with c1:
        symbol = st.selectbox("Symbol", ["NIFTY", "BANKNIFTY"])
    with c2:
        tf_label = st.selectbox("Timeframe", list(TIMEFRAMES.keys()), index=0)
    with c3:
        lookback = st.slider("Lookback Days", 5, 60, 20)

    interval = TIMEFRAMES[tf_label]

    tab1, tab2 = st.tabs(["Live Engine", "Backtest & Win-Rate"])

    df = fetch_ohlcv(symbol, interval, period=f"{lookback}d")
    if not df.empty:
        df = add_indicators(df)

    with tab1:
        if st.button("Run Live Analysis", type="primary"):
            if df.empty:
                st.error("Data nahi mila")
            else:
                live = nse_scraper.get_live_index_and_vix()
                vix = live.get("INDIA_VIX", 14.5)
                oi = nse_scraper.get_realtime_pcr_oi(symbol)

                latest = df.iloc[-1]
                regime = determine_regime(latest)
                signals = generate_signals(df, regime, oi)

                names = {1: "Trending", 2: "Squeeze/Chop", 3: "Shock", 4: "Exhaustion"}
                st.subheader(f"Regime → {regime} ({names.get(regime)})")
                st.info(f"Live: **{latest['close']:.2f}** | VIX: **{vix:.2f}** | PCR: **{oi['pcr']:.2f}** | Strength: **{oi['strength_multiplier']:.2f}**")

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Early Trigger", signals["early"])
                m2.metric("Confirmation", signals["confirm"])
                m3.metric("Final Signal", signals["final"])
                m4.metric("ADX / RSI", f"{latest['adx']:.1f} / {latest['rsi']:.1f}")

                st.write("Note:", signals["note"])

                # Journal save
                save_to_journal({
                    "timestamp": str(latest["timestamp"]),
                    "symbol": symbol,
                    "timeframe": tf_label,
                    "open": float(latest["open"]),
                    "high": float(latest["high"]),
                    "low": float(latest["low"]),
                    "close": float(latest["close"]),
                    "volume": float(latest["volume"]),
                    "adx": float(latest["adx"]),
                    "adx_slope": float(latest["adx_slope"]),
                    "atr": float(latest["atr"]),
                    "atr_ratio": float(latest["atr_ratio"]),
                    "bb_bandwidth": float(latest["bb_bandwidth"]),
                    "bb_bw_percentile": float(latest["bb_bw_percentile"]),
                    "rsi": float(latest["rsi"]),
                    "rsi_slope": float(latest["rsi_slope"]),
                    "vwap": float(latest["vwap"]),
                    "regime": regime,
                    "early_signal": signals["early"],
                    "confirm_signal": signals["confirm"],
                    "final_decision": signals["final"],
                    "structure_note": signals["note"],
                    "india_vix": vix,
                    "gap_pct": 0.0,
                    "oi_pcr": oi["pcr"],
                    "extra_json": {"strength": oi["strength_multiplier"]}
                })
                st.success("Candle journal mein save ho gaya")

                st.line_chart(df.set_index("timestamp")[["close", "vwap"]].tail(80))

    with tab2:
        st.subheader("Historical Performance")
        if st.button("Run Backtest"):
            if df.empty:
                st.error("Data missing")
            else:
                wr, trades, pnl, df_tr = run_vectorized_backtest(df)
                k1, k2, k3 = st.columns(3)
                k1.metric("Win Rate", f"{wr}%")
                k2.metric("Total Trades", trades)
                k3.metric("Net Points", f"{pnl}")

                if not df_tr.empty:
                    st.dataframe(df_tr.tail(20))
                    df_tr["CumPnL"] = df_tr["PnL"].cumsum()
                    st.line_chart(df_tr.set_index("Time")["CumPnL"])

if __name__ == "__main__":
    main()
