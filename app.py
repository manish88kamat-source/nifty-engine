import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime, time
import os

# -------------------------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------------------------
st.set_page_config(page_title="Institutional Engine v15.0", layout="wide", initial_sidebar_state="expanded")
st.title("🏛️ Institutional Engine v15.0 (Expert Multi-Indicator Correlation)")
st.caption("Opening Price Context + Multi-Indicator Correlation Matrix + High Win-Rate Filters")

JOURNAL_FILE = "paper_trade_journal.csv"

TOP_10_TICKERS = [
    "RELIANCE.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS", 
    "TCS.NS", "ITC.NS", "LT.NS", "AXISBANK.NS", "BHARTIARTL.NS", "KOTAKBANK.NS"
]

# -------------------------------------------------------------------
# SIDEBAR CONTROL PANEL
# -------------------------------------------------------------------
st.sidebar.header("🕹️ Institutional Control Panel")
mode = st.sidebar.radio("Mode", ["Intraday (5-min)", "Swing (Daily)"], index=0)
index_choice = st.sidebar.radio("Index Selection", ["Nifty 50", "Bank Nifty"], index=0)

spot_ticker = "^NSEI" if index_choice == "Nifty 50" else "^NSEBANK"
fut_ticker = "NIFTY=F" if index_choice == "Nifty 50" else "BANKNIFTY=F"

capital = st.sidebar.number_input("Capital (₹)", value=1_000_000, step=100_000)
base_risk_pct = st.sidebar.slider("Base Risk per Trade %", 0.5, 2.0, 1.0) / 100

# -------------------------------------------------------------------
# REAL-TIME DATA FETCHING
# -------------------------------------------------------------------
@st.cache_data(ttl=60)
def fetch_market_data(ticker, period="5d", interval="5m"):
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df.dropna()
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_realtime_vix():
    try:
        v = yf.download("^INDIAVIX", period="5d", interval="5m", progress=False, auto_adjust=True)
        if isinstance(v.columns, pd.MultiIndex):
            v.columns = v.columns.get_level_values(0)
        v = v.dropna()
        if not v.empty:
            current_vix = float(v["Close"].iloc[-1])
            prev_vix = float(v["Close"].iloc[-2]) if len(v) > 1 else current_vix
            vix_change_pct = ((current_vix - prev_vix) / (prev_vix + 1e-9)) * 100
            return current_vix, vix_change_pct
    except:
        pass
    return 13.5, 0.0

@st.cache_data(ttl=120)
def fetch_heavyweights():
    try:
        df = yf.download(TOP_10_TICKERS, period="2d", interval="5m", progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            closes = df["Close"]
        else:
            closes = df
        changes = (closes.iloc[-1] - closes.iloc[0]) / closes.iloc[0] * 100
        bullish_count = (changes > 0).sum()
        bearish_count = (changes < 0).sum()
        return bullish_count, bearish_count
    except:
        return 5, 5

# -------------------------------------------------------------------
# EXPERT MULTI-INDICATOR CORRELATION PIPELINE
# -------------------------------------------------------------------
def build_expert_correlation_engine(df_spot, df_fut):
    df = df_spot.copy()
    df['Date'] = df.index.date
    
    # 1. EMAs & VWAP Stack
    df["EMA_fast"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=21, adjust=False).mean()
    
    if not df_fut.empty and "Volume" in df_fut.columns:
        df_fut["TP"] = (df_fut["High"] + df_fut["Low"] + df_fut["Close"]) / 3
        df["VWAP"] = (df_fut["Volume"] * df_fut["TP"]).cumsum() / (df_fut["Volume"].cumsum() + 1e-9)
    else:
        df["VWAP"] = (df["High"] + df["Low"] + df["Close"]) / 3

    # 2. ADX & Directional Indicators
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-df["Close"].shift()).abs(), (df["Low"]-df["Close"].shift()).abs()], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    
    up = df["High"] - df["High"].shift(1)
    dn = df["Low"].shift(1) - df["Low"]
    pos_dm = np.where((up > dn) & (up > 0), up, 0.0)
    neg_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    pos_di = 100 * pd.Series(pos_dm, index=df.index).rolling(14).mean() / (df["ATR"] + 1e-9)
    neg_di = 100 * pd.Series(neg_dm, index=df.index).rolling(14).mean() / (df["ATR"] + 1e-9)
    dx = 100 * (abs(pos_di - neg_di) / (pos_di + neg_di + 1e-9))
    df["ADX"] = dx.rolling(14).mean()
    df["Plus_DI"] = pos_di
    df["Minus_DI"] = neg_di

    # 3. Hilega-Milega Logic (RSI + WMA Correlation)
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(9).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(9).mean()
    rs = gain / (loss + 1e-9)
    df["RSI_9"] = 100 - (100 / (1 + rs))
    df["HM_Fast"] = df["RSI_9"].ewm(span=3, adjust=False).mean()
    df["HM_Slow"] = df["RSI_9"].rolling(window=21).mean()
    df["HM_Bullish"] = df["HM_Fast"] > df["HM_Slow"]

    # 4. Candlesticks
    o, h, l, c = df["Open"], df["High"], df["Low"], df["Close"]
    body = (c - o).abs()
    min_oc = np.minimum(o, c)
    max_oc = np.maximum(o, c)
    lower_wick = min_oc - l
    upper_wick = h - max_oc
    df["Is_Bear_Pinbar"] = (upper_wick > 2 * body) & (lower_wick < 0.2 * body)
    df["Is_Bull_Pinbar"] = (lower_wick > 2 * body) & (upper_wick < 0.2 * body)

    # 5. Context & Opening Ranges
    daily_groups = df.groupby('Date')
    df["Gap_Pct"] = 0.0
    df["Day_Bias"] = "NEUTRAL"
    df["OR_High"] = np.nan
    df["OR_Low"] = np.nan
    prev_close = None
    
    for date, group in daily_groups:
        if len(group) == 0: continue
        first_open = group.iloc[0]["Open"]
        gap = ((first_open - prev_close) / prev_close) * 100 if prev_close is not None else 0.0
        f_open, f_close = group.iloc[0]["Open"], group.iloc[0]["Close"]
        
        or_high = group.iloc[0:6]["High"].max() if len(group) >= 6 else group["High"].max()
        or_low = group.iloc[0:6]["Low"].min() if len(group) >= 6 else group["Low"].min()
        
        bias = "NEUTRAL"
        if gap <= -0.15 and f_close < f_open: bias = "BEARISH"
        elif gap >= 0.15 and f_close > f_open: bias = "BULLISH"
        elif gap > 0.25 and f_close < f_open: bias = "BEARISH (Fade Gap Up)"
        elif gap < -0.25 and f_close > f_open: bias = "BULLISH (Fade Gap Down)"
        
        df.loc[group.index, "Gap_Pct"] = gap
        df.loc[group.index, "Day_Bias"] = bias
        df.loc[group.index, "OR_High"] = or_high
        df.loc[group.index, "OR_Low"] = or_low
        prev_close = group.iloc[-1]["Close"]
        
    return df.dropna()

# -------------------------------------------------------------------
# CORRELATION BACKTEST ENGINE
# -------------------------------------------------------------------
def run_correlation_backtest(df):
    if len(df) < 50: return None
    trades = []
    i = 30
    
    while i < len(df) - 10:
        row = df.iloc[i]
        if row.name.time() < time(9, 45):
            i += 1; continue
            
        close_p = row["Close"]
        vwap_p = row["VWAP"]
        ema_f = row["EMA_fast"]
        ema_s = row["EMA_slow"]
        atr = row["ATR"]
        bias = row["Day_Bias"]
        or_high = row["OR_High"]
        or_low = row["OR_Low"]
        
        # Primary Price Action Trigger
        primary_trigger = None
        if close_p < or_low: primary_trigger = "PUT"
        elif close_p > or_high: primary_trigger = "CALL"
        elif row["Is_Bear_Pinbar"] and close_p < vwap_p: primary_trigger = "PUT"
        elif row["Is_Bull_Pinbar"] and close_p > vwap_p: primary_trigger = "CALL"

        # Multi-Indicator Correlation Score Calculation
        if primary_trigger:
            corr_score = 0
            if primary_trigger == "CALL":
                if close_p > vwap_p: corr_score += 25
                if ema_f > ema_s: corr_score += 25
                if row["Plus_DI"] > row["Minus_DI"]: corr_score += 25
                if row["HM_Bullish"]: corr_score += 25
            elif primary_trigger == "PUT":
                if close_p < vwap_p: corr_score += 25
                if ema_f < ema_s: corr_score += 25
                if row["Minus_DI"] > row["Plus_DI"]: corr_score += 25
                if not row["HM_Bullish"]: corr_score += 25
                
            # EXECUTE ONLY IF CORRELATION SCORE >= 75%
            if corr_score >= 75:
                entry_idx = df.index[i+1]
                entry_price = df.iloc[i+1]["Open"]
                sl_dist = 0.9 * atr
                sl = entry_price - sl_dist if primary_trigger == "CALL" else entry_price + sl_dist
                t1 = entry_price + 1.8 * sl_dist if primary_trigger == "CALL" else entry_price - 1.8 * sl_dist
                
                exited = False
                for j in range(i+2, min(i+40, len(df))):
                    curr_bar = df.iloc[j]
                    if curr_bar.name.time() >= time(15, 15):
                        exit_p = curr_bar["Close"]
                        pnl = exit_p - entry_price if primary_trigger == "CALL" else entry_price - exit_p
                        trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": primary_trigger, "Entry": round(entry_price,1), "Exit": round(exit_p,1), "PnL": round(pnl,1), "Result": "Win" if pnl > 0 else "Loss", "Correlation": f"{corr_score}%"})
                        i = j + 1; exited = True; break
                        
                    if primary_trigger == "CALL":
                        if curr_bar["Low"] <= sl:
                            trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "CALL", "Entry": round(entry_price,1), "Exit": round(sl,1), "PnL": round(sl-entry_price,1), "Result": "Loss", "Correlation": f"{corr_score}%"})
                            i = j + 4; exited = True; break
                        elif curr_bar["High"] >= t1:
                            trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "CALL", "Entry": round(entry_price,1), "Exit": round(t1,1), "PnL": round(t1-entry_price,1), "Result": "Win", "Correlation": f"{corr_score}%"})
                            i = j + 4; exited = True; break
                    elif primary_trigger == "PUT":
                        if curr_bar["High"] >= sl:
                            trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "PUT", "Entry": round(entry_price,1), "Exit": round(sl,1), "PnL": round(entry_price-sl,1), "Result": "Loss", "Correlation": f"{corr_score}%"})
                            i = j + 4; exited = True; break
                        elif curr_bar["Low"] <= t1:
                            trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "PUT", "Entry": round(entry_price,1), "Exit": round(t1,1), "PnL": round(entry_price-t1,1), "Result": "Win", "Correlation": f"{corr_score}%"})
                            i = j + 4; exited = True; break
                
                if not exited:
                    exit_idx = min(i+35, len(df)-1)
                    exit_p = df.iloc[exit_idx]["Close"]
                    pnl = exit_p - entry_price if primary_trigger == "CALL" else entry_price - exit_p
                    trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[exit_idx].strftime("%Y-%m-%d %H:%M"), "Action": primary_trigger, "Entry": round(entry_price,1), "Exit": round(exit_p,1), "PnL": round(pnl,1), "Result": "Win" if pnl > 0 else "Loss", "Correlation": f"{corr_score}%"})
                    i += 6
            else:
                i += 1
        else:
            i += 1
            
    if not trades: return None
    tdf = pd.DataFrame(trades)
    wins = len(tdf[tdf["Result"] == "Win"])
    return {
        "trades": len(tdf), "wins": wins, "winrate": (wins / len(tdf)) * 100,
        "avg_pnl": tdf["PnL"].mean(), "total_pnl": tdf["PnL"].sum(), "details_df": tdf
    }

# -------------------------------------------------------------------
# MAIN EXECUTION PIPELINE
# -------------------------------------------------------------------
df_spot = fetch_market_data(spot_ticker, "5d", "5m")
df_fut = fetch_market_data(fut_ticker, "5d", "5m")

if df_spot.empty or len(df_spot) < 30:
    st.error("Data loading. Please refresh in a moment.")
    st.stop()

df = build_expert_correlation_engine(df_spot, df_fut)
vix_val, vix_chg = fetch_realtime_vix()
bull_hw, bear_hw = fetch_heavyweights()

latest = df.iloc[-1]
close_p = float(latest["Close"])
vwap_p = float(latest["VWAP"])
ema_f = float(latest["EMA_fast"])
ema_s = float(latest["EMA_slow"])
atr = float(latest["ATR"])
bias = str(latest["Day_Bias"])
gap = float(latest["Gap_Pct"])
or_high = float(latest["OR_High"])
or_low = float(latest["OR_Low"])

# -------------------------------------------------------------------
# REAL-TIME CORRELATION SCORING
# -------------------------------------------------------------------
primary_action = "WAIT"
if close_p < or_low or (latest["Is_Bear_Pinbar"] and close_p < vwap_p):
    primary_action = "PUT"
elif close_p > or_high or (latest["Is_Bull_Pinbar"] and close_p > vwap_p):
    primary_action = "CALL"

corr_score = 0
if primary_action == "CALL":
    if close_p > vwap_p: corr_score += 25
    if ema_f > ema_s: corr_score += 25
    if latest["Plus_DI"] > latest["Minus_DI"]: corr_score += 25
    if latest["HM_Bullish"]: corr_score += 25
elif primary_action == "PUT":
    if close_p < vwap_p: corr_score += 25
    if ema_f < ema_s: corr_score += 25
    if latest["Minus_DI"] > latest["Plus_DI"]: corr_score += 25
    if not latest["HM_Bullish"]: corr_score += 25

signal = "NO TRADE"
if primary_action != "WAIT" and corr_score >= 75:
    signal = f"HIGH CONVICTION {primary_action}"

sl_dist = 0.9 * atr
base_qty = max(1, int(capital * base_risk_pct / (sl_dist + 1e-9)))

# -------------------------------------------------------------------
# DASHBOARD DISPLAY & BACKTEST RESULTS
# -------------------------------------------------------------------
st.subheader(f"🏛️ {index_choice} | Day Context: `{bias}` | VIX: `{vix_val:.1f}`")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Spot Price", f"₹{close_p:.1f}", f"VWAP: ₹{vwap_p:.1f}")
c2.metric("Pre-Market Gap", f"{gap:+.2f}%")
c3.metric("Opening Range", f"H: ₹{or_high:.1f} | L: ₹{or_low:.1f}")
c4.metric("Indicator Correlation", f"{corr_score}%", "High Sync" if corr_score >= 75 else "Low Correlation")

st.info(f"**Expert Engine Status:** Primary Action: `{primary_action}` | Indicator Sync Score: `{corr_score}%`. Threshold for trade entry is 75%+ sync.")

if signal != "NO TRADE":
    st.success(f"🚨 **CORRELATION SIGNAL TRIGGERED: {signal}** | Qty: **{base_qty} Units**")
    st.write(f"**Entry:** ₹{close_p:.1f} | **SL:** ₹{close_p+(sl_dist if primary_action=='PUT' else -sl_dist):.1f} | **Target:** ₹{close_p+(-1.8*sl_dist if primary_action=='PUT' else 1.8*sl_dist):.1f}")
else:
    st.warning("**NO TRADE** | Awaiting Price Action + Multi-Indicator Matrix Correlation Sync (>= 75%).")

st.markdown("---")
st.subheader("📊 High Win-Rate Correlation Backtest Engine (Last 5 Days)")
st.caption("Testing Price Action Trigger + 75%+ Multi-Indicator Correlation Matrix.")

bt = run_correlation_backtest(df)
if bt:
    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Total Trades", bt["trades"])
    b2.metric("Wins", bt["wins"])
    b3.metric("Win Rate", f"{bt['winrate']:.1f}%")
    b4.metric("Total Net PnL", f"{bt['total_pnl']:+.1f} pts")
    
    st.markdown("#### 🔍 Historical High-Conviction Trades Table")
    st.dataframe(bt["details_df"], use_container_width=True)
else:
    st.info("No high-correlation trades in recent history (Filter Protected).")
