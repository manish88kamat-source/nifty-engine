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
st.set_page_config(page_title="Adaptive Engine v12.2", layout="wide", initial_sidebar_state="expanded")
st.title("🛡️ Adaptive Engine v12.2 (ValueError Fixed + Backtest)")
st.caption("Intraday 5-min & Swing Backtester | Value Error Resolved | Smart Exits | Auto Logger")

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
# INDICATOR STACK (BUG FIXED HERE)
# -------------------------------------------------------------------
def compute_dynamic_indicators(df_spot, df_fut):
    df = df_spot.copy()
    
    df["Day_High"] = df["High"].cummax()
    df["Day_Low"] = df["Low"].cummin()
    df["Drop_From_High"] = df["Day_High"] - df["Close"]
    df["Rally_From_Low"] = df["Close"] - df["Day_Low"]
    
    df["EMA_fast"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=21, adjust=False).mean()
    
    if not df_fut.empty and "Volume" in df_fut.columns:
        df_fut["TP"] = (df_fut["High"] + df_fut["Low"] + df_fut["Close"]) / 3
        df["VWAP"] = (df_fut["Volume"] * df_fut["TP"]).cumsum() / (df_fut["Volume"].cumsum() + 1e-9)
    else:
        df["VWAP"] = (df["High"] + df["Low"] + df["Close"]) / 3

    tr = pd.concat([df["High"]-df["Low"], (df["High"]-df["Close"].shift()).abs(), (df["Low"]-df["Close"].shift()).abs()], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    df["ATR_Avg"] = df["ATR"].rolling(50).mean()
    df["ATR_Expansion"] = df["ATR"] / (df["ATR_Avg"] + 1e-9)
    
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
    
    # FIXED: PANDAS ELEMENT-WISE MIN/MAX FOR CANDLE WICKS
    o, h, l, c = df["Open"], df["High"], df["Low"], df["Close"]
    body = (c - o).abs()
    min_oc = np.minimum(o, c)
    max_oc = np.maximum(o, c)
    lower_wick = min_oc - l
    upper_wick = h - max_oc
    
    df["Bull_Pinbar"] = (lower_wick > 2 * body) & (upper_wick < 0.2 * body)
    df["Bear_Pinbar"] = (upper_wick > 2 * body) & (lower_wick < 0.2 * body)
    
    return df.dropna()

# -------------------------------------------------------------------
# HISTORICAL BACKTEST ENGINE
# -------------------------------------------------------------------
def run_dynamic_backtest(df, mode):
    if len(df) < 50: return None
    trades = []
    i = 30
    
    while i < len(df) - 10:
        row = df.iloc[i]
        close_p = row["Close"]
        ema_f = row["EMA_fast"]
        atr = row["ATR"]
        drop_pts = row["Drop_From_High"]
        rally_pts = row["Rally_From_Low"]
        trigger_threshold = max(10.0, atr * 1.0)
        
        signal_type = None
        if drop_pts >= trigger_threshold and close_p <= ema_f:
            signal_type = "PUT"
        elif rally_pts >= trigger_threshold and close_p >= ema_f:
            signal_type = "CALL"
            
        if signal_type:
            entry_idx = df.index[i+1]
            entry_price = df.iloc[i+1]["Open"]
            sl_dist = 0.85 * atr
            sl = entry_price - sl_dist if signal_type == "CALL" else entry_price + sl_dist
            t1 = entry_price + 1.5 * sl_dist if signal_type == "CALL" else entry_price - 1.5 * sl_dist
            t2 = entry_price + 2.5 * sl_dist if signal_type == "CALL" else entry_price - 2.5 * sl_dist
            
            exited = False
            for j in range(i+2, min(i+35, len(df))):
                curr_bar = df.iloc[j]
                if signal_type == "CALL":
                    if curr_bar["Low"] <= sl:
                        trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "CALL", "Entry": round(entry_price,1), "Exit": round(sl,1), "PnL": round(sl-entry_price,1), "Result": "Loss"})
                        i = j; exited = True; break
                    elif curr_bar["High"] >= t2:
                        trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "CALL", "Entry": round(entry_price,1), "Exit": round(t2,1), "PnL": round(t2-entry_price,1), "Result": "Win"})
                        i = j; exited = True; break
                elif signal_type == "PUT":
                    if curr_bar["High"] >= sl:
                        trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "PUT", "Entry": round(entry_price,1), "Exit": round(sl,1), "PnL": round(entry_price-sl,1), "Result": "Loss"})
                        i = j; exited = True; break
                    elif curr_bar["Low"] <= t2:
                        trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[j].strftime("%Y-%m-%d %H:%M"), "Action": "PUT", "Entry": round(entry_price,1), "Exit": round(t2,1), "PnL": round(entry_price-t2,1), "Result": "Win"})
                        i = j; exited = True; break
            
            if not exited:
                exit_idx = min(i+30, len(df)-1)
                exit_p = df.iloc[exit_idx]["Close"]
                pnl = exit_p - entry_price if signal_type == "CALL" else entry_price - exit_p
                trades.append({"Entry Date": entry_idx.strftime("%Y-%m-%d %H:%M"), "Exit Date": df.index[exit_idx].strftime("%Y-%m-%d %H:%M"), "Action": signal_type, "Entry": round(entry_price,1), "Exit": round(exit_p,1), "PnL": round(pnl,1), "Result": "Win" if pnl > 0 else "Loss"})
                i += 10
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
# AUTO-LOGGER FOR PAPER TRADES
# -------------------------------------------------------------------
def process_smart_exits_and_logger(signal, action, latest, index_choice, confidence_score, qty, notes):
    cols = ["Date", "Index", "Confidence", "Signal", "Action", "Qty", "Entry", "Current_SL", "T1", "T2", "Status", "Exit_Price", "PnL", "Notes"]
    if not os.path.exists(JOURNAL_FILE):
        pd.DataFrame(columns=cols).to_csv(JOURNAL_FILE, index=False)
    
    journal = pd.read_csv(JOURNAL_FILE)
    if "Status" not in journal.columns: return journal, []

    latest_date_str = latest.name.strftime("%Y-%m-%d %H:%M")
    latest_close = round(float(latest["Close"]), 1)
    latest_high = float(latest["High"])
    latest_low = float(latest["Low"])
    atr = float(latest["ATR"])
    sl_dist = 0.85 * atr

    exit_alerts = []
    updated = False

    for idx, row in journal.iterrows():
        status = str(row["Status"])
        if status in ["Open", "Partial_Booked"]:
            entry, curr_sl, t1, t2, act = float(row["Entry"]), float(row["Current_SL"]), float(row["T1"]), float(row["T2"]), str(row["Action"])
            
            if act == "CALL":
                new_trail_sl = round(latest_close - sl_dist, 1)
                if new_trail_sl > curr_sl:
                    journal.at[idx, "Current_SL"] = new_trail_sl; updated = True

                if latest_low <= curr_sl:
                    journal.at[idx, "Status"] = "Closed_Loss" if status == "Open" else "Closed_Partial_Win"
                    journal.at[idx, "Exit_Price"] = curr_sl; journal.at[idx, "PnL"] = round(curr_sl - entry, 1)
                    exit_alerts.append(f"🛑 CALL SL Hit / Trailing SL Executed at ₹{curr_sl:.1f}"); updated = True
                elif latest_high >= t1 and status == "Open":
                    journal.at[idx, "Status"] = "Partial_Booked"; journal.at[idx, "Current_SL"] = entry
                    exit_alerts.append(f"🎯 CALL T1 HIT! 50% Profit Booked at ₹{t1:.1f}"); updated = True
                elif latest_high >= t2:
                    journal.at[idx, "Status"] = "Closed_Full_Win"; journal.at[idx, "Exit_Price"] = t2; journal.at[idx, "PnL"] = round(t2 - entry, 1)
                    exit_alerts.append(f"🚀 CALL T2 FULL TARGET HIT at ₹{t2:.1f}"); updated = True

            elif act == "PUT":
                new_trail_sl = round(latest_close + sl_dist, 1)
                if new_trail_sl < curr_sl:
                    journal.at[idx, "Current_SL"] = new_trail_sl; updated = True

                if latest_high >= curr_sl:
                    journal.at[idx, "Status"] = "Closed_Loss" if status == "Open" else "Closed_Partial_Win"
                    journal.at[idx, "Exit_Price"] = curr_sl; journal.at[idx, "PnL"] = round(entry - curr_sl, 1)
                    exit_alerts.append(f"🛑 PUT SL Hit / Trailing SL Executed at ₹{curr_sl:.1f}"); updated = True
                elif latest_low <= t1 and status == "Open":
                    journal.at[idx, "Status"] = "Partial_Booked"; journal.at[idx, "Current_SL"] = entry
                    exit_alerts.append(f"🎯 PUT T1 HIT! 50% Profit Booked at ₹{t1:.1f}"); updated = True
                elif latest_low <= t2:
                    journal.at[idx, "Status"] = "Closed_Full_Win"; journal.at[idx, "Exit_Price"] = t2; journal.at[idx, "PnL"] = round(entry - t2, 1)
                    exit_alerts.append(f"🚀 PUT T2 FULL TARGET HIT at ₹{t2:.1f}"); updated = True

    if signal != "NO TRADE":
        already_logged = False
        if not journal.empty:
            already_logged = ((journal["Date"] == latest_date_str) & (journal["Index"] == index_choice)).any()
        
        if not already_logged:
            sl_val = round(latest_close - sl_dist, 1) if action == "CALL" else round(latest_close + sl_dist, 1)
            t1_val = round(latest_close + 1.5 * sl_dist, 1) if action == "CALL" else round(latest_close - 1.5 * sl_dist, 1)
            t2_val = round(latest_close + 2.5 * sl_dist, 1) if action == "CALL" else round(latest_close - 2.5 * sl_dist, 1)
            
            new_entry = {
                "Date": latest_date_str, "Index": index_choice, "Confidence": f"{confidence_score:.0f}%",
                "Signal": signal, "Action": action, "Qty": qty, "Entry": latest_close, "Current_SL": sl_val, 
                "T1": t1_val, "T2": t2_val, "Status": "Open", "Exit_Price": "", "PnL": "", "Notes": notes
            }
            journal = pd.concat([journal, pd.DataFrame([new_entry])], ignore_index=True)
            updated = True

    if updated: journal.to_csv(JOURNAL_FILE, index=False)
    return journal, exit_alerts

# -------------------------------------------------------------------
# MAIN EXECUTION PIPELINE
# -------------------------------------------------------------------
is_intraday = (mode == "Intraday (5-min)")
df_spot = fetch_market_data(spot_ticker, "5d" if is_intraday else "1y", "5m" if is_intraday else "1d")
df_fut = fetch_market_data(fut_ticker, "5d" if is_intraday else "1y", "5m" if is_intraday else "1d")

if df_spot.empty or len(df_spot) < 30:
    st.error("Data loading. Please refresh in a moment.")
    st.stop()

df = compute_dynamic_indicators(df_spot, df_fut)
vix_val, vix_chg = fetch_realtime_vix()
bull_hw, bear_hw = fetch_heavyweights()

latest = df.iloc[-1]
close_p = float(latest["Close"])
vwap_p = float(latest["VWAP"])
ema_f = float(latest["EMA_fast"])
ema_s = float(latest["EMA_slow"])
drop_pts = float(latest["Drop_From_High"])
rally_pts = float(latest["Rally_From_Low"])
adx_val = float(latest["ADX"])
plus_di = float(latest["Plus_DI"])
minus_di = float(latest["Minus_DI"])
atr = float(latest["ATR"])
atr_expansion = float(latest["ATR_Expansion"])

dynamic_multiplier = 1.3 if atr_expansion > 1.2 else (1.0 if atr_expansion >= 0.8 else 0.8)
dynamic_trigger_pts = max(10.0, atr * dynamic_multiplier)

signal, action = "NO TRADE", "WAIT"
primary_direction = "NEUTRAL"

if drop_pts >= dynamic_trigger_pts and close_p <= ema_f:
    signal = "BEARISH MOMENTUM DROP"; action = "PUT"; primary_direction = "BEARISH"
elif rally_pts >= dynamic_trigger_pts and close_p >= ema_f:
    signal = "BULLISH MOMENTUM RALLY"; action = "CALL"; primary_direction = "BULLISH"
elif ema_f < ema_s and close_p < vwap_p:
    signal = "EMA BEARISH CROSSOVER"; action = "PUT"; primary_direction = "BEARISH"
elif ema_f > ema_s and close_p > vwap_p:
    signal = "EMA BULLISH CROSSOVER"; action = "CALL"; primary_direction = "BULLISH"

confidence_score = 45.0
supporter_notes = [f"Dynamic Trigger: {dynamic_trigger_pts:.1f} pts"]

if primary_direction != "NEUTRAL":
    if primary_direction == "BEARISH":
        if close_p < vwap_p: confidence_score += 15.0; supporter_notes.append("Below VWAP")
        if minus_di > plus_di: confidence_score += 15.0; supporter_notes.append("-DI Dominant")
        if adx_val >= 20: confidence_score += 15.0; supporter_notes.append(f"ADX ({adx_val:.1f})")
        if bear_hw > bull_hw: confidence_score += 10.0; supporter_notes.append("Heavyweights Red")
    elif primary_direction == "BULLISH":
        if close_p > vwap_p: confidence_score += 15.0; supporter_notes.append("Above VWAP")
        if plus_di > minus_di: confidence_score += 15.0; supporter_notes.append("+DI Dominant")
        if adx_val >= 20: confidence_score += 15.0; supporter_notes.append(f"ADX ({adx_val:.1f})")
        if bull_hw > bear_hw: confidence_score += 10.0; supporter_notes.append("Heavyweights Green")

confidence_score = min(98.0, confidence_score)

vix_multiplier = 1.15 if vix_chg > 3.0 else 1.0
sl_dist = 0.85 * atr * vix_multiplier

if confidence_score >= 80: conviction_grade = "HIGH CONVICTION"; qty_multiplier = 1.0
elif confidence_score >= 60: conviction_grade = "MODERATE CONVICTION"; qty_multiplier = 0.7
else: conviction_grade = "SCALP CONVICTION"; qty_multiplier = 0.4

base_qty = max(1, int(capital * base_risk_pct / (sl_dist + 1e-9)))
final_qty = max(1, int(base_qty * qty_multiplier)) if action != "WAIT" else 0

note_summary = " + ".join(supporter_notes)
journal, exit_alerts = process_smart_exits_and_logger(signal, action, latest, index_choice, confidence_score, final_qty, note_summary)

# -------------------------------------------------------------------
# DASHBOARD DISPLAY & BACKTEST RESULTS
# -------------------------------------------------------------------
for alert in exit_alerts:
    st.toast(alert, icon="🔔")

st.subheader(f"⚡ {index_choice} | Mode: `{mode}` | Live ATR: `{atr:.1f}`")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Spot Price", f"₹{close_p:.1f}")
c2.metric("VWAP Level", f"₹{vwap_p:.1f}", f"{close_p - vwap_p:+.1f} pts")
c3.metric("Drop / Rally", f"-{drop_pts:.1f} / +{rally_pts:.1f}")
c4.metric("Conviction Grade", conviction_grade if signal != "NO TRADE" else "Neutral")
c5.metric("Supporter Score", f"{confidence_score:.0f}%")

st.info(f"**Live Engine Intelligence:** {note_summary} | India VIX: {vix_val:.2f} ({vix_chg:+.1f}%)")

if signal != "NO TRADE":
    st.success(f"🚨 **ENTRY SIGNAL: {signal} ({conviction_grade})** | **ACTION: {action}** | Qty: **{final_qty} Units**")
    st.write(f"**Entry:** ₹{close_p:.1f} | **Initial SL:** ₹{close_p+(sl_dist if action=='PUT' else -sl_dist):.1f} | **Target 1:** ₹{close_p+(-1.5*sl_dist if action=='PUT' else 1.5*sl_dist):.1f} | **Target 2:** ₹{close_p+(-2.5*sl_dist if action=='PUT' else 2.5*sl_dist):.1f}")
else:
    st.warning(f"**NO TRADE** | Price Action is consolidating within dynamic noise range ({dynamic_trigger_pts:.1f} pts).")

# -------------------------------------------------------------------
# HISTORICAL BACKTEST ENGINE (WIN RATE & PnL TABLE)
# -------------------------------------------------------------------
st.markdown("---")
st.subheader(f"📊 Historical Backtest Engine – Mode: `{mode}`")
st.caption("Available dataset par real-time strategy backtesting output.")

bt = run_dynamic_backtest(df, mode)
if bt:
    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Total Trades", bt["trades"])
    b2.metric("Wins", bt["wins"])
    b3.metric("Win Rate", f"{bt['winrate']:.1f}%")
    b4.metric("Total Net PnL", f"{bt['total_pnl']:+.1f} pts")
    
    st.markdown("#### 🔍 Historical Trade Details Table")
    st.dataframe(bt["details_df"], use_container_width=True)
else:
    st.info("Insufficient historical bars for backtest calculation.")

st.markdown("---")
st.subheader("📝 Live Auto-Logged Paper Journal & Positions")
if not journal.empty:
    st.dataframe(journal.tail(15), use_container_width=True)
