import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime, time
import os
import matplotlib.pyplot as plt

st.set_page_config(page_title="Adaptive Engine v4.1 (Optimized Logic)", layout="wide", initial_sidebar_state="expanded")
st.title("🛡️ Adaptive Engine v4.1 (Logic Optimized for Win-Rate)")
st.caption("Enhanced Combination Logic | Strict Engine Confirmation | Smart Zone Cluster | Correct VIX/Weekly Impact")

# -------------------------------------------------------------------
# CONSTANTS & WEIGHTS
# -------------------------------------------------------------------
JOURNAL_FILE = "paper_trade_journal.csv"
TOP_10 = {
    "HDFCBANK.NS": 11.2, "RELIANCE.NS": 9.8, "ICICIBANK.NS": 7.8, "INFY.NS": 5.8,
    "ITC.NS": 4.2, "TCS.NS": 4.0, "LT.NS": 3.8, "AXISBANK.NS": 3.3,
    "BHARTIARTL.NS": 3.2, "KOTAKBANK.NS": 2.9
}

SWING_WEIGHTS = {
    "Strong Trend": {"E1":0.13,"E2":0.13,"E3":0.15,"E4":0.10,"E5":0.29,"E6":0.20,"threshold":71},
    "Mild Trend":   {"E1":0.13,"E2":0.15,"E3":0.15,"E4":0.13,"E5":0.24,"E6":0.20,"threshold":74},
    "Range":        {"E1":0.10,"E2":0.18,"E3":0.12,"E4":0.22,"E5":0.18,"E6":0.20,"threshold":78},
    "High Volatility":{"E1":0.15,"E2":0.18,"E3":0.12,"E4":0.18,"E5":0.17,"E6":0.20,"threshold":80},
    "Transition":   {"E1":0.15,"E2":0.15,"E3":0.15,"E4":0.15,"E5":0.20,"E6":0.20,"threshold":82},
}

INTRADAY_WEIGHTS = {
    "Strong Trend": {"E1":0.15,"E2":0.08,"E3":0.18,"E4":0.15,"E5":0.24,"E6":0.20,"threshold":67},
    "Mild Trend":   {"E1":0.13,"E2":0.10,"E3":0.20,"E4":0.18,"E5":0.19,"E6":0.20,"threshold":71},
    "Range":        {"E1":0.10,"E2":0.10,"E3":0.20,"E4":0.25,"E5":0.15,"E6":0.20,"threshold":74},
    "High Volatility":{"E1":0.18,"E2":0.10,"E3":0.17,"E4":0.20,"E5":0.15,"E6":0.20,"threshold":77},
    "Transition":   {"E1":0.15,"E2":0.10,"E3":0.18,"E4":0.17,"E5":0.20,"E6":0.20,"threshold":79},
}

# -------------------------------------------------------------------
# SIDEBAR
# -------------------------------------------------------------------
st.sidebar.header("🕹️ Control Panel")
mode = st.sidebar.radio("Mode", ["Swing (Daily)", "Intraday (5-min)"], index=0)
index_choice = st.sidebar.radio("Index", ["Nifty 50", "Bank Nifty"], index=0)
ticker = "^NSEI" if index_choice == "Nifty 50" else "^NSEBANK"

capital = st.sidebar.number_input("Paper Capital (₹)", value=1_000_000, step=100_000)
risk_pct = st.sidebar.slider("Risk per Trade %", 0.5, 2.0, 1.0) / 100

st.sidebar.markdown("---")
st.sidebar.subheader("Option Chain / OI Walls")
max_call_oi = st.sidebar.number_input("Max Call OI", value=24500 if index_choice=="Nifty 50" else 52000)
max_put_oi  = st.sidebar.number_input("Max Put OI", value=24000 if index_choice=="Nifty 50" else 51000)
use_oi_filter = st.sidebar.checkbox("Use OI Wall Filter", value=True)

st.sidebar.markdown("---")
manual_fii = st.sidebar.number_input("FII Net (₹ Cr)", value=0.0)
manual_dii = st.sidebar.number_input("DII Net (₹ Cr)", value=0.0)
use_manual_flow = st.sidebar.checkbox("Use Manual FII/DII", value=False)

st.sidebar.markdown("---")
avoid_mon_fri = st.sidebar.checkbox("Avoid Mon/Fri (Swing)", value=True)
vix_spike_limit = st.sidebar.slider("VIX Change Limit %", 2.0, 6.0, 3.5)

# -------------------------------------------------------------------
# DATA & INDICATORS
# -------------------------------------------------------------------
@st.cache_data(ttl=300)
def get_data(ticker, interval="1d", period="1y"):
    df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df.dropna()

@st.cache_data(ttl=300)
def get_vix():
    v = yf.download("^INDIAVIX", period="3mo", interval="1d", progress=False, auto_adjust=True)
    if isinstance(v.columns, pd.MultiIndex):
        v.columns = v.columns.get_level_values(0)
    return v

@st.cache_data(ttl=300)
def get_heavyweight_data():
    try:
        return yf.download(list(TOP_10.keys()), period="20d", interval="1d", progress=False, auto_adjust=True, threads=True)
    except:
        return None

@st.cache_data(ttl=600)
def get_fii_dii():
    try:
        r = requests.get("https://fii-diidata.mrchartist.com/api/data", timeout=8)
        if r.status_code == 200:
            d = r.json()
            return {
                "fii_net": float(d.get("fn") or d.get("fii_net") or 0),
                "dii_net": float(d.get("dn") or d.get("dii_net") or 0),
                "pcr": float(d.get("pcr") or 1.0),
                "sentiment": float(d.get("sentiment_score") or 50)
            }
    except:
        pass
    return {"fii_net": 0.0, "dii_net": 0.0, "pcr": 1.0, "sentiment": 50.0}

def add_indicators(df, mode):
    df = df.copy()
    if mode == "Swing (Daily)":
        df["EMA_fast"] = df["Close"].ewm(span=20, adjust=False).mean()
        df["EMA_mid"]  = df["Close"].ewm(span=50, adjust=False).mean()
        df["EMA_slow"] = df["Close"].ewm(span=200, adjust=False).mean()
        rlen, alen = 14, 14
    else:
        df["EMA_fast"] = df["Close"].ewm(span=9, adjust=False).mean()
        df["EMA_mid"]  = df["Close"].ewm(span=21, adjust=False).mean()
        df["EMA_slow"] = df["Close"].ewm(span=50, adjust=False).mean()
        rlen, alen = 9, 10

    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(rlen).mean()
    loss = (-delta.clip(upper=0)).rolling(rlen).mean()
    df["RSI"] = 100 - (100 / (1 + gain / loss))

    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift()).abs(),
        (df["Low"] - df["Close"].shift()).abs()
    ], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(alen).mean()
    df["ATR_Pct"] = df["ATR"].rank(pct=True) * 100
    df["Vol_Avg"] = df["Volume"].rolling(20).mean()

    up = df["High"] - df["High"].shift(1)
    dn = df["Low"].shift(1) - df["Low"]
    pos = np.where((up > dn) & (up > 0), up, 0.0)
    neg = np.where((dn > up) & (dn > 0), dn, 0.0)
    pdi = 100 * pd.Series(pos, index=df.index).rolling(alen).mean() / df["ATR"]
    ndi = 100 * pd.Series(neg, index=df.index).rolling(alen).mean() / df["ATR"]
    dx = 100 * abs(pdi - ndi) / (pdi + ndi + 1e-9)
    df["ADX"] = dx.rolling(alen).mean()

    df["Vol_Delta"] = np.where(df["Close"] >= df["Open"], df["Volume"], -df["Volume"])
    df["CVD_Proxy"] = df["Vol_Delta"].rolling(5 if mode == "Swing (Daily)" else 8).sum()
    df["VWAP"] = (df["Volume"] * (df["High"] + df["Low"] + df["Close"]) / 3).cumsum() / df["Volume"].cumsum()
    df["VWAP_Dist"] = (df["Close"] - df["VWAP"]) / (df["ATR"] + 1e-9)
    return df.dropna()

def get_weekly_bias(df):
    if len(df) < 30:
        return 0, "Neutral"
    weekly = df["Close"].resample("W").last().dropna()
    if len(weekly) < 5:
        return 0, "Neutral"
    wema = weekly.ewm(span=5, adjust=False).mean()
    last, prev, ema = weekly.iloc[-1], weekly.iloc[-2], wema.iloc[-1]
    if last > ema and last > prev:
        return 4, "Bullish"
    if last < ema and last < prev:
        return -4, "Bearish"
    return 0, "Neutral"

def calculate_real_breadth(raw):
    if raw is None or raw.empty:
        return 50.0, 50.0, 0.0
    try:
        closes = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
        total_w = sum(TOP_10.values())
        bull = bear = 0.0
        cnt = 0
        for t, w in TOP_10.items():
            if t not in closes.columns:
                continue
            s = closes[t].dropna()
            if len(s) < 21:
                continue
            ema = s.ewm(span=20, adjust=False).mean()
            lc, pc, le = s.iloc[-1], s.iloc[-2], ema.iloc[-1]
            cnt += 1
            if lc > le:
                bull += w
                if lc > pc: bull += w * 0.15
            else:
                bear += w
                if lc < pc: bear += w * 0.15
        if cnt < 6:
            return 50.0, 50.0, 0.0
        b = (bull / total_w) * 100
        r = (bear / total_w) * 100
        return round(b, 1), round(r, 1), round(b - r, 1)
    except:
        return 50.0, 50.0, 0.0

def get_vix_regime(vix_level, vix_chg):
    if vix_level >= 20 or vix_chg > 4.0:
        return "Fear", -8, "High caution"
    if vix_level >= 15 or vix_chg > 2.5:
        return "Elevated", -4, "Be selective"
    if vix_level <= 12 and vix_chg < 0:
        return "Complacent", 3, "Trend favoured"
    return "Normal", 0, "Standard"

# -------------------------------------------------------------------
# ENGINE 6 - Dynamic Zone Clustering + Multi-TF
# -------------------------------------------------------------------
def find_swing_points(df, left=4, right=4):
    swings = []
    highs = df["High"].values
    lows = df["Low"].values
    for i in range(left, len(df) - right):
        if highs[i] == max(highs[i-left:i+right+1]):
            swings.append((df.index[i], highs[i], "high"))
        if lows[i] == min(lows[i-left:i+right+1]):
            swings.append((df.index[i], lows[i], "low"))
    return swings

def detect_zones(df):
    if len(df) < 30:
        return [], []
    swings = find_swing_points(df, left=4, right=4)
    atr = df["ATR"].iloc[-1]
    raw_demands, raw_supplies = [], []

    for idx, price, typ in swings[-12:]:
        loc = df.index.get_loc(idx)
        if loc < 3 or loc >= len(df) - 3:
            continue
        if typ == "low":
            future_move = df["Close"].iloc[loc+1:loc+6].max() - price
            vol_ratio = df["Volume"].iloc[loc] / (df["Vol_Avg"].iloc[loc] + 1e-9)
            strength = min(100, (future_move / (atr + 1e-9)) * 35 + min(vol_ratio, 2.5) * 15)
            if strength > 35:
                raw_demands.append({"price": price, "strength": strength})
        else:
            future_move = price - df["Close"].iloc[loc+1:loc+6].min()
            vol_ratio = df["Volume"].iloc[loc] / (df["Vol_Avg"].iloc[loc] + 1e-9)
            strength = min(100, (future_move / (atr + 1e-9)) * 35 + min(vol_ratio, 2.5) * 15)
            if strength > 35:
                raw_supplies.append({"price": price, "strength": strength})

    # OPTIMIZATION: Cluster zones closer than 0.5 * ATR to prevent duplicates
    def cluster_items(items):
        if not items: return []
        items = sorted(items, key=lambda x: x["price"])
        clustered = []
        curr = items[0]
        for next_item in items[1:]:
            if abs(next_item["price"] - curr["price"]) <= 0.5 * atr:
                curr = {"price": (curr["price"] + next_item["price"])/2, "strength": max(curr["strength"], next_item["strength"])}
            else:
                clustered.append(curr)
                curr = next_item
        clustered.append(curr)
        return clustered

    demands = sorted(cluster_items(raw_demands), key=lambda x: x["strength"], reverse=True)[:3]
    supplies = sorted(cluster_items(raw_supplies), key=lambda x: x["strength"], reverse=True)[:3]
    return demands, supplies

def detect_candle_pattern(row, prev1, prev2):
    o, h, l, c = row["Open"], row["High"], row["Low"], row["Close"]
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    rng = h - l + 1e-9

    if lower > body * 2.0 and lower > upper * 1.8 and body / rng < 0.35:
        return "Bullish Pinbar", 75
    if upper > body * 2.0 and upper > lower * 1.8 and body / rng < 0.35:
        return "Bearish Pinbar", 75
    if c > o and prev1["Close"] < prev1["Open"] and c >= prev1["Open"] and o <= prev1["Close"]:
        return "Bullish Engulfing", 70
    if c < o and prev1["Close"] > prev1["Open"] and c <= prev1["Open"] and o >= prev1["Close"]:
        return "Bearish Engulfing", 70
    if (prev2["Close"] < prev2["Open"] and abs(prev1["Close"] - prev1["Open"]) / (prev1["High"] - prev1["Low"] + 1e-9) < 0.3
        and c > o and c > (prev2["Open"] + prev2["Close"]) / 2):
        return "Morning Star-like", 72
    if (prev2["Close"] > prev2["Open"] and abs(prev1["Close"] - prev1["Open"]) / (prev1["High"] - prev1["Low"] + 1e-9) < 0.3
        and c < o and c < (prev2["Open"] + prev2["Close"]) / 2):
        return "Evening Star-like", 72
    if body / rng < 0.15:
        return "Doji", 50

    return "None", 35

def engine6_price_action(df_current, df_higher, regime, mode):
    if len(df_current) < 20:
        return 50, "Insufficient data", 0, 0

    demands, supplies = detect_zones(df_higher if df_higher is not None and len(df_higher) > 30 else df_current)
    latest = df_current.iloc[-1]
    prev1 = df_current.iloc[-2]
    prev2 = df_current.iloc[-3] if len(df_current) > 3 else prev1
    atr = latest["ATR"]
    close = latest["Close"]

    pattern, pat_score = detect_candle_pattern(latest, prev1, prev2)

    near_demand, near_supply = False, False
    best_dem_str, best_sup_str = 0, 0
    zone_info = "No clear zone"

    for z in demands:
        if abs(close - z["price"]) <= atr * 1.1:
            near_demand = True
            best_dem_str = max(best_dem_str, z["strength"])
            zone_info = f"Near Demand ({z['strength']:.0f})"
    for z in supplies:
        if abs(close - z["price"]) <= atr * 1.1:
            near_supply = True
            best_sup_str = max(best_sup_str, z["strength"])
            zone_info = f"Near Supply ({z['strength']:.0f})"

    loc_score = 40
    if near_demand and pattern in ["Bullish Pinbar", "Bullish Engulfing", "Morning Star-like", "Doji"]:
        loc_score = min(95, pat_score * 0.65 + best_dem_str * 0.35)
    elif near_supply and pattern in ["Bearish Pinbar", "Bearish Engulfing", "Evening Star-like", "Doji"]:
        loc_score = min(95, pat_score * 0.65 + best_sup_str * 0.35)
    elif near_demand or near_supply:
        loc_score = 50 + (best_dem_str + best_sup_str) * 0.15
    else:
        loc_score = pat_score * 0.40   # Unconfirmed pattern in middle of nowhere = Weak

    regime_mult = 1.0
    if regime == "Strong Trend":
        regime_mult = 1.12 if near_demand else (0.88 if near_supply else 1.0)
    elif regime == "Range":
        regime_mult = 1.10 if (near_demand or near_supply) else 0.90

    final_e6 = np.clip(loc_score * regime_mult, 5, 98)
    return round(final_e6, 1), f"{pattern} | {zone_info}", best_dem_str, best_sup_str

# -------------------------------------------------------------------
# ENGINES 1 - 5 (REGIME & LOGIC REFINED)
# -------------------------------------------------------------------
def detect_regime(row, mode):
    adx = row["ADX"]
    atr_pct = row["ATR_Pct"]
    close = row["Close"]
    if atr_pct > 85 or row.get("VIX_Chg", 0) > 4:
        return "High Volatility"
    thresh = 22 if mode == "Intraday (5-min)" else 25
    if adx >= thresh and ((close > row["EMA_mid"] > row["EMA_slow"]) or (close < row["EMA_mid"] < row["EMA_slow"])):
        return "Strong Trend"
    if adx >= 16:
        return "Mild Trend"
    if adx < 16:
        return "Range"
    return "Transition"

def engine1_score(row, regime):
    base = 55 + min(row["ADX"], 40)
    if regime in ["Strong Trend", "Range"]:
        base += 10
    return min(100, base)

def engine2_score(fii, dii, pcr, sentiment):
    net = fii + dii
    score = 50
    if net > 800: score += 28
    elif net > 300: score += 14
    elif net < -800: score -= 22
    elif net < -300: score -= 10
    if pcr < 0.75: score += 8
    elif pcr > 1.3: score -= 5
    score += (sentiment - 50) * 0.25
    return max(0, min(100, score))

def engine3_intraday(row, regime):
    cvd = row["CVD_Proxy"]
    dist = row["VWAP_Dist"]
    cvd_score = 50
    if cvd > 0:
        cvd_score = min(90, 55 + abs(cvd) / (row["Vol_Avg"] + 1) * 25)
    else:
        cvd_score = max(10, 45 - abs(cvd) / (row["Vol_Avg"] + 1) * 25)
    
    # OPTIMIZATION: In Range regime, distance from VWAP means Overbought/Oversold (Reversal Bonus)
    if regime == "Range":
        dist_score = 50 - np.clip(dist * 12, -30, 30)
    else:
        dist_score = 50 + np.clip(dist * 12, -30, 30)

    return max(0, min(100, cvd_score * 0.55 + dist_score * 0.45))

def engine3_swing(net):
    return max(0, min(100, 50 + net * 0.7))

def engine4_trap(row, price_change, vix_chg, regime, mode):
    atr = float(row["ATR"])
    close = float(row["Close"])
    cvd = row["CVD_Proxy"]
    vol = row["Volume"]
    vol_avg = row["Vol_Avg"]
    atr_pct = row["ATR_Pct"]

    if mode == "Swing (Daily)":
        base_mult, pct, min_pct = 0.90, 0.0045, 0.003
    else:
        base_mult = 0.78 if atr_pct < 30 else (0.55 if atr_pct > 70 else 0.65)
        pct, min_pct = 0.0011, 0.0007

    threshold = max((base_mult * atr + close * pct) / 2, close * min_pct)
    if regime == "High Volatility": threshold *= 1.15
    elif regime == "Range": threshold *= 0.88

    # OPTIMIZATION: Genuine Breakdown requires Volume confirmation (> 1.25x Vol_Avg)
    if price_change <= -threshold:
        if vix_chg >= 1.8 and cvd < 0 and vol > 1.25 * vol_avg:
            return 25, "Genuine Breakdown", "PUT"
        return 88, "Bear Trap", "CALL"
    elif price_change >= threshold:
        if vix_chg <= 1.2 and cvd > 0 and vol > 1.25 * vol_avg:
            return 82, "Genuine Breakout", "CALL"
        return 22, "Bull Trap", "PUT"
    return 50, "Normal", "WAIT"

def engine5_technical(row, regime, mode):
    score = 0
    if row["Close"] > row["EMA_fast"] > row["EMA_mid"] > row["EMA_slow"]:
        score += 35
    elif row["Close"] > row["EMA_fast"] > row["EMA_slow"]:
        score += 25
    elif row["Close"] > row["EMA_fast"]:
        score += 12

    # OPTIMIZATION: RSI Regime Specific Dynamic Ranges
    rsi = row["RSI"]
    if regime == "Strong Trend":
        if 55 <= rsi <= 75: score += 25
        elif 45 <= rsi < 55: score += 12
    else:
        if 48 <= rsi <= 65: score += 25
        elif 42 <= rsi <= 70: score += 12

    if row["Volume"] > (1.25 if mode == "Swing (Daily)" else 1.4) * row["Vol_Avg"]:
        score += 20
    if row["Close"] >= row["VWAP"]:
        score += 20
    return min(100, score)

# -------------------------------------------------------------------
# BACKTEST
# -------------------------------------------------------------------
def run_backtest(df, mode):
    if len(df) < 80:
        return None
    trades = []
    i = 40
    while i < len(df) - 8:
        row = df.iloc[i]
        score = 0
        if row["Close"] > row["EMA_fast"] > row["EMA_mid"]: score += 40
        if 50 <= row["RSI"] <= 68: score += 30
        if row["Volume"] > 1.2 * row["Vol_Avg"]: score += 30
        if score >= 70:
            entry = df.iloc[i+1]["Open"]
            atr = row["ATR"]
            sl_mult = 1.25 if mode == "Swing (Daily)" else 0.9
            sl = entry - sl_mult * atr
            t1 = entry + 1.6 * (entry - sl)
            exited = False
            for j in range(i+2, min(i+18, len(df))):
                if df.iloc[j]["Low"] <= sl:
                    trades.append({"pnl": sl - entry, "win": False})
                    i = j
                    exited = True
                    break
                if df.iloc[j]["High"] >= t1:
                    trades.append({"pnl": t1 - entry, "win": True})
                    i = j
                    exited = True
                    break
            if not exited:
                pnl = df.iloc[min(i+15, len(df)-1)]["Close"] - entry
                trades.append({"pnl": pnl, "win": pnl > 0})
                i += 4
        else:
            i += 1
    if not trades:
        return None
    tdf = pd.DataFrame(trades)
    return {
        "trades": len(tdf),
        "wins": int(tdf["win"].sum()),
        "winrate": tdf["win"].mean() * 100,
        "avg_pnl": tdf["pnl"].mean(),
        "equity": np.cumsum(tdf["pnl"])
    }

# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------
if mode == "Swing (Daily)":
    df = get_data(ticker, "1d", "1y")
    df_higher = get_data(ticker, "1d", "2y")
else:
    df = get_data(ticker, "5m", "5d")
    df_higher = get_data(ticker, "1d", "6mo")

df = add_indicators(df, mode)
if df_higher is not None and len(df_higher) > 30:
    df_higher = add_indicators(df_higher, "Swing (Daily)")

vix_df = get_vix()
vix_level = float(vix_df["Close"].iloc[-1]) if not vix_df.empty else 14.0
vix_chg = float(vix_df["Close"].pct_change().iloc[-1] * 100) if len(vix_df) > 1 else 0.0

latest = df.iloc[-1]
prev = df.iloc[-2]
price_change = float(latest["Close"] - prev["Close"])

regime = detect_regime(latest, mode)
weights = (SWING_WEIGHTS if mode == "Swing (Daily)" else INTRADAY_WEIGHTS).get(regime, SWING_WEIGHTS["Transition"])

api = get_fii_dii()
fii = manual_fii if use_manual_flow else api["fii_net"]
dii = manual_dii if use_manual_flow else api["dii_net"]
pcr = api["pcr"]
sentiment = api["sentiment"]

if mode == "Swing (Daily)":
    _, _, net_b = calculate_real_breadth(get_heavyweight_data())
    e3 = engine3_swing(net_b)
else:
    net_b = 0.0
    e3 = engine3_intraday(latest, regime)

weekly_score, weekly_bias = get_weekly_bias(df if mode == "Swing (Daily)" else get_data(ticker, "1d", "1y"))
vix_regime, vix_adj, vix_msg = get_vix_regime(vix_level, vix_chg)

e1 = engine1_score(latest, regime)
e2 = engine2_score(fii, dii, pcr, sentiment)
e4, e4_status, e4_action = engine4_trap(latest, price_change, vix_chg, regime, mode)
e5 = engine5_technical(latest, regime, mode)
e6, e6_detail, dem_str, sup_str = engine6_price_action(df, df_higher, regime, mode)

# Base Weighted Engines Score
raw_engine_score = (e1*weights["E1"] + e2*weights["E2"] + e3*weights["E3"] +
                    e4*weights["E4"] + e5*weights["E5"] + e6*weights["E6"])

# OPTIMIZATION: Direct Additive/Subtractor modifiers instead of Compression Average
final_score = raw_engine_score + weekly_score + vix_adj

# Soft Time Window Penalty (Intraday)
now = datetime.now().time()
time_penalty = 0
if mode == "Intraday (5-min)" and time(11, 30) <= now <= time(13, 30):
    time_penalty = -10 if regime != "Range" else -16
final_score += time_penalty

oi_bias = 0
if use_oi_filter:
    if latest["Close"] > max_call_oi - 40: oi_bias = -10
    elif latest["Close"] < max_put_oi + 40: oi_bias = 8
final_score += oi_bias

threshold = weights["threshold"]

filter_pass = True
msgs = []
if mode == "Swing (Daily)" and avoid_mon_fri and datetime.now().weekday() in [0, 4]:
    filter_pass = False
    msgs.append("Mon/Fri")
if vix_chg > vix_spike_limit:
    filter_pass = False
    msgs.append(f"VIX {vix_chg:.1f}%")
if vix_regime == "Fear":
    msgs.append("VIX Fear")

# OPTIMIZATION: Strict Signal Generation Logic
signal = "NO TRADE"
action = "WAIT"

if filter_pass and final_score >= threshold:
    if e4_action in ["CALL", "PUT"] and e4 >= 70:
        signal = f"TRAP → {e4_action}"
        action = e4_action
    elif final_score >= threshold + 6 and e6 >= 60:  # Must have PA Engine approval for High Conviction
        signal = "HIGH CONVICTION LONG"
        action = "CALL"
    else:
        signal = "MODERATE LONG"
        action = "CALL"

atr = float(latest["ATR"])
sl_mult = 1.25 if mode == "Swing (Daily)" else 0.9
sl_dist = sl_mult * atr
qty = max(1, int(capital * risk_pct / sl_dist)) if sl_dist > 0 else 0

# -------------------------------------------------------------------
# UI DASHBOARD
# -------------------------------------------------------------------
st.subheader(f"{index_choice} | {mode} | Regime: `{regime}` | VIX: `{vix_regime}` | Weekly: `{weekly_bias}`")

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Final Score", f"{final_score:.1f}", f"Thr {threshold}")
m2.metric("Price", f"₹{latest['Close']:.1f}", f"{price_change:+.1f}")
m3.metric("E6 (PA)", f"{e6:.0f}")
m4.metric("VIX", f"{vix_level:.1f}", f"{vix_chg:+.1f}%")
m5.metric("Time Penalty", f"{time_penalty}")

st.info(f"Engine6 Detail: **{e6_detail}** | Demand Str: {dem_str:.0f} | Supply Str: {sup_str:.0f} | {vix_msg}")
st.write(f"Engines → E1:{e1:.0f} | E2:{e2:.0f} | E3:{e3:.0f} | E4:{e4:.0f} ({e4_status}) | E5:{e5:.0f} | **E6:{e6:.0f}**")

if signal != "NO TRADE":
    st.success(f"**{signal}** | **{action}** | Qty: **{qty}**")
    st.write(f"SL: ₹{latest['Close']-sl_dist:.1f} | T1: ₹{latest['Close']+1.5*sl_dist:.1f} | T2: ₹{latest['Close']+2.3*sl_dist:.1f}")
else:
    st.warning(f"**NO TRADE** | {', '.join(msgs) if msgs else 'Below threshold / PA Conflict'}")

# Backtest Snapshot
st.markdown("---")
st.subheader("📊 Backtest Snapshot")
bt = run_backtest(df, mode)
if bt:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Trades", bt["trades"])
    c2.metric("Wins", bt["wins"])
    c3.metric("Win Rate", f"{bt['winrate']:.1f}%")
    c4.metric("Avg PnL", f"{bt['avg_pnl']:.1f}")
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.plot(bt["equity"], color="#00aa55", linewidth=1.5)
    ax.set_title(f"Equity Curve – {mode}")
    ax.grid(True, alpha=0.3)
    st.pyplot(fig)
    plt.close()

# Journal Log
st.markdown("---")
st.subheader("📝 Paper Journal")
if not os.path.exists(JOURNAL_FILE):
    pd.DataFrame(columns=["Date","Mode","Index","Regime","Score","Signal","Action","Entry","SL","T1","T2","Exit","PnL","Result","E6","Notes"]).to_csv(JOURNAL_FILE, index=False)
journal = pd.read_csv(JOURNAL_FILE)

if st.button("📥 Log Paper Entry", use_container_width=True):
    new = {
        "Date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Mode": mode, "Index": index_choice, "Regime": regime,
        "Score": round(final_score, 1), "Signal": signal, "Action": action,
        "Entry": round(float(latest["Close"]), 1),
        "SL": round(float(latest["Close"] - sl_dist), 1),
        "T1": round(float(latest["Close"] + 1.5 * sl_dist), 1),
        "T2": round(float(latest["Close"] + 2.3 * sl_dist), 1),
        "Exit": "", "PnL": "", "Result": "Open",
        "E6": e6, "Notes": e6_detail
    }
    journal = pd.concat([journal, pd.DataFrame([new])], ignore_index=True)
    journal.to_csv(JOURNAL_FILE, index=False)
    st.success("Logged!")
    st.rerun()

st.dataframe(journal.tail(10), use_container_width=True)
