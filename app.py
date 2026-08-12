import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime, time
import os
import matplotlib.pyplot as plt

# -------------------------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------------------------
st.set_page_config(page_title="Adaptive Engine v5.3 (Auto-Logger)", layout="wide", initial_sidebar_state="expanded")
st.title("🛡️ Adaptive Engine v5.3 (Automatic Live Paper Logger)")
st.caption("E6 Gate | Regime VWAP | Volume Traps | Auto Signal Logging | Auto SL/TP Tracking")

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
# DATA FUNCTIONS
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
    pdi = 100 * pd.Series(pos, index=df.index).rolling(alen).mean() / (df["ATR"] + 1e-9)
    ndi = 100 * pd.Series(neg, index=df.index).rolling(alen).mean() / (df["ATR"] + 1e-9)
    dx = 100 * abs(pdi - ndi) / (pdi + ndi + 1e-9)
    df["ADX"] = dx.rolling(alen).mean()

    df["Vol_Delta"] = np.where(df["Close"] >= df["Open"], df["Volume"], -df["Volume"])
    df["CVD_Proxy"] = df["Vol_Delta"].rolling(5 if mode == "Swing (Daily)" else 8).sum()

    if df["Volume"].iloc[-5:].mean() < 10 or df["Volume"].sum() < 1:
        tp = (df["High"] + df["Low"] + df["Close"]) / 3
        df["VWAP"] = tp.cumsum() / np.arange(1, len(df)+1)
        df["VWAP_Reliable"] = False
    else:
        df["VWAP"] = (df["Volume"] * (df["High"] + df["Low"] + df["Close"]) / 3).cumsum() / df["Volume"].cumsum()
        df["VWAP_Reliable"] = True

    df["VWAP_Dist"] = (df["Close"] - df["VWAP"]) / (df["ATR"] + 1e-9)
    return df.dropna()

def get_weekly_bias(df):
    if df is None or len(df) < 30: return 50, "Neutral"
    weekly = df["Close"].resample("W").last().dropna()
    if len(weekly) < 5: return 50, "Neutral"
    wema = weekly.ewm(span=5, adjust=False).mean()
    last, prev, ema = weekly.iloc[-1], weekly.iloc[-2], wema.iloc[-1]
    if last > ema and last > prev: return 75, "Bullish"
    if last < ema and last < prev: return 25, "Bearish"
    return 50, "Neutral"

def calculate_real_breadth(raw):
    if raw is None or raw.empty: return 50.0, 50.0, 0.0
    try:
        closes = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
        total_w = sum(TOP_10.values())
        bull = bear = 0.0
        cnt = 0
        for t, w in TOP_10.items():
            if t not in closes.columns: continue
            s = closes[t].dropna()
            if len(s) < 21: continue
            ema = s.ewm(span=20, adjust=False).mean()
            lc, pc, le = s.iloc[-1], s.iloc[-2], ema.iloc[-1]
            cnt += 1
            if lc > le:
                bull += w
                if lc > pc: bull += w * 0.15
            else:
                bear += w
                if lc < pc: bear += w * 0.15
        if cnt < 6: return 50.0, 50.0, 0.0
        b = (bull / total_w) * 100
        r = (bear / total_w) * 100
        return round(b,1), round(r,1), round(b-r,1)
    except:
        return 50.0, 50.0, 0.0

def get_vix_regime(vix_level, vix_chg):
    if vix_level >= 20 or vix_chg > 4.0: return "Fear", -15, "High caution"
    if vix_level >= 15 or vix_chg > 2.5: return "Elevated", -8, "Be selective"
    if vix_level <= 12 and vix_chg < 0: return "Complacent", 5, "Trend favoured"
    return "Normal", 0, "Standard"

# -------------------------------------------------------------------
# ENGINE 6
# -------------------------------------------------------------------
def find_swings(df, left=4, right=4):
    swings = []
    h, l = df["High"].values, df["Low"].values
    for i in range(left, len(df)-right):
        if h[i] == max(h[i-left:i+right+1]): swings.append((df.index[i], h[i], "high"))
        if l[i] == min(l[i-left:i+right+1]): swings.append((df.index[i], l[i], "low"))
    return swings

def cluster_zones(zones, atr, thresh=0.5):
    if not zones: return []
    zones = sorted(zones, key=lambda x: x["price"])
    clustered = []
    current = zones[0].copy()
    for z in zones[1:]:
        if abs(z["price"] - current["price"]) <= atr * thresh:
            current["strength"] = max(current["strength"], z["strength"])
            current["price"] = (current["price"] + z["price"]) / 2
        else:
            clustered.append(current)
            current = z.copy()
    clustered.append(current)
    return sorted(clustered, key=lambda x: x["strength"], reverse=True)[:3]

def detect_zones(df):
    if len(df) < 30: return [], []
    swings = find_swings(df)
    atr = df["ATR"].iloc[-1]
    demands, supplies = [], []
    for idx, price, typ in swings[-15:]:
        loc = df.index.get_loc(idx)
        if loc < 3 or loc >= len(df)-3: continue
        vol_ratio = df["Volume"].iloc[loc] / (df["Vol_Avg"].iloc[loc] + 1e-9)
        if typ == "low":
            move = df["Close"].iloc[loc+1:loc+6].max() - price
            strength = min(100, (move/(atr+1e-9))*35 + min(vol_ratio,2.5)*15)
            if strength > 35: demands.append({"price": price, "strength": strength})
        else:
            move = price - df["Close"].iloc[loc+1:loc+6].min()
            strength = min(100, (move/(atr+1e-9))*35 + min(vol_ratio,2.5)*15)
            if strength > 35: supplies.append({"price": price, "strength": strength})
    return cluster_zones(demands, atr), cluster_zones(supplies, atr)

def detect_candle(row, prev1, prev2):
    o,h,l,c = row["Open"], row["High"], row["Low"], row["Close"]
    body, upper, lower = abs(c-o), h-max(o,c), min(o,c)-l
    rng = h-l + 1e-9
    if lower > body*2 and lower > upper*1.8 and body/rng < 0.35: return "Bullish Pinbar", 75
    if upper > body*2 and upper > lower*1.8 and body/rng < 0.35: return "Bearish Pinbar", 75
    if c>o and prev1["Close"]<prev1["Open"] and c>=prev1["Open"] and o<=prev1["Close"]: return "Bullish Engulfing", 70
    if c<o and prev1["Close"]>prev1["Open"] and c<=prev1["Open"] and o>=prev1["Close"]: return "Bearish Engulfing", 70
    if (prev2["Close"]<prev2["Open"] and abs(prev1["Close"]-prev1["Open"])/(prev1["High"]-prev1["Low"]+1e-9)<0.3 and c>o and c>(prev2["Open"]+prev2["Close"])/2):
        return "Morning Star-like", 72
    if (prev2["Close"]>prev2["Open"] and abs(prev1["Close"]-prev1["Open"])/(prev1["High"]-prev1["Low"]+1e-9)<0.3 and c<o and c<(prev2["Open"]+prev2["Close"])/2):
        return "Evening Star-like", 72
    if body/rng < 0.15: return "Doji", 55
    return "None", 40

def engine6(df_cur, df_htf, regime, mode):
    if len(df_cur) < 20: return 50, "No data", 0, 0
    demands, supplies = detect_zones(df_htf if df_htf is not None and len(df_htf)>30 else df_cur)
    latest, prev1 = df_cur.iloc[-1], df_cur.iloc[-2]
    prev2 = df_cur.iloc[-3] if len(df_cur)>3 else prev1
    atr, close = latest["ATR"], latest["Close"]
    pattern, pat_score = detect_candle(latest, prev1, prev2)

    near_dem = near_sup = False
    best_d = best_s = 0
    info = "No zone"
    for z in demands:
        if abs(close - z["price"]) <= atr*1.1:
            near_dem = True
            best_d = max(best_d, z["strength"])
            info = f"Near Demand {z['strength']:.0f}"
    for z in supplies:
        if abs(close - z["price"]) <= atr*1.1:
            near_sup = True
            best_s = max(best_s, z["strength"])
            info = f"Near Supply {z['strength']:.0f}"

    if near_dem and pattern in ["Bullish Pinbar","Bullish Engulfing","Morning Star-like","Doji"]:
        loc = min(95, pat_score*0.7 + best_d*0.35)
    elif near_sup and pattern in ["Bearish Pinbar","Bearish Engulfing","Evening Star-like","Doji"]:
        loc = min(95, pat_score*0.7 + best_s*0.35)
    elif near_dem or near_sup:
        loc = 55 + (best_d + best_s)*0.15
    else:
        loc = pat_score * 0.45

    mult = 1.0
    if regime == "Strong Trend": mult = 1.15 if near_dem else (0.85 if near_sup else 1.0)
    elif regime == "Range": mult = 1.10 if (near_dem or near_sup) else 0.95
    elif regime == "High Volatility": mult = 0.90

    return round(np.clip(loc*mult, 5, 98),1), f"{pattern} | {info}", best_d, best_s

# -------------------------------------------------------------------
# OTHER ENGINES
# -------------------------------------------------------------------
def detect_regime(row, mode):
    if row["ATR_Pct"] > 85 or row.get("VIX_Chg",0) > 4: return "High Volatility"
    th = 22 if mode == "Intraday (5-min)" else 25
    if row["ADX"] >= th and ((row["Close"]>row["EMA_mid"]>row["EMA_slow"]) or (row["Close"]<row["EMA_mid"]<row["EMA_slow"])):
        return "Strong Trend"
    if row["ADX"] >= 16: return "Mild Trend"
    if row["ADX"] < 16: return "Range"
    return "Transition"

def engine1(row, regime):
    base = 55 + min(row["ADX"],40)
    if regime in ["Strong Trend","Range"]: base += 10
    return min(100, base)

def engine2(fii, dii, pcr, sentiment):
    net = fii + dii
    score = 50
    if net > 800: score += 28
    elif net > 300: score += 14
    elif net < -800: score -= 22
    elif net < -300: score -= 10
    if pcr < 0.75: score += 8
    elif pcr > 1.3: score -= 5
    score += (sentiment-50)*0.25
    return max(0, min(100, score))

def engine3_intraday(row, regime):
    cvd = row["CVD_Proxy"]
    dist = row["VWAP_Dist"]
    cvd_score = 50 + np.clip(cvd / (row["Vol_Avg"]+1)*20, -30, 30)
    if regime in ["Range", "High Volatility"]: dist_score = 50 + min(abs(dist)*10, 25)
    else: dist_score = 50 + np.clip(dist*12, -30, 30)
    return max(0, min(100, cvd_score*0.55 + dist_score*0.45))

def engine3_swing(net):
    return max(0, min(100, 50 + net*0.7))

def engine4(row, price_change, vix_chg, regime, mode):
    atr, close, cvd = float(row["ATR"]), float(row["Close"]), row["CVD_Proxy"]
    atr_pct, vol_ok = row["ATR_Pct"], row["Volume"] > 1.3 * row["Vol_Avg"]

    if mode == "Swing (Daily)": base_mult, pct, min_pct = 0.90, 0.0045, 0.003
    else:
        base_mult = 0.78 if atr_pct < 30 else (0.55 if atr_pct > 70 else 0.65)
        pct, min_pct = 0.0011, 0.0007

    threshold = max((base_mult*atr + close*pct)/2, close*min_pct)
    if regime == "High Volatility": threshold *= 1.15
    elif regime == "Range": threshold *= 0.88

    if price_change <= -threshold:
        if vix_chg >= 1.8 and cvd < 0 and vol_ok: return 25, "Genuine Breakdown", "PUT"
        return 88, "Bear Trap", "CALL"
    elif price_change >= threshold:
        if vix_chg <= 1.2 and cvd > 0 and vol_ok: return 82, "Genuine Breakout", "CALL"
        return 22, "Bull Trap", "PUT"
    return 50, "Normal", "WAIT"

def engine5(row, regime, mode):
    score = 0
    if row["Close"] > row["EMA_fast"] > row["EMA_mid"] > row["EMA_slow"]: score += 35
    elif row["Close"] > row["EMA_fast"] > row["EMA_slow"]: score += 25
    elif row["Close"] > row["EMA_fast"]: score += 12

    rsi = row["RSI"]
    if regime == "Strong Trend":
        if 60 <= rsi <= 75: score += 25
        elif 52 <= rsi < 60: score += 15
    elif regime == "Range":
        if 48 <= rsi <= 62: score += 25
        elif 40 <= rsi < 48 or 62 < rsi <= 70: score += 10
    else:
        if 52 <= rsi <= 68: score += 25
        elif 45 <= rsi <= 72: score += 12

    if row["Volume"] > (1.25 if mode=="Swing (Daily)" else 1.4)*row["Vol_Avg"]: score += 20
    if row.get("VWAP_Reliable", True) and row["Close"] >= row["VWAP"]: score += 15
    return min(100, score)

# -------------------------------------------------------------------
# AUTOMATIC PAPER TRADE LOGGER & SL/TARGET TRACKER
# -------------------------------------------------------------------
def process_auto_logger(signal, action, latest_row, mode, index_choice, regime, final_score, e6_detail):
    cols = ["Date", "Mode", "Index", "Regime", "Score", "Signal", "Action", "Entry", "SL", "T1", "T2", "Exit", "PnL", "Result", "Notes"]
    if not os.path.exists(JOURNAL_FILE):
        pd.DataFrame(columns=cols).to_csv(JOURNAL_FILE, index=False)
    
    journal = pd.read_csv(JOURNAL_FILE)
    if "Result" not in journal.columns:
        return journal

    latest_date_str = latest_row.name.strftime("%Y-%m-%d %H:%M")
    latest_close = round(float(latest_row["Close"]), 1)
    latest_high = float(latest_row["High"])
    latest_low = float(latest_row["Low"])
    atr = float(latest_row["ATR"])
    sl_dist = (1.25 if mode == "Swing (Daily)" else 0.9) * atr

    # 1. Update Open Trades for SL / Target Hits
    updated = False
    for idx, row in journal.iterrows():
        if row["Result"] == "Open":
            entry = float(row["Entry"])
            sl = float(row["SL"])
            t1 = float(row["T1"])
            act = str(row["Action"])
            
            if act == "CALL":
                if latest_low <= sl:
                    journal.at[idx, "Exit"] = sl
                    journal.at[idx, "PnL"] = round(sl - entry, 1)
                    journal.at[idx, "Result"] = "Loss"
                    updated = True
                elif latest_high >= t1:
                    journal.at[idx, "Exit"] = t1
                    journal.at[idx, "PnL"] = round(t1 - entry, 1)
                    journal.at[idx, "Result"] = "Win"
                    updated = True
            elif act == "PUT":
                if latest_high >= sl:
                    journal.at[idx, "Exit"] = sl
                    journal.at[idx, "PnL"] = round(entry - sl, 1)
                    journal.at[idx, "Result"] = "Loss"
                    updated = True
                elif latest_low <= t1:
                    journal.at[idx, "Exit"] = t1
                    journal.at[idx, "PnL"] = round(entry - t1, 1)
                    journal.at[idx, "Result"] = "Win"
                    updated = True

    # 2. Auto-Log New Signal
    if signal != "NO TRADE":
        # Check if already logged for this timestamp to avoid duplicates
        already_logged = False
        if not journal.empty:
            already_logged = ((journal["Date"] == latest_date_str) & (journal["Mode"] == mode) & (journal["Index"] == index_choice)).any()
        
        if not already_logged:
            sl_val = round(latest_close - sl_dist, 1) if action == "CALL" else round(latest_close + sl_dist, 1)
            t1_val = round(latest_close + 1.6 * sl_dist, 1) if action == "CALL" else round(latest_close - 1.6 * sl_dist, 1)
            t2_val = round(latest_close + 2.3 * sl_dist, 1) if action == "CALL" else round(latest_close - 2.3 * sl_dist, 1)
            
            new_entry = {
                "Date": latest_date_str,
                "Mode": mode,
                "Index": index_choice,
                "Regime": regime,
                "Score": round(final_score, 1),
                "Signal": signal,
                "Action": action,
                "Entry": latest_close,
                "SL": sl_val,
                "T1": t1_val,
                "T2": t2_val,
                "Exit": "",
                "PnL": "",
                "Result": "Open",
                "Notes": e6_detail
            }
            journal = pd.concat([journal, pd.DataFrame([new_entry])], ignore_index=True)
            updated = True

    if updated:
        journal.to_csv(JOURNAL_FILE, index=False)
        
    return journal

# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------
if mode == "Swing (Daily)":
    df = get_data(ticker, "1d", "1y")
    df_htf = get_data(ticker, "1d", "2y")
    daily_for_weekly = df
else:
    df = get_data(ticker, "5m", "5d")
    df_htf = get_data(ticker, "1d", "6mo")
    daily_for_weekly = df_htf

df = add_indicators(df, mode)
if df_htf is not None and len(df_htf) > 30:
    df_htf = add_indicators(df_htf, "Swing (Daily)")

vix_df = get_vix()
vix_level = float(vix_df["Close"].iloc[-1]) if not vix_df.empty else 14.0
vix_chg = float(vix_df["Close"].pct_change().iloc[-1]*100) if len(vix_df)>1 else 0.0

latest = df.iloc[-1]
prev = df.iloc[-2]
price_change = float(latest["Close"] - prev["Close"])
regime = detect_regime(latest, mode)
weights = (SWING_WEIGHTS if mode == "Swing (Daily)" else INTRADAY_WEIGHTS).get(regime, SWING_WEIGHTS["Transition"])

api = get_fii_dii()
fii = manual_fii if use_manual_flow else api["fii_net"]
dii = manual_dii if use_manual_flow else api["dii_net"]

if mode == "Swing (Daily)":
    _, _, net_b = calculate_real_breadth(get_heavyweight_data())
    e3 = engine3_swing(net_b)
else:
    e3 = engine3_intraday(latest, regime)

weekly_score, weekly_bias = get_weekly_bias(daily_for_weekly)
vix_regime, vix_adj, vix_msg = get_vix_regime(vix_level, vix_chg)

e1 = engine1(latest, regime)
e2 = engine2(fii, dii, api["pcr"], api["sentiment"])
e4, e4_status, e4_action = engine4(latest, price_change, vix_chg, regime, mode)
e5 = engine5(latest, regime, mode)
e6, e6_detail, dem_str, sup_str = engine6(df, df_htf, regime, mode)

engine_score = (e1*weights["E1"] + e2*weights["E2"] + e3*weights["E3"] +
                e4*weights["E4"] + e5*weights["E5"] + e6*weights["E6"])
final_score = engine_score + (weekly_score-50)*0.08 + vix_adj*0.6

now = datetime.now().time()
time_penalty = 0
if mode == "Intraday (5-min)" and time(11,30) <= now <= time(13,30):
    time_penalty = -10 if regime != "Range" else -16
final_score += time_penalty

oi_bias = 0
if use_oi_filter:
    if latest["Close"] > max_call_oi - 40: oi_bias = -12
    elif latest["Close"] < max_put_oi + 40: oi_bias = 8
final_score += oi_bias

threshold = weights["threshold"]

filter_pass = True
msgs = []
if mode == "Swing (Daily)" and avoid_mon_fri and datetime.now().weekday() in [0,4]:
    filter_pass = False; msgs.append("Mon/Fri")
if vix_chg > vix_spike_limit:
    filter_pass = False; msgs.append(f"VIX {vix_chg:.1f}%")
if vix_regime == "Fear":
    msgs.append("VIX Fear")

signal, action = "NO TRADE", "WAIT"
if filter_pass and final_score >= threshold:
    if e4_action in ["CALL","PUT"] and e4 >= 70:
        signal = f"TRAP → {e4_action}"; action = e4_action
    elif final_score >= threshold + 6 and e6 >= 60:
        signal = "HIGH CONVICTION LONG"; action = "CALL"
    elif final_score >= threshold:
        signal = "MODERATE LONG"; action = "CALL"

atr = float(latest["ATR"])
sl_dist = (1.25 if mode == "Swing (Daily)" else 0.9) * atr
qty = max(1, int(capital * risk_pct / sl_dist)) if sl_dist > 0 else 0

# PROCESS AUTOMATIC LIVE LOGGER
journal = process_auto_logger(signal, action, latest, mode, index_choice, regime, final_score, e6_detail)

# -------------------------------------------------------------------
# UI DASHBOARD
# -------------------------------------------------------------------
st.subheader(f"{index_choice} | {mode} | Regime: `{regime}` | VIX: `{vix_regime}` | Weekly: `{weekly_bias}`")

c1,c2,c3,c4,c5 = st.columns(5)
c1.metric("Final Score", f"{final_score:.1f}", f"Thr {threshold}")
c2.metric("Price", f"₹{latest['Close']:.1f}", f"{price_change:+.1f}")
c3.metric("E6 (PA)", f"{e6:.0f}")
c4.metric("VIX", f"{vix_level:.1f}", f"{vix_chg:+.1f}%")
c5.metric("Time Penalty", f"{time_penalty}")

vwap_note = "VWAP Reliable" if latest.get("VWAP_Reliable", True) else "VWAP Fallback (low volume)"
st.info(f"E6: **{e6_detail}** | Dem:{dem_str:.0f} Sup:{sup_str:.0f} | {vix_msg} | {vwap_note}")

st.write(f"E1:{e1:.0f} | E2:{e2:.0f} | E3:{e3:.0f} | E4:{e4:.0f} ({e4_status}) | E5:{e5:.0f} | **E6:{e6:.0f}**")

if signal != "NO TRADE":
    st.success(f"**SIGNAL DETECTED: {signal}** | **{action}** | Qty: **{qty}** (Auto-Logged below)")
    st.write(f"SL: ₹{latest['Close']-sl_dist:.1f} | T1: ₹{latest['Close']+1.5*sl_dist:.1f} | T2: ₹{latest['Close']+2.3*sl_dist:.1f}")
else:
    st.warning(f"**NO TRADE** | {', '.join(msgs) if msgs else 'Below threshold / E6 gate'}")

# -------------------------------------------------------------------
# AUTOMATIC JOURNAL LOG TABLE
# -------------------------------------------------------------------
st.markdown("---")
st.subheader("📝 Live Auto-Logged Paper Journal")
st.caption("System live market signals ko automatic log karta hai aur unke SL/Targets monitor karke Result bharta hai.")

if not journal.empty:
    st.dataframe(journal.tail(15), use_container_width=True)
    
    # Live Journal Performance
    closed_trades = journal[journal["Result"].isin(["Win", "Loss"])]
    if not closed_trades.empty:
        total_t = len(closed_trades)
        win_t = len(closed_trades[closed_trades["Result"] == "Win"])
        wr = (win_t / total_t) * 100
        net_pnl = closed_trades["PnL"].sum()
        st.markdown(f"**Live Paper Performance:** Total Trades: `{total_t}` | Win Rate: `{wr:.1f}%` | Net Points PnL: `{net_pnl:+.1f}`")
else:
    st.info("Abhi tak koi live signal trigger nahi hua hai.")
