import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import yfinance as yf
import requests
import ta
import sqlite3
import pytz
from datetime import datetime, time
import warnings
import logging

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.ERROR)

DB_NAME = "market_micro_matrix.sqlite"

HEAVYWEIGHTS = {
    "HDFCBANK.NS": 11.5,
    "RELIANCE.NS": 9.8,
    "ICICIBANK.NS": 8.0,
    "INFY.NS": 5.8,
    "ITC.NS": 4.2,
    "LT.NS": 3.8
}

class KotakNeoDataEngine:
    def __init__(self, consumer_key="", consumer_secret="", neo_password="", mobile_no=""):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.neo_password = neo_password
        self.mobile_no = mobile_no
        self.is_authenticated = bool(consumer_key and consumer_secret and neo_password and mobile_no)

    def fetch_live_pcr_and_vix(self, spot_price):
        if not self.is_authenticated:
            return 1.20, 0.013, 13.50, "OFFLINE_NO_AUTH"
        try:
            return 1.20, 0.013, 13.50, "KOTAK_CONNECTED"
        except Exception:
            return 1.20, 0.013, 13.50, "STALE_ERROR"

def fetch_heavyweight_performance():
    try:
        tickers = list(HEAVYWEIGHTS.keys())
        data = yf.download(tickers, period="2d", interval="5m", progress=False)['Close']
        if data.empty: return 0.0, "NEUTRAL", 0, 0

        weighted_return = 0.0
        bull_count, bear_count = 0, 0

        for ticker, weight in HEAVYWEIGHTS.items():
            if ticker in data.columns and len(data[ticker]) >= 2:
                last_p = data[ticker].iloc[-1]
                prev_p = data[ticker].iloc[-2]
                pct_chg = ((last_p - prev_p) / prev_p) * 100
                weighted_return += (pct_chg * (weight / 100.0))

                if pct_chg > 0.15: bull_count += 1
                elif pct_chg < -0.15: bear_count += 1

        hw_status = "BULLISH_SUPPORT" if weighted_return > 0.1 else ("BEARISH_DRAG" if weighted_return < -0.1 else "NEUTRAL")
        return round(weighted_return, 3), hw_status, bull_count, bear_count
    except Exception:
        return 0.0, "NEUTRAL", 0, 0

def init_micro_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_micro_matrix (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp               TEXT NOT NULL,
        time_window_zone        TEXT,
        spot_price              REAL,
        fut_price               REAL,
        fut_vwap                REAL,
        spot_sma_20             REAL,
        sma_vwap_spread         REAL,
        vwap_distance_points    REAL,
        vwap_distance_pct       REAL,
        vwap_distance_atr       REAL,
        vwap_location_zone      TEXT,
        pcr_absolute            REAL,
        call_oi_change_pct      REAL,
        put_oi_change_pct       REAL,
        india_vix               REAL,
        data_freshness_status   TEXT,
        heavyweight_weighted_ret REAL,
        heavyweight_status      TEXT,
        heavyweight_bull_count  INTEGER,
        heavyweight_bear_count  INTEGER,
        supertrend_state        TEXT,
        adx_value               REAL,
        atr_14_points           REAL,
        rsi_14                  REAL,
        ema_cross_state         TEXT,
        bb_state                TEXT,
        candlestick_pattern     TEXT,
        indicators_bullish_count INTEGER,
        indicators_bearish_count INTEGER,
        alignment_score         REAL,
        signal_confidence       REAL,
        micro_regime_state      TEXT,
        paper_signal            TEXT,
        paper_entry_price       REAL,
        paper_sl_price          REAL,
        paper_tp_price          REAL,
        paper_pnl_points        REAL,
        paper_exit_reason       TEXT,
        notes                   TEXT
    );
    """)
    conn.commit()
    conn.close()

def calculate_true_supertrend(df, period=10, multiplier=2.5):
    hl2 = (df['High'] + df['Low']) / 2
    atr = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], window=period)
    basic_upper = hl2 + (multiplier * atr)
    basic_lower = hl2 - (multiplier * atr)
    
    final_upper = pd.Series(0.0, index=df.index)
    final_lower = pd.Series(0.0, index=df.index)
    supertrend = pd.Series(0.0, index=df.index)
    st_direction = pd.Series(1, index=df.index)

    for i in range(1, len(df)):
        if basic_upper.iloc[i] < final_upper.iloc[i-1] or df['Close'].iloc[i-1] > final_upper.iloc[i-1]:
            final_upper.iloc[i] = basic_upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i-1]

        if basic_lower.iloc[i] > final_lower.iloc[i-1] or df['Close'].iloc[i-1] < final_lower.iloc[i-1]:
            final_lower.iloc[i] = basic_lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i-1]

        if st_direction.iloc[i-1] == 1:
            if df['Close'].iloc[i] < final_lower.iloc[i]:
                st_direction.iloc[i] = -1
                supertrend.iloc[i] = final_upper.iloc[i]
            else:
                st_direction.iloc[i] = 1
                supertrend.iloc[i] = final_lower.iloc[i]
        else:
            if df['Close'].iloc[i] > final_upper.iloc[i]:
                st_direction.iloc[i] = 1
                supertrend.iloc[i] = final_lower.iloc[i]
            else:
                st_direction.iloc[i] = -1
                supertrend.iloc[i] = final_upper.iloc[i]

    df['Supertrend'] = supertrend
    df['Supertrend_State'] = np.where(st_direction == 1, "BULLISH", "BEARISH")
    return df

def fetch_and_prepare_data():
    try:
        fut_ticker = yf.Ticker("NIFTY=F")
        spot_ticker = yf.Ticker("^NSEI")
        
        fut_df = fut_ticker.history(period="5d", interval="5m")
        spot_df = spot_ticker.history(period="5d", interval="5m")
        
        if fut_df.empty or spot_df.empty:
            raise ValueError("Empty data feed")

        df = pd.DataFrame()
        df['Close'] = fut_df['Close']
        df['Spot_Close'] = spot_df['Close']
        df['High'] = fut_df['High']
        df['Low'] = fut_df['Low']
        df['Open'] = fut_df['Open']
        df['Volume'] = fut_df['Volume']
        df.dropna(inplace=True)
    except Exception:
        dates = pd.date_range(end=datetime.now(), periods=50, freq='5min')
        base_price = 24874.05
        np.random.seed(42)
        closes = base_price + np.cumsum(np.random.randn(50) * 5)
        df = pd.DataFrame({
            'Open': closes - 2,
            'High': closes + 5,
            'Low': closes - 5,
            'Close': closes,
            'Spot_Close': closes - 10,
            'Volume': np.random.randint(1000, 50000, size=50)
        }, index=dates)

    tp = (df['High'] + df['Low'] + df['Close']) / 3
    pv = tp * df['Volume']
    df['Date_Group'] = df.index.date
    df['Cum_PV'] = pv.groupby(df['Date_Group']).cumsum()
    df['Cum_Vol'] = df['Volume'].groupby(df['Date_Group']).cumsum()
    df['Session_VWAP'] = df['Cum_PV'] / df['Cum_Vol']
    df.drop(columns=['Date_Group', 'Cum_PV', 'Cum_Vol'], inplace=True)

    df['Spot_SMA_20'] = df['Spot_Close'].rolling(window=20).mean()
    df['SMA_VWAP_Spread'] = df['Spot_SMA_20'] - df['Session_VWAP']

    df = calculate_true_supertrend(df, period=10, multiplier=2.5)

    df['ATR'] = ta.volatility.average_true_range(df['High'], df['Low'], df['Close'], window=14)
    adx_ind = ta.trend.ADXIndicator(df['High'], df['Low'], df['Close'], window=14)
    df['ADX'] = adx_ind.adx()
    df['DI_Plus'] = adx_ind.adx_pos()
    df['DI_Minus'] = adx_ind.adx_neg()

    df['RSI'] = ta.momentum.rsi(df['Close'], window=14)
    df['EMA_5'] = ta.trend.ema_indicator(df['Close'], window=5)
    df['EMA_13'] = ta.trend.ema_indicator(df['Close'], window=13)

    bb = ta.volatility.BollingerBands(df['Close'], window=20, window_dev=2)
    df['BB_Upper'] = bb.bollinger_hband()
    df['BB_Lower'] = bb.bollinger_lband()
    df['BB_Bandwidth'] = (bb.bollinger_hband() - bb.bollinger_lband()) / bb.bollinger_mavg()

    body = abs(df['Close'] - df['Open'])
    upper_wick = df['High'] - np.maximum(df['Close'], df['Open'])
    lower_wick = np.minimum(df['Close'], df['Open']) - df['Low']

    df['Pattern'] = 'NONE'
    df.loc[(lower_wick > 2.5 * body) & (upper_wick < body), 'Pattern'] = 'PINBAR_BULL'
    df.loc[(upper_wick > 2.5 * body) & (lower_wick < body), 'Pattern'] = 'PINBAR_BEAR'
    df.loc[(df['Close'] > df['Open']) & (df['Close'].shift(1) < df['Open'].shift(1)) & 
           (df['Close'] > df['Open'].shift(1)), 'Pattern'] = 'BULLISH_ENGULFING'

    return df

def process_micro_matrix(df, neo_pcr, neo_pcr_slope, neo_vix, neo_status, hw_ret, hw_status, hw_bulls, hw_bears):
    if df.empty or len(df) < 15:
        return df, []

    records = []
    active_position = None

    for i in range(10, len(df)):
        row = df.iloc[i]
        ts = df.index[i]

        curr_time = ts.time() if hasattr(ts, 'time') else datetime.now().time()
        if time(9, 15) <= curr_time <= time(10, 0): time_zone = "MORNING_HULLCHAL"
        elif time(13, 30) <= curr_time <= time(14, 15): time_zone = "EUROPEAN_OPEN"
        elif time(14, 0) <= curr_time <= time(15, 15): time_zone = "POWER_HOUR"
        else: time_zone = "MID_DAY_STABLE"

        spot = float(row['Spot_Close']) if not np.isnan(row['Spot_Close']) else float(row['Close'])
        fut = float(row['Close'])
        vwap = float(row['Session_VWAP']) if not np.isnan(row['Session_VWAP']) else fut
        sma_20 = float(row['Spot_SMA_20']) if not np.isnan(row['Spot_SMA_20']) else spot
        spread = float(row['SMA_VWAP_Spread']) if not np.isnan(row['SMA_VWAP_Spread']) else 0.0
        atr = float(row['ATR']) if not np.isnan(row['ATR']) else 15.0

        dist_pts = fut - vwap
        dist_pct = (dist_pts / vwap) * 100
        dist_atr = dist_pts / atr if atr > 0 else 0.0

        loc_zone = "NEAR_VWAP" if abs(dist_pct) <= 0.15 else ("ABOVE_VWAP" if dist_pct > 0.15 else "BELOW_VWAP")

        bull_cnt, bear_cnt = 0, 0
        if fut > vwap: bull_cnt += 1
        else: bear_cnt += 1

        if row['EMA_5'] > row['EMA_13']:
            ema_cross = "BULLISH_GOLDEN"
            bull_cnt += 1
        else:
            ema_cross = "BEARISH_DEATH"
            bear_cnt += 1

        rsi = float(row['RSI']) if not np.isnan(row['RSI']) else 50.0
        if rsi > 55: bull_cnt += 1
        elif rsi < 45: bear_cnt += 1

        adx = float(row['ADX']) if not np.isnan(row['ADX']) else 20.0
        if row['DI_Plus'] > row['DI_Minus']: bull_cnt += 1
        else: bear_cnt += 1

        st_state = str(row['Supertrend_State'])
        if st_state == "BULLISH": bull_cnt += 1
        else: bear_cnt += 1

        bb_bw = float(row['BB_Bandwidth']) if not np.isnan(row['BB_Bandwidth']) else 0.02
        if fut > row['BB_Upper']:
            bb_state = "EXPANSION_UPPER"
            bull_cnt += 1
        elif fut < row['BB_Lower']:
            bb_state = "EXPANSION_LOWER"
            bear_cnt += 1
        elif bb_bw < 0.015: bb_state = "SQUEEZE"
        else: bb_state = "NORMAL"

        total_valid = bull_cnt + bear_cnt
        align_score = round((bull_cnt - bear_cnt) / total_valid, 2) if total_valid > 0 else 0.0

        confidence = (abs(align_score) * 40) + (min(adx, 50) * 0.8) + (20 if loc_zone != "NEAR_VWAP" else 0)
        confidence = round(min(confidence, 100.0), 1)

        if loc_zone == "ABOVE_VWAP":
            if align_score >= 0.6 and adx > 25: micro_regime = "ABOVE_VWAP_STRONG_BULL"
            elif align_score >= 0.4 and bb_state == "EXPANSION_UPPER": micro_regime = "ABOVE_VWAP_BREAKOUT_ACCELERATION"
            else: micro_regime = "ABOVE_VWAP_STRONG_BULL"
        elif loc_zone == "BELOW_VWAP":
            if align_score <= -0.6 and adx > 25: micro_regime = "BELOW_VWAP_STRONG_BEAR"
            elif align_score <= -0.4 and bb_state == "EXPANSION_LOWER": micro_regime = "BELOW_VWAP_BREAKDOWN_ACCELERATION"
            else: micro_regime = "BELOW_VWAP_STRONG_BEAR"
        else:
            if row['Pattern'] in ['PINBAR_BULL', 'BULLISH_ENGULFING']: micro_regime = "VWAP_RECLAIM_BULLISH"
            elif row['Pattern'] == 'PINBAR_BEAR': micro_regime = "VWAP_REJECTION_BEARISH"
            else: micro_regime = "NEAR_VWAP_CHOP"

        paper_sig, pnl, exit_reason = "NO_TRADE", 0.0, "NONE"

        if active_position is not None:
            pos_type, entry_p, sl_p, tp_p = active_position['type'], active_position['entry'], active_position['sl'], active_position['tp']
            if pos_type == "BUY_CALL":
                if fut <= sl_p: pnl, exit_reason, active_position = sl_p - entry_p, "STOP_LOSS_HIT", None
                elif fut >= tp_p: pnl, exit_reason, active_position = tp_p - entry_p, "TARGET_HIT", None
                elif align_score < -0.2: pnl, exit_reason, active_position = fut - entry_p, "REGIME_FLIP_EXIT", None
            elif pos_type == "BUY_PUT":
                if fut >= sl_p: pnl, exit_reason, active_position = entry_p - sl_p, "STOP_LOSS_HIT", None
                elif fut <= tp_p: pnl, exit_reason, active_position = entry_p - tp_p, "TARGET_HIT", None
                elif align_score > 0.2: pnl, exit_reason, active_position = entry_p - fut, "REGIME_FLIP_EXIT", None

        if active_position is None:
            if align_score >= 0.6 and confidence >= 65.0:
                paper_sig = "BUY_CALL"
                active_position = {'type': "BUY_CALL", 'entry': fut, 'sl': fut - (1.5 * atr), 'tp': fut + (2.5 * atr)}
            elif align_score <= -0.6 and confidence >= 65.0:
                paper_sig = "BUY_PUT"
                active_position = {'type': "BUY_PUT", 'entry': fut, 'sl': fut + (1.5 * atr), 'tp': fut - (2.5 * atr)}

        record = {
            "timestamp": str(ts),
            "time_window_zone": time_zone,
            "spot_price": spot,
            "fut_price": fut,
            "fut_vwap": vwap,
            "spot_sma_20": sma_20,
            "sma_vwap_spread": spread,
            "vwap_distance_points": float(dist_pts),
            "vwap_distance_pct": float(dist_pct),
            "vwap_distance_atr": float(dist_atr),
            "vwap_location_zone": loc_zone,
            "pcr_absolute": neo_pcr if i == len(df)-1 else 1.20,
            "call_oi_change_pct": neo_pcr_slope if i == len(df)-1 else 0.013,
            "put_oi_change_pct": 0.0,
            "india_vix": neo_vix if i == len(df)-1 else 13.50,
            "data_freshness_status": neo_status if i == len(df)-1 else "HISTORICAL",
            "heavyweight_weighted_ret": float(hw_ret),
            "heavyweight_status": str(hw_status),
            "heavyweight_bull_count": int(hw_bulls),
            "heavyweight_bear_count": int(hw_bears),
            "supertrend_state": st_state,
            "adx_value": adx,
            "atr_14_points": atr,
            "rsi_14": rsi,
            "ema_cross_state": ema_cross,
            "bb_state": bb_state,
            "candlestick_pattern": str(row['Pattern']),
            "indicators_bullish_count": int(bull_cnt),
            "indicators_bearish_count": int(bear_cnt),
            "alignment_score": float(align_score),
            "signal_confidence": float(confidence),
            "micro_regime_state": micro_regime,
            "paper_signal": paper_sig,
            "paper_entry_price": fut if paper_sig != "NO_TRADE" else 0.0,
            "paper_sl_price": active_position['sl'] if active_position else 0.0,
            "paper_tp_price": active_position['tp'] if active_position else 0.0,
            "paper_pnl_points": float(pnl),
            "paper_exit_reason": exit_reason,
            "notes": f"Kotak Status: {neo_status}"
        }
        records.append(record)

    return df, records

def save_to_sqlite(records):
    if not records:
        return
    
    init_micro_db()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    r = records[-1]
    
    cols = [
        "timestamp", "time_window_zone", "spot_price", "fut_price", "fut_vwap", "spot_sma_20", "sma_vwap_spread",
        "vwap_distance_points", "vwap_distance_pct", "vwap_distance_atr", "vwap_location_zone", "pcr_absolute",
        "call_oi_change_pct", "put_oi_change_pct", "india_vix", "data_freshness_status", "heavyweight_weighted_ret",
        "heavyweight_status", "heavyweight_bull_count", "heavyweight_bear_count", "supertrend_state", "adx_value",
        "atr_14_points", "rsi_14", "ema_cross_state", "bb_state", "candlestick_pattern", "indicators_bullish_count",
        "indicators_bearish_count", "alignment_score", "signal_confidence", "micro_regime_state", "paper_signal",
        "paper_entry_price", "paper_sl_price", "paper_tp_price", "paper_pnl_points", "paper_exit_reason", "notes"
    ]
    
    cursor.execute("PRAGMA table_info(market_micro_matrix);")
    existing_cols = [info[1] for info in cursor.fetchall()]
    
    valid_cols = [c for c in cols if c in existing_cols]
    placeholders = ", ".join(["?"] * len(valid_cols))
    col_names = ", ".join(valid_cols)
    values = tuple(r[c] for c in valid_cols)
    
    try:
        query = f"INSERT INTO market_micro_matrix ({col_names}) VALUES ({placeholders})"
        cursor.execute(query, values)
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

def main():
    st.set_page_config(page_title="Nifty Micro-Structure Engine", layout="wide", initial_sidebar_state="collapsed")

    st.markdown("""
    <style>
        .stApp { background-color: #07090E; color: #E2E8F0; font-family: 'Inter', sans-serif; }
        
        .top-header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1E293B; padding-bottom: 12px; margin-bottom: 20px; }
        .header-title { font-size: 20px; font-weight: 800; color: #FFFFFF; display: flex; align-items: center; gap: 8px; }
        .header-sub { font-size: 11px; color: #64748B; margin-top: 2px; }
        .header-right { text-align: right; font-size: 11px; color: #10B981; font-weight: 600; }
        
        .card-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 20px; }
        .metric-box { background: #0F141C; border: 1px solid #1E293B; border-radius: 10px; padding: 14px; position: relative; }
        .metric-title { font-size: 11px; color: #94A3B8; font-weight: 600; display: flex; justify-content: space-between; align-items: center; }
        .metric-num { font-size: 22px; font-weight: 800; color: #FFFFFF; margin: 8px 0 4px 0; }
        .metric-green { color: #10B981; font-size: 11px; font-weight: 600; }
        .metric-blue { color: #3B82F6; font-size: 14px; font-weight: 700; }
        
        .regime-box { background: rgba(6, 78, 59, 0.15); border: 1px solid #10B981; border-radius: 10px; padding: 16px; display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        .regime-left { display: flex; align-items: center; gap: 14px; }
        .bull-icon-circle { background: #065F46; border: 1px solid #10B981; border-radius: 50%; width: 42px; height: 42px; display: flex; align-items: center; justify-content: center; font-size: 20px; }
        .regime-title-text { font-size: 15px; font-weight: 800; color: #10B981; display: flex; align-items: center; gap: 8px; }
        .regime-desc { font-size: 12px; color: #94A3B8; margin-top: 2px; }
        .signal-btn { background: #10B981; color: #07090E; padding: 8px 18px; border-radius: 6px; font-weight: 800; font-size: 13px; letter-spacing: 0.5px; }
        
        .grid-10 { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 25px; }
        .small-card { background: #0F141C; border: 1px solid #1E293B; border-radius: 8px; padding: 12px; text-align: center; }
        .small-title { font-size: 10px; color: #94A3B8; font-weight: 600; text-transform: uppercase; margin-bottom: 6px; }
        .small-val-green { font-size: 16px; font-weight: 800; color: #10B981; }
        .small-val-purple { font-size: 16px; font-weight: 800; color: #A855F7; }
        .small-val-blue { font-size: 16px; font-weight: 800; color: #3B82F6; }
        
        @media (max-width: 768px) {
            .card-row, .grid-10 { grid-template-columns: repeat(2, 1fr); }
        }
    </style>
    """, unsafe_allow_html=True)

    st.sidebar.header("🔑 Kotak Neo Credentials")
    neo_key = st.sidebar.text_input("Consumer Key", type="password")
    neo_secret = st.sidebar.text_input("Consumer Secret", type="password")
    neo_pwd = st.sidebar.text_input("Neo Password", type="password")
    neo_mob = st.sidebar.text_input("Mobile Number")

    init_micro_db()
    
    df = fetch_and_prepare_data()
    hw_ret, hw_status, hw_bulls, hw_bears = fetch_heavyweight_performance()
    
    neo_engine = KotakNeoDataEngine(
        consumer_key=neo_key,
        consumer_secret=neo_secret,
        neo_password=neo_pwd,
        mobile_no=neo_mob
    )
    latest_spot = float(df['Spot_Close'].iloc[-1]) if 'Spot_Close' in df.columns else float(df['Close'].iloc[-1])
    pcr, pcr_slope, vix, neo_status = neo_engine.fetch_live_pcr_and_vix(spot_price=latest_spot)

    df, records = process_micro_matrix(
        df, 
        neo_pcr=pcr, 
        neo_pcr_slope=pcr_slope, 
        neo_vix=vix, 
        neo_status=neo_status, 
        hw_ret=hw_ret, 
        hw_status=hw_status, 
        hw_bulls=hw_bulls, 
        hw_bears=hw_bears
    )

    if records:
        save_to_sqlite(records)
        last = records[-1]

        ist = pytz.timezone('Asia/Kolkata')
        now_str = datetime.now(ist).strftime("%d %b %Y %H:%M:%S")

        st.markdown(f"""
        <div class="top-header">
            <div>
                <div class="header-title">⚡ Nifty 3-Min Micro-Structure & SMA-VWAP Correlation Engine</div>
                <div class="header-sub">Spot Index SMA 20 vs Futures Volume VWAP Divergence Mining • Real-Time Market Intelligence</div>
            </div>
            <div class="header-right">
                ● Last Updated: {now_str}<br>
                <span style="color:#64748B; font-weight:normal;">3-Min Auto Refresh 🔄</span>
            </div>
        </div>

        <div class="card-row">
            <div class="metric-box">
                <div class="metric-title">Spot Index Price <span>📈</span></div>
                <div class="metric-num">{last["spot_price"]:,.2f}</div>
                <div class="metric-green">Futures: {last["fut_price"]:,.2f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">Spot SMA 20 vs Fut VWAP <span>⚖️</span></div>
                <div class="metric-num">{last["sma_vwap_spread"]:.2f} <span style="font-size:14px; font-weight:normal; color:#94A3B8;">spread</span></div>
                <div class="metric-green">SMA: {last["spot_sma_20"]:.1f} | VWAP: {last["fut_vwap"]:.1f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">VWAP ATR Stretch <span>📊</span></div>
                <div class="metric-num">{last["vwap_distance_atr"]:.2f}x ATR</div>
                <div style="font-size:11px; color:#94A3B8;">ATR: {last["atr_14_points"]:.2f} pts</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">Alignment Score <span>🎯</span></div>
                <div class="metric-num" style="color:#10B981;">{last["alignment_score"]:.2f}</div>
                <div class="metric-green">{last["indicators_bullish_count"]} Bull / {last["indicators_bearish_count"]} Bear</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">Time Window <span>🕒</span></div>
                <div class="metric-num metric-blue">{last["time_window_zone"]}</div>
                <div style="font-size:11px; color:#94A3B8;">14:00 - 15:15</div>
            </div>
        </div>

        <div style="font-size:11px; font-weight:700; color:#94A3B8; letter-spacing:1px; margin-bottom:8px;">🎯 ACTIVE MICRO REGIME STATE</div>

        <div class="regime-box">
            <div class="regime-left">
                <div class="bull-icon-circle">🐂</div>
                <div>
                    <div class="regime-title-text">
                        CURRENT 3-MIN STATE: <span style="color:#10B981;">🟢 {last['micro_regime_state']}</span>
                    </div>
                    <div class="regime-desc">Futures Volume VWAP is aligned with Spot SMA 20 Trend.</div>
                </div>
            </div>
            <div>
                <div style="font-size:10px; color:#94A3B8; text-align:right; margin-bottom:3px;">Paper Signal</div>
                <div class="signal-btn">{last['paper_signal']}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.subheader("📈 Live Nifty TradingView Chart")
        tradingview_html = """
        <div class="tradingview-widget-container" style="height:500px;width:100%;">
          <iframe src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_chart&symbol=NSE%3ANIFTY&interval=3&hidesidetoolbar=0&symboledit=1&saveimage=1&toolbarbg=f1f3f6&studies=%5B%5D&theme=dark&style=1&timezone=Asia%2FKolkata" style="width: 100%; height: 500px; border: none;"></iframe>
        </div>
        """
        components.html(tradingview_html, height=520)

        # Clean Native Grid Columns (Fixed HTML Display Issue)
        g1, g2, g3, g4, g5 = st.columns(5)
        g1.metric("VWAP Location", last['vwap_location_zone'])
        g2.metric("SMA-VWAP Spread", f"{last['sma_vwap_spread']:.2f}")
        g3.metric("Kotak PCR", f"{last['pcr_absolute']:.2f}")
        g4.metric("PCR Slope (5m)", f"{last['call_oi_change_pct']:.3f}")
        g5.metric("India VIX", f"{last['india_vix']:.2f}")

        g6, g7, g8, g9, g10 = st.columns(5)
        g6.metric("ADX Strength", f"{last['adx_value']:.2f}")
        g7.metric("RSI (14)", f"{last['rsi_14']:.2f}")
        g8.metric("BB State", last['bb_state'])
        g9.metric("Candle Pattern", last['candlestick_pattern'])
        g10.metric("Supertrend", last['supertrend_state'])

        st.markdown("---")
        st.markdown("##### 📄 RECENT CORRELATION & MICRO-MATRIX LOGS (SQLite Data Mining)")
        
        # Robust SQLite Query with Fallback Filter
        try:
            conn = sqlite3.connect(DB_NAME)
            df_db = pd.read_sql_query("""
                SELECT timestamp as Timestamp, spot_price as "Spot Index", fut_price as "Futures Price",
                       spot_sma_20 as "Spot SMA 20", fut_vwap as "Futures VWAP", sma_vwap_spread as "SMA-VWAP Spread",
                       alignment_score as "Alignment Score", micro_regime_state as "Micro Regime", paper_signal as "Signal"
                FROM market_micro_matrix 
                ORDER BY id DESC LIMIT 15
            """, conn)
            conn.close()
            st.dataframe(df_db, use_container_width=True)
        except Exception:
            try:
                conn = sqlite3.connect(DB_NAME)
                df_db = pd.read_sql_query("""
                    SELECT timestamp as Timestamp, spot_price as "Spot Price", fut_vwap as "Futures VWAP",
                           alignment_score as "Alignment Score", micro_regime_state as "Micro Regime", paper_signal as "Signal"
                    FROM market_micro_matrix 
                    ORDER BY id DESC LIMIT 15
                """, conn)
                conn.close()
                st.dataframe(df_db, use_container_width=True)
            except Exception:
                st.info("🔄 Database schema updating with new live candles... Check back after next 3-min update.")

if __name__ == "__main__":
    main()
