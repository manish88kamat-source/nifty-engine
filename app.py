import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import requests
import ta
import sqlite3
from datetime import datetime, time
import warnings
import logging

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.ERROR)

DB_NAME = "market_micro_matrix.sqlite"

# Top Nifty 50 Heavyweights & Weights Configuration
HEAVYWEIGHTS = {
    "HDFCBANK.NS": 11.5,
    "RELIANCE.NS": 9.8,
    "ICICIBANK.NS": 8.0,
    "INFY.NS": 5.8,
    "ITC.NS": 4.2,
    "LT.NS": 3.8
}

# ==========================================
# 1. DIRECT NSE SESSION SCRAPER
# ==========================================
class NSESessionScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nseindia.com/option-chain'
        }
        self.init_cookies()

    def init_cookies(self):
        try:
            self.session.get("https://www.nseindia.com", headers=self.headers, timeout=5)
            self.session.get("https://www.nseindia.com/option-chain", headers=self.headers, timeout=5)
        except Exception:
            pass

    def fetch_live_pcr_and_oi(self, spot_price):
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        try:
            res = self.session.get(url, headers=self.headers, timeout=5)
            if res.status_code == 403:
                self.init_cookies()
                res = self.session.get(url, headers=self.headers, timeout=5)

            if res.status_code == 200:
                data = res.json()
                records = data.get('records', {}).get('data', [])
                
                if spot_price > 0 and records:
                    strike_step = 50
                    atm_strike = round(spot_price / strike_step) * strike_step
                    valid_strikes = [atm_strike + (i * strike_step) for i in range(-5, 6)]

                    call_oi, put_oi = 0, 0
                    call_oi_chg, put_oi_chg = 0, 0

                    for row in records:
                        strike = row.get('strikePrice', 0)
                        if strike in valid_strikes:
                            if 'CE' in row:
                                call_oi += row['CE'].get('openInterest', 0)
                                call_oi_chg += row['CE'].get('changeinOpenInterest', 0)
                            if 'PE' in row:
                                put_oi += row['PE'].get('openInterest', 0)
                                put_oi_chg += row['PE'].get('changeinOpenInterest', 0)

                    pcr = round(put_oi / call_oi, 2) if call_oi > 0 else 1.0
                    call_chg_pct = round((call_oi_chg / call_oi) * 100, 2) if call_oi > 0 else 0.0
                    put_chg_pct = round((put_oi_chg / put_oi) * 100, 2) if put_oi > 0 else 0.0

                    return pcr, call_chg_pct, put_chg_pct
        except Exception:
            pass
        return None, None, None

    def fetch_live_vix(self):
        url = "https://www.nseindia.com/api/allIndices"
        try:
            res = self.session.get(url, headers=self.headers, timeout=5)
            if res.status_code == 200:
                data = res.json()
                for item in data.get('data', []):
                    if item.get('index') == 'INDIA VIX':
                        return float(item.get('last')), float(item.get('percChange'))
        except Exception:
            pass
        return None, None

# ==========================================
# 2. HEAVYWEIGHT ENGINE
# ==========================================
def fetch_heavyweight_performance():
    try:
        tickers = list(HEAVYWEIGHTS.keys())
        data = yf.download(tickers, period="2d", interval="5m", progress=False)['Close']
        
        if data.empty:
            return 0.0, "NEUTRAL", 0, 0

        weighted_return = 0.0
        bull_count, bear_count = 0, 0

        for ticker, weight in HEAVYWEIGHTS.items():
            if ticker in data.columns and len(data[ticker]) >= 2:
                last_price = data[ticker].iloc[-1]
                prev_price = data[ticker].iloc[-2]
                pct_chg = ((last_price - prev_price) / prev_price) * 100

                weighted_return += (pct_chg * (weight / 100.0))

                if pct_chg > 0.15: bull_count += 1
                elif pct_chg < -0.15: bear_count += 1

        hw_status = "BULLISH_SUPPORT" if weighted_return > 0.1 else ("BEARISH_DRAG" if weighted_return < -0.1 else "NEUTRAL")
        return round(weighted_return, 3), hw_status, bull_count, bear_count
    except Exception:
        return 0.0, "NEUTRAL", 0, 0

# ==========================================
# 3. DATABASE INIT & SCHEMA
# ==========================================
def init_micro_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_micro_matrix (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp               TEXT NOT NULL,
        time_window_zone        TEXT,
        spot_price              REAL,
        fut_vwap                REAL,
        vwap_distance_points    REAL,
        vwap_distance_pct       REAL,
        vwap_distance_atr       REAL,
        vwap_location_zone      TEXT,
        pcr_absolute            REAL,
        call_oi_change_pct      REAL,
        put_oi_change_pct       REAL,
        india_vix               REAL,
        vix_change_pct_5m       REAL,
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

# ==========================================
# 4. TECHNICAL PIPELINE & SESSION VWAP
# ==========================================
def calculate_session_vwap(df):
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    pv = tp * df['Volume']
    df['Date_Group'] = df.index.date
    df['Cum_PV'] = pv.groupby(df['Date_Group']).cumsum()
    df['Cum_Vol'] = df['Volume'].groupby(df['Date_Group']).cumsum()
    df['Session_VWAP'] = df['Cum_PV'] / df['Cum_Vol']
    df.drop(columns=['Date_Group', 'Cum_PV', 'Cum_Vol'], inplace=True)
    return df

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

def fetch_and_prepare_data(symbol="^NSEI", interval="3m", period="5d"):
    try:
        df = yf.download(symbol, period=period, interval=interval, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.dropna(inplace=True)
        if df.empty or len(df) < 30:
            return pd.DataFrame()

        df = calculate_session_vwap(df)
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
    except Exception:
        return pd.DataFrame()

# ==========================================
# 5. MICRO-MATRIX ENGINE
# ==========================================
def process_micro_matrix(df, nse_pcr, nse_call_chg, nse_put_chg, nse_vix, nse_vix_chg, hw_ret, hw_status, hw_bulls, hw_bears):
    if df.empty or len(df) < 30:
        return df, []

    records = []
    active_position = None

    for i in range(20, len(df)):
        row = df.iloc[i]
        ts = df.index[i]

        curr_time = ts.time() if hasattr(ts, 'time') else datetime.now().time()
        if time(9, 15) <= curr_time <= time(10, 0): time_zone = "MORNING_HULLCHAL"
        elif time(13, 30) <= curr_time <= time(14, 15): time_zone = "EUROPEAN_OPEN"
        elif time(14, 0) <= curr_time <= time(15, 15): time_zone = "POWER_HOUR"
        else: time_zone = "MID_DAY_STABLE"

        spot = float(row['Close'])
        vwap = float(row['Session_VWAP']) if not np.isnan(row['Session_VWAP']) else spot
        atr = float(row['ATR']) if not np.isnan(row['ATR']) else 15.0

        dist_pts = spot - vwap
        dist_pct = (dist_pts / vwap) * 100
        dist_atr = dist_pts / atr if atr > 0 else 0.0

        loc_zone = "NEAR_VWAP" if abs(dist_pct) <= 0.15 else ("ABOVE_VWAP" if dist_pct > 0.15 else "BELOW_VWAP")

        bull_cnt, bear_cnt = 0, 0
        if spot > vwap: bull_cnt += 1
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
        if spot > row['BB_Upper']:
            bb_state = "EXPANSION_UPPER"
            bull_cnt += 1
        elif spot < row['BB_Lower']:
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
                if spot <= sl_p: pnl, exit_reason, active_position = sl_p - entry_p, "STOP_LOSS_HIT", None
                elif spot >= tp_p: pnl, exit_reason, active_position = tp_p - entry_p, "TARGET_HIT", None
                elif align_score < -0.2: pnl, exit_reason, active_position = spot - entry_p, "REGIME_FLIP_EXIT", None
            elif pos_type == "BUY_PUT":
                if spot >= sl_p: pnl, exit_reason, active_position = entry_p - sl_p, "STOP_LOSS_HIT", None
                elif spot <= tp_p: pnl, exit_reason, active_position = entry_p - tp_p, "TARGET_HIT", None
                elif align_score > 0.2: pnl, exit_reason, active_position = entry_p - spot, "REGIME_FLIP_EXIT", None

        if active_position is None:
            if align_score >= 0.6 and confidence >= 65.0:
                paper_sig = "BUY_CALL"
                active_position = {'type': "BUY_CALL", 'entry': spot, 'sl': spot - (1.5 * atr), 'tp': spot + (2.5 * atr)}
            elif align_score <= -0.6 and confidence >= 65.0:
                paper_sig = "BUY_PUT"
                active_position = {'type': "BUY_PUT", 'entry': spot, 'sl': spot + (1.5 * atr), 'tp': spot - (2.5 * atr)}

        record = {
            "timestamp": str(ts),
            "time_window_zone": time_zone,
            "spot_price": spot,
            "fut_vwap": vwap,
            "vwap_distance_points": float(dist_pts),
            "vwap_distance_pct": float(dist_pct),
            "vwap_distance_atr": float(dist_atr),
            "vwap_location_zone": loc_zone,
            "pcr_absolute": nse_pcr if i == len(df)-1 else None,
            "call_oi_change_pct": nse_call_chg if i == len(df)-1 else None,
            "put_oi_change_pct": nse_put_chg if i == len(df)-1 else None,
            "india_vix": nse_vix if i == len(df)-1 else None,
            "vix_change_pct_5m": nse_vix_chg if i == len(df)-1 else None,
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
            "paper_entry_price": spot if paper_sig != "NO_TRADE" else 0.0,
            "paper_sl_price": active_position['sl'] if active_position else 0.0,
            "paper_tp_price": active_position['tp'] if active_position else 0.0,
            "paper_pnl_points": float(pnl),
            "paper_exit_reason": exit_reason,
            "notes": f"Heavyweight Ret: {hw_ret}% | Status: {hw_status}"
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
    cursor.execute("""
    INSERT INTO market_micro_matrix (
        timestamp, time_window_zone, spot_price, fut_vwap, vwap_distance_points, vwap_distance_pct,
        vwap_distance_atr, vwap_location_zone, pcr_absolute, call_oi_change_pct, put_oi_change_pct,
        india_vix, vix_change_pct_5m, heavyweight_weighted_ret, heavyweight_status, heavyweight_bull_count,
        heavyweight_bear_count, supertrend_state, adx_value, atr_14_points, rsi_14, ema_cross_state,
        bb_state, candlestick_pattern, indicators_bullish_count, indicators_bearish_count,
        alignment_score, signal_confidence, micro_regime_state, paper_signal, paper_entry_price,
        paper_sl_price, paper_tp_price, paper_pnl_points, paper_exit_reason, notes
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """, tuple(r.values()))
    
    conn.commit()
    conn.close()

# ==========================================
# 6. STREAMLIT UI WITH CUSTOM DARK NEON CSS
# ==========================================
def main():
    st.set_page_config(page_title="Nifty Micro Matrix Dark Terminal", layout="wide")

    # Custom Inject CSS for Exact Dark Neon Glassmorphism UI
    st.markdown("""
    <style>
        /* Base Background */
        .stApp {
            background-color: #0B0E14;
            color: #E2E8F0;
            font-family: 'Inter', sans-serif;
        }

        /* Top Title Engine Bar */
        .header-box {
            background: #111622;
            border: 1px solid #1E293B;
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 20px;
        }

        /* Glassmorphic Metric Cards */
        .metric-card {
            background: rgba(17, 22, 34, 0.8);
            border: 1px solid #1E293B;
            border-radius: 10px;
            padding: 14px;
            text-align: left;
        }
        .metric-label {
            font-size: 11px;
            color: #94A3B8;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .metric-value {
            font-size: 22px;
            font-weight: 700;
            color: #F8FAFC;
            margin-top: 4px;
        }
        .metric-sub {
            font-size: 11px;
            margin-top: 2px;
        }
        .text-green { color: #10B981; }
        .text-red { color: #EF4444; }
        .text-blue { color: #3B82F6; }

        /* Neon Active State Banner */
        .neon-bull-box {
            background: rgba(6, 78, 59, 0.2);
            border: 1px solid #10B981;
            box-shadow: 0 0 15px rgba(16, 185, 129, 0.15);
            border-radius: 10px;
            padding: 16px;
            margin: 20px 0;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        /* Table Styling */
        div[data-testid="stDataFrame"] {
            background: #111622;
            border: 1px solid #1E293B;
            border-radius: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

    init_micro_db()
    
    df = fetch_and_prepare_data(symbol="^NSEI", interval="3m", period="5d")
    if df.empty:
        st.error("Failed to fetch live Nifty data.")
        return

    hw_ret, hw_status, hw_bulls, hw_bears = fetch_heavyweight_performance()
    
    nse = NSESessionScraper()
    latest_spot = float(df['Close'].iloc[-1])
    pcr, call_chg, put_chg = nse.fetch_live_pcr_and_oi(spot_price=latest_spot)
    vix, vix_chg = nse.fetch_live_vix()

    df, records = process_micro_matrix(df, nse_pcr=pcr, nse_call_chg=call_chg, nse_put_chg=put_chg, 
                                       nse_vix=vix, nse_vix_chg=vix_chg, hw_ret=hw_ret, 
                                       hw_status=hw_status, hw_bulls=hw_bulls, hw_bears=hw_bears)

    if records:
        save_to_sqlite(records)
        last = records[-1]

        # Engine Title Header
        st.markdown(f"""
        <div class="header-box">
            <h2 style="margin:0; color:#F8FAFC; font-size: 22px;">⚡ Nifty 3-Min Micro-Structure & 12-Regime Matrix Engine</h2>
            <p style="margin:4px 0 0 0; color:#64748B; font-size: 12px;">AI-Powered Micro Structure Analyzer • 12-State Regime Detection • Heavyweight Correlation Data</p>
        </div>
        """, unsafe_allow_html=True)

        # Top 5 Metric Cards (Glassmorphic)
        c1, c2, c3, c4, c5 = st.columns(5)
        
        with c1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Spot Price</div>
                <div class="metric-value">{last['spot_price']:.2f}</div>
                <div class="metric-sub text-green">▲ Active Nifty 50</div>
            </div>
            """, unsafe_allow_html=True)

        with c2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">VWAP Distance</div>
                <div class="metric-value">{last['vwap_distance_points']:.2f} pts</div>
                <div class="metric-sub text-green">{last['vwap_distance_pct']:.2f}%</div>
            </div>
            """, unsafe_allow_html=True)

        with c3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">VWAP ATR Stretch</div>
                <div class="metric-value">{last['vwap_distance_atr']:.2f}x ATR</div>
                <div class="metric-sub" style="color:#94A3B8;">ATR: {last['atr_14_points']:.2f} pts</div>
            </div>
            """, unsafe_allow_html=True)

        with c4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Alignment Score</div>
                <div class="metric-value text-green">{last['alignment_score']:.2f}</div>
                <div class="metric-sub text-green">{last['indicators_bullish_count']} Bull / {last['indicators_bearish_count']} Bear</div>
            </div>
            """, unsafe_allow_html=True)

        with c5:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Time Window</div>
                <div class="metric-value text-blue" style="font-size: 16px;">{last['time_window_zone']}</div>
                <div class="metric-sub" style="color:#94A3B8;">Session Active</div>
            </div>
            """, unsafe_allow_html=True)

        # Active Neon Regime Banner
        st.markdown(f"""
        <div class="neon-bull-box">
            <div>
                <span style="font-size:12px; color:#10B981; font-weight:bold; letter-spacing:1px;">🎯 ACTIVE MICRO REGIME STATE</span>
                <h3 style="margin:4px 0; color:#F8FAFC;">🟢 CURRENT 3-MIN STATE: <span style="color:#10B981;">{last['micro_regime_state']}</span></h3>
                <p style="margin:0; color:#94A3B8; font-size:12px;">Heavyweight Status: <b style="color:#F8FAFC;">{hw_status}</b> ({hw_ret:+.3f}%) | Confidence: <b style="color:#10B981;">{last['signal_confidence']}%</b></p>
            </div>
            <div style="background:#10B981; color:#0B0E14; padding:8px 16px; border-radius:6px; font-weight:bold; font-size:14px;">
                Signal: {last['paper_signal']}
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Secondary Grid Cards
        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric("VWAP Location", last['vwap_location_zone'])
        sc2.metric("Real PCR (ATM ±5)", f"{pcr:.2f}" if pcr else "N/A")
        sc3.metric("Real India VIX", f"{vix:.2f}" if vix else "N/A")
        sc4.metric("ADX Strength", f"{last['adx_value']:.2f}")
        sc5.metric("RSI (14)", f"{last['rsi_14']:.2f}")

        st.markdown("---")

        # SQLite Data Table
        st.subheader("📜 Recent Micro-Matrix Logs (SQLite Database)")
        conn = sqlite3.connect(DB_NAME)
        df_db = pd.read_sql_query("""
            SELECT timestamp, spot_price, vwap_location_zone, vwap_distance_atr, 
                   heavyweight_weighted_ret, heavyweight_status, alignment_score, 
                   micro_regime_state, paper_signal 
            FROM market_micro_matrix 
            ORDER BY id DESC LIMIT 15
        """, conn)
        conn.close()
        st.dataframe(df_db, use_container_width=True)

if __name__ == "__main__":
    main()
