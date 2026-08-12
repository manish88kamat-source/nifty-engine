import pandas as pd
import numpy as np

# ==========================================
# 1. CORE TECHNICAL & SMC INDICATOR ENGINE
# ==========================================

def calculate_indicators(df):
    """Calculates VWAP, ATR, ADX, Bollinger Bands, and VIX metrics."""
    df = df.copy()
    
    # ATR 14
    high_low = df['High'] - df['Low']
    high_close = (df['High'] - df['Close'].shift()).abs()
    low_close = (df['Low'] - df['Close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean()
    df['ATR_MA'] = df['ATR'].rolling(20).mean()

    # Bollinger Bands
    df['BB_Mid'] = df['Close'].rolling(20).mean()
    std = df['Close'].rolling(20).std()
    df['BB_Upper'] = df['BB_Mid'] + (std * 2)
    df['BB_Lower'] = df['BB_Mid'] - (std * 2)
    df['BB_BW'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Mid']

    # ADX (Directional Index)
    up_move = df['High'] - df['High'].shift(1)
    down_move = df['Low'].shift(1) - df['Low']
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    plus_di = 100 * (pd.Series(plus_dm, index=df.index).rolling(14).mean() / df['ATR'])
    minus_di = 100 * (pd.Series(minus_dm, index=df.index).rolling(14).mean() / df['ATR'])
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-6))
    df['ADX'] = dx.rolling(14).mean()

    # Dynamic VWAP (Intraday Reset)
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    if 'Date' in df.columns:
        df['VWAP'] = (tp * df['Volume']).groupby(df['Date']).cumsum() / df['Volume'].groupby(df['Date']).cumsum()
    else:
        df['VWAP'] = (tp * df['Volume']).cumsum() / df['Volume'].cumsum()

    return df

def detect_smc_elements(df):
    """Detects Fair Value Gaps (FVG), Break of Structure (BOS), & Supply/Demand Zones."""
    df = df.copy()
    
    # 1. Fair Value Gap (FVG) Detection
    # Bullish FVG: Low of Candle 3 > High of Candle 1
    df['Bullish_FVG'] = (df['Low'] > df['High'].shift(2)) & (df['Close'].shift(1) > df['High'].shift(2))
    # Bearish FVG: High of Candle 3 < Low of Candle 1
    df['Bearish_FVG'] = (df['High'] < df['Low'].shift(2)) & (df['Close'].shift(1) < df['Low'].shift(2))

    # 2. Break of Structure (BOS)
    df['Swing_High'] = df['High'].rolling(10, center=True).max()
    df['Swing_Low'] = df['Low'].rolling(10, center=True).min()
    
    df['Bullish_BOS'] = (df['Close'] > df['Swing_High'].shift(1)) & (df['Close'].shift(1) <= df['Swing_High'].shift(1))
    df['Bearish_BOS'] = (df['Close'] < df['Swing_Low'].shift(1)) & (df['Close'].shift(1) >= df['Swing_Low'].shift(1))

    # 3. Supply and Demand Zones
    df['Demand_Zone'] = np.where(df['Bullish_BOS'], df['Low'].shift(2), np.nan)
    df['Supply_Zone'] = np.where(df['Bearish_BOS'], df['High'].shift(2), np.nan)
    df['Demand_Zone'] = df['Demand_Zone'].ffill()
    df['Supply_Zone'] = df['Supply_Zone'].ffill()

    return df

# ==========================================
# 2. REGIME IDENTIFIER ENGINE
# ==========================================

def map_macro_regimes(df_daily, df_vix):
    """Combines Daily technicals + Real-time VIX to label 4 Market Regimes."""
    regimes = []
    
    for i in range(len(df_daily)):
        atr = df_daily['ATR'].iloc[i]
        atr_ma = df_daily['ATR_MA'].iloc[i]
        bw = df_daily['BB_BW'].iloc[i]
        adx = df_daily['ADX'].iloc[i]
        vix = df_vix.iloc[i] if i < len(df_vix) else 15.0

        # Regime Selection Tree
        if (atr > 1.4 * atr_ma) or (vix > 20.0):
            regimes.append(3)  # Regime 3: Macro Shock / Panic
        elif (adx > 38) and (atr > atr_ma):
            regimes.append(4)  # Regime 4: V-Shape Exhaustion / Reversal
        elif (adx > 21) and (bw > 0.035):
            regimes.append(1)  # Regime 1: Strong Trending
        else:
            regimes.append(2)  # Regime 2: Volatile Chop / Squeeze

    df_daily['Regime'] = regimes
    return df_daily

# ==========================================
# 3. BACKTEST ENGINE WITH SMC & JOURNAL
# ==========================================

class UnifiedSMCBacktester:
    def __init__(self, df_5m, df_daily, df_vix):
        self.df_5m = df_5m.copy()
        self.df_daily = df_daily.copy()
        self.df_vix = df_vix.copy()
        self.journal = []
        self.position = None
        self.entry_price = 0
        self.sl = 0
        self.tp = 0

    def prepare_data(self):
        # 1. Macro Analysis
        self.df_daily = calculate_indicators(self.df_daily)
        self.df_daily = map_macro_regimes(self.df_daily, self.df_vix)
        
        # 2. Map Regime to 5m Timeframe
        self.df_5m['Date'] = self.df_5m.index.date
        self.df_daily['Date'] = self.df_daily.index.date
        regime_dict = dict(zip(self.df_daily['Date'], self.df_daily['Regime']))
        self.df_5m['Regime'] = self.df_5m['Date'].map(regime_dict).fillna(2)

        # 3. Micro 5m Indicators & SMC Elements
        self.df_5m = calculate_indicators(self.df_5m)
        self.df_5m = detect_smc_elements(self.df_5m)

    def run(self):
        self.prepare_data()

        for i in range(2, len(self.df_5m)):
            curr = self.df_5m.iloc[i]
            prev = self.df_5m.iloc[i-1]
            t_stamp = self.df_5m.index[i]
            regime = curr['Regime']

            # Position Management
            if self.position == 'LONG':
                if curr['Low'] <= self.sl:
                    self._close_trade(t_stamp, self.sl, "SL Hit", regime)
                elif curr['High'] >= self.tp:
                    self._close_trade(t_stamp, self.tp, "Target Hit", regime)
                continue

            elif self.position == 'SHORT':
                if curr['High'] >= self.sl:
                    self._close_trade(t_stamp, self.sl, "SL Hit", regime)
                elif curr['Low'] <= self.tp:
                    self._close_trade(t_stamp, self.tp, "Target Hit", regime)
                continue

            # ==========================================
            # REGIME-SPECIFIC ENTRY CONDITIONS WITH SMC
            # ==========================================

            # REGIME 1: Trending Expansion (BOS + FVG/Demand Retest + Price > VWAP)
            if regime == 1:
                if curr['Bullish_BOS'] or (curr['Bullish_FVG'] and curr['Close'] > curr['VWAP']):
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=curr['VWAP'] - (0.5 * curr['ATR']),
                                    tp=curr['Close'] + (2.0 * curr['ATR']),
                                    setup="Regime 1: Bullish BOS + FVG Expansion", regime=regime)

                elif curr['Bearish_BOS'] or (curr['Bearish_FVG'] and curr['Close'] < curr['VWAP']):
                    self._open_trade('SHORT', t_stamp, curr['Close'], 
                                    sl=curr['VWAP'] + (0.5 * curr['ATR']),
                                    tp=curr['Close'] - (2.0 * curr['ATR']),
                                    setup="Regime 1: Bearish BOS + FVG Expansion", regime=regime)

            # REGIME 2: Volatile Chop (Fade Range Extremes at Demand/Supply Zones)
            elif regime == 2:
                if (curr['Low'] <= curr['Demand_Zone']) and (curr['Close'] > curr['Demand_Zone']):
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=curr['Low'] - 8, tp=curr['VWAP'], 
                                    setup="Regime 2: Demand Zone Mean Reversion", regime=regime)

            # REGIME 4: V-Shape Recovery (Liquidity Sweep + Bullish FVG)
            elif regime == 4:
                if prev['Low'] < prev['BB_Lower'] and curr['Close'] > curr['BB_Lower'] and curr['Bullish_FVG']:
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=prev['Low'], tp=curr['VWAP'] + (1.5 * curr['ATR']), 
                                    setup="Regime 4: V-Shape FVG Reversal", regime=regime)

    def _open_trade(self, side, t_stamp, price, sl, tp, setup, regime):
        self.position = side
        self.entry_price = price
        self.sl = sl
        self.tp = tp
        self.active_trade_log = {
            'Entry_Time': t_stamp, 'Type': side, 'Regime': regime,
            'Entry': round(price, 2), 'SL': round(sl, 2), 'TP': round(tp, 2),
            'Setup': setup
        }

    def _close_trade(self, t_stamp, exit_price, reason, regime):
        pnl = (exit_price - self.entry_price) if self.position == 'LONG' else (self.entry_price - exit_price)
        self.active_trade_log['Exit_Time'] = t_stamp
        self.active_trade_log['Exit_Price'] = round(exit_price, 2)
        self.active_trade_log['Reason'] = reason
        self.active_trade_log['PnL_Points'] = round(pnl, 2)
        self.journal.append(self.active_trade_log)
        self.position = None

    def display_results(self):
        df_j = pd.DataFrame(self.journal)
        if df_j.empty:
            print("No trades executed based on strict SMC/Regime filters.")
            return
        print("\n================ SYSTEM BACKTEST PERFORMANCE JOURNAL ================")
        print(f"Total Trades : {len(df_j)}")
        print(f"Win Rate     : {round((len(df_j[df_j['PnL_Points'] > 0]) / len(df_j)) * 100, 2)}%")
        print(f"Total PnL    : {round(df_j['PnL_Points'].sum(), 2)} Points")
        print("=====================================================================")
        print("\n--- SAMPLE JOURNAL (First 5 Trades) ---")
        print(df_j[['Entry_Time', 'Setup', 'Type', 'Entry', 'Exit_Price', 'Reason', 'PnL_Points']].head())

# ==========================================
# 4. DATA GENERATION & SIMULATION RUN
# ==========================================

if __name__ == "__main__":
    np.random.seed(101)
    dates = pd.date_range("2026-01-01", periods=1200, freq="5min")
    price_series = 24000 + np.cumsum(np.random.randn(1200) * 12)
    
    df_5m = pd.DataFrame({
        'Open': price_series,
        'High': price_series + np.random.uniform(2, 15, 1200),
        'Low': price_series - np.random.uniform(2, 15, 1200),
        'Close': price_series + np.random.randn(1200) * 4,
        'Volume': np.random.randint(2000, 40000, 1200)
    }, index=dates)

    df_daily = df_5m.resample('D').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
    }).dropna()

    df_vix = pd.Series(np.random.uniform(12.0, 18.0, len(df_daily)), index=df_daily.index)

    # Initialize Engine
    tester = UnifiedSMCBacktester(df_5m, df_daily, df_vix)
    tester.run()
    tester.display_results()
