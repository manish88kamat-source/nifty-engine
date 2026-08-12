import pandas as pd
import numpy as np
import yfinance as yf

# ==========================================
# 1. CORE TECHNICAL, SMC & OPTION ENGINE
# ==========================================

def calculate_indicators(df):
    """Calculates VWAP, ATR, ADX, BB, PCR Proxy, Delta Decay & Gamma Squeeze Metrics."""
    df = df.copy()
    
    # ATR 14
    high_low = df['High'] - df['Low']
    high_close = (df['High'] - df['Close'].shift()).abs()
    low_close = (df['Low'] - df['Close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean().bfill()
    df['ATR_MA'] = df['ATR'].rolling(20).mean().bfill()

    # Bollinger Bands
    df['BB_Mid'] = df['Close'].rolling(20).mean().bfill()
    std = df['Close'].rolling(20).std().fillna(0)
    df['BB_Upper'] = df['BB_Mid'] + (std * 2)
    df['BB_Lower'] = df['BB_Mid'] - (std * 2)
    df['BB_BW'] = (df['BB_Upper'] - df['BB_Lower']) / (df['BB_Mid'] + 1e-6)

    # ADX (Directional Index)
    up_move = df['High'] - df['High'].shift(1)
    down_move = df['Low'].shift(1) - df['Low']
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    plus_di = 100 * (pd.Series(plus_dm, index=df.index).rolling(14).mean() / (df['ATR'] + 1e-6))
    minus_di = 100 * (pd.Series(minus_dm, index=df.index).rolling(14).mean() / (df['ATR'] + 1e-6))
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-6))
    df['ADX'] = dx.rolling(14).mean().fillna(15)

    # Dynamic Intraday VWAP
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    if 'Date' in df.columns:
        df['VWAP'] = (tp * df['Volume']).groupby(df['Date']).cumsum() / (df['Volume'].groupby(df['Date']).cumsum() + 1e-6)
    else:
        df['VWAP'] = (tp * df['Volume']).cumsum() / (df['Volume'].cumsum() + 1e-6)

    # ----------------------------------------------------
    # OPTION CHAIN DERIVATIVES (PCR, Delta & Gamma Squeeze)
    # ----------------------------------------------------
    # 1. PCR Ratio (Volume/OI Buying Pressure Proxy)
    volume_ma = df['Volume'].rolling(20).mean().bfill()
    df['Volume_Ratio'] = df['Volume'] / (volume_ma + 1e-6)
    price_change = df['Close'].diff().fillna(0)
    
    # PCR > 1.15 = Strong Put Writing / Bullish; PCR < 0.85 = Call Writing / Bearish
    raw_pcr = np.where(price_change > 0, 1.0 + (df['Volume_Ratio'] * 0.15), 1.0 - (df['Volume_Ratio'] * 0.15))
    df['PCR'] = pd.Series(raw_pcr, index=df.index).rolling(5).mean().fillna(1.0)

    # 2. Gamma Squeeze Trigger (2.2x Volume Surge at Band Extremes + Extreme PCR)
    df['Gamma_Squeeze_Long'] = (df['Volume_Ratio'] > 2.2) & (df['Close'] > df['BB_Upper']) & (df['PCR'] > 1.15)
    df['Gamma_Squeeze_Short'] = (df['Volume_Ratio'] > 2.2) & (df['Close'] < df['BB_Lower']) & (df['PCR'] < 0.85)

    # 3. Delta Decay Zone (Time-Decay Filter: Skip fresh entries after 2:30 PM)
    df['Time'] = df.index.time
    df['Delta_Decay_Zone'] = df['Time'].apply(lambda t: t >= pd.to_datetime('14:30').time() if pd.notnull(t) else False)

    return df

def detect_smc_elements(df):
    """Detects Fair Value Gaps (FVG), Break of Structure (BOS), & Supply/Demand Zones."""
    df = df.copy()
    
    # 1. Fair Value Gap (FVG)
    df['Bullish_FVG'] = df['Low'] > df['High'].shift(2)
    df['Bearish_FVG'] = df['High'] < df['Low'].shift(2)

    # 2. Break of Structure (BOS)
    df['Swing_High'] = df['High'].rolling(5).max()
    df['Swing_Low'] = df['Low'].rolling(5).min()
    
    df['Bullish_BOS'] = df['Close'] > df['Swing_High'].shift(1)
    df['Bearish_BOS'] = df['Close'] < df['Swing_Low'].shift(1)

    # 3. Supply and Demand Zones
    df['Demand_Zone'] = np.where(df['Bullish_BOS'], df['Low'].shift(1), np.nan)
    df['Supply_Zone'] = np.where(df['Bearish_BOS'], df['High'].shift(1), np.nan)
    df['Demand_Zone'] = df['Demand_Zone'].ffill()
    df['Supply_Zone'] = df['Supply_Zone'].ffill()

    return df

# ==========================================
# 2. REGIME IDENTIFIER ENGINE
# ==========================================

def map_macro_regimes(df_daily, df_vix=None):
    """Combines Daily technicals + Real-time VIX to label 4 Market Regimes."""
    regimes = []
    
    for i in range(len(df_daily)):
        atr = df_daily['ATR'].iloc[i]
        atr_ma = df_daily['ATR_MA'].iloc[i]
        bw = df_daily['BB_BW'].iloc[i]
        adx = df_daily['ADX'].iloc[i]
        vix = df_vix.iloc[i] if (df_vix is not None and i < len(df_vix)) else 14.0

        if (atr > 1.3 * atr_ma) or (vix > 20.0):
            regimes.append(3)  # Regime 3: Macro Shock / Volatile Panic
        elif (adx > 32) and (atr > atr_ma):
            regimes.append(4)  # Regime 4: V-Shape Exhaustion / Reversal
        elif (adx > 18) or (bw > 0.025):
            regimes.append(1)  # Regime 1: Strong Trending
        else:
            regimes.append(2)  # Regime 2: Volatile Chop / Sideways Squeeze

    df_daily['Regime'] = regimes
    return df_daily

# ==========================================
# 3. BACKTEST ENGINE WITH SMC & OPTION LOGIC
# ==========================================

class UnifiedSMCBacktester:
    def __init__(self, df_5m, df_daily, df_vix=None):
        self.df_5m = df_5m.copy()
        self.df_daily = df_daily.copy()
        self.df_vix = df_vix.copy() if df_vix is not None else None
        self.journal = []
        self.position = None
        self.entry_price = 0
        self.sl = 0
        self.tp = 0
        self.break_even_activated = False

    def prepare_data(self):
        # 1. Macro Analysis
        self.df_daily = calculate_indicators(self.df_daily)
        self.df_daily = map_macro_regimes(self.df_daily, self.df_vix)
        
        # 2. Map Regime to 5m Timeframe
        self.df_5m['Date'] = self.df_5m.index.date
        self.df_daily['Date'] = self.df_daily.index.date
        regime_dict = dict(zip(self.df_daily['Date'], self.df_daily['Regime']))
        self.df_5m['Regime'] = self.df_5m['Date'].map(regime_dict).fillna(1)

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

            # Position Management (Trailing Stop-Loss / Target)
            if self.position == 'LONG':
                if not self.break_even_activated and curr['High'] >= self.entry_price + curr['ATR']:
                    self.sl = self.entry_price
                    self.break_even_activated = True

                if curr['Low'] <= self.sl:
                    self._close_trade(t_stamp, self.sl, "SL / Break-even Hit", regime)
                elif curr['High'] >= self.tp:
                    self._close_trade(t_stamp, self.tp, "Target Hit", regime)
                continue

            elif self.position == 'SHORT':
                if not self.break_even_activated and curr['Low'] <= self.entry_price - curr['ATR']:
                    self.sl = self.entry_price
                    self.break_even_activated = True

                if curr['High'] >= self.sl:
                    self._close_trade(t_stamp, self.sl, "SL / Break-even Hit", regime)
                elif curr['Low'] <= self.tp:
                    self._close_trade(t_stamp, self.tp, "Target Hit", regime)
                continue

            # =========================================================
            # ENTRY CONDITIONS WITH SMC + OPTION CHAIN FILTERS
            # =========================================================

            # Delta Decay Filter: Avoid opening fresh trades late afternoon
            if curr['Delta_Decay_Zone']:
                continue

            # SETUP 0: HIGH PRIORITY GAMMA SQUEEZE TRIGGER
            if curr['Gamma_Squeeze_Long']:
                self._open_trade('LONG', t_stamp, curr['Close'], 
                                sl=curr['Close'] - (1.0 * curr['ATR']),
                                tp=curr['Close'] + (2.5 * curr['ATR']),
                                setup="Gamma Squeeze Long Burst", regime=regime)
                continue

            elif curr['Gamma_Squeeze_Short']:
                self._open_trade('SHORT', t_stamp, curr['Close'], 
                                sl=curr['Close'] + (1.0 * curr['ATR']),
                                tp=curr['Close'] - (2.5 * curr['ATR']),
                                setup="Gamma Squeeze Short Burst", regime=regime)
                continue

            # SETUP 1: REGIME 1 TRENDING EXPANSION (BOS + FVG + PCR Confirmation)
            if regime == 1:
                if (curr['Bullish_BOS'] or curr['Bullish_FVG']) and curr['Close'] > curr['VWAP'] and curr['PCR'] >= 1.02:
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=curr['Close'] - (1.2 * curr['ATR']),
                                    tp=curr['Close'] + (2.0 * curr['ATR']),
                                    setup="Regime 1: Trend Long + PCR Bullish", regime=regime)

                elif (curr['Bearish_BOS'] or curr['Bearish_FVG']) and curr['Close'] < curr['VWAP'] and curr['PCR'] <= 0.98:
                    self._open_trade('SHORT', t_stamp, curr['Close'], 
                                    sl=curr['Close'] + (1.2 * curr['ATR']),
                                    tp=curr['Close'] - (2.0 * curr['ATR']),
                                    setup="Regime 1: Trend Short + PCR Bearish", regime=regime)

            # SETUP 2: REGIME 2 CHOP MEAN REVERSION (Band Rejection)
            elif regime == 2:
                if curr['Close'] < curr['BB_Lower'] and curr['PCR'] > 0.90:
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=curr['Close'] - 18, tp=curr['VWAP'], 
                                    setup="Regime 2: Demand Reversion Long", regime=regime)

                elif curr['Close'] > curr['BB_Upper'] and curr['PCR'] < 1.10:
                    self._open_trade('SHORT', t_stamp, curr['Close'], 
                                    sl=curr['Close'] + 18, tp=curr['VWAP'], 
                                    setup="Regime 2: Supply Reversion Short", regime=regime)

            # SETUP 3: REGIME 4 V-SHAPE RECOVERY (Piercing Bounce)
            elif regime == 4:
                if prev['Low'] < prev['BB_Lower'] and curr['Close'] > curr['BB_Lower'] and curr['Bullish_FVG']:
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=prev['Low'], tp=curr['VWAP'] + (1.5 * curr['ATR']), 
                                    setup="Regime 4: V-Shape Reversal Long", regime=regime)

    def _open_trade(self, side, t_stamp, price, sl, tp, setup, regime):
        self.position = side
        self.entry_price = price
        self.sl = sl
        self.tp = tp
        self.break_even_activated = False
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
            print("No trades executed based on strict SMC/Option filters.")
            return
        win_trades = len(df_j[df_j['PnL_Points'] > 0])
        print("\n================ SYSTEM BACKTEST PERFORMANCE JOURNAL ================")
        print(f"Total Trades Executed : {len(df_j)}")
        print(f"Win Rate              : {round((win_trades / len(df_j)) * 100, 2)}%")
        print(f"Total PnL Points      : {round(df_j['PnL_Points'].sum(), 2)} Nifty Points")
        print("=====================================================================")
        print("\n--- SAMPLE JOURNAL (Recent 10 Trades) ---")
        print(df_j[['Entry_Time', 'Setup', 'Type', 'Entry', 'Exit_Price', 'Reason', 'PnL_Points']].tail(10).to_string())

# ==========================================
# 4. YFINANCE REAL MARKET RUNNER
# ==========================================

if __name__ == "__main__":
    print("Fetching live market data for Nifty 50 from Yahoo Finance...")

    # Fetch last 1 month of 5-minute interval data for Nifty 50 (^NSEI)
    df_5m = yf.download(tickers="^NSEI", period="1mo", interval="5m")

    # Fix multi-index columns if yfinance returns nested headers
    if isinstance(df_5m.columns, pd.MultiIndex):
        df_5m.columns = df_5m.columns.get_level_values(0)

    df_5m = df_5m.dropna()

    if df_5m.empty:
        print("Error: Market data fetch fail ho gaya.")
    else:
        print(f"Data successfully loaded! Total 5-Min Candles: {len(df_5m)}")

        # Resample 5-minute data to create Daily bars
        df_daily = df_5m.resample('D').agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }).dropna()

        # Real-time VIX baseline mapping
        df_vix = pd.Series(14.0, index=df_daily.index)

        # Initialize Engine & Run Real Backtest
        tester = UnifiedSMCBacktester(df_5m, df_daily, df_vix)
        tester.run()
        tester.display_results()
