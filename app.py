import pandas as pd
import numpy as np

# ==========================================
# 1. CORE TECHNICAL & SMC INDICATOR ENGINE
# ==========================================

def calculate_indicators(df):
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

    # ADX
    up_move = df['High'] - df['High'].shift(1)
    down_move = df['Low'].shift(1) - df['Low']
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    plus_di = 100 * (pd.Series(plus_dm, index=df.index).rolling(14).mean() / (df['ATR'] + 1e-6))
    minus_di = 100 * (pd.Series(minus_dm, index=df.index).rolling(14).mean() / (df['ATR'] + 1e-6))
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-6))
    df['ADX'] = dx.rolling(14).mean().fillna(15)

    # Intraday VWAP
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    if 'Date' in df.columns:
        df['VWAP'] = (tp * df['Volume']).groupby(df['Date']).cumsum() / (df['Volume'].groupby(df['Date']).cumsum() + 1e-6)
    else:
        df['VWAP'] = (tp * df['Volume']).cumsum() / (df['Volume'].cumsum() + 1e-6)

    return df

def detect_smc_elements(df):
    df = df.copy()
    
    # 1. Fair Value Gap (FVG) - Relaxed 3-bar gap
    df['Bullish_FVG'] = df['Low'] > df['High'].shift(2)
    df['Bearish_FVG'] = df['High'] < df['Low'].shift(2)

    # 2. Break of Structure (BOS) - 5 period local swings
    df['Swing_High'] = df['High'].rolling(5).max()
    df['Swing_Low'] = df['Low'].rolling(5).min()
    
    df['Bullish_BOS'] = df['Close'] > df['Swing_High'].shift(1)
    df['Bearish_BOS'] = df['Close'] < df['Swing_Low'].shift(1)

    # 3. Supply & Demand
    df['Demand_Zone'] = np.where(df['Bullish_BOS'], df['Low'].shift(1), np.nan)
    df['Supply_Zone'] = np.where(df['Bearish_BOS'], df['High'].shift(1), np.nan)
    df['Demand_Zone'] = df['Demand_Zone'].ffill()
    df['Supply_Zone'] = df['Supply_Zone'].ffill()

    return df

# ==========================================
# 2. OPTIMIZED REGIME MAPPER
# ==========================================

def map_macro_regimes(df_daily):
    regimes = []
    
    for i in range(len(df_daily)):
        atr = df_daily['ATR'].iloc[i]
        atr_ma = df_daily['ATR_MA'].iloc[i]
        bw = df_daily['BB_BW'].iloc[i]
        adx = df_daily['ADX'].iloc[i]

        # Optimized Thresholds for Real Executions
        if atr > 1.3 * atr_ma:
            regimes.append(3)  # Volatile Expansion
        elif adx > 30:
            regimes.append(4)  # High Momentum / Exhaustion
        elif adx > 18 or bw > 0.025:
            regimes.append(1)  # Trending
        else:
            regimes.append(2)  # Rangebound / Chop

    df_daily['Regime'] = regimes
    return df_daily

# ==========================================
# 3. BACKTEST ENGINE
# ==========================================

class UnifiedSMCBacktester:
    def __init__(self, df_5m, df_daily):
        self.df_5m = df_5m.copy()
        self.df_daily = df_daily.copy()
        self.journal = []
        self.position = None
        self.entry_price = 0
        self.sl = 0
        self.tp = 0

    def prepare_data(self):
        self.df_daily = calculate_indicators(self.df_daily)
        self.df_daily = map_macro_regimes(self.df_daily)
        
        self.df_5m['Date'] = self.df_5m.index.date
        self.df_daily['Date'] = self.df_daily.index.date
        regime_dict = dict(zip(self.df_daily['Date'], self.df_daily['Regime']))
        self.df_5m['Regime'] = self.df_5m['Date'].map(regime_dict).fillna(1)

        self.df_5m = calculate_indicators(self.df_5m)
        self.df_5m = detect_smc_elements(self.df_5m)

    def run(self):
        self.prepare_data()

        for i in range(2, len(self.df_5m)):
            curr = self.df_5m.iloc[i]
            t_stamp = self.df_5m.index[i]
            regime = curr['Regime']

            # Position Management
            if self.position == 'LONG':
                if curr['Low'] <= self.sl:
                    self._close_trade(t_stamp, self.sl, "SL Hit")
                elif curr['High'] >= self.tp:
                    self._close_trade(t_stamp, self.tp, "Target Hit")
                continue

            elif self.position == 'SHORT':
                if curr['High'] >= self.sl:
                    self._close_trade(t_stamp, self.sl, "SL Hit")
                elif curr['Low'] <= self.tp:
                    self._close_trade(t_stamp, self.tp, "Target Hit")
                continue

            # ENTRY TRIGGERS (Optimized SMC + Indicator Rules)
            if regime == 1: # Trending Execution
                if (curr['Bullish_BOS'] or curr['Bullish_FVG']) and curr['Close'] > curr['VWAP']:
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=curr['Close'] - (1.2 * curr['ATR']),
                                    tp=curr['Close'] + (2.0 * curr['ATR']),
                                    setup="Regime 1: Trend Expansion", regime=regime)

                elif (curr['Bearish_BOS'] or curr['Bearish_FVG']) and curr['Close'] < curr['VWAP']:
                    self._open_trade('SHORT', t_stamp, curr['Close'], 
                                    sl=curr['Close'] + (1.2 * curr['ATR']),
                                    tp=curr['Close'] - (2.0 * curr['ATR']),
                                    setup="Regime 1: Trend Breakdown", regime=regime)

            elif regime in [2, 3, 4]: # Reversion & Mean Touch
                if curr['Close'] < curr['BB_Lower']:
                    self._open_trade('LONG', t_stamp, curr['Close'], 
                                    sl=curr['Close'] - 20, tp=curr['VWAP'], 
                                    setup="Mean Reversion Long", regime=regime)

                elif curr['Close'] > curr['BB_Upper']:
                    self._open_trade('SHORT', t_stamp, curr['Close'], 
                                    sl=curr['Close'] + 20, tp=curr['VWAP'], 
                                    setup="Mean Reversion Short", regime=regime)

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

    def _close_trade(self, t_stamp, exit_price, reason):
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
            print("No trades executed.")
            return
        print("\n================ SYSTEM BACKTEST PERFORMANCE JOURNAL ================")
        print(f"Total Trades Executed : {len(df_j)}")
        print(f"Win Rate              : {round((len(df_j[df_j['PnL_Points'] > 0]) / len(df_j)) * 100, 2)}%")
        print(f"Total PnL Points      : {round(df_j['PnL_Points'].sum(), 2)} Nifty Points")
        print("=====================================================================")
        print("\n--- TRADE JOURNAL LOG (First 10 Trades) ---")
        print(df_j[['Entry_Time', 'Setup', 'Type', 'Entry', 'Exit_Price', 'Reason', 'PnL_Points']].head(10).to_string())

# ==========================================
# 4. RUNNER BLOCK
# ==========================================

if __name__ == "__main__":
    import yfinance as yf

    print("Fetching Nifty 50 data from Yahoo Finance...")
    df_5m = yf.download(tickers="^NSEI", period="1mo", interval="5m")

    if isinstance(df_5m.columns, pd.MultiIndex):
        df_5m.columns = df_5m.columns.get_level_values(0)

    df_5m = df_5m.dropna()

    if df_5m.empty:
        print("Error fetching data.")
    else:
        print(f"Data successfully loaded! Total 5-Min Candles: {len(df_5m)}")

        df_daily = df_5m.resample('D').agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }).dropna()

        tester = UnifiedSMCBacktester(df_5m, df_daily)
        tester.run()
        tester.display_results()
