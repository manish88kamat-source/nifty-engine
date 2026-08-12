import pandas as pd
import numpy as np
import yfinance as yf

# 1. Indicator Calculations
def prepare_data(df_5m):
    df = df_5m.copy()
    
    # Simple Technicals
    df['SMA_20'] = df['Close'].rolling(20).mean()
    std = df['Close'].rolling(20).std().fillna(0)
    df['BB_Upper'] = df['SMA_20'] + (std * 2)
    df['BB_Lower'] = df['SMA_20'] - (std * 2)
    
    # VWAP
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    df['VWAP'] = (tp * df['Volume']).cumsum() / (df['Volume'].cumsum() + 1e-6)
    
    # ATR
    tr = (df['High'] - df['Low'])
    df['ATR'] = tr.rolling(14).mean().bfill()
    
    # Simple SMC Signals
    df['Bullish_BOS'] = df['Close'] > df['High'].shift(1)
    df['Bearish_BOS'] = df['Close'] < df['Low'].shift(1)
    
    return df.dropna()

# 2. Execution Engine
def run_backtest():
    print("Fetching Nifty data via yfinance...")
    df_5m = yf.download(tickers="^NSEI", period="1mo", interval="5m")

    if isinstance(df_5m.columns, pd.MultiIndex):
        df_5m.columns = df_5m.columns.get_level_values(0)

    df_5m = prepare_data(df_5m)
    print(f"Data processed! Total Candles: {len(df_5m)}")

    journal = []
    position = None
    entry_price = 0
    sl = 0
    tp = 0

    for i in range(1, len(df_5m)):
        curr = df_5m.iloc[i]
        t_stamp = df_5m.index[i]

        # Manage Open Position
        if position == 'LONG':
            if curr['Low'] <= sl:
                journal.append({'Time': t_stamp, 'Type': 'LONG', 'Entry': entry_price, 'Exit': sl, 'PnL': sl - entry_price, 'Reason': 'SL Hit'})
                position = None
            elif curr['High'] >= tp:
                journal.append({'Time': t_stamp, 'Type': 'LONG', 'Entry': entry_price, 'Exit': tp, 'PnL': tp - entry_price, 'Reason': 'TP Hit'})
                position = None
            continue

        elif position == 'SHORT':
            if curr['High'] >= sl:
                journal.append({'Time': t_stamp, 'Type': 'SHORT', 'Entry': entry_price, 'Exit': sl, 'PnL': entry_price - sl, 'Reason': 'SL Hit'})
                position = None
            elif curr['Low'] <= tp:
                journal.append({'Time': t_stamp, 'Type': 'SHORT', 'Entry': entry_price, 'Exit': tp, 'PnL': entry_price - tp, 'Reason': 'TP Hit'})
                position = None
            continue

        # Strategy Rules (VWAP + BB + BOS)
        if curr['Close'] > curr['VWAP'] and curr['Bullish_BOS']:
            position = 'LONG'
            entry_price = curr['Close']
            sl = entry_price - (1.5 * curr['ATR'])
            tp = entry_price + (2.5 * curr['ATR'])

        elif curr['Close'] < curr['VWAP'] and curr['Bearish_BOS']:
            position = 'SHORT'
            entry_price = curr['Close']
            sl = entry_price + (1.5 * curr['ATR'])
            tp = entry_price - (2.5 * curr['ATR'])

    # Display Results
    df_j = pd.DataFrame(journal)
    print("\n================ BACKTEST RESULTS ================")
    if df_j.empty:
        print("Still no trades. Check indicator calculation logic.")
    else:
        print(f"Total Trades Executed : {len(df_j)}")
        print(f"Win Rate              : {round((len(df_j[df_j['PnL'] > 0]) / len(df_j)) * 100, 2)}%")
        print(f"Total PnL Points      : {round(df_j['PnL'].sum(), 2)} Points")
        print("\n--- FIRST 5 TRADES ---")
        print(df_j[['Time', 'Type', 'Entry', 'Exit', 'PnL', 'Reason']].head().to_string())

if __name__ == "__main__":
    run_backtest()
