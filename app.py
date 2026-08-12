import pandas as pd
import numpy as np
import yfinance as yf

# ==========================================
# 1. INDICATORS & SMC ENGINE
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

    # Dynamic VWAP
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    df['VWAP'] = (tp * df['Volume']).cumsum() / (df['Volume'].cumsum() + 1e-6)

    # Option Derivatives Proxy
    volume_ma = df['Volume'].rolling(20).mean().bfill()
    df['Volume_Ratio'] = df['Volume'] / (volume_ma + 1e-6)
    price_change = df['Close'].diff().fillna(0)
    
    raw_pcr = np.where(price_change > 0, 1.0 + (df['Volume_Ratio'] * 0.15), 1.0 - (df['Volume_Ratio'] * 0.15))
    df['PCR'] = pd.Series(raw_pcr, index=df.index).rolling(5).mean().fillna(1.0)

    df['Gamma_Squeeze_Long'] = (df['Volume_Ratio'] > 2.2) & (df['Close'] > df['BB_Upper']) & (df['PCR'] > 1.15)
    df['Gamma_Squeeze_Short'] = (df['Volume_Ratio'] > 2.2) & (df['Close'] < df['BB_Lower']) & (df['PCR'] < 0.85)

    df['Time'] = df.index.time
    df['Delta_Decay_Zone'] = df['Time'].apply(lambda t: t >= pd.to_datetime('14:30').time() if pd.notnull(t) else False)

    # SMC Signals
    df['Bullish_BOS'] = df['Close'] > df['High'].shift(1)
    df['Bearish_BOS'] = df['Close'] < df['Low'].shift(1)
    df['Bullish_FVG'] = df['Low'] > df['High'].shift(2)
    df['Bearish_FVG'] = df['High'] < df['Low'].shift(2)

    return df

# ==========================================
# 2. MACRO REGIME IDENTIFIER
# ==========================================

def map_macro_regimes(df_daily):
    regimes = []
    for i in range(len(df_daily)):
        atr = float(df_daily['ATR'].iloc[i])
        atr_ma = float(df_daily['ATR_MA'].iloc[i])
        bw = float(df_daily['BB_BW'].iloc[i])
        adx = float(df_daily['ADX'].iloc[i])

        if atr > 1.3 * atr_ma:
            regimes.append(3)
        elif adx > 32 and atr > atr_ma:
            regimes.append(4)
        elif adx > 18 or bw > 0.025:
            regimes.append(1)
        else:
            regimes.append(2)

    df_daily['Regime'] = regimes
    return df_daily

# ==========================================
# 3. BACKTEST ENGINE (FLOAT VALUE WRAPPER)
# ==========================================

def run_backtest():
    print("Fetching live market data for Nifty 50 from Yahoo Finance...")
    df_5m = yf.download(tickers="^NSEI", period="1mo", interval="5m")

    if isinstance(df_5m.columns, pd.MultiIndex):
        df_5m.columns = df_5m.columns.get_level_values(0)

    df_5m = df_5m.dropna()
    
    df_daily = df_5m.resample('D').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
    }).dropna()

    df_daily = calculate_indicators(df_daily)
    df_daily = map_macro_regimes(df_daily)

    df_5m['Date'] = df_5m.index.date
    df_daily['Date'] = df_daily.index.date
    regime_dict = dict(zip(df_daily['Date'], df_daily['Regime']))
    df_5m['Regime'] = df_5m['Date'].map(regime_dict).fillna(1)

    df_5m = calculate_indicators(df_5m)

    journal = []
    position = None
    entry_price = 0.0
    sl = 0.0
    tp = 0.0
    break_even_activated = False

    for i in range(2, len(df_5m)):
        curr_close = float(df_5m['Close'].iloc[i])
        curr_high = float(df_5m['High'].iloc[i])
        curr_low = float(df_5m['Low'].iloc[i])
        curr_atr = float(df_5m['ATR'].iloc[i])
        curr_vwap = float(df_5m['VWAP'].iloc[i])
        curr_bb_upper = float(df_5m['BB_Upper'].iloc[i])
        curr_bb_lower = float(df_5m['BB_Lower'].iloc[i])
        curr_pcr = float(df_5m['PCR'].iloc[i])
        
        prev_low = float(df_5m['Low'].iloc[i-1])
        prev_bb_lower = float(df_5m['BB_Lower'].iloc[i-1])

        t_stamp = df_5m.index[i]
        regime = int(df_5m['Regime'].iloc[i])

        # Manage Position
        if position == 'LONG':
            if not break_even_activated and curr_high >= entry_price + curr_atr:
                sl = entry_price
                break_even_activated = True

            if curr_low <= sl:
                pnl = sl - entry_price
                journal.append({'Time': t_stamp, 'Regime': regime, 'Type': 'LONG', 'Entry': round(entry_price, 2), 'Exit': round(sl, 2), 'PnL': round(pnl, 2), 'Reason': 'SL / Break-even'})
                position = None
                continue
            elif curr_high >= tp:
                pnl = tp - entry_price
                journal.append({'Time': t_stamp, 'Regime': regime, 'Type': 'LONG', 'Entry': round(entry_price, 2), 'Exit': round(tp, 2), 'PnL': round(pnl, 2), 'Reason': 'Target Hit'})
                position = None
                continue

        elif position == 'SHORT':
            if not break_even_activated and curr_low <= entry_price - curr_atr:
                sl = entry_price
                break_even_activated = True

            if curr_high >= sl:
                pnl = entry_price - sl
                journal.append({'Time': t_stamp, 'Regime': regime, 'Type': 'SHORT', 'Entry': round(entry_price, 2), 'Exit': round(sl, 2), 'PnL': round(pnl, 2), 'Reason': 'SL / Break-even'})
                position = None
                continue
            elif curr_low <= tp:
                pnl = entry_price - tp
                journal.append({'Time': t_stamp, 'Regime': regime, 'Type': 'SHORT', 'Entry': round(entry_price, 2), 'Exit': round(tp, 2), 'PnL': round(pnl, 2), 'Reason': 'Target Hit'})
                position = None
                continue

        # ENTRY CONDITIONS
        if df_5m['Delta_Decay_Zone'].iloc[i]:
            continue

        # Gamma Squeeze
        if df_5m['Gamma_Squeeze_Long'].iloc[i]:
            position = 'LONG'
            entry_price = curr_close
            sl = entry_price - (1.0 * curr_atr)
            tp = entry_price + (2.5 * curr_atr)
            break_even_activated = False
            continue
        elif df_5m['Gamma_Squeeze_Short'].iloc[i]:
            position = 'SHORT'
            entry_price = curr_close
            sl = entry_price + (1.0 * curr_atr)
            tp = entry_price - (2.5 * curr_atr)
            break_even_activated = False
            continue

        # Regime 1: Trend Expansion
        if regime == 1:
            if (df_5m['Bullish_BOS'].iloc[i] or df_5m['Bullish_FVG'].iloc[i]) and curr_close > curr_vwap and curr_pcr >= 1.02:
                position = 'LONG'
                entry_price = curr_close
                sl = entry_price - (1.2 * curr_atr)
                tp = entry_price + (2.0 * curr_atr)
                break_even_activated = False

            elif (df_5m['Bearish_BOS'].iloc[i] or df_5m['Bearish_FVG'].iloc[i]) and curr_close < curr_vwap and curr_pcr <= 0.98:
                position = 'SHORT'
                entry_price = curr_close
                sl = entry_price + (1.2 * curr_atr)
                tp = entry_price - (2.0 * curr_atr)
                break_even_activated = False

        # Regime 2: Chop Reversion (Fixing Exit Price VWAP mapping)
        elif regime == 2:
            if curr_close < curr_bb_lower and curr_pcr > 0.90:
                position = 'LONG'
                entry_price = curr_close
                sl = entry_price - 18.0
                tp = curr_vwap
                break_even_activated = False

            elif curr_close > curr_bb_upper and curr_pcr < 1.10:
                position = 'SHORT'
                entry_price = curr_close
                sl = entry_price + 18.0
                tp = curr_vwap
                break_even_activated = False

        # Regime 4: V-Shape Reversal
        elif regime == 4:
            if prev_low < prev_bb_lower and curr_close > curr_bb_lower and df_5m['Bullish_FVG'].iloc[i]:
                position = 'LONG'
                entry_price = curr_close
                sl = prev_low
                tp = curr_vwap + (1.5 * curr_atr)
                break_even_activated = False

    # Summary
    df_j = pd.DataFrame(journal)
    print("\n================ SYSTEM BACKTEST PERFORMANCE JOURNAL ================")
    if df_j.empty:
        print("No trades executed.")
    else:
        win_trades = len(df_j[df_j['PnL'] > 0])
        print(f"Total Trades Executed : {len(df_j)}")
        print(f"Win Rate              : {round((win_trades / len(df_j)) * 100, 2)}%")
        print(f"Total PnL Points      : {round(df_j['PnL'].sum(), 2)} Nifty Points")
        print("=====================================================================")
        print("\n--- SAMPLE JOURNAL (Recent 10 Trades) ---")
        print(df_j[['Time', 'Regime', 'Type', 'Entry', 'Exit', 'PnL', 'Reason']].tail(10).to_string())

if __name__ == "__main__":
    run_backtest()
