NIFTY Next-Day Stock Alpha Engine

Files:
1. app.py                       = existing NIFTY engine + independent display hook
2. next_day_alpha_engine.py    = independent backend stock-ranking engine

Pipeline:
~NIFTY-500 -> quality/liquidity 100 -> momentum 30 -> broad high-score checkpoint 50
-> 7-day volume shock -> Top 5 -> Top 2

The existing NIFTY RegimeEngine, DecisionEngine, LabelEngine, option chain logic,
Kotak feed and NIFTY feature calculations are not used as inputs to the stock ranker.
The stock ranker writes only to ./next_day_alpha/latest.json and renders its own UI.

Schedule:
- After 16:30 IST: backend scan starts automatically in a background thread.
- Next morning: only Top-5 are polled for live 1-minute prices.
- The NIFTY core engine remains independent.

Important:
- The ConfidencePct field is a heuristic ranking confidence, NOT a calibrated ML probability.
- The system must be backtested with leakage-free next-day labels before claiming any 60-70%
  win rate or deploying real-money trades.
