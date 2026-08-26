"""
GSR Regime Engine
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Classify market state from feature snapshots.

Rules:
- No strategy logic
- No trade signal
- No prediction

Only:
- Trend classification
- Volatility classification
- Momentum classification
- Structure classification
- Transition tracking
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional


@dataclass(frozen=True)
class RegimeState:

    timestamp: str
    symbol: str

    trend_state: str
    volatility_state: str
    momentum_state: str
    structure_state: str
    transition_state: str

    composite_state: str
    regime_version: str = "1.0.0"

    def to_dict(self):
        return asdict(self)


class GSRRegimeEngine:

    def __init__(self):
        self.version = "1.0.0"


    def classify_trend(
        self,
        features: Dict[str, Any]
    ) -> str:

        close = features.get("close", 0)
        ema20 = features.get("EMA20", 0)
        ema50 = features.get("EMA50", 0)
        ema200 = features.get("EMA200", 0)

        if close > ema20 > ema50 > ema200:
            return "UP_TREND"

        if close < ema20 < ema50 < ema200:
            return "DOWN_TREND"

        return "SIDEWAYS"


    def classify_volatility(
        self,
        features: Dict[str, Any]
    ) -> str:

        close = features.get("close", 0)
        atr = features.get("ATR", 0)

        if close == 0:
            return "UNKNOWN"

        atr_percent = (atr / close) * 100

        if atr_percent < 0.3:
            return "LOW_VOL"

        if atr_percent > 1:
            return "HIGH_VOL"

        return "NORMAL_VOL"


    def classify_momentum(
        self,
        features: Dict[str, Any]
    ) -> str:

        rsi = features.get("RSI", 50)
        roc = features.get("ROC", 0)

        if rsi >= 60 and roc >= 0:
            return "STRONG"

        if rsi <= 40 and roc < 0:
            return "WEAK"

        return "NEUTRAL"


    def classify_structure(
        self,
        features: Dict[str, Any]
    ) -> str:

        ema20 = features.get("EMA20", 0)
        ema50 = features.get("EMA50", 0)

        if ema20 > ema50:
            return "TRENDING"

        if ema20 < ema50:
            return "RANGE"

        return "COMPRESSION"


    def classify_transition(
        self,
        previous_state: Optional[str],
        current_state: str
    ) -> str:

        if previous_state is None:
            return "INITIAL"

        if previous_state != current_state:
            return "TRANSITION"

        return "STABLE"


    def generate(
        self,
        timestamp: str,
        symbol: str,
        features: Dict[str, Any],
        previous_state: Optional[str] = None
    ) -> RegimeState:

        trend = self.classify_trend(features)
        volatility = self.classify_volatility(features)
        momentum = self.classify_momentum(features)
        structure = self.classify_structure(features)

        transition = self.classify_transition(
            previous_state,
            trend
        )

        composite = (
            f"{trend}_"
            f"{volatility}_"
            f"{momentum}"
        )

        return RegimeState(
            timestamp=timestamp,
            symbol=symbol,
            trend_state=trend,
            volatility_state=volatility,
            momentum_state=momentum,
            structure_state=structure,
            transition_state=transition,
            composite_state=composite
        )


def create_regime_engine():
    return GSRRegimeEngine()



def regime_test():

    engine = GSRRegimeEngine()

    features = {
        "close": 110,
        "EMA20": 108,
        "EMA50": 105,
        "EMA200": 100,
        "ATR": 1,
        "RSI": 65,
        "ROC": 2
    }

    state = engine.generate(
        timestamp="2026-01-01T09:18:00",
        symbol="NIFTY",
        features=features
    )

    assert state.trend_state == "UP_TREND"
    assert state.momentum_state == "STRONG"

    print("GSR REGIME ENGINE TEST: PASS")


if __name__ == "__main__":
    regime_test()
