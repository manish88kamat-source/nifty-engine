"""
GSR Feature Engine
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Calculate normalized market features from validated observations.

Rules:
- No strategy logic
- No signal generation
- No prediction
"""

from typing import Dict, Any, Iterable


class GSRFeatureEngine:

    def __init__(self):
        self.feature_version = "1.0.0"


    def calculate_ema(
        self,
        values: Iterable[float],
        period: int
    ) -> float:

        values = list(values)

        if not values:
            return 0.0

        if len(values) < period:
            return sum(values) / len(values)

        multiplier = 2 / (period + 1)

        ema = sum(values[:period]) / period

        for price in values[period:]:
            ema = (
                price * multiplier
                +
                ema * (1 - multiplier)
            )

        return ema


    def calculate_atr(
        self,
        candles,
        period: int = 14
    ) -> float:

        if len(candles) < 2:
            return 0.0

        true_ranges = []

        for i in range(1, len(candles)):

            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_close = candles[i-1]["close"]

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )

            true_ranges.append(tr)

        if len(true_ranges) < period:
            return sum(true_ranges) / len(true_ranges)

        return sum(true_ranges[-period:]) / period


    def calculate_rsi(
        self,
        closes,
        period: int = 14
    ) -> float:

        if len(closes) <= period:
            return 50.0

        gains = []
        losses = []

        for i in range(1, len(closes)):

            change = closes[i] - closes[i-1]

            if change >= 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))

        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss

        return 100 - (100 / (1 + rs))


    def calculate(
        self,
        candles
    ) -> Dict[str, Any]:

        closes = [
            candle["close"]
            for candle in candles
        ]

        features = {

            "feature_version":
                self.feature_version,

            "EMA20":
                self.calculate_ema(
                    closes,
                    20
                ),

            "EMA50":
                self.calculate_ema(
                    closes,
                    50
                ),

            "EMA200":
                self.calculate_ema(
                    closes,
                    200
                ),

            "ATR":
                self.calculate_atr(
                    candles
                ),

            "RSI":
                self.calculate_rsi(
                    closes
                ),

            "close":
                closes[-1]
                if closes
                else 0.0
        }

        return features



def create_feature_engine():

    return GSRFeatureEngine()



def feature_engine_test():

    engine = GSRFeatureEngine()

    candles = [
        {
            "high": 101 + i,
            "low": 99 + i,
            "close": 100 + i
        }
        for i in range(30)
    ]

    result = engine.calculate(candles)

    assert result["feature_version"] == "1.0.0"
    assert "EMA20" in result
    assert "ATR" in result
    assert "RSI" in result

    print("GSR FEATURE ENGINE TEST: PASS")



if __name__ == "__main__":

    feature_engine_test()
