"""
GSR Trend Classifier
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Classify market trend state from feature inputs.
"""

from typing import Dict, Any


class GSRTrendClassifier:

    def __init__(self):
        self.version = "1.0.0"


    def classify(
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



def create_trend_classifier():
    return GSRTrendClassifier()



def trend_classifier_test():

    classifier = GSRTrendClassifier()

    bullish = classifier.classify(
        {
            "close": 110,
            "EMA20": 108,
            "EMA50": 105,
            "EMA200": 100
        }
    )

    bearish = classifier.classify(
        {
            "close": 90,
            "EMA20": 92,
            "EMA50": 95,
            "EMA200": 100
        }
    )

    assert bullish == "UP_TREND"
    assert bearish == "DOWN_TREND"

    print("GSR TREND CLASSIFIER TEST: PASS")


if __name__ == "__main__":
    trend_classifier_test()
