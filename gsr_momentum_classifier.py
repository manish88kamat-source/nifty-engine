"""
GSR Momentum Classifier
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Classify market momentum state from feature inputs.

Rules:
- No strategy logic
- No signal generation
"""

from typing import Dict, Any


class GSRMomentumClassifier:

    def __init__(self):
        self.version = "1.0.0"


    def classify(
        self,
        features: Dict[str, Any]
    ) -> str:

        rsi = features.get("RSI", 50)
        roc = features.get("ROC", 0)

        if rsi >= 60 and roc > 0:
            return "STRONG"

        if rsi <= 40 and roc < 0:
            return "WEAK"

        return "NEUTRAL"



def create_momentum_classifier():
    return GSRMomentumClassifier()



def momentum_classifier_test():

    classifier = GSRMomentumClassifier()

    strong = classifier.classify(
        {
            "RSI": 65,
            "ROC": 2
        }
    )

    weak = classifier.classify(
        {
            "RSI": 35,
            "ROC": -2
        }
    )

    neutral = classifier.classify(
        {
            "RSI": 50,
            "ROC": 0
        }
    )

    assert strong == "STRONG"
    assert weak == "WEAK"
    assert neutral == "NEUTRAL"

    print("GSR MOMENTUM CLASSIFIER TEST: PASS")


if __name__ == "__main__":
    momentum_classifier_test()
