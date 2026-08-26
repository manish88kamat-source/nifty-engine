"""
GSR Structure Classifier
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Classify market structure state.

Rules:
- No strategy logic
- No signal generation
"""

from typing import Dict, Any


class GSRStructureClassifier:

    def __init__(self):
        self.version = "1.0.0"


    def classify(
        self,
        features: Dict[str, Any]
    ) -> str:

        ema20 = features.get("EMA20", 0)
        ema50 = features.get("EMA50", 0)
        ema200 = features.get("EMA200", 0)

        spread_fast = abs(ema20 - ema50)
        spread_slow = abs(ema50 - ema200)

        # Compression has priority.
        # When moving averages are very close,
        # market structure is considered compressed.
        if spread_fast < 0.5 and spread_slow < 1:
            return "COMPRESSION"

        # Directional alignment
        if ema20 > ema50 > ema200:
            return "TRENDING"

        if ema20 < ema50 < ema200:
            return "TRENDING"

        return "RANGE"



def create_structure_classifier():
    return GSRStructureClassifier()



def structure_classifier_test():

    classifier = GSRStructureClassifier()

    trending = classifier.classify(
        {
            "EMA20": 110,
            "EMA50": 105,
            "EMA200": 100
        }
    )

    compression = classifier.classify(
        {
            "EMA20": 100.2,
            "EMA50": 100,
            "EMA200": 99.5
        }
    )

    range_state = classifier.classify(
        {
            "EMA20": 102,
            "EMA50": 100,
            "EMA200": 105
        }
    )

    assert trending == "TRENDING"
    assert compression == "COMPRESSION"
    assert range_state == "RANGE"

    print("GSR STRUCTURE CLASSIFIER TEST: PASS")


if __name__ == "__main__":
    structure_classifier_test()
