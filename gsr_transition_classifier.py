"""
GSR Transition Classifier
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Detect regime state transitions.

Rules:
- No strategy logic
- No signal generation
"""

from typing import Optional


class GSRTransitionClassifier:

    def __init__(self):
        self.version = "1.0.0"


    def classify(
        self,
        previous_state: Optional[str],
        current_state: str
    ) -> str:

        if previous_state is None:
            return "INITIAL"

        if previous_state != current_state:
            return "TRANSITION"

        return "STABLE"



def create_transition_classifier():
    return GSRTransitionClassifier()



def transition_classifier_test():

    classifier = GSRTransitionClassifier()

    initial = classifier.classify(
        None,
        "UP_TREND"
    )

    stable = classifier.classify(
        "UP_TREND",
        "UP_TREND"
    )

    transition = classifier.classify(
        "UP_TREND",
        "DOWN_TREND"
    )

    assert initial == "INITIAL"
    assert stable == "STABLE"
    assert transition == "TRANSITION"

    print("GSR TRANSITION CLASSIFIER TEST: PASS")


if __name__ == "__main__":
    transition_classifier_test()
