"""
GSR Strategy Similarity Engine
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Compare strategy DNA structures.

Rules:
- No performance ranking
- No trading decision
- Only structural similarity
"""

from typing import Dict, Any, List


class GSRStrategySimilarityEngine:

    def __init__(self):
        self.version = "1.0.0"


    def tag_similarity(
        self,
        tags_a: List[str],
        tags_b: List[str]
    ) -> float:

        set_a = set(tags_a)
        set_b = set(tags_b)

        if not set_a and not set_b:
            return 1.0

        union = set_a | set_b
        intersection = set_a & set_b

        return len(intersection) / len(union)


    def compare(
        self,
        dna_a: Dict[str, Any],
        dna_b: Dict[str, Any]
    ) -> Dict[str, Any]:

        score = 0.0

        if dna_a.get("family") == dna_b.get("family"):
            score += 0.35

        if dna_a.get("mechanism") == dna_b.get("mechanism"):
            score += 0.35

        score += (
            self.tag_similarity(
                dna_a.get("mechanism_tags", []),
                dna_b.get("mechanism_tags", [])
            ) * 0.30
        )

        return {
            "similarity_score": round(score, 4),
            "same_family": (
                dna_a.get("family")
                ==
                dna_b.get("family")
            ),
            "same_mechanism": (
                dna_a.get("mechanism")
                ==
                dna_b.get("mechanism")
            )
        }



def create_similarity_engine():
    return GSRStrategySimilarityEngine()



def strategy_similarity_test():

    engine = GSRStrategySimilarityEngine()

    dna_a = {
        "family": "TREND_FOLLOWING",
        "mechanism": "BREAKOUT",
        "mechanism_tags": [
            "MOMENTUM",
            "BREAKOUT"
        ]
    }

    dna_b = {
        "family": "TREND_FOLLOWING",
        "mechanism": "BREAKOUT",
        "mechanism_tags": [
            "MOMENTUM",
            "BREAKOUT"
        ]
    }

    result = engine.compare(
        dna_a,
        dna_b
    )

    assert result["similarity_score"] == 1.0

    print("GSR STRATEGY SIMILARITY TEST: PASS")


if __name__ == "__main__":
    strategy_similarity_test()
