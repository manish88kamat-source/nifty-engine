"""
GSR Research Recommendation Engine
Version: 1.0.0

Converts research insights into
structured research recommendations.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import List, Dict, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-RECOMMENDATION-1.0.0"



@dataclass
class ResearchRecommendation:

    recommendation_id: str

    insight_id: str

    category: str

    priority: str

    recommendation: str

    affected_strategies: List[str]

    confidence: float

    timestamp: str



class GSRResearchRecommendationEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.recommendations = {}



    def priority_from_confidence(
        self,
        confidence: float
    ):


        if confidence >= 0.8:

            return "HIGH"


        if confidence >= 0.5:

            return "MEDIUM"


        return "LOW"



    def create_recommendation(
        self,
        recommendation_id: str,
        insight_id: str,
        category: str,
        observation: str,
        confidence: float,
        affected_strategies: List[str]
    ):


        priority = self.priority_from_confidence(
            confidence
        )


        recommendation_text = self.generate_action(
            category,
            observation
        )


        result = ResearchRecommendation(

            recommendation_id=
            recommendation_id,

            insight_id=
            insight_id,

            category=
            category,

            priority=
            priority,

            recommendation=
            recommendation_text,

            affected_strategies=
            affected_strategies,

            confidence=
            confidence,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.recommendations[recommendation_id] = result


        return result



    def generate_action(
        self,
        category,
        observation
    ):


        if category == "REGIME_PATTERN":

            return (
                "Introduce regime compatibility "
                "validation before strategy promotion."
            )


        if category == "FAILURE_PATTERN":

            return (
                "Investigate failure cause and "
                "create additional validation gate."
            )


        if category == "RISK_PATTERN":

            return (
                "Review risk controls and "
                "position sizing assumptions."
            )


        return (
            "Perform additional research validation."
        )



    def get_recommendations(
        self
    ):


        return list(
            self.recommendations.values()
        )



    def high_priority_queue(
        self
    ):


        return [

            item

            for item in self.recommendations.values()

            if item.priority == "HIGH"

        ]



    def export(
        self,
        path="gsr_data/research_recommendations.json"
    ):


        Path(path).parent.mkdir(

            parents=True,

            exist_ok=True

        )


        with open(
            path,
            "w",
            encoding="utf-8"
        ) as f:


            json.dump(

                {

                    "engine":
                    self.version,


                    "recommendations":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.recommendations.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchRecommendationEngine()



    result = engine.create_recommendation(

        recommendation_id=
        "REC_001",

        insight_id=
        "INSIGHT_REGIME_001",

        category=
        "REGIME_PATTERN",

        observation=
        "Trend strategies degrade in range regimes.",

        confidence=
        0.85,

        affected_strategies=[

            "GSR_AT_001",

            "GSR_AT_002"

        ]

    )


    assert (
        result.priority
        ==
        "HIGH"
    )


    assert (
        len(
            result.affected_strategies
        )
        ==
        2
    )


    queue = engine.high_priority_queue()


    assert len(queue)==1


    print(
        "GSR RESEARCH RECOMMENDATION TEST: PASS"
    )



if __name__=="__main__":

    self_test()
