"""
GSR Research Insight Engine
Version: 1.0.0

Converts research knowledge into structured insights.

Research intelligence layer.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-INSIGHT-1.0.0"



@dataclass
class ResearchInsight:

    insight_id: str

    insight_type: str

    observation: str

    confidence: float

    evidence_count: int

    related_strategies: List[str]

    recommendation: str

    timestamp: str



class GSRResearchInsightEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.insights = {}



    def generate_insight(
        self,
        insight_id: str,
        insight_type: str,
        observation: str,
        evidence_records: List[Dict[str,Any]],
        recommendation: str
    ):


        confidence = 0


        if evidence_records:

            confidence = min(

                len(evidence_records)
                /
                10,

                1.0

            )


        strategies = []


        for item in evidence_records:

            strategy = item.get(
                "strategy_id"
            )

            if strategy:

                strategies.append(
                    strategy
                )



        insight = ResearchInsight(

            insight_id=insight_id,

            insight_type=insight_type,

            observation=observation,

            confidence=round(
                confidence,
                2
            ),

            evidence_count=len(
                evidence_records
            ),

            related_strategies=strategies,

            recommendation=recommendation,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.insights[insight_id] = insight


        return insight



    def get_insight(
        self,
        insight_id
    ):


        return self.insights.get(
            insight_id
        )



    def list_by_type(
        self,
        insight_type
    ):


        return [

            x

            for x in self.insights.values()

            if x.insight_type == insight_type

        ]



    def export(
        self,
        path="gsr_data/research_insights.json"
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


                    "insights":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.insights.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchInsightEngine()



    evidence = [

        {

            "strategy_id":
            "GSR_AT_001"

        },


        {

            "strategy_id":
            "GSR_AT_002"

        },


        {

            "strategy_id":
            "GSR_AT_003"

        }

    ]



    result = engine.generate_insight(

        insight_id=
        "INSIGHT_REGIME_001",

        insight_type=
        "REGIME_PATTERN",

        observation=
        "Trend strategies require regime confirmation.",

        evidence_records=
        evidence,

        recommendation=
        "Apply regime filter before promotion."

    )



    assert (
        result.evidence_count
        ==
        3
    )


    assert (
        result.confidence
        ==
        0.3
    )


    assert (
        len(
            result.related_strategies
        )
        ==
        3
    )


    print(
        "GSR RESEARCH INSIGHT TEST: PASS"
    )



if __name__=="__main__":

    self_test()
