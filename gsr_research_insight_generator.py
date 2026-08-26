"""
GSR Research Insight Generator
Version: 1.0.0

Generates research insights from
reasoning outcomes.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-INSIGHT-GENERATOR-1.0.0"



@dataclass
class ResearchInsight:

    insight_id: str

    reasoning_id: str

    observation: str

    key_learning: str

    future_research: List[str]

    confidence: float

    category: str



class GSRResearchInsightGenerator:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.insights = {}



    def generate(

        self,

        insight_id: str,

        reasoning_id: str,

        decision: str,

        supporting_factors: List[str],

        risk_factors: List[str],

        confidence: float

    ):


        if decision == "PROMOTE":

            observation = (

                "Research evidence shows a strong "

                "positive relationship between "

                "tested factors and outcome."

            )

            category = "CONFIRMED_PATTERN"


        elif decision == "WATCH":

            observation = (

                "Research shows potential but "

                "requires additional validation."

            )

            category = "EMERGING_PATTERN"


        else:

            observation = (

                "Research evidence is insufficient "

                "for adoption."

            )

            category = "FAILED_PATTERN"



        learning = (

            "Key supporting factors: "

            +

            ", ".join(
                supporting_factors
            )

        )


        future = []


        if risk_factors:

            future.append(

                "Investigate identified risk factors."

            )


        future.append(

            "Validate across additional market regimes."

        )


        insight = ResearchInsight(

            insight_id=insight_id,

            reasoning_id=reasoning_id,

            observation=observation,

            key_learning=learning,

            future_research=future,

            confidence=confidence,

            category=category

        )


        self.insights[insight_id] = insight


        return insight



    def get(

        self,

        insight_id

    ):


        return self.insights.get(

            insight_id

        )



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


    engine = GSRResearchInsightGenerator()



    insight = engine.generate(

        insight_id="INSIGHT_001",

        reasoning_id="REASON_001",

        decision="PROMOTE",

        supporting_factors=[

            "Strong evidence quality",

            "Validation passed",

            "Historical confirmation"

        ],

        risk_factors=[

            "Limited regime samples"

        ],

        confidence=92

    )


    assert (

        insight.category

        ==

        "CONFIRMED_PATTERN"

    )


    assert (

        insight.confidence

        ==

        92

    )


    stored = engine.get(

        "INSIGHT_001"

    )


    assert stored is not None



    print(
        "GSR RESEARCH INSIGHT GENERATOR TEST: PASS"
    )



if __name__=="__main__":

    self_test()
