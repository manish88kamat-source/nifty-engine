"""
GSR Hypothesis Engine
Version: 1.0.0

Scientific hypothesis management layer.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-HYPOTHESIS-1.0.0"



VALID_STATUS = [

    "CREATED",

    "TESTING",

    "VALIDATED",

    "REJECTED",

    "ARCHIVED"

]



@dataclass
class ResearchHypothesis:

    hypothesis_id: str

    title: str

    statement: str

    null_hypothesis: str

    alternative_hypothesis: str

    success_criteria: List[str]

    failure_criteria: List[str]

    status: str

    evidence_links: List[str]

    timestamp: str



class GSRHypothesisEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.hypotheses = {}



    def create_hypothesis(
        self,
        hypothesis_id: str,
        title: str,
        statement: str,
        null_hypothesis: str,
        alternative_hypothesis: str,
        success_criteria: List[str],
        failure_criteria: List[str]
    ):


        record = ResearchHypothesis(

            hypothesis_id=hypothesis_id,

            title=title,

            statement=statement,

            null_hypothesis=null_hypothesis,

            alternative_hypothesis=alternative_hypothesis,

            success_criteria=success_criteria,

            failure_criteria=failure_criteria,

            status="CREATED",

            evidence_links=[],

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.hypotheses[hypothesis_id] = record


        return record



    def update_status(
        self,
        hypothesis_id: str,
        status: str
    ):


        if status not in VALID_STATUS:

            raise ValueError(
                "Invalid hypothesis status"
            )


        if hypothesis_id not in self.hypotheses:

            raise KeyError(
                "Hypothesis not found"
            )


        self.hypotheses[
            hypothesis_id
        ].status = status


        return self.hypotheses[
            hypothesis_id
        ]



    def attach_evidence(
        self,
        hypothesis_id: str,
        evidence_id: str
    ):


        if hypothesis_id not in self.hypotheses:

            raise KeyError(
                "Hypothesis not found"
            )


        self.hypotheses[
            hypothesis_id
        ].evidence_links.append(
            evidence_id
        )



    def get(
        self,
        hypothesis_id
    ):


        return self.hypotheses.get(
            hypothesis_id
        )



    def export(
        self,
        path="gsr_data/hypothesis_registry.json"
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


                    "hypotheses":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.hypotheses.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRHypothesisEngine()



    result = engine.create_hypothesis(

        hypothesis_id=
        "HYP_001",

        title=
        "Regime Filter Validation",

        statement=
        "Adding regime filter improves trend strategy robustness.",

        null_hypothesis=
        "Regime filter has no improvement impact.",

        alternative_hypothesis=
        "Regime filter improves OOS robustness.",

        success_criteria=[

            "OOS score improves >10%",

            "Drawdown does not increase"

        ],

        failure_criteria=[

            "No OOS improvement",

            "Higher instability"

        ]

    )


    assert (
        result.status
        ==
        "CREATED"
    )


    engine.update_status(

        "HYP_001",

        "TESTING"

    )


    engine.attach_evidence(

        "HYP_001",

        "EXP_0001"

    )


    final = engine.get(
        "HYP_001"
    )


    assert (
        final.status
        ==
        "TESTING"
    )


    assert (
        len(
            final.evidence_links
        )
        ==
        1
    )


    print(
        "GSR HYPOTHESIS ENGINE TEST: PASS"
    )



if __name__=="__main__":

    self_test()
