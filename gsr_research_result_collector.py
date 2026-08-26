"""
GSR Research Result Collector
Version: 1.0.0

Collects outputs from GSR research pipeline
into unified research result.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-RESULT-COLLECTOR-1.0.0"



@dataclass
class ResearchResult:

    run_id: str

    hypothesis_id: str

    experiment_results: List[Dict[str,Any]]

    validation_status: str

    evidence_grade: str

    audit_status: str

    final_decision: str

    recommendation: str

    timestamp: str



class GSRResearchResultCollector:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.results = {}



    def collect(

        self,

        run_id: str,

        hypothesis_id: str,

        experiment_results: List[Dict[str,Any]],

        validation_status: str,

        evidence_grade: str,

        audit_status: str,

        recommendation: str

    ):


        decision = self.make_decision(

            validation_status,

            evidence_grade,

            audit_status

        )


        result = ResearchResult(

            run_id=run_id,

            hypothesis_id=hypothesis_id,

            experiment_results=experiment_results,

            validation_status=validation_status,

            evidence_grade=evidence_grade,

            audit_status=audit_status,

            final_decision=decision,

            recommendation=recommendation,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.results[run_id] = result


        return result



    def make_decision(

        self,

        validation_status,

        evidence_grade,

        audit_status

    ):


        if (

            validation_status == "PASS"

            and

            audit_status == "PASS"

            and

            evidence_grade in ["A","B"]

        ):

            return "PROMOTE"



        if validation_status == "PASS":

            return "WATCH"



        return "REJECT"



    def get_result(

        self,

        run_id

    ):


        return self.results.get(
            run_id
        )



    def export(

        self,

        path="gsr_data/research_results.json"

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


                    "results":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.results.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchResultCollector()



    result = engine.collect(

        run_id="GSR_RUN_001",

        hypothesis_id="HYP_001",

        experiment_results=[

            {

                "experiment_id":

                "EXP_001",

                "result":

                "VALIDATED"

            }

        ],

        validation_status="PASS",

        evidence_grade="A",

        audit_status="PASS",

        recommendation=

        "Proceed with promotion review."

    )


    assert (

        result.final_decision

        ==

        "PROMOTE"

    )


    assert (

        result.evidence_grade

        ==

        "A"

    )


    stored = engine.get_result(

        "GSR_RUN_001"

    )


    assert stored is not None


    print(
        "GSR RESEARCH RESULT COLLECTOR TEST: PASS"
    )



if __name__=="__main__":

    self_test()
