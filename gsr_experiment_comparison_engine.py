"""
GSR Experiment Comparison Engine
Version: 1.0.0

Compares research experiments and
identifies improvement/regression.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-COMPARISON-1.0.0"



@dataclass
class ExperimentComparison:

    comparison_id: str

    baseline_experiment: str

    candidate_experiment: str

    baseline_score: float

    candidate_score: float

    improvement_delta: float

    outcome: str

    recommendation: str

    timestamp: str



class GSRExperimentComparisonEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.comparisons = {}



    def compare(
        self,
        comparison_id: str,
        baseline: Dict[str,Any],
        candidate: Dict[str,Any]
    ):


        baseline_score = float(
            baseline.get(
                "score",
                0
            )
        )


        candidate_score = float(
            candidate.get(
                "score",
                0
            )
        )


        delta = round(

            candidate_score
            -
            baseline_score,

            2

        )


        if delta > 0:

            outcome = "IMPROVED"

            recommendation = (
                "Candidate experiment "
                "shows improvement over baseline."
            )


        elif delta < 0:

            outcome = "REGRESSION"

            recommendation = (
                "Candidate experiment "
                "degraded performance."
            )


        else:

            outcome = "NO_CHANGE"

            recommendation = (
                "No measurable improvement."
            )



        result = ExperimentComparison(

            comparison_id=
            comparison_id,

            baseline_experiment=
            baseline.get(
                "experiment_id",
                "UNKNOWN"
            ),

            candidate_experiment=
            candidate.get(
                "experiment_id",
                "UNKNOWN"
            ),

            baseline_score=
            baseline_score,

            candidate_score=
            candidate_score,

            improvement_delta=
            delta,

            outcome=
            outcome,

            recommendation=
            recommendation,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.comparisons[comparison_id] = result


        return result



    def best_candidate(
        self,
        experiments: List[Dict[str,Any]]
    ):


        return max(

            experiments,

            key=lambda x:
            x.get(
                "score",
                0
            )

        )



    def export(
        self,
        path="gsr_data/experiment_comparisons.json"
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


                    "comparisons":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.comparisons.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRExperimentComparisonEngine()


    baseline = {

        "experiment_id":
        "EXP_001",

        "score":
        72

    }


    candidate = {

        "experiment_id":
        "EXP_002",

        "score":
        86

    }


    result = engine.compare(

        "COMP_001",

        baseline,

        candidate

    )


    assert (

        result.outcome

        ==

        "IMPROVED"

    )


    assert (

        result.improvement_delta

        ==

        14

    )


    winner = engine.best_candidate(

        [

            baseline,

            candidate

        ]

    )


    assert (

        winner["experiment_id"]

        ==

        "EXP_002"

    )


    print(
        "GSR EXPERIMENT COMPARISON TEST: PASS"
    )



if __name__=="__main__":

    self_test()

