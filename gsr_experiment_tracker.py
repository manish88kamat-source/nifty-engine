"""
GSR Experiment Tracker
Version: 1.0.0

Scientific research experiment tracking layer.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-EXPERIMENT-1.0.0"



@dataclass
class ExperimentRecord:

    experiment_id: str

    hypothesis: str

    strategy_id: str

    dataset_version: str

    parameters: Dict[str,Any]

    result: str

    decision: str

    lessons: str

    timestamp: str



class GSRExperimentTracker:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.experiments = {}



    def create_experiment(
        self,
        experiment_id: str,
        hypothesis: str,
        strategy_id: str,
        dataset_version: str,
        parameters: Dict[str,Any]
    ):


        record = ExperimentRecord(

            experiment_id=experiment_id,

            hypothesis=hypothesis,

            strategy_id=strategy_id,

            dataset_version=dataset_version,

            parameters=parameters,

            result="PENDING",

            decision="PENDING",

            lessons="",

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.experiments[experiment_id] = record


        return record



    def complete_experiment(
        self,
        experiment_id: str,
        result: str,
        decision: str,
        lessons: str
    ):


        if experiment_id not in self.experiments:

            raise KeyError(
                "Experiment not found"
            )


        record = self.experiments[
            experiment_id
        ]


        record.result = result

        record.decision = decision

        record.lessons = lessons


        return record



    def get_experiment(
        self,
        experiment_id
    ):


        return self.experiments.get(
            experiment_id
        )



    def list_by_strategy(
        self,
        strategy_id
    ):


        return [

            x

            for x in self.experiments.values()

            if x.strategy_id == strategy_id

        ]



    def export(
        self,
        path="gsr_data/experiment_history.json"
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


                    "experiments":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.experiments.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRExperimentTracker()



    engine.create_experiment(

        experiment_id=
        "EXP_0001",

        hypothesis=
        "Regime filter improves breakout stability.",

        strategy_id=
        "GSR_AT_001",

        dataset_version=
        "NIFTY_HISTORICAL_V1",

        parameters=

        {

            "regime_filter":
            True,

            "atr_period":
            14

        }

    )



    result = engine.complete_experiment(

        "EXP_0001",

        "VALIDATED",

        "PROMOTE",

        "Regime filter improved robustness."

    )


    assert (
        result.result
        ==
        "VALIDATED"
    )


    assert (
        result.decision
        ==
        "PROMOTE"
    )


    history = engine.list_by_strategy(
        "GSR_AT_001"
    )


    assert len(history)==1


    print(
        "GSR EXPERIMENT TRACKER TEST: PASS"
    )



if __name__=="__main__":

    self_test()
