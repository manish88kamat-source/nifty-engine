"""
GSR Market Knowledge Experiment Designer
Version: 1.0.0

Converts research hypotheses into
structured experiment blueprints.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-MARKET-KNOWLEDGE-EXPERIMENT-DESIGNER-1.0.0"



@dataclass
class ResearchExperiment:


    experiment_id: str

    source_hypothesis_id: str

    experiment_name: str

    research_question: str

    variables: List[str]

    metrics: List[str]

    success_criteria: str

    created_at: str



class GSRMarketKnowledgeExperimentDesigner:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.experiments = {}



    def design(

        self,

        experiment_id: str,

        hypothesis_id: str,

        hypothesis_statement: str

    ):


        variables = []

        metrics = []


        if "volatility" in hypothesis_statement.lower():

            experiment_name = (

                "Volatility Adaptation Validation"

            )


            variables = [

                "volatility_filter",

                "regime_detection",

                "risk_model"

            ]


            metrics = [

                "sharpe_ratio",

                "drawdown",

                "win_rate",

                "stability"

            ]


            success = (

                "Improved risk adjusted performance "

                "across volatility regimes"

            )


        else:


            experiment_name = (

                "Market Behavior Validation"

            )


            variables = [

                "market_condition",

                "strategy_parameter"

            ]


            metrics = [

                "performance",

                "robustness"

            ]


            success = (

                "Evidence of repeatable market behavior"

            )



        experiment = ResearchExperiment(

            experiment_id=experiment_id,

            source_hypothesis_id=hypothesis_id,

            experiment_name=experiment_name,

            research_question=hypothesis_statement,

            variables=variables,

            metrics=metrics,

            success_criteria=success,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.experiments[experiment_id] = experiment


        return experiment



    def get(

        self,

        experiment_id

    ):


        return self.experiments.get(

            experiment_id

        )



    def export(

        self,

        path="gsr_data/research_experiments.json"

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


    engine = GSRMarketKnowledgeExperimentDesigner()



    result = engine.design(

        experiment_id="EXP_001",

        hypothesis_id="HYP_001",

        hypothesis_statement=

        "Adaptive volatility controls may improve risk adjusted robustness"

    )


    assert (

        result.experiment_name

        ==

        "Volatility Adaptation Validation"

    )


    assert (

        "sharpe_ratio"

        in

        result.metrics

    )


    stored = engine.get(

        "EXP_001"

    )


    assert stored is not None



    print(

        "GSR MARKET KNOWLEDGE EXPERIMENT DESIGNER TEST: PASS"

    )



if __name__=="__main__":

    self_test()
