"""
GSR Strategy Promotion Engine
Version: 1.0.0

Research promotion governance layer.

Ranking does not equal approval.

No live trading permission.
Research only.
"""


from dataclasses import dataclass, asdict
from typing import Dict, Any, List
from datetime import datetime, timezone
from pathlib import Path
import json


ENGINE_VERSION = "GSR-PROMOTION-1.0.0"



@dataclass
class PromotionDecision:

    strategy_id: str

    decision: str

    promotion_stage: str

    score: float

    passed_gates: List[str]

    failed_gates: List[str]

    timestamp: str



class StrategyPromotionEngine:


    def __init__(self):

        self.version = ENGINE_VERSION



    def evaluate(
        self,
        strategy: Dict[str, Any]
    ):


        passed = []

        failed = []


        gates = {


            "minimum_score":
            strategy.get(
                "final_score",
                0
            ) >= 80,


            "evidence_complete":
            strategy.get(
                "evidence_complete",
                False
            ),


            "rule_verified":
            strategy.get(
                "rule_verified",
                False
            ),


            "replay_verified":
            strategy.get(
                "replay_verified",
                False
            ),


            "oos_validation":
            strategy.get(
                "oos_verified",
                False
            ),


            "drawdown_control":
            strategy.get(
                "max_drawdown",
                100
            ) <= 30


        }



        for name, result in gates.items():

            if result:

                passed.append(name)

            else:

                failed.append(name)



        if len(failed) == 0:


            decision = "PROMOTED"


            stage = "RESEARCH_APPROVED"



        elif "minimum_score" in failed:


            decision = "REJECTED"


            stage = "INSUFFICIENT_SCORE"



        else:


            decision = "PENDING"


            stage = "NEEDS_MORE_EVIDENCE"



        return PromotionDecision(

            strategy_id=
            strategy.get(
                "strategy_id",
                "UNKNOWN"
            ),

            decision=decision,

            promotion_stage=stage,

            score=float(
                strategy.get(
                    "final_score",
                    0
                )
            ),

            passed_gates=passed,

            failed_gates=failed,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )



    def batch_evaluate(
        self,
        strategies
    ):


        return [

            self.evaluate(x)

            for x in strategies

        ]



    def export(
        self,
        decisions,
        path=
        "gsr_data/promotion_decisions.json"
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


                    "decisions":

                    [

                        asdict(x)

                        for x in decisions

                    ]

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = StrategyPromotionEngine()



    strategy = {


        "strategy_id":
        "GSR_AT_001",


        "final_score":
        88,


        "evidence_complete":
        True,


        "rule_verified":
        True,


        "replay_verified":
        True,


        "oos_verified":
        True,


        "max_drawdown":
        12

    }



    result = engine.evaluate(
        strategy
    )


    assert (
        result.decision
        ==
        "PROMOTED"
    )


    assert (
        len(
            result.failed_gates
        )
        ==
        0
    )


    print(
        "GSR STRATEGY PROMOTION TEST: PASS"
    )



if __name__=="__main__":

    self_test()
