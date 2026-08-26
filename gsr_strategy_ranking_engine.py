"""
GSR Strategy Ranking Engine
Version: 1.1.0-INSTITUTIONAL

Evidence driven strategy ranking layer.

Research only.
No live execution.
No trading permission.
"""


from dataclasses import dataclass, asdict
from typing import List, Dict, Any
from datetime import datetime, timezone
from pathlib import Path
import json


ENGINE_VERSION = "GSR-RANKING-1.1.0"


@dataclass
class StrategyRank:

    strategy_id: str

    evidence_grade: str

    robustness_score: float

    oos_score: float

    regime_score: float

    sample_confidence: float

    cost_stability: float

    parameter_stability: float

    win_rate: float

    profit_factor: float

    max_drawdown: float

    final_score: float

    status: str

    explanation: str



class StrategyRankingEngine:


    def __init__(self):

        self.version = ENGINE_VERSION



    def grade_weight(
        self,
        grade: str
    ):

        return {

            "A":100,
            "B":75,
            "C":50,
            "D":25

        }.get(
            grade,
            0
        )



    def evidence_gate(
        self,
        report: Dict[str,Any]
    ):

        required = [

            "rule_verified",

            "replay_verified",

            "validation_verified"

        ]


        passed = 0


        for key in required:

            if report.get(
                key,
                False
            ):

                passed += 1


        return (
            passed /
            len(required)
        )



    def calculate_score(
        self,
        report: Dict[str,Any]
    ):


        score = 0.0


        score += (

            self.grade_weight(
                report.get(
                    "evidence_grade",
                    "D"
                )
            )
            *
            0.15

        )


        score += (

            float(
                report.get(
                    "robustness_score",
                    0
                )
            )
            *
            0.15

        )


        score += (

            float(
                report.get(
                    "oos_score",
                    0
                )
            )
            *
            0.20

        )


        score += (

            float(
                report.get(
                    "regime_score",
                    0
                )
            )
            *
            0.15

        )


        score += (

            float(
                report.get(
                    "sample_confidence",
                    0
                )
            )
            *
            0.10

        )


        score += (

            float(
                report.get(
                    "cost_stability",
                    0
                )
            )
            *
            0.10

        )


        score += (

            float(
                report.get(
                    "parameter_stability",
                    0
                )
            )
            *
            0.05

        )


        score += (

            float(
                report.get(
                    "win_rate",
                    0
                )
            )
            *
            100
            *
            0.05

        )


        score -= (

            min(

                float(
                    report.get(
                        "max_drawdown",
                        0
                    )
                ),

                50

            )
            *
            0.10

        )


        score *= self.evidence_gate(
            report
        )


        return round(
            max(score,0),
            2
        )



    def classify(
        self,
        score
    ):


        if score >=80:

            return "PROMOTE"


        if score >=60:

            return "WATCH"


        return "REJECT"



    def explain(
        self,
        score,
        status
    ):


        return (

            f"Score={score}. "

            f"Research status={status}. "

            "Decision based on evidence, "
            "validation, robustness and stability."

        )



    def rank(
        self,
        reports:List[Dict[str,Any]]
    ):


        results=[]


        for report in reports:


            score = self.calculate_score(
                report
            )


            status = self.classify(
                score
            )


            results.append(

                StrategyRank(

                    strategy_id=
                    report["strategy_id"],


                    evidence_grade=
                    report.get(
                        "evidence_grade",
                        "D"
                    ),


                    robustness_score=
                    report.get(
                        "robustness_score",
                        0
                    ),


                    oos_score=
                    report.get(
                        "oos_score",
                        0
                    ),


                    regime_score=
                    report.get(
                        "regime_score",
                        0
                    ),


                    sample_confidence=
                    report.get(
                        "sample_confidence",
                        0
                    ),


                    cost_stability=
                    report.get(
                        "cost_stability",
                        0
                    ),


                    parameter_stability=
                    report.get(
                        "parameter_stability",
                        0
                    ),


                    win_rate=
                    report.get(
                        "win_rate",
                        0
                    ),


                    profit_factor=
                    report.get(
                        "profit_factor",
                        0
                    ),


                    max_drawdown=
                    report.get(
                        "max_drawdown",
                        0
                    ),


                    final_score=score,


                    status=status,


                    explanation=
                    self.explain(
                        score,
                        status
                    )

                )

            )


        return sorted(

            results,

            key=lambda x:
            x.final_score,

            reverse=True

        )



    def export_json(
        self,
        ranking,
        path="gsr_data/strategy_leaderboard.json"
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

                    "generated_at":
                    datetime.now(
                        timezone.utc
                    ).isoformat(),

                    "ranking":

                    [

                        asdict(x)

                        for x in ranking

                    ]

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = StrategyRankingEngine()


    reports=[

        {

            "strategy_id":
            "GSR_AT_001",

            "evidence_grade":
            "A",

            "robustness_score":
            95,

            "oos_score":
            90,

            "regime_score":
            85,

            "sample_confidence":
            90,

            "cost_stability":
            85,

            "parameter_stability":
            90,

            "win_rate":
            0.65,

            "profit_factor":
            2.0,

            "max_drawdown":
            8,

            "rule_verified":
            True,

            "replay_verified":
            True,

            "validation_verified":
            True

        }

    ]


    ranking = engine.rank(
        reports
    )


    assert ranking[0].status == "PROMOTE"


    engine.export_json(
        ranking
    )


    print(
        "GSR STRATEGY RANKING TEST: PASS"
    )



if __name__=="__main__":

    self_test()
