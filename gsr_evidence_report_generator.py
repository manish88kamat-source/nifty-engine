"""
GSR Evidence Report Generator
Version: 1.0.0

Purpose:
Convert replay metrics into research evidence reports.

No trading decision.
No live execution.
Research governance only.
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any
import json
from datetime import datetime, timezone


@dataclass
class EvidenceReport:

    strategy_id: str

    trades: int

    win_rate: float

    profit_factor: float

    max_drawdown: float

    robustness_score: float

    evidence_grade: str

    promotion_status: str

    generated_at: str



class EvidenceEngine:


    def __init__(self):

        self.version = "GSR-EVIDENCE-1.0.0"



    def calculate_score(
        self,
        metrics: Dict[str, Any]
    ) -> float:


        score = 0.0


        win_rate = float(
            metrics.get(
                "win_rate",
                0
            )
        )


        profit_factor = float(
            metrics.get(
                "profit_factor",
                0
            )
        )


        drawdown = abs(
            float(
                metrics.get(
                    "max_drawdown",
                    0
                )
            )
        )


        if win_rate >= 0.50:
            score += 30


        if profit_factor >= 1.2:
            score += 30


        if drawdown < 20:
            score += 20


        trades = int(
            metrics.get(
                "trades",
                0
            )
        )


        if trades >= 30:
            score += 20


        return min(
            score,
            100
        )



    def grade(
        self,
        score: float
    ) -> str:


        if score >= 80:
            return "A"

        if score >= 60:
            return "B"

        if score >= 40:
            return "C"

        return "D"



    def decision(
        self,
        grade: str
    ) -> str:


        if grade == "A":
            return "PROMOTE"

        if grade == "B":
            return "WATCH"

        return "REJECT"



    def generate(
        self,
        strategy_id: str,
        metrics: Dict[str, Any]
    ) -> EvidenceReport:


        score = self.calculate_score(
            metrics
        )


        grade = self.grade(
            score
        )


        return EvidenceReport(

            strategy_id=strategy_id,

            trades=int(
                metrics.get(
                    "trades",
                    0
                )
            ),

            win_rate=float(
                metrics.get(
                    "win_rate",
                    0
                )
            ),

            profit_factor=float(
                metrics.get(
                    "profit_factor",
                    0
                )
            ),

            max_drawdown=float(
                metrics.get(
                    "max_drawdown",
                    0
                )
            ),

            robustness_score=score,

            evidence_grade=grade,

            promotion_status=self.decision(
                grade
            ),

            generated_at=datetime.now(
                timezone.utc
            ).isoformat()
        )



def self_test():

    engine = EvidenceEngine()


    metrics = {

        "trades": 50,

        "win_rate": 0.58,

        "profit_factor": 1.45,

        "max_drawdown": 12

    }


    report = engine.generate(

        "GSR_AT_001",

        metrics

    )


    assert report.evidence_grade == "A"

    assert report.promotion_status == "PROMOTE"


    print(
        "GSR EVIDENCE REPORT TEST: PASS"
    )



if __name__ == "__main__":

    self_test()
