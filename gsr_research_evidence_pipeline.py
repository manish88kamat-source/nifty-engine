"""
GSR Research Evidence Pipeline
Version: 1.0.0

Purpose:
Connect replay performance output
with evidence generation layer.

Research only.
No live trading.
"""


from pathlib import Path
import json
from datetime import datetime, timezone

from gsr_evidence_report_generator import (
    EvidenceEngine
)


OUTPUT_PATH = Path(
    "gsr_data/research_evidence"
)


class ResearchEvidencePipeline:


    def __init__(self):

        self.engine = EvidenceEngine()



    def normalize_metrics(self, raw):

        return {

            "trades": int(
                raw.get(
                    "trades",
                    0
                )
            ),

            "win_rate": float(
                raw.get(
                    "win_rate",
                    0
                )
            ),

            "profit_factor": float(
                raw.get(
                    "profit_factor",
                    0
                )
            ),

            "max_drawdown": float(
                raw.get(
                    "max_drawdown",
                    0
                )
            )
        }



    def generate(
        self,
        strategy_id,
        replay_metrics
    ):

        metrics = self.normalize_metrics(
            replay_metrics
        )


        report = self.engine.generate(

            strategy_id,

            metrics

        )


        return report



    def save(
        self,
        report
    ):

        OUTPUT_PATH.mkdir(
            parents=True,
            exist_ok=True
        )


        path = OUTPUT_PATH / (
            report.strategy_id
            +
            "_evidence.json"
        )


        with open(
            path,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                report.__dict__,
                f,
                indent=2
            )


        return path



def self_test():


    pipeline = ResearchEvidencePipeline()


    replay_metrics = {

        "trades": 60,

        "win_rate": 0.62,

        "profit_factor": 1.55,

        "max_drawdown": 10

    }


    report = pipeline.generate(

        "GSR_AT_001",

        replay_metrics

    )


    assert (
        report.evidence_grade
        ==
        "A"
    )


    assert (
        report.promotion_status
        ==
        "PROMOTE"
    )


    path = pipeline.save(
        report
    )


    assert path.exists()


    print(
        "GSR RESEARCH EVIDENCE PIPELINE TEST: PASS"
    )



if __name__ == "__main__":

    self_test()
