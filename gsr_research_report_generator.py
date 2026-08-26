"""
GSR Research Report Generator
Version: 1.0.0

Generates institutional research reports
from validated GSR results.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-REPORT-GENERATOR-1.0.0"



@dataclass
class ResearchReport:

    report_id: str

    run_id: str

    title: str

    executive_summary: str

    hypothesis_summary: str

    experiment_summary: List[Dict[str,Any]]

    validation_status: str

    evidence_grade: str

    audit_status: str

    final_decision: str

    recommendation: str

    timestamp: str



class GSRResearchReportGenerator:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.reports = {}



    def generate(

        self,

        report_id: str,

        run_id: str,

        title: str,

        hypothesis_summary: str,

        experiment_summary: List[Dict[str,Any]],

        validation_status: str,

        evidence_grade: str,

        audit_status: str,

        final_decision: str,

        recommendation: str

    ):


        executive_summary = (

            f"Research run {run_id} evaluated. "

            f"Validation status: {validation_status}. "

            f"Evidence grade: {evidence_grade}. "

            f"Final decision: {final_decision}."

        )


        report = ResearchReport(

            report_id=report_id,

            run_id=run_id,

            title=title,

            executive_summary=executive_summary,

            hypothesis_summary=hypothesis_summary,

            experiment_summary=experiment_summary,

            validation_status=validation_status,

            evidence_grade=evidence_grade,

            audit_status=audit_status,

            final_decision=final_decision,

            recommendation=recommendation,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.reports[report_id] = report


        return report



    def get_report(

        self,

        report_id

    ):


        return self.reports.get(
            report_id
        )



    def export(

        self,

        path="gsr_data/research_reports.json"

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


                    "reports":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.reports.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchReportGenerator()



    report = engine.generate(

        report_id="REPORT_001",

        run_id="GSR_RUN_001",

        title="Trend Regime Filter Study",

        hypothesis_summary=

        "Regime filter improves trend strategy robustness.",

        experiment_summary=[

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

        final_decision="PROMOTE",

        recommendation=

        "Proceed to lifecycle review."

    )



    assert (

        report.final_decision

        ==

        "PROMOTE"

    )


    assert (

        report.evidence_grade

        ==

        "A"

    )


    stored = engine.get_report(

        "REPORT_001"

    )


    assert stored is not None



    print(
        "GSR RESEARCH REPORT GENERATOR TEST: PASS"
    )



if __name__=="__main__":

    self_test()
