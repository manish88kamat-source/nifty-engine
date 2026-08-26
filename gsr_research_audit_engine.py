"""
GSR Research Audit Engine
Version: 1.0.0

Independent audit layer for GSR research runs.

Research governance only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json


ENGINE_VERSION = "GSR-AUDIT-1.0.0"



@dataclass
class AuditResult:

    run_id: str

    integrity_status: str

    audit_score: float

    passed_checks: List[str]

    failed_checks: List[str]

    timestamp: str



class GSRResearchAuditEngine:


    def __init__(self):

        self.version = ENGINE_VERSION



    def check_modules(
        self,
        run: Dict[str,Any]
    ):

        required = [

            "historical_replay",

            "evidence_pipeline",

            "ranking_engine",

            "leaderboard",

            "promotion_engine",

            "lifecycle_manager"

        ]


        passed = []
        failed = []


        modules = run.get(
            "modules",
            {}
        )


        for module in required:

            if modules.get(module) == "PASS":

                passed.append(
                    module
                )

            else:

                failed.append(
                    module
                )


        return passed, failed



    def audit(
        self,
        run: Dict[str,Any]
    ):


        passed = []
        failed = []


        module_pass, module_fail = self.check_modules(
            run
        )


        passed.extend(
            module_pass
        )


        failed.extend(
            module_fail
        )


        checks = {


            "completed_status":
            run.get(
                "status"
            )
            ==
            "COMPLETED",


            "strategies_processed":
            run.get(
                "strategies_processed",
                0
            )
            >
            0,


            "module_integrity":
            len(module_fail)
            ==
            0


        }



        for name,value in checks.items():

            if value:

                passed.append(
                    name
                )

            else:

                failed.append(
                    name
                )



        total = len(passed)+len(failed)


        score = round(

            (

                len(passed)
                /
                total

            )
            *
            100,

            2

        )



        status = (

            "PASS"

            if len(failed)==0

            else

            "FAIL"

        )



        return AuditResult(

            run_id=
            run.get(
                "run_id",
                "UNKNOWN"
            ),

            integrity_status=status,

            audit_score=score,

            passed_checks=passed,

            failed_checks=failed,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )



    def export(
        self,
        result: AuditResult,
        path="gsr_data/research_audit.json"
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

                    "audit":
                    asdict(result)

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchAuditEngine()


    run = {


        "run_id":
        "GSR_RUN_TEST_001",


        "status":
        "COMPLETED",


        "strategies_processed":
        113,


        "modules":

        {

            "historical_replay":
            "PASS",

            "evidence_pipeline":
            "PASS",

            "ranking_engine":
            "PASS",

            "leaderboard":
            "PASS",

            "promotion_engine":
            "PASS",

            "lifecycle_manager":
            "PASS"

        }

    }



    result = engine.audit(
        run
    )


    assert (
        result.integrity_status
        ==
        "PASS"
    )


    assert (
        result.audit_score
        ==
        100
    )


    print(
        "GSR RESEARCH AUDIT TEST: PASS"
    )



if __name__=="__main__":

    self_test()
