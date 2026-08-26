"""
GSR Research Run Manager
Version: 1.0.0

Central controller for GSR research executions.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-RUN-MANAGER-1.0.0"



VALID_STATUS = [

    "CREATED",

    "RUNNING",

    "COMPLETED",

    "FAILED",

    "ARCHIVED"

]



@dataclass
class ResearchRun:


    run_id: str

    hypothesis_id: str

    status: str

    jobs: List[str]

    experiments: List[str]

    audit_result: str

    created_at: str

    completed_at: str



class GSRResearchRunManager:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.runs = {}



    def create_run(
        self,
        run_id: str,
        hypothesis_id: str
    ):


        run = ResearchRun(

            run_id=run_id,

            hypothesis_id=hypothesis_id,

            status="CREATED",

            jobs=[],

            experiments=[],

            audit_result="PENDING",

            created_at=
            datetime.now(
                timezone.utc
            ).isoformat(),

            completed_at=""

        )


        self.runs[run_id] = run


        return run



    def start_run(
        self,
        run_id
    ):


        run = self.runs[run_id]


        run.status = "RUNNING"


        return run



    def attach_job(
        self,
        run_id,
        job_id
    ):


        self.runs[
            run_id
        ].jobs.append(
            job_id
        )



    def attach_experiment(
        self,
        run_id,
        experiment_id
    ):


        self.runs[
            run_id
        ].experiments.append(
            experiment_id
        )



    def complete_run(
        self,
        run_id,
        audit_result="PASS"
    ):


        run = self.runs[run_id]


        run.status = "COMPLETED"


        run.audit_result = audit_result


        run.completed_at = (

            datetime.now(
                timezone.utc
            )
            .isoformat()

        )


        return run



    def get_run(
        self,
        run_id
    ):


        return self.runs.get(
            run_id
        )



    def active_runs(self):


        return [

            run

            for run in self.runs.values()

            if run.status == "RUNNING"

        ]



    def export(
        self,
        path="gsr_data/research_runs.json"
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


                    "runs":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.runs.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchRunManager()



    run = engine.create_run(

        "GSR_RUN_001",

        "HYP_001"

    )


    assert (

        run.status

        ==

        "CREATED"

    )



    engine.attach_job(

        "GSR_RUN_001",

        "JOB_001"

    )


    engine.attach_experiment(

        "GSR_RUN_001",

        "EXP_001"

    )



    engine.start_run(

        "GSR_RUN_001"

    )



    result = engine.complete_run(

        "GSR_RUN_001",

        "PASS"

    )


    assert (

        result.status

        ==

        "COMPLETED"

    )


    assert (

        result.audit_result

        ==

        "PASS"

    )


    assert (

        len(result.jobs)

        ==

        1

    )


    print(
        "GSR RESEARCH RUN MANAGER TEST: PASS"
    )



if __name__=="__main__":

    self_test()
