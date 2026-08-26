"""
GSR Research Scheduler
Version: 1.0.0

Research job orchestration layer.

Manages:
- research queue
- dependencies
- execution states

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-SCHEDULER-1.0.0"



VALID_STATES = [

    "PENDING",

    "READY",

    "RUNNING",

    "COMPLETED",

    "FAILED"

]



@dataclass
class ResearchJob:

    job_id: str

    job_type: str

    priority: str

    dependencies: List[str]

    status: str

    created_at: str

    completed_at: str



class GSRResearchScheduler:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.jobs = {}



    def register_job(
        self,
        job_id: str,
        job_type: str,
        priority: str,
        dependencies=None
    ):


        if dependencies is None:

            dependencies = []


        job = ResearchJob(

            job_id=job_id,

            job_type=job_type,

            priority=priority,

            dependencies=dependencies,

            status="PENDING",

            created_at=
            datetime.now(
                timezone.utc
            ).isoformat(),

            completed_at=""

        )


        self.jobs[job_id] = job


        return job



    def dependency_check(
        self,
        job: ResearchJob
    ):


        for dependency in job.dependencies:


            if dependency not in self.jobs:

                return False


            if self.jobs[dependency].status != "COMPLETED":

                return False


        return True



    def update_ready_jobs(self):


        ready = []


        for job in self.jobs.values():


            if job.status == "PENDING":


                if self.dependency_check(job):


                    job.status = "READY"

                    ready.append(
                        job.job_id
                    )


        return ready



    def start_job(
        self,
        job_id
    ):


        job = self.jobs[job_id]


        if job.status != "READY":

            raise ValueError(
                "Job is not ready"
            )


        job.status = "RUNNING"


        return job



    def complete_job(
        self,
        job_id
    ):


        job = self.jobs[job_id]


        job.status = "COMPLETED"


        job.completed_at = (

            datetime.now(
                timezone.utc
            )
            .isoformat()

        )


        return job



    def queue(self):


        return list(
            self.jobs.values()
        )



    def export(
        self,
        path="gsr_data/research_job_queue.json"
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


                    "jobs":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.jobs.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchScheduler()



    engine.register_job(

        "JOB_001",

        "HYPOTHESIS_VALIDATION",

        "HIGH"

    )



    engine.register_job(

        "JOB_002",

        "REPLAY_ANALYSIS",

        "HIGH",

        [

            "JOB_001"

        ]

    )



    ready = engine.update_ready_jobs()



    assert (

        "JOB_001"

        in

        ready

    )


    engine.start_job(
        "JOB_001"
    )


    engine.complete_job(
        "JOB_001"
    )


    ready = engine.update_ready_jobs()



    assert (

        "JOB_002"

        in

        ready

    )


    print(
        "GSR RESEARCH SCHEDULER TEST: PASS"
    )



if __name__=="__main__":

    self_test()
