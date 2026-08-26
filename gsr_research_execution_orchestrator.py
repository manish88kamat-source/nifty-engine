"""
GSR Research Execution Orchestrator
Version: 1.0.0

Manages execution lifecycle of
autonomous research plans.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-EXECUTION-ORCHESTRATOR-1.0.0"



VALID_STATUS = [

    "INITIALIZED",

    "DATA_READY",

    "RUNNING",

    "VALIDATING",

    "COMPLETED",

    "FAILED"

]



@dataclass
class ResearchExecution:


    execution_id: str

    plan_id: str

    status: str

    stages: List[str]

    current_stage: str

    history: List[str]

    created_at: str



class GSRResearchExecutionOrchestrator:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.executions = {}



    def initialize_execution(

        self,

        execution_id: str,

        plan_id: str

    ):


        execution = ResearchExecution(

            execution_id=execution_id,

            plan_id=plan_id,

            status="INITIALIZED",

            stages=[

                "DATA_READY",

                "EXPERIMENT_RUNNING",

                "VALIDATION",

                "REPORT_GENERATION"

            ],

            current_stage="DATA_READY",

            history=[

                "Execution initialized"

            ],

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.executions[execution_id] = execution


        return execution



    def advance_stage(

        self,

        execution_id: str

    ):


        execution = self.executions.get(

            execution_id

        )


        if execution is None:

            raise KeyError(

                "Execution not found"

            )


        current_index = (

            execution.stages.index(

                execution.current_stage

            )

        )


        if current_index + 1 < len(

            execution.stages

        ):

            execution.current_stage = (

                execution.stages[

                    current_index + 1

                ]

            )


            execution.history.append(

                "Moved to "

                +

                execution.current_stage

            )


        else:

            execution.status = "COMPLETED"

            execution.history.append(

                "Execution completed"

            )


        if execution.current_stage == "VALIDATION":

            execution.status = "VALIDATING"


        elif execution.current_stage == "EXPERIMENT_RUNNING":

            execution.status = "RUNNING"



        return execution



    def fail_execution(

        self,

        execution_id: str,

        reason: str

    ):


        execution = self.executions.get(

            execution_id

        )


        if execution:

            execution.status = "FAILED"

            execution.history.append(

                "FAILED: "

                +

                reason

            )


        return execution



    def get_execution(

        self,

        execution_id

    ):


        return self.executions.get(

            execution_id

        )



    def export(

        self,

        path="gsr_data/research_executions.json"

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


                    "executions":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.executions.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchExecutionOrchestrator()



    execution = engine.initialize_execution(

        execution_id="EXEC_001",

        plan_id="PLAN_001"

    )


    assert (

        execution.status

        ==

        "INITIALIZED"

    )


    engine.advance_stage(

        "EXEC_001"

    )


    engine.advance_stage(

        "EXEC_001"

    )


    current = engine.get_execution(

        "EXEC_001"

    )


    assert current is not None


    assert (

        current.current_stage

        ==

        "VALIDATION"

    )


    engine.fail_execution(

        "EXEC_001",

        "Test failure"

    )


    assert (

        current.status

        ==

        "FAILED"

    )



    print(

        "GSR RESEARCH EXECUTION ORCHESTRATOR TEST: PASS"

    )



if __name__=="__main__":

    self_test()
