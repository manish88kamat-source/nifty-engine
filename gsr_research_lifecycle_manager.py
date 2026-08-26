"""
GSR Research Lifecycle Manager
Version: 1.0.0

Tracks complete research artifact
lifecycle and state transitions.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-RESEARCH-LIFECYCLE-MANAGER-1.0.0"



@dataclass
class LifecycleArtifact:


    artifact_id: str

    research_name: str

    current_state: str

    history: List[str]

    created_at: str

    updated_at: str



class GSRResearchLifecycleManager:


    VALID_STATES = [

        "CREATED",

        "EXPERIMENT_DESIGNED",

        "EXECUTED",

        "RESULT_AVAILABLE",

        "EVIDENCE_VALIDATED",

        "PROMOTED",

        "REJECTED"

    ]


    def __init__(self):

        self.version = ENGINE_VERSION

        self.artifacts = {}



    def create_artifact(

        self,

        artifact_id: str,

        research_name: str

    ):


        now = datetime.now(

            timezone.utc

        ).isoformat()


        artifact = LifecycleArtifact(

            artifact_id=artifact_id,

            research_name=research_name,

            current_state="CREATED",

            history=[

                "CREATED"

            ],

            created_at=now,

            updated_at=now

        )


        self.artifacts[artifact_id] = artifact


        return artifact



    def transition(

        self,

        artifact_id: str,

        new_state: str

    ):


        artifact = self.artifacts.get(

            artifact_id

        )


        if artifact is None:

            raise ValueError(

                "Unknown artifact"

            )


        if new_state not in self.VALID_STATES:

            raise ValueError(

                "Invalid lifecycle state"

            )


        artifact.current_state = new_state

        artifact.history.append(

            new_state

        )

        artifact.updated_at = datetime.now(

            timezone.utc

        ).isoformat()


        return artifact



    def get(

        self,

        artifact_id

    ):


        return self.artifacts.get(

            artifact_id

        )



    def export(

        self,

        path="gsr_data/research_lifecycle.json"

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


                    "artifacts":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.artifacts.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchLifecycleManager()



    artifact = engine.create_artifact(

        "RESEARCH_001",

        "Volatility Adaptation Study"

    )


    assert (

        artifact.current_state

        ==

        "CREATED"

    )


    engine.transition(

        "RESEARCH_001",

        "EXPERIMENT_DESIGNED"

    )


    engine.transition(

        "RESEARCH_001",

        "EVIDENCE_VALIDATED"

    )


    engine.transition(

        "RESEARCH_001",

        "PROMOTED"

    )


    final = engine.get(

        "RESEARCH_001"

    )


    assert (

        final.current_state

        ==

        "PROMOTED"

    )


    assert len(

        final.history

    ) == 4



    print(

        "GSR RESEARCH LIFECYCLE MANAGER TEST: PASS"

    )



if __name__=="__main__":

    self_test()
