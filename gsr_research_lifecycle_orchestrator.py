"""
GSR Research Lifecycle Orchestrator
Version: 1.0.0

Manages lifecycle state of approved
research assets.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-LIFECYCLE-ORCHESTRATOR-1.0.0"



VALID_STATES = [

    "RESEARCH_COMPLETE",

    "PROMOTED",

    "ACTIVE",

    "MONITORED",

    "UPDATED",

    "DEPRECATED",

    "ARCHIVED"

]



ALLOWED_TRANSITIONS = {

    "RESEARCH_COMPLETE":
    [
        "PROMOTED"
    ],

    "PROMOTED":
    [
        "ACTIVE"
    ],

    "ACTIVE":
    [
        "MONITORED",
        "UPDATED",
        "DEPRECATED"
    ],

    "MONITORED":
    [
        "UPDATED",
        "DEPRECATED"
    ],

    "UPDATED":
    [
        "ACTIVE",
        "MONITORED"
    ],

    "DEPRECATED":
    [
        "ARCHIVED"
    ],

    "ARCHIVED":
    []

}



@dataclass
class ResearchAsset:


    asset_id: str

    strategy_id: str

    version: str

    state: str

    history: List[str]

    created_at: str



class GSRResearchLifecycleOrchestrator:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.assets = {}



    def register_asset(

        self,

        asset_id: str,

        strategy_id: str,

        version="1.0"

    ):


        asset = ResearchAsset(

            asset_id=asset_id,

            strategy_id=strategy_id,

            version=version,

            state="RESEARCH_COMPLETE",

            history=[

                "Asset registered"

            ],

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.assets[asset_id] = asset


        return asset



    def transition(

        self,

        asset_id: str,

        new_state: str

    ):


        asset = self.assets.get(

            asset_id

        )


        if asset is None:

            raise KeyError(

                "Asset not found"

            )


        allowed = ALLOWED_TRANSITIONS.get(

            asset.state,

            []

        )


        if new_state not in allowed:

            raise ValueError(

                f"Invalid transition {asset.state} -> {new_state}"

            )


        asset.state = new_state


        asset.history.append(

            new_state

        )


        return asset



    def get_asset(

        self,

        asset_id

    ):


        return self.assets.get(

            asset_id

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


                    "assets":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.assets.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchLifecycleOrchestrator()



    asset = engine.register_asset(

        asset_id="ASSET_001",

        strategy_id="GSR_AT_001"

    )


    assert (

        asset.state

        ==

        "RESEARCH_COMPLETE"

    )


    engine.transition(

        "ASSET_001",

        "PROMOTED"

    )


    engine.transition(

        "ASSET_001",

        "ACTIVE"

    )


    engine.transition(

        "ASSET_001",

        "MONITORED"

    )


    current = engine.get_asset(

        "ASSET_001"

    )


    assert (

        current.state

        ==

        "MONITORED"

    )


    assert len(

        current.history

    ) == 4



    print(

        "GSR RESEARCH LIFECYCLE ORCHESTRATOR TEST: PASS"

    )



if __name__=="__main__":

    self_test()
