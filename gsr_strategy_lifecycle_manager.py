"""
GSR Strategy Lifecycle Manager
Version: 1.0.0

Controls strategy research lifecycle.

No live trading.
Research governance only.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json


ENGINE_VERSION = "GSR-LIFECYCLE-1.0.0"


VALID_STATES = [

    "CAPTURED",

    "RULE_PENDING",

    "REPLAY_VALIDATED",

    "EVIDENCE_READY",

    "RANKED",

    "PROMOTED",

    "RESEARCH_ACTIVE",

    "RETIRED"

]



ALLOWED_TRANSITIONS = {


    "CAPTURED":
    [
        "RULE_PENDING"
    ],


    "RULE_PENDING":
    [
        "REPLAY_VALIDATED"
    ],


    "REPLAY_VALIDATED":
    [
        "EVIDENCE_READY"
    ],


    "EVIDENCE_READY":
    [
        "RANKED"
    ],


    "RANKED":
    [
        "PROMOTED"
    ],


    "PROMOTED":
    [
        "RESEARCH_ACTIVE"
    ],


    "RESEARCH_ACTIVE":
    [
        "RETIRED"
    ],


    "RETIRED":
    []

}



@dataclass
class StrategyState:

    strategy_id: str

    state: str

    history: List[Dict[str,Any]]



class StrategyLifecycleManager:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.strategies = {}



    def register_strategy(
        self,
        strategy_id: str,
        initial_state="CAPTURED"
    ):


        if initial_state not in VALID_STATES:

            raise ValueError(
                "Invalid initial state"
            )


        self.strategies[strategy_id] = StrategyState(

            strategy_id=strategy_id,

            state=initial_state,

            history=[

                {

                    "state":
                    initial_state,

                    "timestamp":
                    datetime.now(
                        timezone.utc
                    ).isoformat(),

                    "event":
                    "REGISTERED"

                }

            ]

        )



    def transition(
        self,
        strategy_id: str,
        new_state: str
    ):


        if strategy_id not in self.strategies:

            raise KeyError(
                "Strategy not registered"
            )


        strategy = self.strategies[
            strategy_id
        ]


        current = strategy.state


        if new_state not in ALLOWED_TRANSITIONS[current]:

            raise ValueError(

                f"Invalid transition {current} -> {new_state}"

            )


        strategy.state = new_state


        strategy.history.append(

            {

                "state":
                new_state,

                "timestamp":
                datetime.now(
                    timezone.utc
                ).isoformat(),

                "event":
                "STATE_TRANSITION"

            }

        )


        return strategy



    def get_state(
        self,
        strategy_id
    ):


        return self.strategies.get(
            strategy_id
        )



    def export(
        self,
        path=
        "gsr_data/strategy_lifecycle.json"
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


                    "strategies":

                    {

                        k:
                        asdict(v)

                        for k,v

                        in self.strategies.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = StrategyLifecycleManager()


    engine.register_strategy(
        "GSR_AT_001"
    )


    engine.transition(
        "GSR_AT_001",
        "RULE_PENDING"
    )


    engine.transition(
        "GSR_AT_001",
        "REPLAY_VALIDATED"
    )


    engine.transition(
        "GSR_AT_001",
        "EVIDENCE_READY"
    )


    state = engine.get_state(
        "GSR_AT_001"
    )


    assert (
        state.state
        ==
        "EVIDENCE_READY"
    )


    assert (
        len(
            state.history
        )
        ==
        4
    )


    print(
        "GSR STRATEGY LIFECYCLE TEST: PASS"
    )



if __name__=="__main__":

    self_test()
