"""
GSR Research Memory Engine
Version: 1.0.0

Persistent research knowledge memory.

Stores:
- strategy history
- research decisions
- experiment outcomes

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-MEMORY-1.0.0"



@dataclass
class ResearchMemoryRecord:

    strategy_id: str

    events: List[Dict[str,Any]]



class GSRResearchMemoryEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.memory = {}



    def register_strategy(
        self,
        strategy_id: str
    ):


        if strategy_id not in self.memory:


            self.memory[strategy_id] = ResearchMemoryRecord(

                strategy_id=strategy_id,

                events=[]

            )



    def add_event(
        self,
        strategy_id: str,
        event: str,
        details: Dict[str,Any]
    ):


        if strategy_id not in self.memory:

            self.register_strategy(
                strategy_id
            )


        self.memory[strategy_id].events.append(

            {

                "event":
                event,


                "details":
                details,


                "timestamp":
                datetime.now(
                    timezone.utc
                ).isoformat()

            }

        )



    def get_history(
        self,
        strategy_id: str
    ):


        record = self.memory.get(
            strategy_id
        )


        if record:

            return record.events


        return []



    def strategy_summary(
        self,
        strategy_id
    ):


        history = self.get_history(
            strategy_id
        )


        return {

            "strategy_id":
            strategy_id,


            "event_count":
            len(history),


            "latest_event":

            history[-1]["event"]

            if history

            else None

        }



    def export(
        self,
        path=
        "gsr_data/research_memory.json"
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


                    "memory":

                    {

                        k:
                        asdict(v)

                        for k,v

                        in self.memory.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchMemoryEngine()


    engine.register_strategy(
        "GSR_AT_001"
    )


    engine.add_event(

        "GSR_AT_001",

        "REPLAY_COMPLETED",

        {

            "status":
            "PASS"

        }

    )


    engine.add_event(

        "GSR_AT_001",

        "PROMOTION_DECISION",

        {

            "decision":
            "PROMOTED"

        }

    )


    history = engine.get_history(
        "GSR_AT_001"
    )


    assert len(history)==2


    summary = engine.strategy_summary(
        "GSR_AT_001"
    )


    assert (
        summary["latest_event"]
        ==
        "PROMOTION_DECISION"
    )


    print(
        "GSR RESEARCH MEMORY TEST: PASS"
    )



if __name__=="__main__":

    self_test()
