"""
GSR Research Learning Memory Engine
Version: 1.0.0

Stores research learnings generated from
feedback outcomes.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-LEARNING-MEMORY-ENGINE-1.0.0"



@dataclass
class LearningMemory:


    memory_id: str

    source_feedback_id: str

    category: str

    learning: str

    confidence: float

    usage_count: int

    tags: List[str]

    created_at: str



class GSRResearchLearningMemoryEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.memory = {}



    def store_learning(

        self,

        memory_id: str,

        source_feedback_id: str,

        category: str,

        learning: str,

        confidence: float,

        tags: List[str]

    ):


        item = LearningMemory(

            memory_id=memory_id,

            source_feedback_id=source_feedback_id,

            category=category,

            learning=learning,

            confidence=confidence,

            usage_count=0,

            tags=tags,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.memory[memory_id] = item


        return item



    def retrieve_by_category(

        self,

        category: str

    ):


        return [

            item

            for item in self.memory.values()

            if item.category == category

        ]



    def increase_usage(

        self,

        memory_id: str

    ):


        item = self.memory.get(

            memory_id

        )


        if item:

            item.usage_count += 1


        return item



    def get(

        self,

        memory_id

    ):


        return self.memory.get(

            memory_id

        )



    def export(

        self,

        path="gsr_data/learning_memory.json"

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

                        key:

                        asdict(value)

                        for key,value

                        in self.memory.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchLearningMemoryEngine()



    result = engine.store_learning(

        memory_id="MEM_001",

        source_feedback_id="FB_001",

        category="ROBUSTNESS",

        learning=(

            "Adaptive volatility filtering improved robustness."

        ),

        confidence=0.90,

        tags=[

            "volatility",

            "robustness"

        ]

    )


    assert (

        result.confidence

        ==

        0.90

    )


    engine.increase_usage(

        "MEM_001"

    )


    stored = engine.get(

        "MEM_001"

    )


    assert (

        stored.usage_count

        ==

        1

    )


    found = engine.retrieve_by_category(

        "ROBUSTNESS"

    )


    assert len(found) == 1



    print(

        "GSR RESEARCH LEARNING MEMORY ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
