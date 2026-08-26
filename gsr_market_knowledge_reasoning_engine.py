"""
GSR Market Knowledge Reasoning Engine
Version: 1.0.0

Interprets relationships inside
market knowledge graph.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-MARKET-KNOWLEDGE-REASONING-ENGINE-1.0.0"



@dataclass
class ReasoningResult:


    reasoning_id: str

    source_node: str

    target_node: str

    interpretation: str

    evidence_score: float

    conclusion: str

    created_at: str



class GSRMarketKnowledgeReasoningEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.reasoning_results = {}



    def reason(

        self,

        reasoning_id: str,

        source_node: str,

        target_node: str,

        relationship: str,

        evidence_count: int

    ):


        if relationship == "SUPPORTS":

            interpretation = (

                f"{source_node} provides "

                f"evidence toward {target_node}"

            )


            conclusion = (

                "Observed relationship indicates "

                "a recurring market behavior pattern."

            )


        elif relationship == "HAS_DNA":

            interpretation = (

                f"{source_node} contains "

                f"strategy component {target_node}"

            )


            conclusion = (

                "The strategy can be decomposed "

                "into reusable intelligence units."

            )


        else:

            interpretation = (

                "Relationship requires further research."

            )


            conclusion = (

                "Insufficient reasoning context."

            )



        score = min(

            evidence_count * 10,

            100

        )



        result = ReasoningResult(

            reasoning_id=reasoning_id,

            source_node=source_node,

            target_node=target_node,

            interpretation=interpretation,

            evidence_score=score,

            conclusion=conclusion,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.reasoning_results[reasoning_id] = result


        return result



    def get(

        self,

        reasoning_id

    ):


        return self.reasoning_results.get(

            reasoning_id

        )



    def export(

        self,

        path="gsr_data/knowledge_reasoning.json"

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


                    "reasoning":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.reasoning_results.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRMarketKnowledgeReasoningEngine()



    result = engine.reason(

        reasoning_id="REASON_001",

        source_node="DNA_001",

        target_node="PRINCIPLE_001",

        relationship="SUPPORTS",

        evidence_count=9

    )


    assert (

        result.evidence_score

        ==

        90

    )


    assert (

        "recurring"

        in

        result.conclusion

    )



    stored = engine.get(

        "REASON_001"

    )


    assert stored is not None



    print(

        "GSR MARKET KNOWLEDGE REASONING ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()

