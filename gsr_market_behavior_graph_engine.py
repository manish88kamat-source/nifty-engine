"""
GSR Market Behavior Graph Engine
Version: 1.0.0

Builds relationships between
market principles.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-MARKET-BEHAVIOR-GRAPH-ENGINE-1.0.0"



@dataclass
class BehaviorNode:


    node_id: str

    principle_name: str

    category: str

    created_at: str



@dataclass
class BehaviorRelationship:


    relationship_id: str

    source_node: str

    target_node: str

    relationship_type: str

    strength: float



class GSRMarketBehaviorGraphEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.nodes = {}

        self.relationships = {}



    def add_node(

        self,

        node_id: str,

        principle_name: str,

        category: str

    ):


        node = BehaviorNode(

            node_id=node_id,

            principle_name=principle_name,

            category=category,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.nodes[node_id] = node


        return node



    def add_relationship(

        self,

        relationship_id: str,

        source_node: str,

        target_node: str,

        relationship_type: str,

        strength: float

    ):


        relation = BehaviorRelationship(

            relationship_id=relationship_id,

            source_node=source_node,

            target_node=target_node,

            relationship_type=relationship_type,

            strength=strength

        )


        self.relationships[relationship_id] = relation


        return relation



    def get_connections(

        self,

        node_id

    ):


        return [

            r

            for r in self.relationships.values()

            if r.source_node == node_id

            or r.target_node == node_id

        ]



    def export(

        self,

        path="gsr_data/market_behavior_graph.json"

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


                    "nodes":

                    {

                        k:

                        asdict(v)

                        for k,v in self.nodes.items()

                    },


                    "relationships":

                    {

                        k:

                        asdict(v)

                        for k,v in self.relationships.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRMarketBehaviorGraphEngine()



    engine.add_node(

        "NODE_001",

        "VOLATILITY_REGIME_AWARENESS",

        "RISK"

    )


    engine.add_node(

        "NODE_002",

        "POSITION_SIZING",

        "EXECUTION"

    )


    relation = engine.add_relationship(

        "REL_001",

        "NODE_001",

        "NODE_002",

        "INFLUENCES",

        0.85

    )


    assert (

        relation.strength

        ==

        0.85

    )


    connections = engine.get_connections(

        "NODE_001"

    )


    assert len(connections) == 1



    print(

        "GSR MARKET BEHAVIOR GRAPH ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
