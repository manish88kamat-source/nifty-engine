"""
GSR Market Knowledge Graph Engine
Version: 1.0.0

Creates interconnected research knowledge graph
from strategies, DNA, families and principles.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-MARKET-KNOWLEDGE-GRAPH-ENGINE-1.0.0"



@dataclass
class KnowledgeNode:


    node_id: str

    node_type: str

    name: str

    metadata: Dict

    created_at: str



@dataclass
class KnowledgeEdge:


    edge_id: str

    source_id: str

    target_id: str

    relation: str

    confidence: float



class GSRMarketKnowledgeGraphEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.nodes = {}

        self.edges = {}



    def add_node(

        self,

        node_id: str,

        node_type: str,

        name: str,

        metadata=None

    ):


        node = KnowledgeNode(

            node_id=node_id,

            node_type=node_type,

            name=name,

            metadata=metadata or {},

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.nodes[node_id] = node


        return node



    def add_edge(

        self,

        edge_id: str,

        source_id: str,

        target_id: str,

        relation: str,

        confidence: float

    ):


        edge = KnowledgeEdge(

            edge_id=edge_id,

            source_id=source_id,

            target_id=target_id,

            relation=relation,

            confidence=confidence

        )


        self.edges[edge_id] = edge


        return edge



    def get_related_nodes(

        self,

        node_id

    ):


        connections = []


        for edge in self.edges.values():


            if edge.source_id == node_id:


                connections.append(

                    edge.target_id

                )


            elif edge.target_id == node_id:


                connections.append(

                    edge.source_id

                )


        return connections



    def export(

        self,

        path="gsr_data/market_knowledge_graph.json"

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

                        for k,v

                        in self.nodes.items()

                    },


                    "edges":

                    {

                        k:

                        asdict(v)

                        for k,v

                        in self.edges.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRMarketKnowledgeGraphEngine()



    engine.add_node(

        "STR_001",

        "STRATEGY",

        "ATR Volatility Strategy"

    )


    engine.add_node(

        "DNA_001",

        "STRATEGY_DNA",

        "VOLATILITY_ADAPTATION"

    )


    engine.add_node(

        "PR_001",

        "MARKET_PRINCIPLE",

        "VOLATILITY_REGIME_AWARENESS"

    )



    engine.add_edge(

        "EDGE_001",

        "STR_001",

        "DNA_001",

        "HAS_DNA",

        0.90

    )


    engine.add_edge(

        "EDGE_002",

        "DNA_001",

        "PR_001",

        "SUPPORTS",

        0.85

    )



    related = engine.get_related_nodes(

        "STR_001"

    )


    assert (

        "DNA_001"

        in

        related

    )


    assert len(

        engine.nodes

    ) == 3


    assert len(

        engine.edges

    ) == 2



    print(

        "GSR MARKET KNOWLEDGE GRAPH ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
