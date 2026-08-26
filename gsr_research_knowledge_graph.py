"""
GSR Research Knowledge Graph
Version: 1.0.0

Relationship graph for GSR research artifacts.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-KNOWLEDGE-GRAPH-1.0.0"



@dataclass
class ResearchNode:

    node_id: str

    node_type: str

    metadata: Dict[str,Any]



@dataclass
class ResearchEdge:

    source: str

    relation: str

    target: str



class GSRResearchKnowledgeGraph:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.nodes = {}

        self.edges = []



    def add_node(

        self,

        node_id: str,

        node_type: str,

        metadata=None

    ):


        if metadata is None:

            metadata = {}


        self.nodes[node_id] = ResearchNode(

            node_id=node_id,

            node_type=node_type,

            metadata=metadata

        )


        return self.nodes[node_id]



    def add_relationship(

        self,

        source: str,

        relation: str,

        target: str

    ):


        if source not in self.nodes:

            raise KeyError(
                "Source node missing"
            )


        if target not in self.nodes:

            raise KeyError(
                "Target node missing"
            )


        self.edges.append(

            ResearchEdge(

                source=source,

                relation=relation,

                target=target

            )

        )



    def get_connections(

        self,

        node_id

    ):


        results = []


        for edge in self.edges:


            if edge.source == node_id:


                results.append(edge)


            elif edge.target == node_id:


                results.append(edge)



        return results



    def lineage(

        self,

        start_node

    ):


        visited = []



        def walk(node):

            for edge in self.edges:

                if edge.source == node:

                    if edge.target not in visited:

                        visited.append(
                            edge.target
                        )

                        walk(
                            edge.target
                        )



        walk(start_node)


        return visited



    def export(

        self,

        path="gsr_data/research_knowledge_graph.json"

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

                        key:

                        asdict(value)

                        for key,value

                        in self.nodes.items()

                    },


                    "edges":

                    [

                        asdict(edge)

                        for edge

                        in self.edges

                    ]

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchKnowledgeGraph()



    engine.add_node(

        "HYP_001",

        "HYPOTHESIS"

    )


    engine.add_node(

        "EXP_001",

        "EXPERIMENT"

    )


    engine.add_node(

        "REPORT_001",

        "REPORT"

    )



    engine.add_relationship(

        "HYP_001",

        "TESTED_BY",

        "EXP_001"

    )


    engine.add_relationship(

        "EXP_001",

        "GENERATED",

        "REPORT_001"

    )



    connections = engine.get_connections(

        "EXP_001"

    )


    assert len(connections) == 2



    chain = engine.lineage(

        "HYP_001"

    )


    assert (

        "REPORT_001"

        in

        chain

    )



    print(
        "GSR RESEARCH KNOWLEDGE GRAPH TEST: PASS"
    )



if __name__=="__main__":

    self_test()
