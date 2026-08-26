"""
GSR Research Dependency Graph
Version: 1.0.0

Directed graph layer for research workflow dependencies.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json



ENGINE_VERSION = "GSR-DEPENDENCY-GRAPH-1.0.0"



@dataclass
class ResearchNode:

    node_id: str

    node_type: str

    status: str



class GSRResearchDependencyGraph:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.nodes = {}

        self.edges = {}



    def add_node(
        self,
        node_id: str,
        node_type: str
    ):


        self.nodes[node_id] = ResearchNode(

            node_id=node_id,

            node_type=node_type,

            status="PENDING"

        )


        self.edges[node_id] = []



    def add_dependency(
        self,
        node_id: str,
        depends_on: str
    ):


        if node_id not in self.nodes:

            raise KeyError(
                "Node not found"
            )


        if depends_on not in self.nodes:

            raise KeyError(
                "Dependency node not found"
            )


        self.edges[node_id].append(
            depends_on
        )



    def detect_cycle(self):


        visited = set()

        stack = set()



        def visit(node):

            if node in stack:

                return True


            if node in visited:

                return False


            visited.add(node)

            stack.add(node)



            for dep in self.edges.get(
                node,
                []
            ):

                if visit(dep):

                    return True



            stack.remove(node)


            return False



        for node in self.nodes:

            if visit(node):

                return True


        return False



    def execution_order(self):


        if self.detect_cycle():

            raise ValueError(
                "Circular dependency detected"
            )



        visited = set()

        order = []



        def visit(node):

            if node in visited:

                return


            for dep in self.edges.get(
                node,
                []
            ):

                visit(dep)


            visited.add(node)

            order.append(node)



        for node in self.nodes:

            visit(node)



        return order



    def blocked_nodes(self):


        blocked = []


        for node,deps in self.edges.items():

            for dep in deps:

                if self.nodes[dep].status != "COMPLETED":

                    blocked.append(node)

                    break


        return blocked



    def export(
        self,
        path="gsr_data/research_dependency_graph.json"
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


                    "edges":
                    self.edges

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchDependencyGraph()



    engine.add_node(

        "HYP_001",

        "HYPOTHESIS"

    )


    engine.add_node(

        "EXP_001",

        "EXPERIMENT"

    )


    engine.add_node(

        "VAL_001",

        "VALIDATION"

    )



    engine.add_dependency(

        "EXP_001",

        "HYP_001"

    )


    engine.add_dependency(

        "VAL_001",

        "EXP_001"

    )



    assert (

        engine.detect_cycle()

        is

        False

    )



    order = engine.execution_order()



    assert (

        order.index("HYP_001")

        <

        order.index("EXP_001")

    )


    assert (

        order.index("EXP_001")

        <

        order.index("VAL_001")

    )


    print(
        "GSR RESEARCH DEPENDENCY GRAPH TEST: PASS"
    )



if __name__=="__main__":

    self_test()
