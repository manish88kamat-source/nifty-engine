"""
GSR Market Knowledge Query Engine
Version: 1.0.0

Retrieves intelligence from
market knowledge graph.

Research only.
No live trading.
"""


from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-MARKET-KNOWLEDGE-QUERY-ENGINE-1.0.0"



@dataclass
class QueryResult:


    query_id: str

    query_type: str

    target: str

    results: List[str]

    created_at: str



class GSRMarketKnowledgeQueryEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.nodes = {}

        self.edges = {}

        self.query_history = {}



    def load_graph(

        self,

        nodes: Dict,

        edges: Dict

    ):

        self.nodes = nodes

        self.edges = edges



    def find_by_type(

        self,

        node_type: str

    ):


        return [

            node_id

            for node_id,node

            in self.nodes.items()

            if node.get(

                "node_type"

            )

            == node_type

        ]



    def connected_nodes(

        self,

        node_id: str

    ):


        results = []


        for edge in self.edges.values():


            if edge.get(

                "source_id"

            ) == node_id:


                results.append(

                    edge.get(

                        "target_id"

                    )

                )


            elif edge.get(

                "target_id"

            ) == node_id:


                results.append(

                    edge.get(

                        "source_id"

                    )

                )


        return results



    def query(

        self,

        query_id: str,

        query_type: str,

        target: str

    ):


        if query_type == "CONNECTED":


            results = self.connected_nodes(

                target

            )


        elif query_type == "TYPE":


            results = self.find_by_type(

                target

            )


        else:

            results = []



        result = QueryResult(

            query_id=query_id,

            query_type=query_type,

            target=target,

            results=results,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.query_history[query_id] = result


        return result



    def get_query(

        self,

        query_id

    ):


        return self.query_history.get(

            query_id

        )



def self_test():


    engine = GSRMarketKnowledgeQueryEngine()



    nodes = {

        "STR_001": {

            "node_type":

            "STRATEGY"

        },


        "DNA_001": {

            "node_type":

            "STRATEGY_DNA"

        },


        "PR_001": {

            "node_type":

            "MARKET_PRINCIPLE"

        }

    }



    edges = {

        "E1": {

            "source_id":

            "STR_001",

            "target_id":

            "DNA_001"

        },


        "E2": {

            "source_id":

            "DNA_001",

            "target_id":

            "PR_001"

        }

    }



    engine.load_graph(

        nodes,

        edges

    )


    result = engine.query(

        "QUERY_001",

        "CONNECTED",

        "DNA_001"

    )


    assert (

        len(result.results)

        ==

        2

    )


    result2 = engine.query(

        "QUERY_002",

        "TYPE",

        "STRATEGY"

    )


    assert (

        "STR_001"

        in

        result2.results

    )


    print(

        "GSR MARKET KNOWLEDGE QUERY ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
