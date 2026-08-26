"""
GSR Research Cluster Engine
Version: 1.0.0

Groups related research artifacts
into knowledge clusters.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-CLUSTER-1.0.0"



@dataclass
class ResearchCluster:

    cluster_id: str

    name: str

    theme: str

    members: List[str]

    metadata: Dict[str,Any]



class GSRResearchClusterEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.clusters = {}

        self.records = {}



    def register_record(

        self,

        record_id: str,

        record: Dict[str,Any]

    ):

        self.records[record_id] = record



    def create_cluster(

        self,

        cluster_id: str,

        name: str,

        theme: str

    ):


        cluster = ResearchCluster(

            cluster_id=cluster_id,

            name=name,

            theme=theme,

            members=[],

            metadata={}

        )


        self.clusters[cluster_id] = cluster


        return cluster



    def add_to_cluster(

        self,

        cluster_id: str,

        record_id: str

    ):


        if cluster_id not in self.clusters:

            raise KeyError(
                "Cluster not found"
            )


        if record_id not in self.records:

            raise KeyError(
                "Record not found"
            )


        if record_id not in self.clusters[cluster_id].members:

            self.clusters[cluster_id].members.append(
                record_id
            )



    def get_cluster(

        self,

        cluster_id

    ):


        return self.clusters.get(
            cluster_id
        )



    def cluster_size(

        self,

        cluster_id

    ):


        return len(

            self.clusters[cluster_id].members

        )



    def export(

        self,

        path="gsr_data/research_clusters.json"

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


                    "clusters":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.clusters.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchClusterEngine()



    engine.register_record(

        "EXP_001",

        {

            "strategy":

            "trend breakout",

            "theme":

            "momentum"

        }

    )


    engine.register_record(

        "EXP_002",

        {

            "strategy":

            "trend continuation",

            "theme":

            "momentum"

        }

    )



    engine.create_cluster(

        "CLUSTER_001",

        "Trend Research Family",

        "Momentum Strategies"

    )



    engine.add_to_cluster(

        "CLUSTER_001",

        "EXP_001"

    )


    engine.add_to_cluster(

        "CLUSTER_001",

        "EXP_002"

    )



    cluster = engine.get_cluster(

        "CLUSTER_001"

    )


    assert cluster is not None


    assert (

        len(cluster.members)

        ==

        2

    )


    assert (

        cluster.theme

        ==

        "Momentum Strategies"

    )


    print(
        "GSR RESEARCH CLUSTER TEST: PASS"
    )



if __name__=="__main__":

    self_test()

