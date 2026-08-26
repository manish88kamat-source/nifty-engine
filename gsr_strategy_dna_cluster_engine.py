"""
GSR Strategy DNA Cluster Engine
Version: 1.0.0

Groups similar Strategy DNA into
research families.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-DNA-CLUSTER-ENGINE-1.0.0"



@dataclass
class StrategyDNACluster:


    cluster_id: str

    theme: str

    members: List[str]

    confidence: float

    created_at: str



class GSRStrategyDNAClusterEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.clusters = {}



    def create_cluster(

        self,

        cluster_id: str,

        dna_items: List[Dict]

    ):


        if not dna_items:

            raise ValueError(

                "No DNA supplied"

            )


        factor_count = {}


        for dna in dna_items:

            factor = dna.get(

                "factor",

                "UNKNOWN"

            )

            factor_count[factor] = (

                factor_count.get(

                    factor,

                    0

                )

                +

                1

            )



        theme = max(

            factor_count,

            key=factor_count.get

        )


        confidence = round(

            (

                factor_count[theme]

                /

                len(dna_items)

            )

            *

            100,

            2

        )


        cluster = StrategyDNACluster(

            cluster_id=cluster_id,

            theme=theme,

            members=[

                dna["dna_id"]

                for dna in dna_items

            ],

            confidence=confidence,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.clusters[cluster_id] = cluster


        return cluster



    def get(

        self,

        cluster_id

    ):


        return self.clusters.get(

            cluster_id

        )



    def find_by_theme(

        self,

        theme

    ):


        return [

            cluster

            for cluster

            in self.clusters.values()

            if cluster.theme == theme

        ]



    def export(

        self,

        path="gsr_data/dna_clusters.json"

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


    engine = GSRStrategyDNAClusterEngine()



    cluster = engine.create_cluster(

        cluster_id="CLUSTER_001",

        dna_items=[

            {

                "dna_id":

                "DNA_001",

                "factor":

                "VOLATILITY_ADAPTATION"

            },

            {

                "dna_id":

                "DNA_002",

                "factor":

                "VOLATILITY_ADAPTATION"

            },

            {

                "dna_id":

                "DNA_003",

                "factor":

                "VOLATILITY_ADAPTATION"

            }

        ]

    )


    assert (

        cluster.theme

        ==

        "VOLATILITY_ADAPTATION"

    )


    assert (

        cluster.confidence

        ==

        100.0

    )


    assert len(

        cluster.members

    ) == 3



    stored = engine.get(

        "CLUSTER_001"

    )


    assert stored is not None



    print(

        "GSR STRATEGY DNA CLUSTER ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
