"""
GSR Strategy Family Engine
Version: 1.0.0

Converts Strategy DNA clusters into
higher level strategy families.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-STRATEGY-FAMILY-ENGINE-1.0.0"



@dataclass
class StrategyFamily:


    family_id: str

    family_name: str

    source_cluster_id: str

    core_factor: str

    core_principle: str

    dna_members: List[str]

    confidence: float

    created_at: str



class GSRStrategyFamilyEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.families = {}



    def create_family(

        self,

        family_id: str,

        cluster_id: str,

        theme: str,

        members: List[str]

    ):


        principle_map = {

            "VOLATILITY_ADAPTATION":
            "Adaptive volatility based risk management",

            "TREND_FOLLOWING":
            "Directional trend exploitation",

            "MEAN_REVERSION":
            "Price normalization capture",

            "MOMENTUM":
            "Persistent directional strength capture"

        }



        principle = principle_map.get(

            theme,

            "Research discovered market behavior"

        )



        family_name = (

            theme.replace(

                "_",

                " "

            )

            +

            " FAMILY"

        )



        confidence = round(

            min(

                len(members) * 25,

                100

            ),

            2

        )



        family = StrategyFamily(

            family_id=family_id,

            family_name=family_name,

            source_cluster_id=cluster_id,

            core_factor=theme,

            core_principle=principle,

            dna_members=members,

            confidence=confidence,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )



        self.families[family_id] = family


        return family



    def get(

        self,

        family_id

    ):


        return self.families.get(

            family_id

        )



    def search_by_factor(

        self,

        factor

    ):


        return [

            family

            for family

            in self.families.values()

            if family.core_factor == factor

        ]



    def export(

        self,

        path="gsr_data/strategy_families.json"

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


                    "families":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.families.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRStrategyFamilyEngine()



    family = engine.create_family(

        family_id="FAMILY_001",

        cluster_id="CLUSTER_001",

        theme="VOLATILITY_ADAPTATION",

        members=[

            "DNA_001",

            "DNA_002",

            "DNA_003"

        ]

    )


    assert (

        family.core_factor

        ==

        "VOLATILITY_ADAPTATION"

    )


    assert (

        family.confidence

        ==

        75

    )


    assert len(

        family.dna_members

    ) == 3



    stored = engine.get(

        "FAMILY_001"

    )


    assert stored is not None



    print(

        "GSR STRATEGY FAMILY ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
