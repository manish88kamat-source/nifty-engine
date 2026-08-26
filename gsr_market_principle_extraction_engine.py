"""
GSR Market Principle Extraction Engine
Version: 1.0.0

Extracts universal market principles
from strategy families.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-MARKET-PRINCIPLE-ENGINE-1.0.0"



@dataclass
class MarketPrinciple:


    principle_id: str

    source_family_id: str

    principle_name: str

    description: str

    evidence_count: int

    confidence: float

    tags: List[str]

    created_at: str



class GSRMarketPrincipleExtractionEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.principles = {}



    def extract(

        self,

        principle_id: str,

        family_id: str,

        family_factor: str,

        evidence_count: int

    ):


        principle_map = {

            "VOLATILITY_ADAPTATION":
            (
                "VOLATILITY_REGIME_AWARENESS",
                "Market volatility changes require adaptive risk control."
            ),

            "TREND_FOLLOWING":
            (
                "TREND_PERSISTENCE",
                "Markets can exhibit persistent directional movement."
            ),

            "MEAN_REVERSION":
            (
                "PRICE_NORMALIZATION",
                "Extreme price moves can revert toward equilibrium."
            ),

            "MOMENTUM":
            (
                "MOMENTUM_CONTINUATION",
                "Strong price movement can continue due to persistent demand."
            )

        }


        name, description = principle_map.get(

            family_factor,

            (
                "UNKNOWN_MARKET_BEHAVIOR",

                "Research discovered market behavior."

            )

        )


        confidence = min(

            evidence_count * 10,

            100

        )


        principle = MarketPrinciple(

            principle_id=principle_id,

            source_family_id=family_id,

            principle_name=name,

            description=description,

            evidence_count=evidence_count,

            confidence=confidence,

            tags=[

                family_factor,

                "MARKET_BEHAVIOR"

            ],

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.principles[principle_id] = principle


        return principle



    def get(

        self,

        principle_id

    ):


        return self.principles.get(

            principle_id

        )



    def search(

        self,

        keyword

    ):


        return [

            p

            for p in self.principles.values()

            if keyword.lower()

            in p.principle_name.lower()

        ]



    def export(

        self,

        path="gsr_data/market_principles.json"

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


                    "principles":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.principles.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRMarketPrincipleExtractionEngine()



    principle = engine.extract(

        principle_id="PRINCIPLE_001",

        family_id="FAMILY_001",

        family_factor="VOLATILITY_ADAPTATION",

        evidence_count=9

    )


    assert (

        principle.principle_name

        ==

        "VOLATILITY_REGIME_AWARENESS"

    )


    assert (

        principle.confidence

        ==

        90

    )


    stored = engine.get(

        "PRINCIPLE_001"

    )


    assert stored is not None



    print(

        "GSR MARKET PRINCIPLE EXTRACTION ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
