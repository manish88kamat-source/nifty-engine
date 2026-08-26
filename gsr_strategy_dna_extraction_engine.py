"""
GSR Strategy DNA Extraction Engine
Version: 1.0.0

Converts research patterns into
atomic Strategy-DNA structures.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-STRATEGY-DNA-ENGINE-1.0.0"



@dataclass
class StrategyDNA:


    dna_id: str

    source_pattern_id: str

    factor: str

    market_context: str

    risk_model: str

    edge_type: str

    confidence: float

    tags: List[str]

    created_at: str



class GSRStrategyDNAExtractionEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.dna_store = {}



    def extract(

        self,

        dna_id: str,

        source_pattern_id: str,

        factor: str,

        market_context: str,

        risk_model: str,

        edge_type: str,

        confidence: float,

        tags: List[str]

    ):


        dna = StrategyDNA(

            dna_id=dna_id,

            source_pattern_id=source_pattern_id,

            factor=factor,

            market_context=market_context,

            risk_model=risk_model,

            edge_type=edge_type,

            confidence=confidence,

            tags=tags,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.dna_store[dna_id] = dna


        return dna



    def find(

        self,

        dna_id

    ):


        return self.dna_store.get(

            dna_id

        )



    def search_by_factor(

        self,

        factor

    ):


        return [

            dna

            for dna in self.dna_store.values()

            if dna.factor == factor

        ]



    def export(

        self,

        path="gsr_data/strategy_dna.json"

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


                    "dna":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.dna_store.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRStrategyDNAExtractionEngine()



    dna = engine.extract(

        dna_id="DNA_VOL_001",

        source_pattern_id="PATTERN_001",

        factor="VOLATILITY_ADAPTATION",

        market_context="TREND",

        risk_model="ATR_NORMALIZATION",

        edge_type="RISK_ADJUSTED_STABILITY",

        confidence=0.85,

        tags=[

            "volatility",

            "robustness"

        ]

    )


    assert (

        dna.factor

        ==

        "VOLATILITY_ADAPTATION"

    )


    assert (

        dna.confidence

        ==

        0.85

    )


    found = engine.search_by_factor(

        "VOLATILITY_ADAPTATION"

    )


    assert len(found) == 1



    stored = engine.find(

        "DNA_VOL_001"

    )


    assert stored is not None



    print(

        "GSR STRATEGY DNA EXTRACTION ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
