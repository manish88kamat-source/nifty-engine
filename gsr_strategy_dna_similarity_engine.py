"""
GSR Strategy DNA Similarity Engine
Version: 1.0.0

Calculates similarity between
atomic Strategy DNA structures.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json



ENGINE_VERSION = "GSR-DNA-SIMILARITY-ENGINE-1.0.0"



@dataclass
class SimilarityResult:


    comparison_id: str

    dna_a: str

    dna_b: str

    similarity_score: float

    common_features: List[str]

    differences: List[str]



class GSRStrategyDNASimilarityEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.results = {}



    def compare(

        self,

        comparison_id: str,

        dna_a: Dict,

        dna_b: Dict

    ):


        common = []

        differences = []


        fields = [

            "factor",

            "market_context",

            "risk_model",

            "edge_type"

        ]



        matches = 0



        for field in fields:


            if dna_a.get(field) == dna_b.get(field):

                matches += 1

                common.append(field)


            else:

                differences.append(field)



        similarity = round(

            (

                matches

                /

                len(fields)

            )

            *

            100,

            2

        )



        result = SimilarityResult(

            comparison_id=comparison_id,

            dna_a=dna_a["dna_id"],

            dna_b=dna_b["dna_id"],

            similarity_score=similarity,

            common_features=common,

            differences=differences

        )


        self.results[comparison_id] = result


        return result



    def get(

        self,

        comparison_id

    ):


        return self.results.get(

            comparison_id

        )



    def export(

        self,

        path="gsr_data/dna_similarity.json"

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


                    "results":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.results.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRStrategyDNASimilarityEngine()



    dna1 = {

        "dna_id":

        "DNA_001",

        "factor":

        "VOLATILITY_ADAPTATION",

        "market_context":

        "TREND",

        "risk_model":

        "ATR_NORMALIZATION",

        "edge_type":

        "STABILITY"

    }



    dna2 = {

        "dna_id":

        "DNA_002",

        "factor":

        "VOLATILITY_ADAPTATION",

        "market_context":

        "TREND",

        "risk_model":

        "DYNAMIC_ATR",

        "edge_type":

        "STABILITY"

    }



    result = engine.compare(

        "CMP_001",

        dna1,

        dna2

    )



    assert (

        result.similarity_score

        ==

        75.0

    )


    assert (

        "factor"

        in

        result.common_features

    )


    stored = engine.get(

        "CMP_001"

    )


    assert stored is not None



    print(

        "GSR STRATEGY DNA SIMILARITY ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()

