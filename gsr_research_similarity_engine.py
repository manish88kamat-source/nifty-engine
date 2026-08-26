"""
GSR Research Similarity Engine
Version: 1.0.0

Finds similarity between research artifacts.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-SIMILARITY-1.0.0"



@dataclass
class SimilarityResult:

    source_id: str

    target_id: str

    similarity_score: float

    matched_terms: List[str]



class GSRResearchSimilarityEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.records = {}



    def register_record(
        self,
        record_id: str,
        data: Dict[str,Any]
    ):

        self.records[record_id] = data



    def tokenize(
        self,
        text: str
    ):

        return set(

            text.lower()

            .replace(",", " ")

            .replace(".", " ")

            .split()

        )



    def calculate_similarity(
        self,
        source_id: str,
        target_id: str
    ):


        source = self.records[source_id]

        target = self.records[target_id]



        source_text = json.dumps(
            source
        )


        target_text = json.dumps(
            target
        )


        source_words = self.tokenize(
            source_text
        )


        target_words = self.tokenize(
            target_text
        )


        matched = list(

            source_words.intersection(
                target_words
            )

        )


        total = len(

            source_words.union(
                target_words
            )

        )


        score = 0


        if total:

            score = round(

                (
                    len(matched)
                    /
                    total

                )
                *
                100,

                2

            )


        return SimilarityResult(

            source_id=source_id,

            target_id=target_id,

            similarity_score=score,

            matched_terms=matched

        )



    def find_similar(
        self,
        record_id: str,
        threshold=20
    ):


        results = []


        for other in self.records:


            if other == record_id:

                continue


            result = self.calculate_similarity(

                record_id,

                other

            )


            if result.similarity_score >= threshold:

                results.append(
                    result
                )


        return sorted(

            results,

            key=lambda x:
            x.similarity_score,

            reverse=True

        )



    def export(
        self,
        path="gsr_data/research_similarity.json"
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

                    "records":
                    self.records

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchSimilarityEngine()



    engine.register_record(

        "EXP_001",

        {

            "hypothesis":

            "volatility filter improves breakout robustness",

            "strategy":

            "trend breakout"

        }

    )


    engine.register_record(

        "EXP_002",

        {

            "hypothesis":

            "ATR regime filter improves breakout stability",

            "strategy":

            "trend breakout"

        }

    )



    result = engine.calculate_similarity(

        "EXP_001",

        "EXP_002"

    )


    assert (

        result.similarity_score > 0

    )


    assert (

        "breakout"

        in

        result.matched_terms

    )



    print(
        "GSR RESEARCH SIMILARITY TEST: PASS"
    )



if __name__=="__main__":

    self_test()
