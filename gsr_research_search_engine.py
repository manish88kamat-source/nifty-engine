"""
GSR Research Search Engine
Version: 1.0.0

Search and discovery layer for archived
GSR research artifacts.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-SEARCH-1.0.0"



@dataclass
class SearchResult:

    archive_id: str

    run_id: str

    relevance_score: float

    matched_fields: List[str]



class GSRResearchSearchEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.index = {}



    def index_record(
        self,
        archive_id: str,
        record: Dict[str,Any]
    ):

        self.index[archive_id] = record



    def search(
        self,
        query: str
    ):


        results = []

        query = query.lower()



        for archive_id, record in self.index.items():

            score = 0

            matched = []



            searchable = json.dumps(
                record
            ).lower()



            if query in searchable:

                score += 1

                matched.append(
                    "content"
                )



            for key,value in record.items():

                if query in str(value).lower():

                    score += 1

                    matched.append(
                        key
                    )



            if score > 0:

                results.append(

                    SearchResult(

                        archive_id=archive_id,

                        run_id=
                        record.get(
                            "run_id",
                            "UNKNOWN"
                        ),

                        relevance_score=score,

                        matched_fields=matched

                    )

                )



        return sorted(

            results,

            key=lambda x:
            x.relevance_score,

            reverse=True

        )



    def search_by_tag(
        self,
        tag: str
    ):


        return self.search(
            tag
        )



    def export_index(
        self,
        path="gsr_data/research_search_index.json"
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

                    "index":
                    self.index

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchSearchEngine()



    engine.index_record(

        "ARCH_001",

        {

            "run_id":
            "GSR_RUN_001",

            "strategy_id":
            "GSR_AT_001",

            "decision":
            "PROMOTE",

            "evidence":
            "A",

            "tags":
            [

                "TREND",

                "REGIME"

            ]

        }

    )



    results = engine.search(
        "PROMOTE"
    )


    assert len(results)==1


    assert (

        results[0].archive_id

        ==

        "ARCH_001"

    )


    tag_results = engine.search_by_tag(
        "TREND"
    )


    assert len(tag_results)==1



    print(
        "GSR RESEARCH SEARCH ENGINE TEST: PASS"
    )



if __name__=="__main__":

    self_test()
