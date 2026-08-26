"""
GSR Research Archive Manager
Version: 1.0.0

Permanent archive layer for GSR research artifacts.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-ARCHIVE-1.0.0"



VALID_STATUS = [

    "CREATED",

    "ARCHIVED",

    "RETRIEVED",

    "DEPRECATED"

]



@dataclass
class ArchiveRecord:


    archive_id: str

    run_id: str

    report_id: str

    version: str

    status: str

    tags: List[str]

    metadata: Dict[str,Any]

    created_at: str



class GSRResearchArchiveManager:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.archive = {}



    def archive_report(

        self,

        archive_id: str,

        run_id: str,

        report_id: str,

        version="1.0",

        tags=None,

        metadata=None

    ):


        if tags is None:

            tags = []


        if metadata is None:

            metadata = {}



        record = ArchiveRecord(

            archive_id=archive_id,

            run_id=run_id,

            report_id=report_id,

            version=version,

            status="ARCHIVED",

            tags=tags,

            metadata=metadata,

            created_at=

            datetime.now(
                timezone.utc
            ).isoformat()

        )


        self.archive[archive_id] = record


        return record



    def retrieve(

        self,

        archive_id

    ):


        record = self.archive.get(
            archive_id
        )


        if record:

            record.status = "RETRIEVED"


        return record



    def search_tag(

        self,

        tag

    ):


        return [

            record

            for record in self.archive.values()

            if tag in record.tags

        ]



    def list_archives(self):


        return list(
            self.archive.values()
        )



    def export(

        self,

        path="gsr_data/research_archive.json"

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


                    "archive":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.archive.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchArchiveManager()



    result = engine.archive_report(

        archive_id="ARCH_001",

        run_id="GSR_RUN_001",

        report_id="REPORT_001",

        version="1.0",

        tags=[

            "TREND",

            "REGIME"

        ],

        metadata={

            "decision":

            "PROMOTE",

            "evidence":

            "A"

        }

    )



    assert (

        result.status

        ==

        "ARCHIVED"

    )


    stored = engine.retrieve(

        "ARCH_001"

    )


    assert stored is not None


    assert (

        stored.status

        ==

        "RETRIEVED"

    )


    results = engine.search_tag(

        "TREND"

    )


    assert len(results)==1



    print(
        "GSR RESEARCH ARCHIVE MANAGER TEST: PASS"
    )



if __name__=="__main__":

    self_test()
