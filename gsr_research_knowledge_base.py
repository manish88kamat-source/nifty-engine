"""
GSR Research Knowledge Base
Version: 1.0.0

Stores validated research insights.

Research intelligence layer.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from datetime import datetime, timezone
from pathlib import Path
import json



ENGINE_VERSION = "GSR-KNOWLEDGE-1.0.0"



@dataclass
class KnowledgeRecord:

    insight_id: str

    category: str

    strategy_id: str

    observation: str

    evidence_source: str

    confidence: float

    timestamp: str



class GSRResearchKnowledgeBase:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.records = {}



    def add_insight(
        self,
        insight_id: str,
        category: str,
        strategy_id: str,
        observation: str,
        evidence_source: str,
        confidence: float
    ):


        self.records[insight_id] = KnowledgeRecord(

            insight_id=insight_id,

            category=category,

            strategy_id=strategy_id,

            observation=observation,

            evidence_source=evidence_source,

            confidence=confidence,

            timestamp=
            datetime.now(
                timezone.utc
            ).isoformat()

        )



    def get_insight(
        self,
        insight_id: str
    ):


        return self.records.get(
            insight_id
        )



    def search_category(
        self,
        category: str
    ):


        return [

            record

            for record in self.records.values()

            if record.category == category

        ]



    def strategy_lessons(
        self,
        strategy_id: str
    ):


        return [

            record

            for record in self.records.values()

            if record.strategy_id == strategy_id

        ]



    def export(
        self,
        path="gsr_data/research_knowledge_base.json"
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


                    "knowledge":

                    {

                        key:
                        asdict(value)

                        for key,value

                        in self.records.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchKnowledgeBase()



    engine.add_insight(

        insight_id=
        "INSIGHT_001",

        category=
        "REGIME_LESSON",

        strategy_id=
        "GSR_AT_001",

        observation=
        "Trend strategies degrade during low volatility range regimes.",

        evidence_source=
        "Historical Replay Validation",

        confidence=
        0.85

    )



    result = engine.get_insight(

        "INSIGHT_001"

    )


    assert result is not None


    assert (

        result.category

        ==

        "REGIME_LESSON"

    )


    lessons = engine.strategy_lessons(

        "GSR_AT_001"

    )


    assert len(lessons)==1



    print(
        "GSR RESEARCH KNOWLEDGE BASE TEST: PASS"
    )



if __name__=="__main__":

    self_test()
