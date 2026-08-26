"""
GSR Strategy Leaderboard
Version: 1.0.0

Research leaderboard layer.

Consumes:
    Strategy Ranking Engine output

Produces:
    Ranked research dashboard

No live trading.
Research only.
"""


from dataclasses import dataclass, asdict
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime, timezone
import json


ENGINE_VERSION = "GSR-LEADERBOARD-1.0.0"



@dataclass
class LeaderboardEntry:

    rank: int

    strategy_id: str

    score: float

    evidence_grade: str

    status: str

    family: str

    regime: str



class StrategyLeaderboard:


    def __init__(self):

        self.version = ENGINE_VERSION



    def build(
        self,
        ranking_records: List[Dict[str,Any]]
    ):

        leaderboard = []


        for index, item in enumerate(
            ranking_records,
            start=1
        ):

            leaderboard.append(

                LeaderboardEntry(

                    rank=index,

                    strategy_id=
                    item.get(
                        "strategy_id",
                        "UNKNOWN"
                    ),

                    score=
                    float(
                        item.get(
                            "final_score",
                            0
                        )
                    ),

                    evidence_grade=
                    item.get(
                        "evidence_grade",
                        "D"
                    ),

                    status=
                    item.get(
                        "status",
                        "UNKNOWN"
                    ),

                    family=
                    item.get(
                        "family",
                        "UNCLASSIFIED"
                    ),

                    regime=
                    item.get(
                        "regime",
                        "UNKNOWN"
                    )

                )

            )


        return leaderboard



    def top(
        self,
        leaderboard,
        count=5
    ):

        return leaderboard[:count]



    def promotion_queue(
        self,
        leaderboard
    ):

        return [

            x

            for x in leaderboard

            if x.status == "PROMOTE"

        ]



    def watch_queue(
        self,
        leaderboard
    ):

        return [

            x

            for x in leaderboard

            if x.status == "WATCH"

        ]



    def family_summary(
        self,
        leaderboard
    ):

        summary={}


        for item in leaderboard:

            summary.setdefault(

                item.family,

                []

            ).append(

                item.strategy_id

            )


        return summary



    def regime_summary(
        self,
        leaderboard
    ):

        summary={}


        for item in leaderboard:

            summary.setdefault(

                item.regime,

                []

            ).append(

                item.strategy_id

            )


        return summary



    def export(
        self,
        leaderboard,
        path="gsr_data/strategy_leaderboard.json"
    ):


        Path(path).parent.mkdir(

            parents=True,

            exist_ok=True

        )


        payload={

            "engine":

            self.version,


            "generated_at":

            datetime.now(
                timezone.utc
            ).isoformat(),


            "leaderboard":

            [

                asdict(x)

                for x in leaderboard

            ]

        }


        with open(
            path,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(

                payload,

                f,

                indent=2

            )


        return path



def self_test():


    engine = StrategyLeaderboard()


    ranking=[


        {

            "strategy_id":
            "GSR_AT_001",

            "final_score":
            88,

            "evidence_grade":
            "A",

            "status":
            "PROMOTE",

            "family":
            "TREND_FOLLOWING",

            "regime":
            "TREND_UP"

        },


        {

            "strategy_id":
            "GSR_AT_002",

            "final_score":
            68,

            "evidence_grade":
            "B",

            "status":
            "WATCH",

            "family":
            "MEAN_REVERSION",

            "regime":
            "RANGE"

        }

    ]



    board = engine.build(
        ranking
    )


    assert len(board)==2


    assert (
        board[0].strategy_id
        ==
        "GSR_AT_001"
    )


    assert (
        len(
            engine.promotion_queue(
                board
            )
        )
        ==
        1
    )


    print(
        "GSR STRATEGY LEADERBOARD TEST: PASS"
    )



if __name__=="__main__":

    self_test()
