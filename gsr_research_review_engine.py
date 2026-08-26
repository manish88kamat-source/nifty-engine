"""
GSR Research Review Engine
Version: 1.0.0

Handles structured review process
for research assets after alerts.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-REVIEW-ENGINE-1.0.0"



@dataclass
class ResearchReview:


    review_id: str

    alert_id: str

    asset_id: str

    status: str

    issue_type: str

    analysis_notes: List[str]

    decision: str

    recommendation: str

    created_at: str



class GSRResearchReviewEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.reviews = {}



    def create_review(

        self,

        review_id: str,

        alert_id: str,

        asset_id: str,

        issue_type: str

    ):


        review = ResearchReview(

            review_id=review_id,

            alert_id=alert_id,

            asset_id=asset_id,

            status="REVIEW_OPEN",

            issue_type=issue_type,

            analysis_notes=[

                "Review initiated"

            ],

            decision="PENDING",

            recommendation="Awaiting analysis",

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.reviews[review_id] = review


        return review



    def analyze(

        self,

        review_id: str,

        note: str

    ):


        review = self.reviews.get(

            review_id

        )


        if review is None:

            raise KeyError(

                "Review not found"

            )


        review.status="UNDER_ANALYSIS"


        review.analysis_notes.append(

            note

        )


        return review



    def finalize(

        self,

        review_id: str,

        decision: str

    ):


        review = self.reviews.get(

            review_id

        )


        if review is None:

            raise KeyError(

                "Review not found"

            )


        review.status="DECISION_READY"


        review.decision = decision



        if decision == "KEEP":

            review.recommendation = (

                "Continue monitoring asset."

            )


        elif decision == "UPDATE":

            review.recommendation = (

                "Update research parameters and revalidate."

            )


        elif decision == "RETIRE":

            review.recommendation = (

                "Move asset towards retirement."

            )


        return review



    def get(

        self,

        review_id

    ):


        return self.reviews.get(

            review_id

        )



    def export(

        self,

        path="gsr_data/research_reviews.json"

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


                    "reviews":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.reviews.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchReviewEngine()



    review = engine.create_review(

        review_id="REVIEW_001",

        alert_id="ALERT_001",

        asset_id="ASSET_001",

        issue_type="ROBUSTNESS_DECAY"

    )


    assert (

        review.status

        ==

        "REVIEW_OPEN"

    )


    engine.analyze(

        "REVIEW_001",

        "Robustness decreased after regime change."

    )


    engine.finalize(

        "REVIEW_001",

        "UPDATE"

    )


    result = engine.get(

        "REVIEW_001"

    )


    assert (

        result.decision

        ==

        "UPDATE"

    )


    assert (

        result.status

        ==

        "DECISION_READY"

    )


    print(

        "GSR RESEARCH REVIEW ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
