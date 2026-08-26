"""
GSR Research Feedback Loop Engine
Version: 1.0.0

Captures outcomes of governance actions
and converts them into research learning.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-FEEDBACK-LOOP-ENGINE-1.0.0"



@dataclass
class ResearchFeedback:


    feedback_id: str

    action_id: str

    asset_id: str

    outcome: str

    improvement_score: float

    learning_points: List[str]

    future_recommendations: List[str]

    created_at: str



class GSRResearchFeedbackLoopEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.feedbacks = {}



    def record_feedback(

        self,

        feedback_id: str,

        action_id: str,

        asset_id: str,

        outcome: str,

        previous_score: float,

        new_score: float

    ):


        improvement = round(

            new_score - previous_score,

            2

        )


        learning = []

        recommendations = []



        if improvement > 0:


            learning.append(

                "Research action improved asset quality."

            )

            recommendations.append(

                "Apply similar improvement process."

            )


        elif improvement < 0:


            learning.append(

                "Research action did not improve quality."

            )

            recommendations.append(

                "Review assumptions and methodology."

            )


        else:


            learning.append(

                "No measurable change detected."

            )

            recommendations.append(

                "Collect additional evidence."

            )



        feedback = ResearchFeedback(

            feedback_id=feedback_id,

            action_id=action_id,

            asset_id=asset_id,

            outcome=outcome,

            improvement_score=improvement,

            learning_points=learning,

            future_recommendations=recommendations,

            created_at=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.feedbacks[feedback_id] = feedback


        return feedback



    def get(

        self,

        feedback_id

    ):


        return self.feedbacks.get(

            feedback_id

        )



    def export(

        self,

        path="gsr_data/research_feedback.json"

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


                    "feedbacks":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.feedbacks.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchFeedbackLoopEngine()



    feedback = engine.record_feedback(

        feedback_id="FB_001",

        action_id="ACTION_001",

        asset_id="ASSET_001",

        outcome="REVALIDATION_COMPLETED",

        previous_score=70,

        new_score=85

    )


    assert (

        feedback.improvement_score

        ==

        15

    )


    assert len(

        feedback.learning_points

    ) > 0



    stored = engine.get(

        "FB_001"

    )


    assert stored is not None



    print(

        "GSR RESEARCH FEEDBACK LOOP ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
