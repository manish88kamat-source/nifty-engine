"""
GSR Research Promotion Gate
Version: 1.0.0

Final institutional approval gate
for validated research artifacts.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-PROMOTION-GATE-1.0.0"



@dataclass
class PromotionDecision:


    promotion_id: str

    research_id: str

    decision: str

    confidence_score: float

    approved_checks: List[str]

    rejected_checks: List[str]

    rationale: str



class GSRResearchPromotionGate:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.decisions = {}



    def evaluate(

        self,

        promotion_id: str,

        research_id: str,

        evidence_grade: str,

        evidence_score: float,

        validation_status: str,

        robustness_score: float,

        lifecycle_status: str

    ):


        approved = []

        rejected = []

        score = 0



        # Evidence grade

        if evidence_grade in ["A", "B"]:

            approved.append(

                "Evidence grade accepted"

            )

            score += 25

        else:

            rejected.append(

                "Evidence grade insufficient"

            )



        # Evidence score

        if evidence_score >= 80:

            approved.append(

                "Evidence score threshold passed"

            )

            score += 25

        else:

            rejected.append(

                "Evidence score below threshold"

            )



        # Validation

        if validation_status == "VALIDATED":

            approved.append(

                "Validation completed"

            )

            score += 20

        else:

            rejected.append(

                "Validation incomplete"

            )



        # Robustness

        if robustness_score >= 75:

            approved.append(

                "Robustness requirement passed"

            )

            score += 20

        else:

            rejected.append(

                "Robustness requirement failed"

            )



        # Lifecycle

        if lifecycle_status in [

            "ACTIVE",

            "RESEARCH_COMPLETE"

        ]:

            approved.append(

                "Lifecycle state acceptable"

            )

            score += 10

        else:

            rejected.append(

                "Lifecycle state blocked"

            )



        if score >= 80:

            decision = "PROMOTION_APPROVED"

            rationale = (

                "Research satisfies institutional promotion criteria."

            )

        else:

            decision = "PROMOTION_BLOCKED"

            rationale = (

                "Research does not satisfy promotion criteria."

            )



        result = PromotionDecision(

            promotion_id=promotion_id,

            research_id=research_id,

            decision=decision,

            confidence_score=score,

            approved_checks=approved,

            rejected_checks=rejected,

            rationale=rationale

        )


        self.decisions[promotion_id] = result


        return result



    def get(

        self,

        promotion_id

    ):


        return self.decisions.get(

            promotion_id

        )



    def export(

        self,

        path="gsr_data/promotion_decisions.json"

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


                    "decisions":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.decisions.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchPromotionGate()



    result = engine.evaluate(

        promotion_id="PROMO_001",

        research_id="GSR_AT_001",

        evidence_grade="A",

        evidence_score=92,

        validation_status="VALIDATED",

        robustness_score=88,

        lifecycle_status="ACTIVE"

    )


    assert (

        result.decision

        ==

        "PROMOTION_APPROVED"

    )


    assert (

        result.confidence_score

        >=

        80

    )


    stored = engine.get(

        "PROMO_001"

    )


    assert stored is not None



    print(

        "GSR RESEARCH PROMOTION GATE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
