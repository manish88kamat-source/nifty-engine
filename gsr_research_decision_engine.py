"""
GSR Research Decision Engine
Version: 1.0.0

Final governance decision layer
for research assets.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-DECISION-ENGINE-1.0.0"



@dataclass
class ResearchDecision:


    decision_id: str

    asset_id: str

    final_decision: str

    confidence_score: float

    supporting_factors: List[str]

    risk_factors: List[str]

    timestamp: str



class GSRResearchDecisionEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.decisions = {}



    def decide(

        self,

        decision_id: str,

        asset_id: str,

        review_decision: str,

        evidence_score: float,

        health_score: float,

        lifecycle_state: str

    ):


        score = 0

        supporting = []

        risks = []



        # Review result

        if review_decision == "KEEP":

            score += 30

            supporting.append(

                "Review approved continuation"

            )


        elif review_decision == "UPDATE":

            score += 15

            risks.append(

                "Research requires update"

            )


        elif review_decision == "RETIRE":

            score -= 50

            risks.append(

                "Review recommended retirement"

            )



        # Evidence quality

        if evidence_score >= 80:

            score += 30

            supporting.append(

                "Strong evidence quality"

            )

        else:

            risks.append(

                "Evidence quality below threshold"

            )



        # Health

        if health_score >= 80:

            score += 25

            supporting.append(

                "Asset health stable"

            )

        elif health_score < 60:

            score -= 20

            risks.append(

                "Asset health degraded"

            )



        # Lifecycle

        if lifecycle_state in [

            "ACTIVE",

            "MONITORED"

        ]:

            score += 15

            supporting.append(

                "Lifecycle state acceptable"

            )

        else:

            risks.append(

                "Lifecycle state restricted"

            )



        if score >= 80:

            decision = "CONTINUE"


        elif score >= 50:

            decision = "REVALIDATE_REQUIRED"


        else:

            decision = "RETIRE_ASSET"



        result = ResearchDecision(

            decision_id=decision_id,

            asset_id=asset_id,

            final_decision=decision,

            confidence_score=max(

                score,

                0

            ),

            supporting_factors=supporting,

            risk_factors=risks,

            timestamp=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.decisions[decision_id] = result


        return result



    def get(

        self,

        decision_id

    ):


        return self.decisions.get(

            decision_id

        )



    def export(

        self,

        path="gsr_data/research_decisions.json"

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


    engine = GSRResearchDecisionEngine()



    result = engine.decide(

        decision_id="DECISION_001",

        asset_id="ASSET_001",

        review_decision="KEEP",

        evidence_score=92,

        health_score=90,

        lifecycle_state="ACTIVE"

    )


    assert (

        result.final_decision

        ==

        "CONTINUE"

    )


    assert (

        result.confidence_score

        >=

        80

    )


    stored = engine.get(

        "DECISION_001"

    )


    assert stored is not None



    print(

        "GSR RESEARCH DECISION ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
