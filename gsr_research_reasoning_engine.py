"""
GSR Research Reasoning Engine
Version: 1.0.0

Evidence aggregation and research conclusion layer.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-REASONING-1.0.0"



@dataclass
class ResearchConclusion:

    reasoning_id: str

    conclusion: str

    confidence_score: float

    supporting_factors: List[str]

    risk_factors: List[str]

    decision: str



class GSRResearchReasoningEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.conclusions = {}



    def evaluate(

        self,

        reasoning_id: str,

        evidence_grade: str,

        validation_status: str,

        experiment_count: int,

        similarity_score: float,

        historical_success: bool

    ):


        score = 0

        supporting = []

        risks = []



        grade_score = {

            "A":40,

            "B":30,

            "C":20,

            "D":10

        }.get(

            evidence_grade,

            0

        )



        score += grade_score



        if evidence_grade in ["A","B"]:

            supporting.append(

                "Strong evidence quality"

            )

        else:

            risks.append(

                "Weak evidence grade"

            )



        if validation_status == "PASS":

            score += 25

            supporting.append(

                "Validation passed"

            )

        else:

            risks.append(

                "Validation failed"

            )



        if experiment_count >= 3:

            score += 15

            supporting.append(

                "Multiple experiments available"

            )

        else:

            risks.append(

                "Limited experiment sample"

            )



        if similarity_score >= 70:

            score += 10

            supporting.append(

                "Matches previous successful research"

            )



        if historical_success:

            score += 10

            supporting.append(

                "Positive historical evidence"

            )

        else:

            risks.append(

                "No historical confirmation"

            )



        confidence = min(

            score,

            100

        )



        if confidence >= 80:

            decision = "PROMOTE"

            conclusion = (

                "Research evidence strongly supports continuation."

            )


        elif confidence >= 60:

            decision = "WATCH"

            conclusion = (

                "Research shows potential but requires monitoring."

            )


        else:

            decision = "REJECT"

            conclusion = (

                "Research evidence is insufficient."

            )



        result = ResearchConclusion(

            reasoning_id=reasoning_id,

            conclusion=conclusion,

            confidence_score=confidence,

            supporting_factors=supporting,

            risk_factors=risks,

            decision=decision

        )


        self.conclusions[reasoning_id] = result


        return result



    def get(

        self,

        reasoning_id

    ):


        return self.conclusions.get(

            reasoning_id

        )



    def export(

        self,

        path="gsr_data/research_reasoning.json"

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


                    "conclusions":

                    {

                        k:

                        asdict(v)

                        for k,v

                        in self.conclusions.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchReasoningEngine()



    result = engine.evaluate(

        reasoning_id="REASON_001",

        evidence_grade="A",

        validation_status="PASS",

        experiment_count=5,

        similarity_score=85,

        historical_success=True

    )


    assert (

        result.decision

        ==

        "PROMOTE"

    )


    assert (

        result.confidence_score

        >=

        80

    )


    stored = engine.get(

        "REASON_001"

    )


    assert stored is not None



    print(

        "GSR RESEARCH REASONING ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
