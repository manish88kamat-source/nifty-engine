
"""
GSR Research Evidence Scoring Engine
Version: 1.0.0

Quantitative evidence quality grading
for research validation.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List, Any
from pathlib import Path
import json



ENGINE_VERSION = "GSR-EVIDENCE-SCORING-1.0.0"



@dataclass
class EvidenceScore:


    evidence_id: str

    total_score: float

    grade: str

    quality: str

    factors: Dict[str,float]

    strengths: List[str]

    weaknesses: List[str]



class GSRResearchEvidenceScoringEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.results = {}



    def calculate(

        self,

        evidence_id: str,

        experiment_count: int,

        oos_score: float,

        robustness_score: float,

        reproducibility_score: float,

        risk_score: float

    ):


        factors = {}

        strengths = []

        weaknesses = []



        # Experiment depth

        experiment_factor = min(

            experiment_count * 5,

            20

        )


        factors["experiment_depth"] = experiment_factor



        # OOS

        factors["out_of_sample"] = oos_score * 0.25



        # Robustness

        factors["robustness"] = robustness_score * 0.25



        # Reproducibility

        factors["reproducibility"] = reproducibility_score * 0.15



        # Risk

        factors["risk_stability"] = risk_score * 0.15



        total = round(

            sum(factors.values()),

            2

        )



        if total >= 85:

            grade = "A"

            quality = "HIGH"


        elif total >= 70:

            grade = "B"

            quality = "GOOD"


        elif total >= 50:

            grade = "C"

            quality = "MODERATE"


        else:

            grade = "D"

            quality = "LOW"



        if robustness_score >= 80:

            strengths.append(

                "Strong robustness"

            )

        else:

            weaknesses.append(

                "Weak robustness"

            )



        if oos_score >= 80:

            strengths.append(

                "Strong out-of-sample evidence"

            )

        else:

            weaknesses.append(

                "Limited OOS confirmation"

            )



        if reproducibility_score >= 80:

            strengths.append(

                "High reproducibility"

            )



        result = EvidenceScore(

            evidence_id=evidence_id,

            total_score=total,

            grade=grade,

            quality=quality,

            factors=factors,

            strengths=strengths,

            weaknesses=weaknesses

        )


        self.results[evidence_id] = result


        return result



    def get(

        self,

        evidence_id

    ):


        return self.results.get(

            evidence_id

        )



    def export(

        self,

        path="gsr_data/evidence_scores.json"

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


                    "results":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.results.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchEvidenceScoringEngine()



    result = engine.calculate(

        evidence_id="EVIDENCE_001",

        experiment_count=10,

        oos_score=90,

        robustness_score=88,

        reproducibility_score=85,

        risk_score=90

    )


    assert (

        result.grade

        ==

        "A"

    )


    assert (

        result.total_score

        >=

        85

    )


    stored = engine.get(

        "EVIDENCE_001"

    )


    assert stored is not None



    print(

        "GSR RESEARCH EVIDENCE SCORING ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
