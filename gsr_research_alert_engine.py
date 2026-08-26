"""
GSR Research Alert Engine
Version: 1.0.0

Generates research governance alerts
from monitoring results.

Research only.
No live trading.
"""


from dataclasses import dataclass, asdict
from typing import Dict, List
from pathlib import Path
import json
from datetime import datetime, timezone



ENGINE_VERSION = "GSR-ALERT-ENGINE-1.0.0"



@dataclass
class ResearchAlert:


    alert_id: str

    asset_id: str

    severity: str

    alert_type: str

    message: str

    recommended_action: str

    timestamp: str



class GSRResearchAlertEngine:


    def __init__(self):

        self.version = ENGINE_VERSION

        self.alerts = {}



    def generate_alert(

        self,

        alert_id: str,

        asset_id: str,

        health_status: str,

        health_score: float,

        evidence_score: float,

        robustness_score: float

    ):


        severity = "LOW"

        alert_type = "HEALTH"

        message = "Research asset healthy."

        action = "Continue monitoring"



        if health_status == "AT_RISK" or health_score < 60:


            severity = "CRITICAL"

            alert_type = "ASSET_DEGRADATION"

            message = (

                "Research asset health critically degraded."

            )

            action = (

                "Immediate validation review required."

            )


        elif (

            health_status == "REVIEW_REQUIRED"

            or

            health_score < 75

        ):


            severity = "HIGH"

            alert_type = "REVIEW_TRIGGER"

            message = (

                "Research asset requires review."

            )

            action = (

                "Perform additional validation."

            )


        elif evidence_score < 70:


            severity = "MEDIUM"

            alert_type = "EVIDENCE_DECAY"

            message = (

                "Evidence quality has weakened."

            )

            action = (

                "Collect additional evidence."

            )


        elif robustness_score < 70:


            severity = "MEDIUM"

            alert_type = "ROBUSTNESS_DECAY"

            message = (

                "Robustness degradation detected."

            )

            action = (

                "Run robustness analysis."

            )



        result = ResearchAlert(

            alert_id=alert_id,

            asset_id=asset_id,

            severity=severity,

            alert_type=alert_type,

            message=message,

            recommended_action=action,

            timestamp=

            datetime.now(

                timezone.utc

            ).isoformat()

        )


        self.alerts[alert_id] = result


        return result



    def get(

        self,

        alert_id

    ):


        return self.alerts.get(

            alert_id

        )



    def export(

        self,

        path="gsr_data/research_alerts.json"

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


                    "alerts":

                    {

                        key:

                        asdict(value)

                        for key,value

                        in self.alerts.items()

                    }

                },

                f,

                indent=2

            )


        return path



def self_test():


    engine = GSRResearchAlertEngine()



    alert = engine.generate_alert(

        alert_id="ALERT_001",

        asset_id="ASSET_001",

        health_status="AT_RISK",

        health_score=55,

        evidence_score=60,

        robustness_score=65

    )


    assert (

        alert.severity

        ==

        "CRITICAL"

    )


    assert (

        alert.alert_type

        ==

        "ASSET_DEGRADATION"

    )


    stored = engine.get(

        "ALERT_001"

    )


    assert stored is not None



    print(

        "GSR RESEARCH ALERT ENGINE TEST: PASS"

    )



if __name__=="__main__":

    self_test()
