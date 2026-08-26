"""
GSR Strategy DNA Engine
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Convert strategy registry records into normalized strategy DNA.

Rules:
- No execution logic
- No trading signal
- No prediction
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any, List
import hashlib
import json


@dataclass(frozen=True)
class StrategyDNA:

    strategy_id: str
    family: str
    mechanism: str
    asset_class: str

    mechanism_tags: List[str]
    rule_precision: str
    evidence_grade: str

    dna_hash: str

    def to_dict(self):
        return asdict(self)



class GSRStrategyDNAEngine:

    def __init__(self):
        self.version = "1.0.0"


    def create_hash(
        self,
        payload: Dict[str, Any]
    ) -> str:

        raw = json.dumps(
            payload,
            sort_keys=True
        )

        return hashlib.sha256(
            raw.encode()
        ).hexdigest()[:16]


    def generate(
        self,
        strategy: Dict[str, Any]
    ) -> StrategyDNA:

        dna_payload = {
            "family": strategy.get("family"),
            "mechanism": strategy.get("mechanism"),
            "tags": strategy.get("mechanism_tags", []),
            "asset": strategy.get("asset_class")
        }

        return StrategyDNA(
            strategy_id=strategy.get("atomic_strategy_id"),
            family=strategy.get("family"),
            mechanism=strategy.get("mechanism"),
            asset_class=strategy.get("asset_class"),
            mechanism_tags=strategy.get(
                "mechanism_tags",
                []
            ),
            rule_precision=strategy.get(
                "rule_precision",
                "UNKNOWN"
            ),
            evidence_grade=strategy.get(
                "evidence_grade",
                "UNKNOWN"
            ),
            dna_hash=self.create_hash(
                dna_payload
            )
        )


def create_strategy_dna_engine():
    return GSRStrategyDNAEngine()



def strategy_dna_test():

    engine = GSRStrategyDNAEngine()

    strategy = {
        "atomic_strategy_id": "GSR_AT_001",
        "family": "TREND_FOLLOWING",
        "mechanism": "MOMENTUM_BREAKOUT",
        "asset_class": "EQUITY",
        "mechanism_tags": [
            "BREAKOUT",
            "MOMENTUM"
        ],
        "rule_precision": "HIGH",
        "evidence_grade": "A"
    }

    dna = engine.generate(strategy)

    assert dna.strategy_id == "GSR_AT_001"
    assert dna.family == "TREND_FOLLOWING"
    assert len(dna.dna_hash) == 16

    print("GSR STRATEGY DNA TEST: PASS")


if __name__ == "__main__":
    strategy_dna_test()
