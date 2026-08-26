"""
GSR Models
Version: GSR_1.0.0_IMPLEMENTATION

Core immutable research data structures.

No:
- strategy execution
- prediction
- alpha generation

Only:
- structured records
- traceability
- version binding
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any, List


@dataclass(frozen=True)
class RegimeLabel:
    timestamp: str
    symbol: str
    regime_version: str

    trend_state: str
    volatility_state: str
    momentum_state: str
    market_structure_state: str
    transition_state: str

    composite_regime_state: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FeatureSnapshotRecord:
    timestamp: str
    symbol: str
    feature_version: str
    source_data_hash: str
    features: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyDNARecord:

    dna_id: str
    strategy_name: str
    dna_version: str

    components: Dict[str, Any]

    lifecycle_state: str = "DISCOVERED"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationRecord:

    validation_id: str
    object_id: str

    validation_type: str
    validation_status: str

    evidence: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResearchEvent:

    event_id: str
    timestamp: str

    event_type: str

    source_version: str

    payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def model_test():

    regime = RegimeLabel(
        timestamp="2026-01-01T09:18:00",
        symbol="NIFTY",
        regime_version="1.0.0",
        trend_state="UP_TREND",
        volatility_state="NORMAL_VOL",
        momentum_state="STRONG",
        market_structure_state="TRENDING",
        transition_state="STABLE",
        composite_regime_state="UP_TREND_NORMAL_VOL_STRONG"
    )

    dna = StrategyDNARecord(
        dna_id="DNA001",
        strategy_name="TEST_STRATEGY",
        dna_version="1.0.0",
        components={
            "entry": "momentum",
            "exit": "risk"
        }
    )

    assert regime.to_dict()["symbol"] == "NIFTY"
    assert dna.to_dict()["dna_id"] == "DNA001"

    print("GSR MODELS TEST: PASS")


if __name__ == "__main__":
    model_test()
