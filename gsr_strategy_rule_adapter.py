"""
GSR Strategy Rule Adapter
Version: GSR_1.0.0

Purpose:
Convert explicit strategy rule specifications
into executable ReplayRule objects.

Rules:
- No rule inference
- No missing parameter guessing
- Explicit specs only
"""

from dataclasses import dataclass
from typing import Dict, Any, Sequence, Optional, Mapping

from gsr_historical_replay import ReplayRule


@dataclass(frozen=True)
class StrategyRuleSpec:

    strategy_id: str
    version: str
    description: str
    holding_bars: int

    signal_function: Any

    source_type: str = "REPRODUCIBLE_RULE"



class GSRStrategyRuleAdapter:

    def __init__(self):
        self.version = "1.0.0"


    def validate_spec(
        self,
        spec: StrategyRuleSpec
    ) -> None:

        if not spec.strategy_id:
            raise ValueError(
                "strategy_id required"
            )

        if not spec.version:
            raise ValueError(
                "version required"
            )

        if not callable(
            spec.signal_function
        ):
            raise ValueError(
                "signal_function required"
            )

        if spec.holding_bars < 1:
            raise ValueError(
                "holding_bars must be >=1"
            )


    def build(
        self,
        spec: StrategyRuleSpec
    ) -> ReplayRule:

        self.validate_spec(spec)

        return ReplayRule(
            strategy_id=spec.strategy_id,
            version=spec.version,
            description=spec.description,
            signal=spec.signal_function,
            holding_bars=spec.holding_bars,
            source_type=spec.source_type
        )



def sample_signal(
    history: Sequence[Any],
    index: int
) -> Optional[str]:

    if index <= 0:
        return None

    return "LONG"



def strategy_rule_adapter_test():

    adapter = GSRStrategyRuleAdapter()

    spec = StrategyRuleSpec(
        strategy_id="GSR_AT_001",
        version="1.0.0",
        description="Explicit test breakout rule",
        holding_bars=5,
        signal_function=sample_signal
    )

    rule = adapter.build(spec)

    rule.validate()

    assert rule.strategy_id == "GSR_AT_001"

    print(
        "GSR STRATEGY RULE ADAPTER TEST: PASS"
    )


if __name__ == "__main__":
    strategy_rule_adapter_test()
