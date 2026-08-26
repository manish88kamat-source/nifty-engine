"""
GSR Strategy Rule Adapter
Version: GSR_1.0.1

Purpose:
Convert explicitly approved strategy rule specifications
into executable ReplayRule objects.

Governance:
- No strategy rule inference
- No missing parameter guessing
- Registry gate mandatory
- Only executable=true strategies allowed
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Sequence, Optional
import json

from gsr_historical_replay import ReplayRule


RULE_STATUS_FILE = (
    Path("gsr_rule_specs")
    / "rule_status_registry.json"
)


@dataclass(frozen=True)
class StrategyRuleSpec:

    strategy_id: str
    version: str
    description: str
    holding_bars: int
    signal_function: Any

    source_type: str = "REPRODUCIBLE_RULE"



class RuleGovernanceGate:

    def __init__(
        self,
        path: Path = RULE_STATUS_FILE
    ):
        self.path = path
        self.registry = self._load()


    def _load(self) -> Dict[str, Any]:

        if not self.path.exists():
            raise FileNotFoundError(
                f"Missing rule registry: {self.path}"
            )

        with open(
            self.path,
            "r",
            encoding="utf-8"
        ) as f:
            return json.load(f)


    def is_executable(
        self,
        strategy_id: str
    ) -> bool:

        rules = self.registry.get(
            "rules",
            {}
        )

        entry = rules.get(
            strategy_id
        )

        if not entry:
            return False

        return bool(
            entry.get(
                "executable",
                False
            )
        )


    def reason(
        self,
        strategy_id: str
    ) -> str:

        rules = self.registry.get(
            "rules",
            {}
        )

        entry = rules.get(
            strategy_id,
            {}
        )

        return entry.get(
            "reason",
            "Not approved"
        )



class GSRStrategyRuleAdapter:


    def __init__(self):

        self.version = "1.0.1"

        self.gate = RuleGovernanceGate()



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

        if not spec.description:
            raise ValueError(
                "description required"
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

        self.validate_spec(
            spec
        )


        if not self.gate.is_executable(
            spec.strategy_id
        ):

            raise PermissionError(
                "Strategy execution blocked: "
                + self.gate.reason(
                    spec.strategy_id
                )
            )


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


    blocked = False

    try:

        spec = StrategyRuleSpec(

            strategy_id="GSR_AT_001",

            version="1.0.0",

            description="Test rule",

            holding_bars=5,

            signal_function=sample_signal
        )


        adapter.build(spec)


    except PermissionError:

        blocked = True



    assert blocked is True


    print(
        "GSR STRATEGY RULE ADAPTER GOVERNANCE TEST: PASS"
    )



if __name__ == "__main__":

    strategy_rule_adapter_test()
