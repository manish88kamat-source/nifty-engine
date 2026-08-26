"""
GSR Rule Governance Integration Test
Version: 1.0.0

Verifies:
1. Non executable strategies are blocked.
2. Missing strategies are blocked.
3. Governance gate is active.
"""

from typing import Sequence, Any, Optional

from gsr_strategy_rule_adapter import (
    GSRStrategyRuleAdapter,
    StrategyRuleSpec
)


def dummy_signal(
    history: Sequence[Any],
    index: int
) -> Optional[str]:

    return None



def test_block_non_executable():

    adapter = GSRStrategyRuleAdapter()

    blocked = False

    spec = StrategyRuleSpec(
        strategy_id="GSR_AT_001",
        version="1.0.0",
        description="Governance block test",
        holding_bars=5,
        signal_function=dummy_signal
    )


    try:

        adapter.build(spec)

    except PermissionError:

        blocked = True


    assert blocked is True



def test_block_unknown_strategy():

    adapter = GSRStrategyRuleAdapter()

    blocked = False

    spec = StrategyRuleSpec(
        strategy_id="UNKNOWN_STRATEGY",
        version="1.0.0",
        description="Unknown strategy test",
        holding_bars=5,
        signal_function=dummy_signal
    )


    try:

        adapter.build(spec)

    except PermissionError:

        blocked = True


    assert blocked is True



def run_test():

    test_block_non_executable()

    test_block_unknown_strategy()

    print(
        "GSR RULE GOVERNANCE TEST: PASS"
    )


if __name__ == "__main__":

    run_test()
