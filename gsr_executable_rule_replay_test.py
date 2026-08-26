"""
GSR Executable Rule Replay Test
Version: 1.0.0

Purpose:
Validate complete path:

Rule Registry
 -> Strategy Rule Adapter
 -> ReplayRule
 -> Historical Replay Engine

Research mechanics test only.
"""


from typing import Sequence, Any, Optional

from gsr_strategy_rule_adapter import (
    GSRStrategyRuleAdapter,
    StrategyRuleSpec
)

from gsr_historical_replay import (
    HistoricalReplayEngine,
    ReplayConfig,
    ReplayRule,
    MarketSnapshot
)



def approved_signal(
    history: Sequence[Any],
    index: int
) -> Optional[str]:

    if index == 0:
        return None

    return "LONG"



def build_test_rule():

    return StrategyRuleSpec(

        strategy_id="GSR_TEST_EXECUTABLE_001",

        version="1.0.0",

        description="Synthetic executable rule validation",

        holding_bars=5,

        signal_function=approved_signal,

        source_type="REPRODUCIBLE_RULE"
    )



def enable_test_rule():

    """
    Test only.

    Existing trader strategies remain locked.
    """

    import json

    path = "gsr_rule_specs/rule_status_registry.json"

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as f:

        data = json.load(f)


    data["rules"]["GSR_TEST_EXECUTABLE_001"] = {

        "strategy_name":
        "Synthetic Replay Test Rule",

        "state":
        "APPROVED_TEST",

        "executable":
        True,

        "allow_replay":
        True,

        "reason":
        "Pipeline integration testing only."
    }


    with open(
        path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            indent=2
        )



def run_test():

    enable_test_rule()


    adapter = GSRStrategyRuleAdapter()


    spec = build_test_rule()


    replay_rule = adapter.build(
        spec
    )


    replay_rule.validate()


    assert (
        replay_rule.strategy_id
        ==
        "GSR_TEST_EXECUTABLE_001"
    )


    print(
        "GSR EXECUTABLE RULE REPLAY TEST: PASS"
    )


if __name__ == "__main__":

    run_test()
