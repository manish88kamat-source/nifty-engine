"""
GSR Executable Replay Pipeline Test
Version: 1.0.1

Purpose:
Validate:

Frozen Registry Strategy
        |
        v
ReplayRule
        |
        v
Historical Replay
        |
        v
Validation Output

Research mechanics test only.
"""

from typing import Sequence, Optional, Any
from datetime import datetime, timedelta, timezone

from gsr_historical_replay import (
    HistoricalReplayEngine,
    ReplayConfig,
    ReplayRule,
    MarketSnapshot,
)


def build_market_data():

    rows = []

    start = datetime(
        2025,
        1,
        1,
        9,
        15,
        tzinfo=timezone.utc
    )

    price = 100.0

    for i in range(100):

        price += 0.25

        rows.append(
            MarketSnapshot(
                timestamp=start + timedelta(minutes=i),
                symbol="TEST",
                open=price,
                high=price + 1,
                low=price - 1,
                close=price,
                volume=1000
            )
        )

    return rows



def test_signal(
    history: Sequence[Any],
    index: int
) -> Optional[str]:

    if index < 5:
        return None

    if index % 10 == 0:
        return "LONG"

    return None



def build_rule():

    return ReplayRule(

        strategy_id="GSR_AT_001",

        version="1.0.0",

        description=
        "Internal replay pipeline validation rule",

        signal=test_signal,

        holding_bars=5,

        source_type="REPRODUCIBLE_RULE"
    )



def run_test():

    data = build_market_data()


    engine = HistoricalReplayEngine(
        config=ReplayConfig()
    )


    engine.snapshots = data


    rule = build_rule()


    rule.validate()


    engine.register_rule(
        rule
    )


    result = engine.evaluate_registered_rules()


    assert result is not None


    assert (
        "GSR_AT_001"
        in str(result)
    )


    print(
        "GSR EXECUTABLE REPLAY PIPELINE TEST: PASS"
    )


if __name__ == "__main__":

    run_test()
