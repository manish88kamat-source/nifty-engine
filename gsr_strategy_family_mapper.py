"""
GSR Strategy Family Mapper
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Map strategy DNA into normalized strategy families.

Rules:
- No trading decision
- No performance ranking
- Only classification
"""

from typing import Dict, Any


class GSRStrategyFamilyMapper:

    def __init__(self):
        self.version = "1.0.0"


    def map_family(
        self,
        strategy: Dict[str, Any]
    ) -> str:

        family = strategy.get(
            "family",
            ""
        ).upper()

        mechanism = strategy.get(
            "mechanism",
            ""
        ).upper()

        tags = [
            str(x).upper()
            for x in strategy.get(
                "mechanism_tags",
                []
            )
        ]

        text = (
            family
            + " "
            + mechanism
            + " "
            + " ".join(tags)
        )


        if any(
            x in text
            for x in [
                "TREND",
                "MOMENTUM",
                "FOLLOW"
            ]
        ):
            return "TREND_FOLLOWING"


        if any(
            x in text
            for x in [
                "MEAN",
                "REVERS",
                "MR"
            ]
        ):
            return "MEAN_REVERSION"


        if any(
            x in text
            for x in [
                "BREAKOUT",
                "RANGE_EXPANSION"
            ]
        ):
            return "BREAKOUT"


        if any(
            x in text
            for x in [
                "VOLATILITY",
                "VOL"
            ]
        ):
            return "VOLATILITY"


        if any(
            x in text
            for x in [
                "OPTION",
                "SPREAD",
                "ARBITRAGE"
            ]
        ):
            return "OPTIONS_ARBITRAGE"


        return "OTHER"



def create_family_mapper():
    return GSRStrategyFamilyMapper()



def family_mapper_test():

    mapper = GSRStrategyFamilyMapper()


    trend = mapper.map_family(
        {
            "family": "Trend Following",
            "mechanism": "Momentum Breakout",
            "mechanism_tags": [
                "MOMENTUM"
            ]
        }
    )


    mean_rev = mapper.map_family(
        {
            "family": "Mean Reversion",
            "mechanism": "RSI Reversal",
            "mechanism_tags": [
                "REVERSAL"
            ]
        }
    )


    assert trend == "TREND_FOLLOWING"
    assert mean_rev == "MEAN_REVERSION"


    print("GSR STRATEGY FAMILY MAPPER TEST: PASS")


if __name__ == "__main__":
    family_mapper_test()
