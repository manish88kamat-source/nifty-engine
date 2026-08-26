"""
GSR Performance Analyzer
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Analyze research/replay performance outputs.

Rules:
- No execution logic
- No signal generation
- Analytics only
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any, List


@dataclass(frozen=True)
class PerformanceReport:

    strategy_id: str
    trades: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    average_return: float

    def to_dict(self):
        return asdict(self)



class GSRPerformanceAnalyzer:

    def __init__(self):
        self.version = "1.0.0"


    def calculate_win_rate(
        self,
        returns: List[float]
    ) -> float:

        if not returns:
            return 0.0

        wins = [
            x for x in returns
            if x > 0
        ]

        return round(
            len(wins) / len(returns),
            4
        )


    def calculate_profit_factor(
        self,
        returns: List[float]
    ) -> float:

        gains = sum(
            x for x in returns
            if x > 0
        )

        losses = abs(
            sum(
                x for x in returns
                if x < 0
            )
        )

        if losses == 0:
            return float("inf")

        return round(
            gains / losses,
            4
        )


    def calculate_drawdown(
        self,
        returns: List[float]
    ) -> float:

        equity = 0
        peak = 0
        max_dd = 0

        for value in returns:

            equity += value

            if equity > peak:
                peak = equity

            dd = peak - equity

            if dd > max_dd:
                max_dd = dd

        return round(
            max_dd,
            4
        )


    def analyze(
        self,
        strategy_id: str,
        returns: List[float]
    ) -> PerformanceReport:

        return PerformanceReport(
            strategy_id=strategy_id,
            trades=len(returns),
            win_rate=self.calculate_win_rate(
                returns
            ),
            profit_factor=self.calculate_profit_factor(
                returns
            ),
            max_drawdown=self.calculate_drawdown(
                returns
            ),
            average_return=round(
                sum(returns) / len(returns),
                4
            ) if returns else 0.0
        )



def create_performance_analyzer():
    return GSRPerformanceAnalyzer()



def performance_analyzer_test():

    analyzer = GSRPerformanceAnalyzer()

    report = analyzer.analyze(
        "GSR_AT_001",
        [
            10,
            -5,
            8,
            -2
        ]
    )

    assert report.trades == 4
    assert report.win_rate == 0.5
    assert report.max_drawdown == 5

    print("GSR PERFORMANCE ANALYZER TEST: PASS")


if __name__ == "__main__":
    performance_analyzer_test()
