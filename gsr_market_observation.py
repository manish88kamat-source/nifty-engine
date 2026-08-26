"""
GSR Market Observation
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Normalized raw market observation container.

Rules:
- No signal
- No strategy opinion
- No execution decision
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass(frozen=True)
class MarketObservation:

    timestamp: str
    symbol: str

    open: float
    high: float
    low: float
    close: float

    volume: float

    metadata: Dict[str, Any]

    observation_version: str = "1.0.0"


    def to_dict(self):
        return asdict(self)



class GSRMarketObservationEngine:

    def __init__(self):
        self.version = "1.0.0"


    def create(
        self,
        data: Dict[str, Any]
    ) -> MarketObservation:

        return MarketObservation(
            timestamp=data.get(
                "timestamp",
                ""
            ),
            symbol=data.get(
                "symbol",
                ""
            ),
            open=float(
                data.get("open", 0)
            ),
            high=float(
                data.get("high", 0)
            ),
            low=float(
                data.get("low", 0)
            ),
            close=float(
                data.get("close", 0)
            ),
            volume=float(
                data.get("volume", 0)
            ),
            metadata=data.get(
                "metadata",
                {}
            )
        )



def create_observation_engine():
    return GSRMarketObservationEngine()



def market_observation_test():

    engine = GSRMarketObservationEngine()

    obs = engine.create(
        {
            "timestamp": "2026-01-01T09:15:00",
            "symbol": "NIFTY",
            "open": 22000,
            "high": 22050,
            "low": 21980,
            "close": 22030,
            "volume": 100000
        }
    )

    assert obs.symbol == "NIFTY"
    assert obs.close == 22030

    print("GSR MARKET OBSERVATION TEST: PASS")


if __name__ == "__main__":
    market_observation_test()
