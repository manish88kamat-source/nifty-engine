"""
GSR Execution Contract
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Define execution boundary contract.

Rules:
- No execution
- No broker connection
- No trading decision
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass(frozen=True)
class ExecutionContract:

    event_id: str
    timestamp: str
    symbol: str

    quantity: float
    price: float

    source: str
    metadata: Dict[str, Any]

    contract_version: str = "1.0.0"


    def to_dict(self):
        return asdict(self)



class GSRExecutionContractValidator:

    def __init__(self):
        self.version = "1.0.0"


    def validate(
        self,
        payload: Dict[str, Any]
    ) -> bool:

        required = [
            "event_id",
            "timestamp",
            "symbol",
            "quantity",
            "price",
            "source"
        ]

        for field in required:
            if field not in payload:
                return False

        return True



    def create(
        self,
        payload: Dict[str, Any]
    ) -> ExecutionContract:

        if not self.validate(payload):
            raise ValueError(
                "Invalid execution contract"
            )

        return ExecutionContract(
            event_id=payload["event_id"],
            timestamp=payload["timestamp"],
            symbol=payload["symbol"],
            quantity=float(
                payload["quantity"]
            ),
            price=float(
                payload["price"]
            ),
            source=payload["source"],
            metadata=payload.get(
                "metadata",
                {}
            )
        )



def create_execution_contract_validator():
    return GSRExecutionContractValidator()



def execution_contract_test():

    validator = GSRExecutionContractValidator()

    payload = {
        "event_id": "EV001",
        "timestamp": "2026-01-01T09:15:00",
        "symbol": "NIFTY",
        "quantity": 50,
        "price": 22000,
        "source": "GSR_BRIDGE"
    }

    contract = validator.create(
        payload
    )

    assert contract.symbol == "NIFTY"
    assert contract.quantity == 50

    print("GSR EXECUTION CONTRACT TEST: PASS")


if __name__ == "__main__":
    execution_contract_test()
