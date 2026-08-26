"""
GSR Feature Snapshot
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Create immutable feature snapshots for research traceability.

No:
- strategy logic
- prediction
- execution
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any
import hashlib
import json


@dataclass(frozen=True)
class GSRFeatureSnapshot:

    timestamp: str
    symbol: str
    feature_version: str

    features: Dict[str, Any]

    source_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)



class GSRFeatureSnapshotEngine:

    def __init__(self):
        self.snapshot_version = "1.0.0"


    def create_hash(
        self,
        data: Dict[str, Any]
    ) -> str:

        encoded = json.dumps(
            data,
            sort_keys=True
        ).encode("utf-8")

        return hashlib.sha256(
            encoded
        ).hexdigest()


    def create_snapshot(
        self,
        timestamp: str,
        symbol: str,
        features: Dict[str, Any]
    ) -> GSRFeatureSnapshot:

        source_hash = self.create_hash(
            features
        )

        return GSRFeatureSnapshot(
            timestamp=timestamp,
            symbol=symbol,
            feature_version=self.snapshot_version,
            features=features,
            source_hash=source_hash
        )


def create_snapshot_engine():
    return GSRFeatureSnapshotEngine()



def snapshot_test():

    engine = GSRFeatureSnapshotEngine()

    snapshot = engine.create_snapshot(
        timestamp="2026-01-01T09:18:00",
        symbol="NIFTY",
        features={
            "EMA20": 100,
            "ATR": 2,
            "RSI": 60
        }
    )

    data = snapshot.to_dict()

    assert data["symbol"] == "NIFTY"
    assert data["source_hash"]

    print("GSR FEATURE SNAPSHOT TEST: PASS")


if __name__ == "__main__":
    snapshot_test()
