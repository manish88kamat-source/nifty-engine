"""
GSR Storage Layer
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Persistent storage layer for GSR research records.

Rules:
- No strategy logic
- No prediction
- No modification of historical records
"""

import json
from pathlib import Path
from typing import Dict, Any, List


class GSRStorage:

    def __init__(self, storage_path: str = "gsr_storage.json"):
        self.storage_path = Path(storage_path)

    def save_record(
        self,
        record_id: str,
        record_type: str,
        payload: Dict[str, Any]
    ) -> Dict[str, Any]:

        records = self.load_all()

        if record_id in records:
            raise ValueError(
                f"Record already exists: {record_id}"
            )

        records[record_id] = {
            "record_id": record_id,
            "record_type": record_type,
            "payload": payload
        }

        self.storage_path.write_text(
            json.dumps(
                records,
                indent=2
            ),
            encoding="utf-8"
        )

        return records[record_id]


    def load_all(self) -> Dict[str, Any]:

        if not self.storage_path.exists():
            return {}

        return json.loads(
            self.storage_path.read_text(
                encoding="utf-8"
            )
        )


    def get_record(
        self,
        record_id: str
    ) -> Dict[str, Any]:

        records = self.load_all()

        if record_id not in records:
            raise KeyError(
                f"Unknown record: {record_id}"
            )

        return records[record_id]


    def count(self) -> int:
        return len(self.load_all())


    def export_records(self) -> List[Dict[str, Any]]:
        return list(
            self.load_all().values()
        )


def create_storage():
    return GSRStorage()


def storage_test():

    test_file = "test_gsr_storage.json"

    storage = GSRStorage(test_file)

    storage.save_record(
        record_id="TEST001",
        record_type="validation",
        payload={
            "status": "PASS",
            "version": "1.0.0"
        }
    )

    record = storage.get_record(
        "TEST001"
    )

    assert record["payload"]["status"] == "PASS"

    assert storage.count() == 1

    Path(test_file).unlink()

    print("GSR STORAGE TEST: PASS")


if __name__ == "__main__":
    storage_test()
