"""
Market Data Hub
GSR / NIFTY / Next-Day shared raw-data infrastructure.

Version:
    MDH_1.0.0

Architecture:
    External Market Source
            |
            v
      MarketDataHub
        /   |   \
       /    |    \
    NIFTY  GSR  Next-Day

IMPORTANT:
- This layer handles RAW market observations only.
- No strategy logic.
- No signals.
- No regime decisions.
- No alpha scores.
- No engine opinions.

Responsibilities:
- Accept raw observations.
- Validate minimal transport/schema fields.
- Persist raw observations.
- Fan-out observations to registered consumers.
- Replay persisted observations.
- Monitor source freshness.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Optional
import hashlib
import json
import time


HUB_VERSION = "1.0.0"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def safe_float(value: Any) -> Optional[float]:
    try:
        x = float(value)

        if x != x:
            return None

        if x in (float("inf"), float("-inf")):
            return None

        return x

    except (TypeError, ValueError):
        return None


def normalize_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        dt = datetime.fromisoformat(text)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).isoformat()


def observation_hash(observation: Dict[str, Any]) -> str:
    payload = json.dumps(
        observation,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")

    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class RawMarketObservation:
    """
    Transport-level raw market observation.

    The hub deliberately does not calculate indicators or
    interpret market direction.
    """

    source: str
    symbol: str
    timestamp: str
    received_at: str

    ltp: Optional[float] = None

    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None

    volume: Optional[float] = None
    oi: Optional[float] = None

    instrument_token: str = ""
    exchange_segment: str = ""

    raw: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RawObservationValidator:

    REQUIRED = (
        "source",
        "symbol",
        "timestamp",
        "received_at",
    )

    @classmethod
    def validate(
        cls,
        observation: RawMarketObservation,
    ) -> None:

        data = observation.to_dict()

        for field in cls.REQUIRED:
            value = data.get(field)

            if value is None:
                raise ValueError(
                    f"Missing required field: {field}"
                )

            if isinstance(value, str) and not value.strip():
                raise ValueError(
                    f"Empty required field: {field}"
                )

        normalize_timestamp(
            observation.timestamp
        )

        normalize_timestamp(
            observation.received_at
        )

        if observation.ltp is not None:
            if safe_float(observation.ltp) is None:
                raise ValueError(
                    "Invalid LTP"
                )

        for field in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "oi",
        ):
            value = getattr(
                observation,
                field,
            )

            if value is not None:
                if safe_float(value) is None:
                    raise ValueError(
                        f"Invalid numeric field: {field}"
                    )


class RawObservationStore:

    def __init__(
        self,
        path: str = "market_data/raw_observations.jsonl",
    ):
        self.path = Path(path)
        self.lock = RLock()

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

    def append(
        self,
        observation: RawMarketObservation,
    ) -> str:

        payload = observation.to_dict()

        record_hash = observation_hash(
            payload
        )

        record = {
            "hub_version": HUB_VERSION,
            "record_hash": record_hash,
            "observation": payload,
        }

        line = json.dumps(
            record,
            sort_keys=True,
            default=str,
        )

        with self.lock:
            with self.path.open(
                "a",
                encoding="utf-8",
            ) as handle:
                handle.write(line + "\n")

        return record_hash

    def read_all(
        self,
    ) -> Iterable[Dict[str, Any]]:

        if not self.path.exists():
            return []

        records: List[Dict[str, Any]] = []

        with self.lock:
            with self.path.open(
                "r",
                encoding="utf-8",
            ) as handle:

                for line in handle:
                    line = line.strip()

                    if not line:
                        continue

                    try:
                        records.append(
                            json.loads(line)
                        )
                    except json.JSONDecodeError:
                        continue

        return records


class MarketDataHub:

    def __init__(
        self,
        store: Optional[RawObservationStore] = None,
    ):

        self.version = HUB_VERSION

        self.store = (
            store
            if store is not None
            else RawObservationStore()
        )

        self.lock = RLock()

        self.consumers: Dict[
            str,
            Callable[[RawMarketObservation], None],
        ] = {}

        self.received_count = 0
        self.persisted_count = 0
        self.rejected_count = 0
        self.consumer_error_count = 0

        self.last_received_at: Optional[datetime] = None

        self.last_error = ""

    # ---------------------------------------------------------
    # CONSUMER REGISTRATION
    # ---------------------------------------------------------

    def register_consumer(
        self,
        name: str,
        callback: Callable[
            [RawMarketObservation],
            None,
        ],
    ) -> None:

        name = str(name).strip()

        if not name:
            raise ValueError(
                "Consumer name cannot be empty"
            )

        if not callable(callback):
            raise TypeError(
                "Consumer callback must be callable"
            )

        with self.lock:
            self.consumers[name] = callback

    def unregister_consumer(
        self,
        name: str,
    ) -> None:

        with self.lock:
            self.consumers.pop(
                str(name),
                None,
            )

    # ---------------------------------------------------------
    # RAW INGESTION
    # ---------------------------------------------------------

    def ingest(
        self,
        observation: RawMarketObservation,
    ) -> bool:

        try:
            RawObservationValidator.validate(
                observation
            )

        except Exception as exc:

            with self.lock:
                self.rejected_count += 1
                self.last_error = str(exc)

            return False

        with self.lock:
            self.received_count += 1
            self.last_received_at = utc_now()

        try:
            self.store.append(
                observation
            )

            with self.lock:
                self.persisted_count += 1

        except Exception as exc:

            with self.lock:
                self.last_error = (
                    f"storage: {exc}"
                )

            return False

        self._fanout(
            observation
        )

        return True

    def ingest_mapping(
        self,
        mapping: Dict[str, Any],
        source: str,
        symbol: str,
        timestamp: Any,
        instrument_token: str = "",
        exchange_segment: str = "",
    ) -> bool:

        received_at = utc_now_iso()

        def num(
            *keys: str,
        ) -> Optional[float]:

            for key in keys:
                if key in mapping:
                    value = safe_float(
                        mapping.get(key)
                    )

                    if value is not None:
                        return value

            return None

        observation = RawMarketObservation(
            source=str(source),
            symbol=str(symbol),
            timestamp=normalize_timestamp(
                timestamp
            ),
            received_at=received_at,

            ltp=num(
                "ltp",
                "lp",
                "last_price",
                "last_traded_price",
                "lastPrice",
                "close",
            ),

            open=num("open", "o"),
            high=num("high", "h"),
            low=num("low", "l"),
            close=num("close", "c"),

            volume=num(
                "volume",
                "v",
                "vol",
            ),

            oi=num(
                "oi",
                "open_interest",
                "openInterest",
            ),

            instrument_token=str(
                instrument_token
            ),

            exchange_segment=str(
                exchange_segment
            ),

            raw=dict(mapping),
        )

        return self.ingest(
            observation
        )

    # ---------------------------------------------------------
    # FAN-OUT
    # ---------------------------------------------------------

    def _fanout(
        self,
        observation: RawMarketObservation,
    ) -> None:

        with self.lock:
            consumers = list(
                self.consumers.items()
            )

        for name, callback in consumers:

            try:
                callback(
                    observation
                )

            except Exception as exc:

                with self.lock:
                    self.consumer_error_count += 1
                    self.last_error = (
                        f"consumer={name}: {exc}"
                    )

    # ---------------------------------------------------------
    # REPLAY
    # ---------------------------------------------------------

    def replay(
        self,
        callback: Callable[
            [RawMarketObservation],
            None,
        ],
        limit: Optional[int] = None,
    ) -> int:

        if not callable(callback):
            raise TypeError(
                "Replay callback must be callable"
            )

        records = self.store.read_all()

        count = 0

        for record in records:

            if limit is not None:
                if count >= limit:
                    break

            payload = record.get(
                "observation"
            )

            if not isinstance(
                payload,
                dict,
            ):
                continue

            try:
                observation = (
                    RawMarketObservation(
                        **payload
                    )
                )

                callback(
                    observation
                )

                count += 1

            except Exception as exc:

                with self.lock:
                    self.last_error = (
                        f"replay: {exc}"
                    )

        return count

    # ---------------------------------------------------------
    # HEALTH
    # ---------------------------------------------------------

    def data_age_seconds(self) -> Optional[float]:

        with self.lock:

            if self.last_received_at is None:
                return None

            return max(
                0.0,
                (
                    utc_now()
                    - self.last_received_at
                ).total_seconds(),
            )

    def health(
        self,
        max_age_seconds: float = 30.0,
    ) -> Dict[str, Any]:

        age = self.data_age_seconds()

        if age is None:
            status = "NO_DATA"

        elif age <= max_age_seconds:
            status = "HEALTHY"

        else:
            status = "STALE"

        with self.lock:
            return {
                "hub_version": self.version,
                "status": status,
                "received_count": self.received_count,
                "persisted_count": self.persisted_count,
                "rejected_count": self.rejected_count,
                "consumer_error_count": (
                    self.consumer_error_count
                ),
                "consumer_count": len(
                    self.consumers
                ),
                "data_age_seconds": age,
                "last_error": self.last_error,
            }

    def stats(self) -> Dict[str, Any]:
        return self.health()


# =============================================================
# SELF TEST
# =============================================================

def market_data_hub_test() -> None:

    test_path = Path(
        "market_data/"
        "hub_self_test.jsonl"
    )

    if test_path.exists():
        test_path.unlink()

    store = RawObservationStore(
        str(test_path)
    )

    hub = MarketDataHub(
        store=store
    )

    received = []

    def consumer(
        observation: RawMarketObservation,
    ):
        received.append(
            observation
        )

    hub.register_consumer(
        "TEST_ENGINE",
        consumer,
    )

    timestamp = utc_now_iso()

    raw = {
        "ltp": 25000.0,
        "open": 24950.0,
        "high": 25050.0,
        "low": 24900.0,
        "close": 25000.0,
        "volume": 100000.0,
    }

    accepted = hub.ingest_mapping(
        mapping=raw,
        source="SELF_TEST",
        symbol="NIFTY",
        timestamp=timestamp,
        instrument_token="TEST",
        exchange_segment="TEST",
    )

    assert accepted is True
    assert hub.received_count == 1
    assert hub.persisted_count == 1
    assert len(received) == 1

    health = hub.health()

    assert health["status"] == "HEALTHY"

    replayed = []

    count = hub.replay(
        lambda obs: replayed.append(obs)
    )

    assert count == 1
    assert len(replayed) == 1
    assert replayed[0].symbol == "NIFTY"

    print(
        "MARKET DATA HUB TEST: PASS"
    )

    print(
        f"  received   : {hub.received_count}"
    )

    print(
        f"  persisted  : {hub.persisted_count}"
    )

    print(
        f"  consumers  : {len(hub.consumers)}"
    )

    print(
        f"  replayed   : {count}"
    )


if __name__ == "__main__":
    market_data_hub_test()
