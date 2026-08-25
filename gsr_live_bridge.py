"""
GSR-1.1.0 Ã¢â‚¬â€ Live Shadow Bridge
===============================

Purpose
-------
Controlled boundary between an EXTERNAL RAW MARKET SOURCE and the isolated
Global Strategy Research Engine (GSR).

ARCHITECTURAL CONTRACT
----------------------
1. This module does NOT fetch from Kotak Neo directly.
2. It accepts raw observations from a caller-owned source/producer.
3. It forwards raw observations through gsr_data_adapter.py.
4. Valid normalized observations are persisted first in gsr_data_store.py.
5. Only then are observations handed to gsr_engine.py.
6. It never imports app.py or next_day_alpha_engine.py.
7. It never consumes alpha, confidence, prediction, regime, signal, weights,
   position, decision, or any other opinion from another engine.
8. It never places orders and has no broker/order/execution API.
9. A source failure cannot silently become a market observation.
10. A store failure stops the live path by default; we never pretend data was
    safely archived when persistence failed.
11. Duplicate raw observations are recognized by the store's content hash.
12. The bridge is intentionally source-agnostic so the existing Kotak Neo
    integration can be connected later without changing GSR core logic.
13. Historical replay is explicit and cannot be confused with live mode.
14. Health, ingestion, rejection, duplicate, and engine-error counters are
    retained for audit.
15. The bridge does not continuously retune GSR. It only transports observations.

EXPECTED REPOSITORY LAYOUT
--------------------------
nifty-engine/
    app.py
    next_day_alpha_engine.py
    strategy_registry.py
    GSR_1.1.0_MASTER_STRATEGY_REGISTRY.txt
    gsr_engine.py
    gsr_data_adapter.py
    gsr_data_store.py
    gsr_live_bridge.py                  <-- this file

LIVE FLOW
---------
external raw producer
        |
        v
gsr_live_bridge.py
        |
        v
gsr_data_adapter.py
        |
        +---- reject invalid/opinion-contaminated observation
        |
        v
gsr_data_store.py
        |
        +---- durable raw observation
        |
        v
gsr_engine.py
        |
        v
GSR research records only

IMPORTANT
---------
This bridge deliberately does NOT contain Kotak Neo credentials, TOTP, MPIN,
broker methods, order methods, or API-specific field extraction.

The future Kotak integration should call:

    bridge.submit(raw_kotak_observation)

or:

    bridge.submit_many(iterable_of_raw_kotak_observations)

The raw observation may contain Kotak-specific fields only if the existing
GSRDataContract knows how to normalize them. Otherwise the observation is
rejected instead of guessed.

STANDARD LIBRARY ONLY
---------------------
No external package is required by this module itself.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional


# ============================================================================
# 0. VERSION / CONTRACT
# ============================================================================

BRIDGE_VERSION = "GSR-1.1.0-LIVE-BRIDGE"
BRIDGE_SCHEMA_VERSION = "GSR_LIVE_BRIDGE_1.1"

DEFAULT_HEARTBEAT_SECONDS = float(os.getenv("GSR_BRIDGE_HEARTBEAT_SEC", "30"))
DEFAULT_SOURCE_TIMEOUT_SECONDS = float(
    os.getenv("GSR_BRIDGE_SOURCE_TIMEOUT_SEC", "10")
)

# Fields that must never cross into GSR from another engine's opinion layer.
# Keep this list intentionally broad. The adapter/store perform their own
# independent checks as a second boundary.
FORBIDDEN_OPINION_FIELDS = frozenset(
    {
        "alpha",
        "alpha_score",
        "alpha_probability",
        "confidence",
        "confidence_score",
        "prediction",
        "predicted_direction",
        "predicted_return",
        "signal",
        "signal_score",
        "signal_type",
        "external_regime",
        "regime",
        "regime_label",
        "regime_score",
        "position",
        "position_size",
        "decision",
        "trade_decision",
        "entry_signal",
        "exit_signal",
        "model_score",
        "model_prediction",
        "engine_opinion",
        "recommendation",
        "weight",
        "weights",
    }
)


# ============================================================================
# 1. HELPERS
# ============================================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def monotonic() -> float:
    return time.monotonic()


def json_safe(value: Any) -> Any:
    """Return a JSON-safe representation for audit records."""
    try:
        json.dumps(value, ensure_ascii=False, default=str)
        return value
    except Exception:
        return str(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        if result != result or result in (float("inf"), float("-inf")):
            return default
        return result
    except (TypeError, ValueError):
        return default


# ============================================================================
# 2. CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class BridgeConfig:
    bridge_version: str = BRIDGE_VERSION
    schema_version: str = BRIDGE_SCHEMA_VERSION

    # Live shadow mode only. "historical_replay" must be requested explicitly.
    mode: str = "LIVE_SHADOW_RESEARCH"

    # Never silently feed duplicate observations into the engine.
    process_duplicate_store_rows: bool = False

    # A persistence failure is a hard failure by default.
    fail_closed_on_store_error: bool = True

    # Adapter chronology enforcement remains enabled by default.
    enforce_chronology: bool = True

    # Opinion fields are rejected before adapter/store.
    strict_opinion_boundary: bool = True

    # Source health.
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS
    source_timeout_seconds: float = DEFAULT_SOURCE_TIMEOUT_SECONDS

    # Audit log is separate from raw market storage.
    audit_path: str = os.getenv(
        "GSR_BRIDGE_AUDIT_PATH",
        "./gsr_data/gsr_live_bridge_audit.jsonl",
    )

    # Maximum audit payload length to prevent accidental huge log records.
    max_audit_payload_chars: int = 8000

    def validate(self) -> None:
        if self.mode not in {"LIVE_SHADOW_RESEARCH", "HISTORICAL_REPLAY"}:
            raise ValueError(f"Unsupported bridge mode: {self.mode}")

        if self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be > 0")

        if self.source_timeout_seconds <= 0:
            raise ValueError("source_timeout_seconds must be > 0")

        if self.max_audit_payload_chars < 1000:
            raise ValueError("max_audit_payload_chars is too small")


# ============================================================================
# 3. BRIDGE METRICS
# ============================================================================

@dataclass
class BridgeMetrics:
    started_at: str = field(default_factory=utc_now)

    received: int = 0
    accepted: int = 0
    rejected: int = 0
    duplicates: int = 0

    adapter_errors: int = 0
    store_errors: int = 0
    engine_errors: int = 0
    boundary_violations: int = 0

    empty_source_polls: int = 0
    source_errors: int = 0

    last_received_at: Optional[str] = None
    last_accepted_at: Optional[str] = None
    last_duplicate_at: Optional[str] = None
    last_rejected_at: Optional[str] = None
    last_engine_success_at: Optional[str] = None
    last_error_at: Optional[str] = None

    last_symbol: Optional[str] = None
    last_timestamp: Optional[str] = None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "bridge_version": BRIDGE_VERSION,
            "schema_version": BRIDGE_SCHEMA_VERSION,
            **self.__dict__,
        }


# ============================================================================
# 4. AUDIT LOGGER
# ============================================================================

class BridgeAudit:
    """
    Small append-only JSONL audit stream.

    This is NOT the raw market store.
    It records bridge events such as accepted/rejected/duplicate/error/heartbeat.
    """

    def __init__(self, path: str | Path, max_payload_chars: int = 8000) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_payload_chars = int(max_payload_chars)
        self._lock = threading.Lock()

    def write(self, event: str, **payload: Any) -> None:
        record: Dict[str, Any] = {
            "timestamp": utc_now(),
            "bridge_version": BRIDGE_VERSION,
            "schema_version": BRIDGE_SCHEMA_VERSION,
            "event": event,
        }

        for key, value in payload.items():
            record[key] = json_safe(value)

        encoded = canonical_json(record)
        if len(encoded) > self.max_payload_chars:
            record["payload_truncated"] = True
            encoded = canonical_json(record)[: self.max_payload_chars]

        with self._lock:
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(encoded + "\n")


# ============================================================================
# 5. SOURCE ABSTRACTION
# ============================================================================

class RawSource:
    """
    Optional source protocol-like base class.

    A live source can implement:
        start()
        stop()
        poll()

    poll() may return:
        - None
        - one mapping
        - an iterable of mappings

    This module does not know whether the source is Kotak, file, websocket,
    yfinance, a test fixture, or another approved raw feed.
    """

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    def poll(self) -> Any:
        raise NotImplementedError


class CallableSource(RawSource):
    """Wrap a simple zero-argument callable as a source."""

    def __init__(self, fn: Callable[[], Any]) -> None:
        if not callable(fn):
            raise TypeError("fn must be callable")
        self.fn = fn

    def poll(self) -> Any:
        return self.fn()


# ============================================================================
# 6. RESULT CONTRACT
# ============================================================================

@dataclass(frozen=True)
class BridgeResult:
    status: str
    timestamp: str
    symbol: Optional[str] = None
    observation_timestamp: Optional[str] = None
    store_status: Optional[str] = None
    engine_status: Optional[str] = None
    reason: Optional[str] = None
    error_type: Optional[str] = None
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def to_mapping(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "observation_timestamp": self.observation_timestamp,
            "store_status": self.store_status,
            "engine_status": self.engine_status,
            "reason": self.reason,
            "error_type": self.error_type,
            "event_id": self.event_id,
        }


# ============================================================================
# 7. GSR LIVE BRIDGE
# ============================================================================

class GSRLiveBridge:
    """
    Controlled transport boundary:

        raw source -> adapter -> durable store -> GSR engine

    The bridge expects dependency injection so it remains isolated from:
        - Kotak Neo
        - app.py
        - next_day_alpha_engine.py
        - broker/order APIs
    """

    def __init__(
        self,
        engine: Any,
        adapter: Any,
        store: Any,
        *,
        config: Optional[BridgeConfig] = None,
        audit: Optional[BridgeAudit] = None,
    ) -> None:
        self.config = config or BridgeConfig()
        self.config.validate()

        if engine is None:
            raise TypeError("engine is required")

        if adapter is None:
            raise TypeError("adapter is required")

        if store is None:
            raise TypeError("store is required")

        if not hasattr(engine, "ingest_snapshot"):
            raise TypeError("engine must expose ingest_snapshot(raw_mapping)")

        if not hasattr(adapter, "normalize"):
            raise TypeError("adapter must expose normalize(raw_mapping)")

        if not hasattr(store, "append"):
            raise TypeError("store must expose append(observation)")

        self.engine = engine
        self.adapter = adapter
        self.store = store

        self.audit = audit or BridgeAudit(
            self.config.audit_path,
            self.config.max_audit_payload_chars,
        )

        self.metrics = BridgeMetrics()
        self._running = False
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._last_source_activity = monotonic()

        self.audit.write(
            "BRIDGE_INITIALIZED",
            mode=self.config.mode,
            strict_opinion_boundary=self.config.strict_opinion_boundary,
            chronology_enforced=self.config.enforce_chronology,
        )

    # ---------------------------------------------------------------------
    # Boundary checks
    # ---------------------------------------------------------------------

    @staticmethod
    def _find_forbidden_fields(raw: Mapping[str, Any]) -> List[str]:
        return sorted(
            str(key)
            for key in raw.keys()
            if str(key).strip().lower() in FORBIDDEN_OPINION_FIELDS
        )

    def _assert_raw_mapping(self, raw: Mapping[str, Any]) -> None:
        if not isinstance(raw, Mapping):
            raise TypeError(
                f"GSR bridge accepts mappings only, got {type(raw).__name__}"
            )

        if not raw:
            raise ValueError("Empty raw observation")

        if self.config.strict_opinion_boundary:
            forbidden = self._find_forbidden_fields(raw)
            if forbidden:
                self.metrics.boundary_violations += 1
                raise ValueError(
                    "GSR isolation violation: forbidden opinion fields supplied: "
                    + ", ".join(forbidden)
                )

    # ---------------------------------------------------------------------
    # Adapter chronology
    # ---------------------------------------------------------------------

    def _normalize(self, raw: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Normalize using the existing GSRDataAdapter.

        Chronology is checked through the adapter's public ingest contract when
        available. We deliberately do not bypass the adapter with homemade
        field mappings.
        """
        self._assert_raw_mapping(raw)

        try:
            observation = self.adapter.normalize(raw)
        except Exception:
            self.metrics.adapter_errors += 1
            raise

        # Existing GSRDataAdapter exposes ChronologyGuard internally.
        # We use it when available so direct bridge submission cannot bypass
        # the adapter's chronological contract.
        if self.config.enforce_chronology:
            chronology = getattr(self.adapter, "chronology", None)
            check = getattr(chronology, "check", None)
            if callable(check):
                try:
                    check(observation)
                except Exception:
                    self.metrics.adapter_errors += 1
                    raise

        if hasattr(observation, "to_mapping"):
            normalized = observation.to_mapping()
        elif isinstance(observation, Mapping):
            normalized = dict(observation)
        else:
            raise TypeError(
                "adapter.normalize() must return a mapping or an object "
                "with to_mapping()"
            )

        # Defensive second check after normalization.
        self._assert_raw_mapping(normalized)
        return dict(normalized)

    # ---------------------------------------------------------------------
    # Persistence
    # ---------------------------------------------------------------------

    def _persist(self, normalized: Mapping[str, Any]) -> Any:
        try:
            return self.store.append(
                normalized,
                source=str(normalized.get("source") or "GSR_LIVE_BRIDGE"),
            )
        except TypeError:
            # Compatibility with stores whose append() accepts only the
            # observation mapping.
            try:
                return self.store.append(normalized)
            except Exception:
                self.metrics.store_errors += 1
                raise
        except Exception:
            self.metrics.store_errors += 1
            raise

    @staticmethod
    def _store_status(store_result: Any) -> str:
        if store_result is None:
            return "unknown"

        status = getattr(store_result, "status", None)
        if status is not None:
            return str(status)

        if isinstance(store_result, Mapping):
            return str(store_result.get("status", "unknown"))

        return "unknown"

    # ---------------------------------------------------------------------
    # Engine handoff
    # ---------------------------------------------------------------------

    def _engine_ingest(self, normalized: Mapping[str, Any]) -> Any:
        try:
            return self.engine.ingest_snapshot(dict(normalized))
        except Exception:
            self.metrics.engine_errors += 1
            raise

    # ---------------------------------------------------------------------
    # Main submission API
    # ---------------------------------------------------------------------

    def submit(self, raw: Mapping[str, Any]) -> BridgeResult:
        """
        Submit ONE raw observation.

        Ordering is intentional:
            1. boundary check
            2. adapter normalization
            3. durable store
            4. engine ingest

        If persistence fails, engine ingest does not occur.
        """
        received_at = utc_now()
        self.metrics.received += 1
        self.metrics.last_received_at = received_at
        self._last_source_activity = monotonic()

        symbol = None
        observation_timestamp = None

        try:
            if isinstance(raw, Mapping):
                symbol = raw.get("symbol")
                observation_timestamp = raw.get("timestamp")

            normalized = self._normalize(raw)

            symbol = str(normalized.get("symbol") or symbol or "") or None
            observation_timestamp = (
                normalized.get("timestamp") or observation_timestamp
            )

            store_result = self._persist(normalized)
            store_status = self._store_status(store_result)

            # A duplicate means the exact raw observation already exists.
            # Default behavior: do not run the same observation through GSR
            # twice.
            if store_status.lower() in {"duplicate", "already_exists"}:
                self.metrics.duplicates += 1
                self.metrics.last_duplicate_at = utc_now()

                if not self.config.process_duplicate_store_rows:
                    self.audit.write(
                        "OBSERVATION_DUPLICATE",
                        symbol=symbol,
                        observation_timestamp=observation_timestamp,
                        store_status=store_status,
                    )
                    return BridgeResult(
                        status="duplicate",
                        timestamp=utc_now(),
                        symbol=symbol,
                        observation_timestamp=observation_timestamp,
                        store_status=store_status,
                        engine_status="not_processed",
                        reason="raw observation already persisted",
                    )

            # Store result is intentionally checked before engine handoff.
            try:
                engine_result = self._engine_ingest(normalized)
            except Exception as exc:
                self.metrics.last_error_at = utc_now()
                self.audit.write(
                    "ENGINE_INGEST_ERROR",
                    symbol=symbol,
                    observation_timestamp=observation_timestamp,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                raise

            self.metrics.accepted += 1
            self.metrics.last_accepted_at = utc_now()
            self.metrics.last_engine_success_at = utc_now()
            self.metrics.last_symbol = symbol
            self.metrics.last_timestamp = observation_timestamp

            self.audit.write(
                "OBSERVATION_ACCEPTED",
                symbol=symbol,
                observation_timestamp=observation_timestamp,
                store_status=store_status,
                engine_status="accepted",
            )

            return BridgeResult(
                status="accepted",
                timestamp=utc_now(),
                symbol=symbol,
                observation_timestamp=observation_timestamp,
                store_status=store_status,
                engine_status="accepted",
            )

        except Exception as exc:
            self.metrics.rejected += 1
            self.metrics.last_rejected_at = utc_now()
            self.metrics.last_error_at = utc_now()

            event = (
                "BOUNDARY_REJECTION"
                if isinstance(exc, (TypeError, ValueError))
                else "OBSERVATION_ERROR"
            )

            self.audit.write(
                event,
                symbol=symbol,
                observation_timestamp=observation_timestamp,
                error_type=type(exc).__name__,
                error=str(exc),
            )

            return BridgeResult(
                status="rejected",
                timestamp=utc_now(),
                symbol=symbol,
                observation_timestamp=observation_timestamp,
                reason=str(exc),
                error_type=type(exc).__name__,
            )

    def submit_many(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> Iterator[BridgeResult]:
        """
        Submit rows in source order.

        stop_on_error=False is useful for historical ingestion because one bad
        record should be auditable without hiding subsequent records.
        """
        for row in rows:
            result = self.submit(row)
            yield result

            if stop_on_error and result.status == "rejected":
                raise RuntimeError(
                    f"GSR bridge stopped on rejected observation: {result.reason}"
                )

    # ---------------------------------------------------------------------
    # Source polling
    # ---------------------------------------------------------------------

    @staticmethod
    def _coerce_source_output(value: Any) -> List[Mapping[str, Any]]:
        if value is None:
            return []

        if isinstance(value, Mapping):
            return [value]

        if isinstance(value, (str, bytes, bytearray)):
            raise TypeError("Source returned text/bytes instead of mapping(s)")

        try:
            rows = list(value)
        except TypeError as exc:
            raise TypeError(
                "Source poll must return a mapping, iterable of mappings, or None"
            ) from exc

        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError(
                    "Source iterable contains a non-mapping observation: "
                    + type(row).__name__
                )

        return rows

    def poll_once(self, source: RawSource | Callable[[], Any]) -> List[BridgeResult]:
        """
        Perform one source poll.

        This method never catches a source error as a valid observation.
        """
        if callable(source) and not hasattr(source, "poll"):
            source = CallableSource(source)

        if not hasattr(source, "poll"):
            raise TypeError("source must expose poll() or be callable")

        try:
            output = source.poll()
        except Exception as exc:
            self.metrics.source_errors += 1
            self.metrics.last_error_at = utc_now()
            self.audit.write(
                "SOURCE_POLL_ERROR",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise

        rows = self._coerce_source_output(output)

        if not rows:
            self.metrics.empty_source_polls += 1
            return []

        return list(self.submit_many(rows))

    # ---------------------------------------------------------------------
    # Long-running loop
    # ---------------------------------------------------------------------

    def run(
        self,
        source: RawSource | Callable[[], Any],
        *,
        poll_interval_seconds: float = 1.0,
        stop_event: Optional[threading.Event] = None,
        max_iterations: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Long-running LIVE_SHADOW_RESEARCH loop.

        This is intentionally a transport loop, not a trading loop.
        """
        if self.config.mode != "LIVE_SHADOW_RESEARCH":
            raise RuntimeError(
                "run() is reserved for LIVE_SHADOW_RESEARCH. "
                "Use replay() for historical data."
            )

        if poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be >= 0")

        external_stop = stop_event
        own_stop = self._stop_event

        self._running = True
        iterations = 0

        self.audit.write(
            "LIVE_LOOP_STARTED",
            poll_interval_seconds=poll_interval_seconds,
        )

        try:
            if hasattr(source, "start"):
                source.start()

            while not own_stop.is_set():
                if external_stop is not None and external_stop.is_set():
                    break

                if max_iterations is not None and iterations >= max_iterations:
                    break

                iterations += 1

                try:
                    self.poll_once(source)
                except Exception as exc:
                    # Source failures are observable. The loop remains alive
                    # unless the caller configured a source that stops itself.
                    self.audit.write(
                        "LIVE_LOOP_ITERATION_ERROR",
                        iteration=iterations,
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )

                self._maybe_heartbeat()

                if poll_interval_seconds > 0:
                    own_stop.wait(poll_interval_seconds)

        finally:
            try:
                if hasattr(source, "stop"):
                    source.stop()
            finally:
                self._running = False
                self.audit.write(
                    "LIVE_LOOP_STOPPED",
                    iterations=iterations,
                    metrics=self.metrics.snapshot(),
                )

        return {
            "status": "stopped",
            "iterations": iterations,
            "metrics": self.metrics.snapshot(),
        }

    def stop(self) -> None:
        self._stop_event.set()
        self.audit.write("STOP_REQUESTED")

    # ---------------------------------------------------------------------
    # Historical replay
    # ---------------------------------------------------------------------

    def replay(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> Dict[str, Any]:
        """
        Explicit historical replay path.

        It is separated from live mode so a historical file cannot silently be
        treated as live feed data.
        """
        # Historical replay is an explicit mode boundary.  A live-shadow
        # bridge must never be allowed to consume a historical iterable through
        # this method, even if the caller accidentally invokes replay().
        if self.config.mode != "HISTORICAL_REPLAY":
            raise RuntimeError(
                "replay() is reserved for HISTORICAL_REPLAY. "
                "Live shadow mode must use run()/poll_once() with a live source."
            )

        results = {
            "mode": "HISTORICAL_REPLAY",
            "received": 0,
            "accepted": 0,
            "duplicates": 0,
            "rejected": 0,
        }

        self.audit.write("HISTORICAL_REPLAY_STARTED")

        for result in self.submit_many(rows, stop_on_error=stop_on_error):
            results["received"] += 1

            if result.status == "accepted":
                results["accepted"] += 1
            elif result.status == "duplicate":
                results["duplicates"] += 1
            elif result.status == "rejected":
                results["rejected"] += 1

        self.audit.write(
            "HISTORICAL_REPLAY_FINISHED",
            replay_results=results,
        )
        return results

    # ---------------------------------------------------------------------
    # Health
    # ---------------------------------------------------------------------

    def source_silence_seconds(self) -> float:
        return max(0.0, monotonic() - self._last_source_activity)

    def health(self) -> Dict[str, Any]:
        silence = self.source_silence_seconds()

        if not self._running:
            status = "STOPPED"
        elif silence > self.config.source_timeout_seconds:
            status = "SOURCE_SILENT"
        elif self.metrics.store_errors > 0 and self.metrics.accepted == 0:
            status = "STORE_ERROR"
        elif self.metrics.engine_errors > 0:
            status = "ENGINE_ERROR"
        else:
            status = "HEALTHY"

        return {
            "status": status,
            "running": self._running,
            "mode": self.config.mode,
            "source_silence_seconds": round(silence, 3),
            "source_timeout_seconds": self.config.source_timeout_seconds,
            "metrics": self.metrics.snapshot(),
        }

    def _maybe_heartbeat(self) -> None:
        now = monotonic()
        last = getattr(self, "_last_heartbeat", 0.0)

        if now - last < self.config.heartbeat_seconds:
            return

        self._last_heartbeat = now
        self.audit.write(
            "HEARTBEAT",
            health=self.health(),
        )

    # ---------------------------------------------------------------------
    # Status / lifecycle
    # ---------------------------------------------------------------------

    @property
    def running(self) -> bool:
        return self._running

    def status(self) -> Dict[str, Any]:
        return {
            "bridge_version": BRIDGE_VERSION,
            "schema_version": BRIDGE_SCHEMA_VERSION,
            "config": {
                "mode": self.config.mode,
                "process_duplicate_store_rows": (
                    self.config.process_duplicate_store_rows
                ),
                "fail_closed_on_store_error": (
                    self.config.fail_closed_on_store_error
                ),
                "enforce_chronology": self.config.enforce_chronology,
                "strict_opinion_boundary": self.config.strict_opinion_boundary,
            },
            "health": self.health(),
        }

    def close(self) -> None:
        """
        Close only bridge-owned resources.

        Engine/store lifecycle remains caller-owned because the bridge does not
        know whether they are shared with another research process.
        """
        self.stop()
        self.audit.write("BRIDGE_CLOSED")


# ============================================================================
# 8. FACTORY
# ============================================================================

def build_gsr_live_bridge(
    *,
    engine: Optional[Any] = None,
    adapter: Optional[Any] = None,
    store: Optional[Any] = None,
    config: Optional[BridgeConfig] = None,
) -> GSRLiveBridge:
    """
    Build the bridge with existing GSR components.

    Imports are deferred to keep module import lightweight and to avoid
    accidental circular imports.

    No broker package is imported here.
    """
    if engine is None:
        from gsr_engine import GSREngine

        engine = GSREngine()

    if adapter is None:
        from gsr_data_adapter import GSRDataAdapter

        adapter = GSRDataAdapter(
            engine,
            strict_opinion_rejection=True,
            enforce_chronology=True,
        )

    if store is None:
        from gsr_data_store import open_default_store

        store = open_default_store()

    return GSRLiveBridge(
        engine=engine,
        adapter=adapter,
        store=store,
        config=config,
    )


# ============================================================================
# 9. FILE-BASED SOURCE
# ============================================================================

class JSONLSource(RawSource):
    """
    Simple historical/live-file source.

    This is intentionally read-only and preserves file order.
    It is mainly useful for replay/testing.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._fh = None

    def start(self) -> None:
        self._fh = self.path.open("r", encoding="utf-8")

    def stop(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def poll(self) -> Optional[Dict[str, Any]]:
        if self._fh is None:
            self.start()

        while True:
            line = self._fh.readline()
            if not line:
                return None

            text = line.strip()
            if not text:
                continue

            try:
                value = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSONL in {self.path}: {exc}"
                ) from exc

            if not isinstance(value, dict):
                raise ValueError("JSONL source record must be an object")

            return value


# ============================================================================
# 10. TEST / MOCK COMPONENTS
# ============================================================================

class _FakeStoreResult:
    def __init__(self, status: str) -> None:
        self.status = status


class _FakeStore:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []
        self.hashes: set[str] = set()

    def append(self, observation: Mapping[str, Any], **_: Any) -> _FakeStoreResult:
        import hashlib

        key = hashlib.sha256(
            canonical_json(dict(observation)).encode("utf-8")
        ).hexdigest()

        if key in self.hashes:
            return _FakeStoreResult("duplicate")

        self.hashes.add(key)
        self.rows.append(dict(observation))
        return _FakeStoreResult("inserted")


class _FakeAdapterObservation:
    def __init__(self, mapping: Mapping[str, Any]) -> None:
        self.mapping = dict(mapping)

    def to_mapping(self) -> Dict[str, Any]:
        return dict(self.mapping)


class _FakeAdapter:
    class _Chronology:
        def check(self, _: Any) -> None:
            return None

    def __init__(self) -> None:
        self.chronology = self._Chronology()

    def normalize(self, raw: Mapping[str, Any]) -> _FakeAdapterObservation:
        required = ("timestamp", "symbol", "open", "high", "low", "close")
        missing = [x for x in required if x not in raw]
        if missing:
            raise ValueError("missing fields: " + ", ".join(missing))
        return _FakeAdapterObservation(dict(raw))


class _FakeEngine:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []

    def ingest_snapshot(self, raw_mapping: Mapping[str, Any]) -> Dict[str, Any]:
        self.rows.append(dict(raw_mapping))
        return {"status": "accepted"}


def _self_test() -> None:
    """
    No network, broker credentials, or production data required.
    """
    with tempfile_directory() as tmp:
        audit = BridgeAudit(Path(tmp) / "audit.jsonl")
        engine = _FakeEngine()
        adapter = _FakeAdapter()
        store = _FakeStore()

        bridge = GSRLiveBridge(
            engine,
            adapter,
            store,
            config=BridgeConfig(
                audit_path=str(Path(tmp) / "audit.jsonl"),
                heartbeat_seconds=1.0,
                source_timeout_seconds=5.0,
            ),
            audit=audit,
        )

        base = {
            "timestamp": "2026-01-02T09:15:00+00:00",
            "symbol": "NIFTY",
            "open": 25000.0,
            "high": 25020.0,
            "low": 24990.0,
            "close": 25010.0,
            "volume": 1000.0,
            "spot_close": 25010.0,
            "futures_close": 25018.0,
            "instrument_type": "index_spot",
        }

        first = bridge.submit(base)
        assert first.status == "accepted"
        assert len(store.rows) == 1
        assert len(engine.rows) == 1

        duplicate = bridge.submit(base)
        assert duplicate.status == "duplicate"
        assert len(engine.rows) == 1

        bad = dict(base)
        bad["confidence"] = 0.99
        rejected = bridge.submit(bad)
        assert rejected.status == "rejected"
        assert bridge.metrics.boundary_violations == 1

        health = bridge.health()
        assert health["status"] in {"STOPPED", "HEALTHY"}

        bridge.close()


class tempfile_directory:
    """
    Tiny context manager wrapper to avoid importing tempfile at module load.
    """

    def __enter__(self) -> str:
        import tempfile

        self._ctx = tempfile.TemporaryDirectory(prefix="gsr_bridge_test_")
        return self._ctx.__enter__()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return self._ctx.__exit__(exc_type, exc, tb)


# ============================================================================
# 11. SAFE SIGNAL HANDLING
# ============================================================================

def install_stop_signals(bridge: GSRLiveBridge) -> None:
    """
    Install SIGINT/SIGTERM handlers for a foreground research process.

    This function must be called by the top-level process only.
    """

    def _handler(signum: int, _: Any) -> None:
        bridge.audit.write("PROCESS_STOP_SIGNAL", signal=signum)
        bridge.stop()

    try:
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, RuntimeError):
        # Signal handlers can only be installed safely in the main thread.
        bridge.audit.write(
            "SIGNAL_HANDLER_NOT_INSTALLED",
            reason="not_main_thread_or_signal_unavailable",
        )


# ============================================================================
# 12. OPTIONAL CLI SELF-TEST
# ============================================================================

def main() -> int:
    """
    Only runs the local contract self-test.

    It does NOT connect to Kotak Neo.
    It does NOT start a live feed.
    It does NOT place trades.
    """
    _self_test()
    print("GSR live bridge self-test: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
