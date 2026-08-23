"""
GSR-1.1.0 â€” Historical Raw Data Store
=====================================

Purpose
-------
Durable, append-only storage boundary for the Global Strategy Research Engine.

This module stores RAW / NORMALIZED MARKET OBSERVATIONS only. It does not:
- calculate alpha,
- calculate strategy scores,
- calculate confidence,
- calculate a trading signal,
- consume another engine's regime/opinion,
- modify an existing observation,
- fetch data from Kotak Neo or any broker.

The store is intentionally independent from:
    app.py
    next_day_alpha_engine.py
    gsr_engine.py

The expected flow is:

    external/raw feed
          |
          v
    gsr_data_adapter.py
          |
          v
    gsr_data_store.py
          |
          +---- immutable raw observations
          |
          +---- audit / ingestion metadata
          |
          v
    gsr_engine.py

Why SQLite?
-----------
For a one-year accumulation phase, SQLite gives us:
- durable local storage,
- transactional writes,
- indexed timestamp queries,
- duplicate protection,
- easy backups,
- no external database server,
- standard Python library only.

The raw observations are append-only. A duplicate insert is ignored when its
content hash is already present. Existing raw rows are never updated.

IMPORTANT
---------
This file does not create synthetic market history. The included self-test
uses synthetic observations only for testing the storage mechanics.

Expected repository layout
--------------------------
nifty-engine/
    app.py
    next_day_alpha_engine.py
    strategy_registry.py
    GSR_1.1.0_MASTER_STRATEGY_REGISTRY.txt
    gsr_engine.py
    gsr_data_adapter.py
    gsr_data_store.py                 <-- this file
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple


STORE_VERSION = "GSR-1.1.0-RAW-STORE"
SCHEMA_VERSION = "GSR_RAW_STORE_1.1"

DEFAULT_DB_PATH = os.getenv(
    "GSR_DATA_STORE_PATH",
    "./gsr_data/gsr_raw_market.sqlite3",
)

STREAMS = frozenset({
    "index_spot",
    "index_future",
    "equity",
    "option",
    "option_chain",
    "unknown",
})

REQUIRED_FIELDS = (
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
)

FORBIDDEN_OPINION_FIELDS = frozenset({
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
})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def content_hash(observation: Mapping[str, Any]) -> str:
    """
    Hash the complete canonical observation.

    The hash is deliberately based on the normalized observation, not on a
    database row id. Therefore the same observation produces the same hash.
    """
    payload = canonical_json(dict(observation)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _parse_timestamp(value: Any) -> str:
    if value is None or value == "":
        raise ValueError("timestamp is required")

    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        x = float(value)
        if x > 10_000_000_000:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc).isoformat()

    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.isoformat()


def _timestamp_epoch(value: str) -> float:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def infer_stream(observation: Mapping[str, Any]) -> str:
    """
    Infer only a storage stream, never a market regime.

    Explicit instrument_type/asset_class wins. Unknown values remain unknown.
    """
    instrument = str(
        observation.get("instrument_type")
        or observation.get("asset_class")
        or ""
    ).strip().lower()

    option_type = str(observation.get("option_type") or "").strip().upper()

    if "option" in instrument or option_type in {"CE", "PE", "CALL", "PUT"}:
        return "option"

    if "future" in instrument or "futures" in instrument:
        return "index_future"

    if "equity" in instrument or "stock" in instrument:
        return "equity"

    symbol = str(observation.get("symbol") or "").upper()
    if symbol in {"NIFTY", "NIFTY50", "NIFTY 50"}:
        return "index_spot"

    return "unknown"


def validate_observation(
    observation: Mapping[str, Any],
    *,
    require_ohlc: bool = True,
) -> List[str]:
    errors: List[str] = []

    if not isinstance(observation, Mapping):
        return ["observation_not_mapping"]

    for key in observation:
        if str(key).strip().lower() in FORBIDDEN_OPINION_FIELDS:
            errors.append(f"forbidden_opinion_field:{key}")

    for key in ("timestamp", "symbol"):
        if not observation.get(key):
            errors.append(f"missing_{key}")

    if observation.get("timestamp"):
        try:
            _parse_timestamp(observation["timestamp"])
        except Exception:
            errors.append("invalid_timestamp")

    if require_ohlc:
        for key in REQUIRED_FIELDS[2:]:
            value = observation.get(key)
            if value is None or not _finite(value):
                errors.append(f"invalid_{key}")

        try:
            o = float(observation["open"])
            h = float(observation["high"])
            l = float(observation["low"])
            c = float(observation["close"])

            if h < l:
                errors.append("high_below_low")
            if o < l or o > h:
                errors.append("open_outside_range")
            if c < l or c > h:
                errors.append("close_outside_range")
        except Exception:
            pass

    for key in ("volume", "oi"):
        if observation.get(key) is not None:
            try:
                if float(observation[key]) < 0:
                    errors.append(f"negative_{key}")
            except Exception:
                errors.append(f"invalid_{key}")

    bid = observation.get("bid")
    ask = observation.get("ask")
    if bid is not None and ask is not None:
        try:
            if float(ask) < float(bid):
                errors.append("ask_below_bid")
        except Exception:
            errors.append("invalid_bid_ask")

    return errors


@dataclass(frozen=True)
class StoreResult:
    status: str
    row_id: Optional[int]
    content_hash: str
    stream: str
    symbol: str
    timestamp: str
    error: Optional[str] = None


class GSRRawDataStore:
    """
    Append-only SQLite store for GSR raw observations.

    One instance should normally be used by the process responsible for
    ingestion. SQLite can safely read concurrently; writes remain transactional.
    """

    def __init__(
        self,
        db_path: str | Path = DEFAULT_DB_PATH,
        *,
        create: bool = True,
    ) -> None:
        self.db_path = Path(db_path)

        if str(self.db_path) != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row

        self._configure()
        if create:
            self._create_schema()

    def _configure(self) -> None:
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = FULL")
        self._conn.execute("PRAGMA temp_store = MEMORY")

    def _create_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS store_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_hash TEXT NOT NULL UNIQUE,
                contract_version TEXT NOT NULL,
                stream TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                timestamp_epoch REAL NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT,
                market TEXT,
                instrument_type TEXT,
                asset_class TEXT,
                timeframe TEXT,
                session TEXT,

                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL,
                oi REAL,
                bid REAL,
                ask REAL,
                mid REAL,

                futures_close REAL,
                spot_close REAL,

                iv REAL,
                atm_iv REAL,
                iv_change REAL,
                iv_rank REAL,
                iv_percentile REAL,
                iv_skew REAL,
                iv_term_structure REAL,
                realized_vol REAL,
                iv_rv_spread REAL,

                pcr_oi REAL,
                pcr_volume REAL,
                ce_oi REAL,
                pe_oi REAL,
                ce_oi_change REAL,
                pe_oi_change REAL,
                atm_straddle REAL,
                chain_completeness REAL,

                delta REAL,
                gamma REAL,
                theta REAL,
                vega REAL,
                vanna REAL,
                charm REAL,
                dte REAL,
                strike REAL,
                option_type TEXT,
                moneyness REAL,
                expiry TEXT,

                metadata_json TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_raw_symbol_time
                ON raw_observations(symbol, timestamp_epoch);

            CREATE INDEX IF NOT EXISTS idx_raw_stream_time
                ON raw_observations(stream, timestamp_epoch);

            CREATE INDEX IF NOT EXISTS idx_raw_time
                ON raw_observations(timestamp_epoch);

            CREATE INDEX IF NOT EXISTS idx_raw_expiry
                ON raw_observations(expiry);

            CREATE TABLE IF NOT EXISTS rejected_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ingestion_batches (
                batch_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                source TEXT,
                input_count INTEGER NOT NULL DEFAULT 0,
                accepted_count INTEGER NOT NULL DEFAULT 0,
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL
            );
            """
        )

        meta = {
            "store_version": STORE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "created_or_verified_at": utc_now(),
            "append_only": "true",
        }

        for key, value in meta.items():
            self._conn.execute(
                """
                INSERT INTO store_meta(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                (key, value),
            )

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()

    def __enter__(self) -> "GSRRawDataStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _prepare(self, observation: Mapping[str, Any]) -> Dict[str, Any]:
        errors = validate_observation(observation)
        if errors:
            raise ValueError("Invalid raw observation: " + ", ".join(errors))

        data = dict(observation)
        data["timestamp"] = _parse_timestamp(data["timestamp"])
        data["symbol"] = str(data["symbol"]).strip()
        data["stream"] = (
            str(data.get("stream") or infer_stream(data)).strip().lower()
        )

        if data["stream"] not in STREAMS:
            data["stream"] = "unknown"

        data["content_hash"] = content_hash(data)
        data["contract_version"] = str(
            data.get("data_contract_version")
            or data.get("contract_version")
            or SCHEMA_VERSION
        )
        data["metadata"] = dict(data.get("metadata") or {})
        data["ingested_at"] = utc_now()

        return data

    def _insert_sql(self) -> str:
        return """
            INSERT INTO raw_observations (
                content_hash,
                contract_version,
                stream,
                timestamp,
                timestamp_epoch,
                symbol,
                exchange,
                market,
                instrument_type,
                asset_class,
                timeframe,
                session,

                open,
                high,
                low,
                close,
                volume,
                oi,
                bid,
                ask,
                mid,

                futures_close,
                spot_close,

                iv,
                atm_iv,
                iv_change,
                iv_rank,
                iv_percentile,
                iv_skew,
                iv_term_structure,
                realized_vol,
                iv_rv_spread,

                pcr_oi,
                pcr_volume,
                ce_oi,
                pe_oi,
                ce_oi_change,
                pe_oi_change,
                atm_straddle,
                chain_completeness,

                delta,
                gamma,
                theta,
                vega,
                vanna,
                charm,
                dte,
                strike,
                option_type,
                moneyness,
                expiry,

                metadata_json,
                ingested_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?
            )
        """

    def append(
        self,
        observation: Mapping[str, Any],
        *,
        reject_invalid: bool = True,
    ) -> StoreResult:
        """
        Append exactly one normalized raw observation.

        Returns:
            inserted   -> new immutable row
            duplicate  -> same content already exists
            rejected   -> invalid and reject_invalid=False

        Invalid input is never silently inserted.
        """
        try:
            data = self._prepare(observation)
        except Exception as exc:
            if reject_invalid:
                raise
            digest = content_hash(
                {"rejected_payload": dict(observation)}
            )
            self._record_rejection(str(exc), observation)
            return StoreResult(
                status="rejected",
                row_id=None,
                content_hash=digest,
                stream="unknown",
                symbol=str(observation.get("symbol") or ""),
                timestamp=str(observation.get("timestamp") or ""),
                error=str(exc),
            )

        params = self._params(data)

        try:
            cursor = self._conn.execute(self._insert_sql(), params)
        except sqlite3.IntegrityError as exc:
            # UNIQUE(content_hash) is the intended duplicate guard.
            if "content_hash" not in str(exc).lower():
                raise

            existing = self._conn.execute(
                """
                SELECT id
                FROM raw_observations
                WHERE content_hash = ?
                """,
                (data["content_hash"],),
            ).fetchone()

            return StoreResult(
                status="duplicate",
                row_id=int(existing["id"]) if existing else None,
                content_hash=data["content_hash"],
                stream=data["stream"],
                symbol=data["symbol"],
                timestamp=data["timestamp"],
            )

        return StoreResult(
            status="inserted",
            row_id=int(cursor.lastrowid),
            content_hash=data["content_hash"],
            stream=data["stream"],
            symbol=data["symbol"],
            timestamp=data["timestamp"],
        )

    def append_many(
        self,
        observations: Iterable[Mapping[str, Any]],
        *,
        batch_id: Optional[str] = None,
        source: Optional[str] = None,
        reject_invalid: bool = True,
        commit_every: int = 500,
    ) -> Dict[str, Any]:
        """
        Transactional batch ingestion.

        The default behavior is:
        - each batch is committed periodically,
        - duplicates are harmless,
        - invalid rows either raise or go to rejected_observations,
        - no existing raw row is updated.
        """
        batch_id = batch_id or hashlib.sha256(
            f"{utc_now()}:{id(observations)}".encode()
        ).hexdigest()[:24]

        started = utc_now()
        self._conn.execute(
            """
            INSERT OR REPLACE INTO ingestion_batches(
                batch_id, started_at, source, status
            )
            VALUES (?, ?, ?, ?)
            """,
            (batch_id, started, source, "RUNNING"),
        )

        counts = {
            "input": 0,
            "inserted": 0,
            "duplicate": 0,
            "rejected": 0,
        }

        try:
            for observation in observations:
                counts["input"] += 1

                try:
                    result = self.append(
                        observation,
                        reject_invalid=reject_invalid,
                    )
                except Exception:
                    if reject_invalid:
                        raise
                    counts["rejected"] += 1
                    continue

                if result.status == "inserted":
                    counts["inserted"] += 1
                elif result.status == "duplicate":
                    counts["duplicate"] += 1
                elif result.status == "rejected":
                    counts["rejected"] += 1

                if (
                    commit_every > 0
                    and counts["input"] % commit_every == 0
                ):
                    self._conn.commit()

            self._conn.commit()

            self._conn.execute(
                """
                UPDATE ingestion_batches
                SET finished_at = ?,
                    input_count = ?,
                    accepted_count = ?,
                    duplicate_count = ?,
                    rejected_count = ?,
                    status = 'COMPLETED'
                WHERE batch_id = ?
                """,
                (
                    utc_now(),
                    counts["input"],
                    counts["inserted"],
                    counts["duplicate"],
                    counts["rejected"],
                    batch_id,
                ),
            )
            self._conn.commit()

            return {
                "batch_id": batch_id,
                "source": source,
                "status": "COMPLETED",
                **counts,
            }

        except Exception:
            self._conn.rollback()
            self._conn.execute(
                """
                UPDATE ingestion_batches
                SET finished_at = ?, status = 'FAILED'
                WHERE batch_id = ?
                """,
                (utc_now(), batch_id),
            )
            self._conn.commit()
            raise

    def _record_rejection(
        self,
        reason: str,
        observation: Mapping[str, Any],
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO rejected_observations(
                received_at, reason, payload_json
            )
            VALUES (?, ?, ?)
            """,
            (
                utc_now(),
                reason,
                canonical_json(dict(observation)),
            ),
        )

    @staticmethod
    def _params(data: Mapping[str, Any]) -> Tuple[Any, ...]:
        def v(key: str) -> Any:
            return data.get(key)

        return (
            data["content_hash"],
            data["contract_version"],
            data["stream"],
            data["timestamp"],
            _timestamp_epoch(data["timestamp"]),
            data["symbol"],
            v("exchange"),
            v("market"),
            v("instrument_type"),
            v("asset_class"),
            v("timeframe"),
            v("session"),

            v("open"),
            v("high"),
            v("low"),
            v("close"),
            v("volume"),
            v("oi"),
            v("bid"),
            v("ask"),
            v("mid"),

            v("futures_close"),
            v("spot_close"),

            v("iv"),
            v("atm_iv"),
            v("iv_change"),
            v("iv_rank"),
            v("iv_percentile"),
            v("iv_skew"),
            v("iv_term_structure"),
            v("realized_vol"),
            v("iv_rv_spread"),

            v("pcr_oi"),
            v("pcr_volume"),
            v("ce_oi"),
            v("pe_oi"),
            v("ce_oi_change"),
            v("pe_oi_change"),
            v("atm_straddle"),
            v("chain_completeness"),

            v("delta"),
            v("gamma"),
            v("theta"),
            v("vega"),
            v("vanna"),
            v("charm"),
            v("dte"),
            v("strike"),
            v("option_type"),
            v("moneyness"),
            v("expiry"),

            canonical_json(v("metadata") or {}),
            data["ingested_at"],
        )

    def get_by_hash(self, digest: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM raw_observations
            WHERE content_hash = ?
            """,
            (digest,),
        ).fetchone()

        return self._row_to_dict(row) if row else None

    def get(
        self,
        *,
        symbol: Optional[str] = None,
        stream: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = 1000,
        ascending: bool = True,
    ) -> Iterator[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []

        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)

        if stream:
            clauses.append("stream = ?")
            params.append(stream)

        if start:
            start_iso = _parse_timestamp(start)
            clauses.append("timestamp_epoch >= ?")
            params.append(_timestamp_epoch(start_iso))

        if end:
            end_iso = _parse_timestamp(end)
            clauses.append("timestamp_epoch <= ?")
            params.append(_timestamp_epoch(end_iso))

        where = ""
        if clauses:
            where = "WHERE " + " AND ".join(clauses)

        order = "ASC" if ascending else "DESC"

        sql = f"""
            SELECT *
            FROM raw_observations
            {where}
            ORDER BY timestamp_epoch {order}, id {order}
        """

        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(0, int(limit)))

        cursor = self._conn.execute(sql, params)

        for row in cursor:
            yield self._row_to_dict(row)

    def count(
        self,
        *,
        symbol: Optional[str] = None,
        stream: Optional[str] = None,
    ) -> int:
        clauses: List[str] = []
        params: List[Any] = []

        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)

        if stream:
            clauses.append("stream = ?")
            params.append(stream)

        where = ""
        if clauses:
            where = "WHERE " + " AND ".join(clauses)

        row = self._conn.execute(
            f"SELECT COUNT(*) AS n FROM raw_observations {where}",
            params,
        ).fetchone()

        return int(row["n"])

    def first_timestamp(self) -> Optional[str]:
        row = self._conn.execute(
            """
            SELECT timestamp
            FROM raw_observations
            ORDER BY timestamp_epoch ASC, id ASC
            LIMIT 1
            """
        ).fetchone()
        return str(row["timestamp"]) if row else None

    def last_timestamp(self) -> Optional[str]:
        row = self._conn.execute(
            """
            SELECT timestamp
            FROM raw_observations
            ORDER BY timestamp_epoch DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row["timestamp"]) if row else None

    def coverage(self) -> Dict[str, Any]:
        first = self.first_timestamp()
        last = self.last_timestamp()

        days = None
        if first and last:
            seconds = max(
                0.0,
                _timestamp_epoch(last) - _timestamp_epoch(first),
            )
            days = seconds / 86400.0

        rows = self._conn.execute(
            """
            SELECT stream, COUNT(*) AS n
            FROM raw_observations
            GROUP BY stream
            ORDER BY stream
            """
        ).fetchall()

        return {
            "store_version": STORE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "rows": self.count(),
            "first_timestamp": first,
            "last_timestamp": last,
            "coverage_days": days,
            "streams": {
                str(row["stream"]): int(row["n"])
                for row in rows
            },
        }

    def symbols(self) -> List[str]:
        rows = self._conn.execute(
            """
            SELECT DISTINCT symbol
            FROM raw_observations
            ORDER BY symbol
            """
        ).fetchall()
        return [str(row["symbol"]) for row in rows]

    def stream_counts(self) -> Dict[str, int]:
        rows = self._conn.execute(
            """
            SELECT stream, COUNT(*) AS n
            FROM raw_observations
            GROUP BY stream
            """
        ).fetchall()
        return {
            str(row["stream"]): int(row["n"])
            for row in rows
        }

    def rejected_count(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM rejected_observations"
        ).fetchone()
        return int(row["n"])

    def batch_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM ingestion_batches
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (max(0, int(limit)),),
        ).fetchall()

        return [dict(row) for row in rows]

    def export_jsonl(
        self,
        output_path: str | Path,
        *,
        symbol: Optional[str] = None,
        stream: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Export a read-only snapshot in chronological order.

        The SQLite store is not changed by export.
        """
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        rows = self.get(
            symbol=symbol,
            stream=stream,
            start=start,
            end=end,
            limit=limit,
            ascending=True,
        )

        count = 0
        with output.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                count += 1

        return {
            "output": str(output),
            "rows_exported": count,
        }

    def backup(self, output_path: str | Path) -> Dict[str, Any]:
        """
        Consistent SQLite backup using the SQLite backup API.
        """
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        if output.exists():
            raise FileExistsError(
                f"Backup destination already exists: {output}"
            )

        target = sqlite3.connect(str(output))
        try:
            self._conn.backup(target)
        finally:
            target.close()

        return {
            "backup_path": str(output),
            "store_version": STORE_VERSION,
            "schema_version": SCHEMA_VERSION,
        }

    def integrity_check(self) -> Dict[str, Any]:
        row = self._conn.execute(
            "PRAGMA integrity_check"
        ).fetchone()

        result = str(row[0]) if row else "unknown"

        foreign_keys = self._conn.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()

        return {
            "integrity_check": result,
            "foreign_key_errors": len(foreign_keys),
            "ok": result.lower() == "ok" and not foreign_keys,
        }

    def vacuum(self) -> None:
        """
        Intentionally disabled.

        A raw research store is append-only and audit-oriented. Repacking/
        maintenance should be performed manually after a verified backup.
        """
        raise RuntimeError(
            "VACUUM is intentionally disabled by the GSR raw-store contract. "
            "Back up first and perform maintenance as a separate controlled "
            "operation."
        )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)

        metadata_raw = data.pop("metadata_json", "{}")
        try:
            data["metadata"] = json.loads(metadata_raw)
        except Exception:
            data["metadata"] = {
                "_raw_metadata_json": metadata_raw
            }

        return data


def open_default_store() -> GSRRawDataStore:
    return GSRRawDataStore(DEFAULT_DB_PATH)


def ingest_from_adapter(
    adapter: Any,
    store: GSRRawDataStore,
    rows: Iterable[Mapping[str, Any]],
    *,
    source: str = "adapter",
) -> Dict[str, Any]:
    """
    Explicit bridge from gsr_data_adapter.py to the raw store.

    The adapter remains responsible for normalization/isolation. The store
    remains responsible for persistence. No GSR calculation happens here.
    """
    normalized: List[Dict[str, Any]] = []

    for row in rows:
        observation = adapter.normalize(row)
        normalized.append(observation.to_mapping())

    return store.append_many(
        normalized,
        source=source,
        reject_invalid=True,
    )


def _self_test() -> None:
    """
    Storage-only test. Uses an isolated temporary SQLite database.
    """
    with tempfile.TemporaryDirectory(prefix="gsr_store_test_") as tmp:
        db = Path(tmp) / "test.sqlite3"

        with GSRRawDataStore(db) as store:
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
                "source": "SELF_TEST",
                "metadata": {
                    "bar_closed": True,
                },
            }

            first = store.append(base)
            assert first.status == "inserted"
            assert store.count() == 1

            duplicate = store.append(dict(base))
            assert duplicate.status == "duplicate"
            assert store.count() == 1

            second = dict(base)
            second["timestamp"] = "2026-01-02T09:18:00+00:00"
            second["open"] = 25010.0
            second["high"] = 25030.0
            second["low"] = 25000.0
            second["close"] = 25025.0

            third = dict(base)
            third["timestamp"] = "2026-01-02T09:21:00+00:00"
            third["open"] = 25025.0
            third["high"] = 25040.0
            third["low"] = 25015.0
            third["close"] = 25035.0

            batch = store.append_many(
                [second, third],
                source="SELF_TEST",
            )
            assert batch["inserted"] == 2
            assert store.count(symbol="NIFTY") == 3

            rows = list(
                store.get(
                    symbol="NIFTY",
                    ascending=True,
                )
            )
            assert len(rows) == 3
            assert rows[0]["timestamp"] < rows[1]["timestamp"] < rows[2]["timestamp"]

            # Isolation test: GSR opinions cannot enter raw storage.
            bad = dict(base)
            bad["timestamp"] = "2026-01-02T09:24:00+00:00"
            bad["confidence"] = 0.99

            try:
                store.append(bad)
            except ValueError:
                pass
            else:
                raise AssertionError(
                    "Forbidden opinion field crossed raw-store boundary"
                )

            coverage = store.coverage()
            assert coverage["rows"] == 3
            assert coverage["coverage_days"] is not None

            integrity = store.integrity_check()
            assert integrity["ok"] is True

            # Export test.
            export_path = Path(tmp) / "export.jsonl"
            result = store.export_jsonl(export_path)
            assert result["rows_exported"] == 3
            assert export_path.exists()

        # Backup test.
        with GSRRawDataStore(db) as store:
            backup_path = Path(tmp) / "backup.sqlite3"
            backup = store.backup(backup_path)
            assert backup_path.exists()
            assert backup["schema_version"] == SCHEMA_VERSION

    print("GSR RAW DATA STORE SELF-TEST: PASS")


if __name__ == "__main__":
    _self_test()
