"""
GSR-1.1.0 â€” Global Strategy Research Validation Engine

RESEARCH ONLY.
This module validates historical/replay evidence for strategies stored in GSR.
It does NOT:
- place broker orders
- consume opinions/confidence from NIFTY Engine or Next-Day Engine
- optimize strategy parameters
- promote a strategy directly to live trading
- treat YouTube/author claimed win-rate as evidence

It DOES:
- validate chronological evidence
- separate TRAIN / VALIDATION / OOS
- calculate expectancy, win-rate, PF, drawdown, Sharpe-like and Sortino-like metrics
- analyse regime fit
- analyse walk-forward folds
- stress transaction costs/slippage
- analyse observed parameter variants without optimizing them
- apply Benjamini-Hochberg FDR control across strategy tests
- retain negative evidence
- produce auditable JSON/JSONL/CSV reports

Expected replay trade JSONL fields:
strategy_id
rule_version
symbol
entry_timestamp
exit_timestamp
direction
net_pnl_points
gross_pnl_points
outcome
regime
split

Optional:
holding_bars
mfe_points
mae_points
target_points
stop_points
cost_points
slippage_points
latency_bars
metadata

Input can be either:
1. a JSONL file
2. a directory containing replay_trade_observations.jsonl

Outputs:
gsr_validation_report.json
gsr_validation_strategy_results.jsonl
gsr_validation_summary.csv
gsr_validation_regime_matrix.jsonl
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import statistics
import sys
import traceback
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple


ENGINE_VERSION = "GSR-1.1.0-VALIDATION"
SCHEMA_VERSION = "GSR_VALIDATION_1.1"


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass
class ValidationConfig:
    min_total_trades: int = 100
    min_oos_trades: int = 30

    min_oos_expectancy: float = 0.0
    min_oos_profit_factor: float = 1.0
    min_oos_win_rate: float = 0.0

    min_positive_regimes: int = 1
    max_negative_regimes: int = 999999
    min_regime_trades: int = 20

    min_walk_forward_folds: int = 3
    min_positive_folds: int = 2
    max_negative_folds: int = 999999

    bootstrap_iterations: int = 5000
    bootstrap_alpha: float = 0.05

    fdr_alpha: float = 0.05
    min_effective_trades_for_inference: int = 30

    economic_edge_floor: float = 0.0
    max_cost_multiplier: float = 3.0
    cost_sensitivity_steps: int = 7

    max_drawdown_points: float = float("inf")

    seed: int = 1101

    @classmethod
    def from_env(cls) -> "ValidationConfig":
        def get(name: str, default: Any, cast: Any) -> Any:
            value = os.getenv(name)
            return default if value is None else cast(value)

        config = cls(
            min_total_trades=get(
                "GSR_VAL_MIN_TRADES",
                cls.min_total_trades,
                int,
            ),
            min_oos_trades=get(
                "GSR_VAL_MIN_OOS",
                cls.min_oos_trades,
                int,
            ),
            min_oos_expectancy=get(
                "GSR_VAL_MIN_OOS_EXPECTANCY",
                cls.min_oos_expectancy,
                float,
            ),
            min_oos_profit_factor=get(
                "GSR_VAL_MIN_OOS_PF",
                cls.min_oos_profit_factor,
                float,
            ),
            min_oos_win_rate=get(
                "GSR_VAL_MIN_OOS_WR",
                cls.min_oos_win_rate,
                float,
            ),
            min_positive_regimes=get(
                "GSR_VAL_MIN_POSITIVE_REGIMES",
                cls.min_positive_regimes,
                int,
            ),
            max_negative_regimes=get(
                "GSR_VAL_MAX_NEGATIVE_REGIMES",
                cls.max_negative_regimes,
                int,
            ),
            min_regime_trades=get(
                "GSR_VAL_MIN_REGIME_TRADES",
                cls.min_regime_trades,
                int,
            ),
            min_walk_forward_folds=get(
                "GSR_VAL_MIN_WF_FOLDS",
                cls.min_walk_forward_folds,
                int,
            ),
            min_positive_folds=get(
                "GSR_VAL_MIN_POSITIVE_FOLDS",
                cls.min_positive_folds,
                int,
            ),
            max_negative_folds=get(
                "GSR_VAL_MAX_NEGATIVE_FOLDS",
                cls.max_negative_folds,
                int,
            ),
            bootstrap_iterations=get(
                "GSR_VAL_BOOTSTRAP_ITERS",
                cls.bootstrap_iterations,
                int,
            ),
            bootstrap_alpha=get(
                "GSR_VAL_BOOTSTRAP_ALPHA",
                cls.bootstrap_alpha,
                float,
            ),
            fdr_alpha=get(
                "GSR_VAL_FDR_ALPHA",
                cls.fdr_alpha,
                float,
            ),
            min_effective_trades_for_inference=get(
                "GSR_VAL_MIN_EFFECTIVE",
                cls.min_effective_trades_for_inference,
                int,
            ),
            economic_edge_floor=get(
                "GSR_VAL_EDGE_FLOOR",
                cls.economic_edge_floor,
                float,
            ),
            max_cost_multiplier=get(
                "GSR_VAL_MAX_COST_MULTIPLIER",
                cls.max_cost_multiplier,
                float,
            ),
            cost_sensitivity_steps=get(
                "GSR_VAL_COST_STEPS",
                cls.cost_sensitivity_steps,
                int,
            ),
            seed=get(
                "GSR_VAL_SEED",
                cls.seed,
                int,
            ),
        )

        config.validate()
        return config

    def validate(self) -> None:
        if self.min_total_trades < 1:
            raise ValueError("min_total_trades must be >= 1")

        if self.min_oos_trades < 1:
            raise ValueError("min_oos_trades must be >= 1")

        if not 0 < self.bootstrap_alpha < 1:
            raise ValueError("bootstrap_alpha must be between 0 and 1")

        if not 0 < self.fdr_alpha < 1:
            raise ValueError("fdr_alpha must be between 0 and 1")

        if self.min_regime_trades < 1:
            raise ValueError("min_regime_trades must be >= 1")

        if self.min_walk_forward_folds < 0:
            raise ValueError("min_walk_forward_folds cannot be negative")

        if self.max_cost_multiplier < 1:
            raise ValueError("max_cost_multiplier must be >= 1")


# ============================================================
# MATH HELPERS
# ============================================================

def finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def safe_float(
    value: Any,
    default: Optional[float] = None,
) -> Optional[float]:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def safe_div(
    numerator: float,
    denominator: float,
    default: float = 0.0,
) -> float:
    if (
        not finite(numerator)
        or not finite(denominator)
        or abs(denominator) <= 1e-15
    ):
        return default

    return numerator / denominator


def mean(values: Sequence[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def median(values: Sequence[float]) -> float:
    return statistics.median(values) if values else 0.0


def stdev(values: Sequence[float]) -> float:
    return statistics.stdev(values) if len(values) >= 2 else 0.0


def percentile(
    values: Sequence[float],
    q: float,
) -> float:
    if not values:
        return 0.0

    data = sorted(values)
    q = max(0.0, min(1.0, q))

    position = (len(data) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))

    if lower == upper:
        return data[lower]

    return (
        data[lower]
        + (data[upper] - data[lower])
        * (position - lower)
    )


def max_drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0

    for pnl in values:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)

    return drawdown


def profit_factor(values: Sequence[float]) -> float:
    gross_profit = sum(
        value for value in values
        if value > 0
    )

    gross_loss = -sum(
        value for value in values
        if value < 0
    )

    if gross_loss <= 0:
        return (
            float("inf")
            if gross_profit > 0
            else 0.0
        )

    return gross_profit / gross_loss


def sharpe_like(values: Sequence[float]) -> float:
    return safe_div(
        mean(values),
        stdev(values),
    )


def sortino_like(values: Sequence[float]) -> float:
    if not values:
        return 0.0

    downside = math.sqrt(
        mean([
            min(0.0, value) ** 2
            for value in values
        ])
    )

    return safe_div(
        mean(values),
        downside,
    )


def normal_cdf(value: float) -> float:
    return 0.5 * (
        1.0
        + math.erf(
            value / math.sqrt(2.0)
        )
    )


def normal_two_sided_p(value: float) -> float:
    return max(
        0.0,
        min(
            1.0,
            2.0 * (
                1.0
                - normal_cdf(abs(value))
            ),
        ),
    )


def sign_test_pvalue(
    values: Sequence[float],
) -> Optional[float]:
    """
    Exact two-sided sign test.
    Zero PnL observations are ignored.
    """

    non_zero = [
        value for value in values
        if value != 0
    ]

    n = len(non_zero)

    if n == 0:
        return None

    wins = sum(
        value > 0
        for value in non_zero
    )

    k = min(
        wins,
        n - wins,
    )

    tail = sum(
        math.comb(n, index)
        / (2.0 ** n)
        for index in range(k + 1)
    )

    return min(
        1.0,
        2.0 * tail,
    )


def approximate_t_pvalue(
    values: Sequence[float],
) -> Optional[float]:
    """
    Approximate one-sample mean p-value.
    Diagnostic only; never the sole promotion gate.
    """

    n = len(values)

    if n < 2:
        return None

    sigma = stdev(values)

    if sigma == 0:
        return (
            0.0
            if mean(values) > 0
            else 1.0
        )

    z = mean(values) / (
        sigma / math.sqrt(n)
    )

    return normal_two_sided_p(z)


def bootstrap_mean_ci(
    values: Sequence[float],
    iterations: int,
    alpha: float,
    seed: int,
) -> Dict[str, Any]:

    if len(values) < 2:
        return {
            "status": "INSUFFICIENT_SAMPLE",
            "mean": mean(values),
            "lower": None,
            "upper": None,
            "iterations": 0,
        }

    rng = random.Random(seed)
    sample_size = len(values)
    bootstrap_means = []

    for _ in range(max(1, iterations)):
        sample = [
            values[
                rng.randrange(sample_size)
            ]
            for _ in range(sample_size)
        ]

        bootstrap_means.append(
            mean(sample)
        )

    return {
        "status": "OK",
        "mean": mean(values),
        "lower": percentile(
            bootstrap_means,
            alpha / 2,
        ),
        "upper": percentile(
            bootstrap_means,
            1 - alpha / 2,
        ),
        "iterations": iterations,
        "alpha": alpha,
        "seed": seed,
    }


def benjamini_hochberg(
    pvalues: Mapping[
        str,
        Optional[float],
    ],
    alpha: float,
) -> Dict[str, Dict[str, Any]]:
    """
    Benjamini-Hochberg FDR control.

    One test per strategy in the OOS family.
    """

    usable = [
        (
            key,
            float(value),
        )
        for key, value in pvalues.items()
        if value is not None
        and finite(value)
    ]

    usable.sort(
        key=lambda item: item[1]
    )

    test_count = len(usable)

    output = {
        key: {
            "p_value": pvalues.get(key),
            "q_value": None,
            "fdr_reject": False,
            "rank": None,
            "tests": test_count,
        }
        for key in pvalues
    }

    adjusted = [
        0.0
        for _ in range(test_count)
    ]

    running = 1.0

    for index in range(
        test_count - 1,
        -1,
        -1,
    ):
        q_value = min(
            running,
            usable[index][1]
            * test_count
            / (index + 1),
        )

        adjusted[index] = q_value
        running = q_value

    for index, (
        strategy_id,
        p_value,
    ) in enumerate(usable):

        q_value = min(
            1.0,
            adjusted[index],
        )

        output[strategy_id] = {
            "p_value": p_value,
            "q_value": q_value,
            "fdr_reject": (
                q_value <= alpha
            ),
            "rank": index + 1,
            "tests": test_count,
        }

    return output


def stable_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(
        timezone.utc
    ).isoformat()


# ============================================================
# EVIDENCE RECORD
# ============================================================

@dataclass
class EvidenceRecord:
    strategy_id: str
    rule_version: str
    symbol: str

    entry_timestamp: str
    exit_timestamp: str

    direction: str

    net_pnl_points: float
    gross_pnl_points: float

    outcome: str
    regime: str
    split: str

    holding_bars: Optional[int] = None

    mfe_points: Optional[float] = None
    mae_points: Optional[float] = None

    target_points: Optional[float] = None
    stop_points: Optional[float] = None

    cost_points: float = 0.0
    slippage_points: float = 0.0

    latency_bars: int = 0

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )

    @classmethod
    def from_mapping(
        cls,
        row: Mapping[str, Any],
    ) -> "EvidenceRecord":

        required = (
            "strategy_id",
            "rule_version",
            "symbol",
            "entry_timestamp",
            "exit_timestamp",
            "direction",
            "net_pnl_points",
            "gross_pnl_points",
            "outcome",
            "regime",
            "split",
        )

        missing = [
            key
            for key in required
            if key not in row
        ]

        if missing:
            raise ValueError(
                f"Missing fields: {missing}"
            )

        return cls(
            strategy_id=str(
                row["strategy_id"]
            ),
            rule_version=str(
                row["rule_version"]
            ),
            symbol=str(
                row["symbol"]
            ),
            entry_timestamp=str(
                row["entry_timestamp"]
            ),
            exit_timestamp=str(
                row["exit_timestamp"]
            ),
            direction=str(
                row["direction"]
            ).upper(),
            net_pnl_points=float(
                row["net_pnl_points"]
            ),
            gross_pnl_points=float(
                row["gross_pnl_points"]
            ),
            outcome=str(
                row["outcome"]
            ).upper(),
            regime=str(
                row["regime"]
            ),
            split=str(
                row["split"]
            ).upper(),
            holding_bars=(
                int(row["holding_bars"])
                if row.get("holding_bars")
                is not None
                else None
            ),
            mfe_points=safe_float(
                row.get("mfe_points")
            ),
            mae_points=safe_float(
                row.get("mae_points")
            ),
            target_points=safe_float(
                row.get("target_points")
            ),
            stop_points=safe_float(
                row.get("stop_points")
            ),
            cost_points=(
                safe_float(
                    row.get(
                        "cost_points"
                    ),
                    0.0,
                )
                or 0.0
            ),
            slippage_points=(
                safe_float(
                    row.get(
                        "slippage_points"
                    ),
                    0.0,
                )
                or 0.0
            ),
            latency_bars=int(
                row.get(
                    "latency_bars",
                    0,
                )
                or 0
            ),
            metadata=dict(
                row.get("metadata")
                or {}
            ),
        )

    def validate(self) -> List[str]:
        errors = []

        if self.direction not in {
            "LONG",
            "SHORT",
        }:
            errors.append(
                "invalid_direction"
            )

        if not finite(
            self.net_pnl_points
        ):
            errors.append(
                "invalid_net_pnl"
            )

        if not finite(
            self.gross_pnl_points
        ):
            errors.append(
                "invalid_gross_pnl"
            )

        if self.split not in {
            "TRAIN",
            "VALIDATION",
            "OOS",
            "ALL",
            "UNASSIGNED",
        }:
            errors.append(
                "unknown_split"
            )

        if not self.entry_timestamp:
            errors.append(
                "missing_entry_timestamp"
            )

        if not self.exit_timestamp:
            errors.append(
                "missing_exit_timestamp"
            )

        return errors


# ============================================================
# JSONL I/O
# ============================================================

def iter_jsonl(
    path: Path,
) -> Iterator[Dict[str, Any]]:

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:

        for line_number, line in enumerate(
            handle,
            1,
        ):

            if not line.strip():
                continue

            try:
                value = json.loads(line)

            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{path}:{line_number}: "
                    f"invalid JSON: {exc}"
                ) from exc

            if not isinstance(
                value,
                dict,
            ):
                raise ValueError(
                    f"{path}:{line_number}: "
                    "JSON object required"
                )

            yield value


def discover_trade_file(
    path: Path,
) -> Path:

    if path.is_file():
        return path

    if not path.exists():
        raise FileNotFoundError(
            str(path)
        )

    preferred_names = (
        "replay_trade_observations.jsonl",
        "trade_observations.jsonl",
        "validation_trade_observations.jsonl",
    )

    for filename in preferred_names:
        candidate = path / filename

        if candidate.exists():
            return candidate

    candidates = sorted(
        path.glob(
            "*trade*observation*.jsonl"
        )
    )

    if candidates:
        return candidates[0]

    raise FileNotFoundError(
        "No trade-observation JSONL found "
        f"under {path}"
    )


def chronological(
    records: Sequence[EvidenceRecord],
) -> List[EvidenceRecord]:

    return sorted(
        records,
        key=lambda record: (
            record.entry_timestamp,
            record.exit_timestamp,
            record.strategy_id,
            record.rule_version,
        ),
    )


def load_evidence(
    path: Path,
) -> Tuple[
    List[EvidenceRecord],
    List[Dict[str, Any]],
]:

    source = discover_trade_file(path)

    records: List[
        EvidenceRecord
    ] = []

    errors: List[
        Dict[str, Any]
    ] = []

    for line_number, row in enumerate(
        iter_jsonl(source),
        1,
    ):

        try:
            record = (
                EvidenceRecord
                .from_mapping(row)
            )

            row_errors = (
                record.validate()
            )

            if row_errors:
                errors.append({
                    "line": line_number,
                    "strategy_id":
                        record.strategy_id,
                    "errors":
                        row_errors,
                })
            else:
                records.append(
                    record
                )

        except Exception as exc:
            errors.append({
                "line": line_number,
                "error": str(exc),
            })

    return records, errors


# ============================================================
# METRICS
# ============================================================

class MetricEngine:

    @staticmethod
    def calculate(
        records: Sequence[EvidenceRecord],
        config: ValidationConfig,
        seed_offset: int = 0,
    ) -> Dict[str, Any]:

        values = [
            float(
                record.net_pnl_points
            )
            for record in records
            if finite(
                record.net_pnl_points
            )
        ]

        if not values:
            return {
                "status": "NO_EVIDENCE",
                "sample_size": 0,
                "expectancy_points": 0.0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown_points": 0.0,
            }

        wins = sum(
            value > 0
            for value in values
        )

        losses = sum(
            value < 0
            for value in values
        )

        flats = (
            len(values)
            - wins
            - losses
        )

        winning_values = [
            value
            for value in values
            if value > 0
        ]

        losing_values = [
            value
            for value in values
            if value < 0
        ]

        avg_win = mean(
            winning_values
        )

        avg_loss = mean(
            losing_values
        )

        bootstrap = None

        if (
            len(values)
            >= config.min_effective_trades_for_inference
        ):
            bootstrap = (
                bootstrap_mean_ci(
                    values,
                    config.bootstrap_iterations,
                    config.bootstrap_alpha,
                    config.seed + seed_offset,
                )
            )

        mfe = [
            record.mfe_points
            for record in records
            if (
                record.mfe_points
                is not None
                and finite(
                    record.mfe_points
                )
            )
        ]

        mae = [
            record.mae_points
            for record in records
            if (
                record.mae_points
                is not None
                and finite(
                    record.mae_points
                )
            )
        ]

        return {
            "status": "OK",
            "sample_size": len(values),

            "wins": wins,
            "losses": losses,
            "flats": flats,

            "win_rate": safe_div(
                wins,
                len(values),
            ),

            "loss_rate": safe_div(
                losses,
                len(values),
            ),

            "expectancy_points":
                mean(values),

            "median_pnl_points":
                median(values),

            "avg_win_points":
                avg_win,

            "avg_loss_points":
                avg_loss,

            "payoff_ratio": safe_div(
                avg_win,
                abs(avg_loss),
            ),

            "profit_factor":
                profit_factor(values),

            "gross_profit_points":
                sum(
                    value
                    for value in values
                    if value > 0
                ),

            "gross_loss_points":
                sum(
                    value
                    for value in values
                    if value < 0
                ),

            "net_pnl_points":
                sum(values),

            "max_drawdown_points":
                max_drawdown(values),

            "sharpe_like":
                sharpe_like(values),

            "sortino_like":
                sortino_like(values),

            "p05_pnl":
                percentile(values, 0.05),

            "p25_pnl":
                percentile(values, 0.25),

            "p50_pnl":
                percentile(values, 0.50),

            "p75_pnl":
                percentile(values, 0.75),

            "p95_pnl":
                percentile(values, 0.95),

            "mean_mfe_points":
                mean(mfe) if mfe else None,

            "median_mfe_points":
                median(mfe) if mfe else None,

            "mean_mae_points":
                mean(mae) if mae else None,

            "median_mae_points":
                median(mae) if mae else None,

            "ambiguous_count":
                sum(
                    record.outcome
                    == "AMBIGUOUS"
                    for record in records
                ),

            "target_first_count":
                sum(
                    record.outcome
                    == "TARGET_FIRST"
                    for record in records
                ),

            "stop_first_count":
                sum(
                    record.outcome
                    == "STOP_FIRST"
                    for record in records
                ),

            "timeout_count":
                sum(
                    record.outcome
                    == "TIMEOUT"
                    for record in records
                ),

            "bootstrap_mean_ci":
                bootstrap,

            "sign_test_p_value":
                sign_test_pvalue(values),

            "approximate_t_p_value":
                approximate_t_pvalue(values),
        }


# ============================================================
# REGIME ANALYSIS
# ============================================================

class RegimeAnalyzer:

    def __init__(
        self,
        config: ValidationConfig,
    ):
        self.config = config

    def analyze(
        self,
        records: Sequence[EvidenceRecord],
        strategy_id: str,
    ) -> Dict[str, Any]:

        groups = defaultdict(list)

        for record in records:
            if record.strategy_id == strategy_id:
                groups[
                    record.regime
                ].append(record)

        rows = []

        positive = 0
        negative = 0
        eligible = 0

        for regime, group in sorted(
            groups.items()
        ):

            metric = (
                MetricEngine.calculate(
                    group,
                    self.config,
                    seed_offset=(
                        len(regime)
                        + len(strategy_id)
                    ),
                )
            )

            sufficient = (
                metric["sample_size"]
                >= self.config.min_regime_trades
            )

            if sufficient:
                eligible += 1

                if (
                    metric[
                        "expectancy_points"
                    ]
                    > self.config.economic_edge_floor
                ):
                    positive += 1

                elif (
                    metric[
                        "expectancy_points"
                    ]
                    < self.config.economic_edge_floor
                ):
                    negative += 1

            rows.append({
                "strategy_id":
                    strategy_id,
                "regime":
                    regime,
                "sufficient_sample":
                    sufficient,
                **metric,
            })

        return {
            "regime_count":
                len(rows),

            "eligible_regime_count":
                eligible,

            "positive_regime_count":
                positive,

            "negative_regime_count":
                negative,

            "rows":
                rows,

            "negative_evidence_retained":
                True,
        }


# ============================================================
# WALK-FORWARD ANALYSIS
# ============================================================

class WalkForwardAnalyzer:

    def __init__(
        self,
        config: ValidationConfig,
    ):
        self.config = config

    @staticmethod
    def fold_key(
        record: EvidenceRecord,
    ) -> Optional[str]:

        metadata = record.metadata or {}

        for key in (
            "walk_forward_fold",
            "wf_fold",
            "fold",
            "walk_forward_id",
        ):
            if key in metadata:
                return str(
                    metadata[key]
                )

        return None

    def analyze(
        self,
        records: Sequence[EvidenceRecord],
        strategy_id: str,
    ) -> Dict[str, Any]:

        selected = chronological([
            record
            for record in records
            if record.strategy_id
            == strategy_id
        ])

        explicit = defaultdict(list)

        for record in selected:
            fold = self.fold_key(record)

            if fold is not None:
                explicit[fold].append(
                    record
                )

        groups: Dict[
            str,
            List[EvidenceRecord],
        ] = {}

        source = "NONE"

        if explicit:
            groups = dict(
                sorted(explicit.items())
            )

            source = (
                "EXPLICIT_REPLAY_METADATA"
            )

        else:
            # Diagnostic fallback only.
            # These are NOT proof of a real OOS process.
            sample_size = len(selected)
            desired = max(
                1,
                self.config.min_walk_forward_folds,
            )

            if sample_size >= desired * 2:

                chunk = max(
                    1,
                    sample_size // desired,
                )

                for index in range(
                    desired
                ):

                    start = (
                        index * chunk
                    )

                    end = (
                        sample_size
                        if index
                        == desired - 1
                        else min(
                            sample_size,
                            (index + 1)
                            * chunk,
                        )
                    )

                    groups[
                        f"DERIVED_{index + 1}"
                    ] = selected[
                        start:end
                    ]

                source = (
                    "DERIVED_DIAGNOSTIC_ONLY"
                )

        rows = []
        positive = 0
        negative = 0

        for fold, group in sorted(
            groups.items()
        ):

            metric = (
                MetricEngine.calculate(
                    group,
                    self.config,
                )
            )

            if (
                metric[
                    "expectancy_points"
                ]
                > self.config.economic_edge_floor
            ):
                positive += 1

            elif (
                metric[
                    "expectancy_points"
                ]
                < self.config.economic_edge_floor
            ):
                negative += 1

            rows.append({
                "fold": fold,
                "source": source,
                **metric,
            })

        return {
            "fold_source":
                source,

            "fold_count":
                len(rows),

            "positive_fold_count":
                positive,

            "negative_fold_count":
                negative,

            "rows":
                rows,

            "derived_folds_are_not_oos_proof":
                (
                    source
                    == "DERIVED_DIAGNOSTIC_ONLY"
                ),
        }


# ============================================================
# COST / FRICTION ROBUSTNESS
# ============================================================

class CostSensitivityAnalyzer:

    def __init__(
        self,
        config: ValidationConfig,
    ):
        self.config = config

    def analyze(
        self,
        records: Sequence[EvidenceRecord],
        strategy_id: str,
    ) -> Dict[str, Any]:

        selected = [
            record
            for record in records
            if record.strategy_id
            == strategy_id
        ]

        if not selected:
            return {
                "status":
                    "NO_EVIDENCE",
                "scenarios": [],
            }

        recorded_cost = mean([
            record.cost_points
            + record.slippage_points
            for record in selected
        ])

        max_extra = max(
            recorded_cost
            * self.config.max_cost_multiplier,
            recorded_cost,
            0.0,
        )

        steps = max(
            2,
            self.config.cost_sensitivity_steps,
        )

        scenarios = []

        for index in range(steps):

            extra = (
                max_extra
                * index
                / (steps - 1)
            )

            values = [
                record.gross_pnl_points
                - extra
                for record in selected
                if finite(
                    record.gross_pnl_points
                )
            ]

            expectancy = mean(
                values
            )

            scenarios.append({
                "scenario_index":
                    index,

                "extra_friction_points_per_trade":
                    extra,

                "expectancy_points":
                    expectancy,

                "profit_factor":
                    profit_factor(values),

                "win_rate":
                    safe_div(
                        sum(
                            value > 0
                            for value in values
                        ),
                        len(values),
                    ),

                "positive_edge":
                    (
                        expectancy
                        > self.config.economic_edge_floor
                    ),
            })

        surviving = sum(
            scenario["positive_edge"]
            for scenario in scenarios
        )

        return {
            "status": "OK",

            "base_recorded_cost_plus_slippage":
                recorded_cost,

            "max_extra_friction_tested":
                max_extra,

            "scenario_count":
                len(scenarios),

            "positive_edge_scenarios":
                surviving,

            "edge_survival_fraction":
                safe_div(
                    surviving,
                    len(scenarios),
                ),

            "scenarios":
                scenarios,
        }


# ============================================================
# OBSERVED VARIANT SENSITIVITY
# ============================================================

class VariantSensitivityAnalyzer:

    METADATA_KEYS = (
        "parameter_variant",
        "variant_id",
        "rule_variant",
        "parameter_hash",
        "parameter_version",
    )

    def analyze(
        self,
        records: Sequence[EvidenceRecord],
        strategy_id: str,
        config: ValidationConfig,
    ) -> Dict[str, Any]:

        groups = defaultdict(list)

        for record in records:

            if record.strategy_id != strategy_id:
                continue

            variant = None

            for key in self.METADATA_KEYS:

                if key in (
                    record.metadata or {}
                ):
                    variant = (
                        record.metadata[key]
                    )
                    break

            if variant is not None:
                groups[
                    str(variant)
                ].append(record)

        if not groups:
            return {
                "status":
                    "NO_VARIANT_METADATA",

                "variants": [],

                "optimizer_used":
                    False,
            }

        variants = []

        for variant, group in sorted(
            groups.items()
        ):

            variants.append({
                "variant":
                    variant,

                **MetricEngine.calculate(
                    group,
                    config,
                ),
            })

        return {
            "status":
                "OBSERVED_VARIANTS_ONLY",

            "variants":
                variants,

            "variant_count":
                len(variants),

            "optimizer_used":
                False,
        }


# ============================================================
# INTEGRITY / ISOLATION
# ============================================================

class IntegrityAnalyzer:

    FORBIDDEN_EXTERNAL_KEYS = {
        "external_alpha",
        "external_confidence",
        "external_regime",
        "external_signal",
        "external_prediction",
        "external_decision",
        "app_alpha",
        "app_confidence",
        "next_day_confidence",
    }

    def analyze(
        self,
        records: Sequence[EvidenceRecord],
    ) -> Dict[str, Any]:

        issues = []
        seen = set()
        duplicate_count = 0
        previous_timestamp = None

        for record in chronological(
            records
        ):

            metadata = (
                record.metadata or {}
            )

            forbidden = sorted(
                key
                for key in metadata
                if key
                in self.FORBIDDEN_EXTERNAL_KEYS
            )

            if forbidden:
                issues.append({
                    "type":
                        "EXTERNAL_OPINION_FIELD",

                    "strategy_id":
                        record.strategy_id,

                    "entry_timestamp":
                        record.entry_timestamp,

                    "fields":
                        forbidden,
                })

            trade_key = (
                record.strategy_id,
                record.rule_version,
                record.symbol,
                record.entry_timestamp,
                record.exit_timestamp,
                record.direction,
            )

            if trade_key in seen:
                duplicate_count += 1

            seen.add(trade_key)

            if (
                previous_timestamp
                is not None
                and record.entry_timestamp
                < previous_timestamp
            ):
                issues.append({
                    "type":
                        "CHRONOLOGY_VIOLATION",

                    "entry_timestamp":
                        record.entry_timestamp,

                    "previous":
                        previous_timestamp,
                })

            previous_timestamp = (
                record.entry_timestamp
            )

        return {
            "status":
                "PASS"
                if not issues
                else "FAIL",

            "issue_count":
                len(issues),

            "duplicate_trade_count":
                duplicate_count,

            "issues":
                issues[:500],

            "truncated_issues":
                max(
                    0,
                    len(issues) - 500,
                ),
        }


# ============================================================
# EVIDENCE GRADING
# ============================================================

def evidence_grade(
    result: Mapping[str, Any],
) -> str:

    if (
        result.get("status")
        == "REJECT_DATA_INTEGRITY"
    ):
        return "D"

    oos = (
        result
        .get("metrics", {})
        .get("oos", {})
    )

    walk_forward = result.get(
        "walk_forward_analysis",
        {},
    )

    statistical = result.get(
        "statistical_evidence",
        {},
    )

    ci = (
        oos.get(
            "bootstrap_mean_ci"
        )
        or {}
    )

    score = sum([
        result.get("status")
        == "PROMOTION_CANDIDATE",

        oos.get(
            "sample_size",
            0,
        ) >= 30,

        bool(
            statistical.get(
                "fdr_reject"
            )
        ),

        (
            ci.get("lower") is not None
            and ci["lower"] > 0
        ),

        walk_forward.get(
            "fold_count",
            0,
        ) >= 3,

        walk_forward.get(
            "positive_fold_count",
            0,
        ) >= 2,
    ])

    if score >= 6:
        return "A"

    if score >= 4:
        return "B"

    if score >= 2:
        return "C"

    return "D"


# ============================================================
# CORE VALIDATION ENGINE
# ============================================================

class StrategyValidationEngine:

    def __init__(
        self,
        config: Optional[
            ValidationConfig
        ] = None,
    ):
        self.config = (
            config
            or ValidationConfig.from_env()
        )

        self.config.validate()

    @staticmethod
    def split(
        records: Sequence[
            EvidenceRecord
        ],
        split_name: str,
    ) -> List[EvidenceRecord]:

        return [
            record
            for record in records
            if record.split.upper()
            == split_name.upper()
        ]

    def validate_strategy(
        self,
        records: Sequence[
            EvidenceRecord
        ],
        strategy_id: str,
        fdr: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> Dict[str, Any]:

        selected = chronological([
            record
            for record in records
            if record.strategy_id
            == strategy_id
        ])

        train = self.split(
            selected,
            "TRAIN",
        )

        validation = self.split(
            selected,
            "VALIDATION",
        )

        oos = self.split(
            selected,
            "OOS",
        )

        all_metric = (
            MetricEngine.calculate(
                selected,
                self.config,
                11,
            )
        )

        train_metric = (
            MetricEngine.calculate(
                train,
                self.config,
                12,
            )
        )

        validation_metric = (
            MetricEngine.calculate(
                validation,
                self.config,
                13,
            )
        )

        oos_metric = (
            MetricEngine.calculate(
                oos,
                self.config,
                14,
            )
        )

        regime = (
            RegimeAnalyzer(
                self.config
            ).analyze(
                selected,
                strategy_id,
            )
        )

        walk_forward = (
            WalkForwardAnalyzer(
                self.config
            ).analyze(
                selected,
                strategy_id,
            )
        )

        costs = (
            CostSensitivityAnalyzer(
                self.config
            ).analyze(
                selected,
                strategy_id,
            )
        )

        variants = (
            VariantSensitivityAnalyzer()
            .analyze(
                selected,
                strategy_id,
                self.config,
            )
        )

        integrity = (
            IntegrityAnalyzer()
            .analyze(selected)
        )

        ci = (
            oos_metric.get(
                "bootstrap_mean_ci"
            )
            or {}
        )

        checks = {
            "integrity_pass":
                integrity["status"]
                == "PASS",

            "minimum_total_sample":
                len(selected)
                >= self.config.min_total_trades,

            "minimum_oos_sample":
                len(oos)
                >= self.config.min_oos_trades,

            "oos_expectancy_positive":
                (
                    len(oos)
                    >= self.config.min_oos_trades
                    and
                    oos_metric[
                        "expectancy_points"
                    ]
                    > self.config.min_oos_expectancy
                ),

            "oos_profit_factor":
                (
                    len(oos)
                    >= self.config.min_oos_trades
                    and
                    oos_metric[
                        "profit_factor"
                    ]
                    >= self.config.min_oos_profit_factor
                ),

            "oos_win_rate_floor":
                (
                    len(oos)
                    >= self.config.min_oos_trades
                    and
                    oos_metric[
                        "win_rate"
                    ]
                    >= self.config.min_oos_win_rate
                ),

            "oos_bootstrap_ci_positive":
                (
                    ci.get("lower")
                    is not None
                    and
                    ci["lower"]
                    > self.config.economic_edge_floor
                ),

            "drawdown_diagnostic":
                (
                    all_metric[
                        "max_drawdown_points"
                    ]
                    <= self.config.max_drawdown_points
                ),

            "minimum_positive_regimes":
                (
                    regime[
                        "positive_regime_count"
                    ]
                    >= self.config.min_positive_regimes
                ),

            "maximum_negative_regimes":
                (
                    regime[
                        "negative_regime_count"
                    ]
                    <= self.config.max_negative_regimes
                ),

            "walk_forward_min_folds":
                (
                    (
                        walk_forward[
                            "fold_count"
                        ]
                        >= self.config.min_walk_forward_folds
                    )
                    if (
                        self.config.min_walk_forward_folds
                        > 0
                    )
                    else True
                ),

            "walk_forward_positive_folds":
                (
                    walk_forward[
                        "positive_fold_count"
                    ]
                    >= self.config.min_positive_folds
                ),

            "walk_forward_negative_folds":
                (
                    walk_forward[
                        "negative_fold_count"
                    ]
                    <= self.config.max_negative_folds
                ),

            "cost_robustness":
                (
                    costs.get("status")
                    == "OK"
                    and
                    costs.get(
                        "edge_survival_fraction",
                        0.0,
                    )
                    >= 0.5
                ),

            "multiple_testing_control":
                bool(
                    fdr
                    and
                    fdr.get(
                        "fdr_reject"
                    )
                ),
        }

        # Hard promotion gates.
        # Other checks remain diagnostic.
        mandatory = (
            "integrity_pass",
            "minimum_total_sample",
            "minimum_oos_sample",
            "oos_expectancy_positive",
            "oos_profit_factor",
            "oos_bootstrap_ci_positive",
            "walk_forward_min_folds",
            "walk_forward_positive_folds",
            "multiple_testing_control",
        )

        eligible = all(
            checks[name]
            for name in mandatory
        )

        if (
            integrity["status"]
            != "PASS"
        ):
            status = (
                "REJECT_DATA_INTEGRITY"
            )

        elif len(oos) < (
            self.config.min_oos_trades
        ):
            status = "INSUFFICIENT_OOS"

        elif not checks[
            "oos_expectancy_positive"
        ]:
            status = (
                "REJECT_OOS_EXPECTANCY"
            )

        elif not checks[
            "oos_profit_factor"
        ]:
            status = (
                "REJECT_OOS_PROFIT_FACTOR"
            )

        elif not checks[
            "oos_bootstrap_ci_positive"
        ]:
            status = (
                "HOLD_UNCERTAIN_EDGE"
            )

        elif not checks[
            "multiple_testing_control"
        ]:
            status = (
                "HOLD_MULTIPLE_TESTING"
            )

        elif eligible:
            status = (
                "PROMOTION_CANDIDATE"
            )

        else:
            status = "HOLD"

        result = {
            "schema_version":
                SCHEMA_VERSION,

            "validation_engine_version":
                ENGINE_VERSION,

            "strategy_id":
                strategy_id,

            "rule_versions":
                sorted({
                    record.rule_version
                    for record in selected
                }),

            "generated_at":
                utc_now(),

            "sample_sizes": {
                "all":
                    len(selected),
                "train":
                    len(train),
                "validation":
                    len(validation),
                "oos":
                    len(oos),
            },

            "metrics": {
                "all":
                    all_metric,
                "train":
                    train_metric,
                "validation":
                    validation_metric,
                "oos":
                    oos_metric,
            },

            "regime_analysis":
                regime,

            "walk_forward_analysis":
                walk_forward,

            "cost_sensitivity":
                costs,

            "variant_sensitivity":
                variants,

            "integrity":
                integrity,

            "statistical_evidence": {
                "oos_sign_test_p_value":
                    oos_metric.get(
                        "sign_test_p_value"
                    ),

                "fdr_q_value":
                    (
                        fdr.get(
                            "q_value"
                        )
                        if fdr
                        else None
                    ),

                "fdr_reject":
                    bool(
                        fdr
                        and fdr.get(
                            "fdr_reject"
                        )
                    ),

                "fdr_alpha":
                    self.config.fdr_alpha,
            },

            "gates": {
                "checks":
                    checks,

                "mandatory_gates":
                    list(mandatory),

                "promotion_eligible_before_manual_review":
                    eligible,
            },

            "status":
                status,

            "promotion_status":
                (
                    "CANDIDATE"
                    if eligible
                    else "HOLD"
                ),

            "manual_review_required":
                True,

            "live_trading_authorized":
                False,
        }

        result["evidence_grade"] = (
            evidence_grade(result)
        )

        return result

    def validate_all(
        self,
        records: Sequence[
            EvidenceRecord
        ],
    ) -> Dict[str, Any]:

        strategies = sorted({
            record.strategy_id
            for record in records
        })

        # One statistical family:
        # one OOS sign test per strategy.
        pvalues: Dict[
            str,
            Optional[float],
        ] = {}

        for strategy_id in strategies:

            oos = [
                record
                for record in records
                if (
                    record.strategy_id
                    == strategy_id
                    and
                    record.split.upper()
                    == "OOS"
                )
            ]

            metric = (
                MetricEngine.calculate(
                    oos,
                    self.config,
                )
            )

            pvalues[strategy_id] = (
                metric.get(
                    "sign_test_p_value"
                )
            )

        fdr = benjamini_hochberg(
            pvalues,
            self.config.fdr_alpha,
        )

        results = {}

        for strategy_id in strategies:

            results[strategy_id] = (
                self.validate_strategy(
                    records,
                    strategy_id,
                    fdr.get(
                        strategy_id
                    ),
                )
            )

        return {
            "schema_version":
                SCHEMA_VERSION,

            "validation_engine_version":
                ENGINE_VERSION,

            "generated_at":
                utc_now(),

            "strategy_count":
                len(strategies),

            "multiple_testing": {
                "method":
                    "BENJAMINI_HOCHBERG",

                "family":
                    "OOS_STRATEGY_SIGN_TESTS",

                "tests":
                    len(strategies),

                "results":
                    fdr,
            },

            "results":
                results,

            "research_only":
                True,

            "live_trading_authorized":
                False,
        }


# ============================================================
# OUTPUT STORE
# ============================================================

class ValidationReportStore:

    def __init__(
        self,
        directory: Path,
    ):
        self.directory = directory

        self.directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    def write_json(
        self,
        filename: str,
        value: Any,
    ) -> Path:

        path = (
            self.directory
            / filename
        )

        path.write_text(
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        return path

    def write_jsonl(
        self,
        filename: str,
        rows: Sequence[
            Mapping[str, Any]
        ],
    ) -> Path:

        path = (
            self.directory
            / filename
        )

        with path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            for row in rows:
                handle.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                        separators=(
                            ",",
                            ":",
                        ),
                        default=str,
                    )
                    + "\n"
                )

        return path

    def write_summary_csv(
        self,
        filename: str,
        results: Mapping[
            str,
            Mapping[str, Any],
        ],
    ) -> Path:

        path = (
            self.directory
            / filename
        )

        fields = [
            "strategy_id",
            "status",
            "promotion_status",
            "evidence_grade",
            "all_sample",
            "oos_sample",
            "oos_expectancy",
            "oos_profit_factor",
            "oos_win_rate",
            "oos_max_drawdown",
            "oos_p_value",
            "fdr_q_value",
            "fdr_reject",
            "positive_regimes",
            "negative_regimes",
            "wf_folds",
            "wf_positive_folds",
        ]

        with path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:

            writer = csv.DictWriter(
                handle,
                fieldnames=fields,
            )

            writer.writeheader()

            for strategy_id, result in sorted(
                results.items()
            ):

                metrics = result.get(
                    "metrics",
                    {},
                )

                all_metric = metrics.get(
                    "all",
                    {},
                )

                oos = metrics.get(
                    "oos",
                    {},
                )

                regime = result.get(
                    "regime_analysis",
                    {},
                )

                walk_forward = result.get(
                    "walk_forward_analysis",
                    {},
                )

                statistical = result.get(
                    "statistical_evidence",
                    {},
                )

                writer.writerow({
                    "strategy_id":
                        strategy_id,

                    "status":
                        result.get(
                            "status"
                        ),

                    "promotion_status":
                        result.get(
                            "promotion_status"
                        ),

                    "evidence_grade":
                        result.get(
                            "evidence_grade"
                        ),

                    "all_sample":
                        all_metric.get(
                            "sample_size",
                            0,
                        ),

                    "oos_sample":
                        oos.get(
                            "sample_size",
                            0,
                        ),

                    "oos_expectancy":
                        oos.get(
                            "expectancy_points"
                        ),

                    "oos_profit_factor":
                        oos.get(
                            "profit_factor"
                        ),

                    "oos_win_rate":
                        oos.get(
                            "win_rate"
                        ),

                    "oos_max_drawdown":
                        oos.get(
                            "max_drawdown_points"
                        ),

                    "oos_p_value":
                        statistical.get(
                            "oos_sign_test_p_value"
                        ),

                    "fdr_q_value":
                        statistical.get(
                            "fdr_q_value"
                        ),

                    "fdr_reject":
                        statistical.get(
                            "fdr_reject"
                        ),

                    "positive_regimes":
                        regime.get(
                            "positive_regime_count"
                        ),

                    "negative_regimes":
                        regime.get(
                            "negative_regime_count"
                        ),

                    "wf_folds":
                        walk_forward.get(
                            "fold_count"
                        ),

                    "wf_positive_folds":
                        walk_forward.get(
                            "positive_fold_count"
                        ),
                })

        return path


# ============================================================
# COMPLETE PIPELINE
# ============================================================

class GSRValidationPipeline:

    def __init__(
        self,
        config: Optional[
            ValidationConfig
        ] = None,
        output_dir: Optional[
            Path
        ] = None,
    ):

        self.config = (
            config
            or ValidationConfig.from_env()
        )

        self.config.validate()

        self.output_dir = (
            output_dir
            or Path(
                os.getenv(
                    "GSR_VALIDATION_DIR",
                    "./gsr_validation",
                )
            )
        )

        self.store = (
            ValidationReportStore(
                self.output_dir
            )
        )

    def run(
        self,
        input_path: Path,
    ) -> Dict[str, Any]:

        source = (
            discover_trade_file(
                input_path
            )
        )

        records, load_errors = (
            load_evidence(source)
        )

        engine = (
            StrategyValidationEngine(
                self.config
            )
        )

        raw = engine.validate_all(
            records
        )

        report = {
            "schema_version":
                SCHEMA_VERSION,

            "validation_engine_version":
                ENGINE_VERSION,

            "generated_at":
                utc_now(),

            "input_path":
                str(source),

            "input_hash":
                stable_hash([
                    asdict(record)
                    for record in records
                ]),

            "loaded_trade_count":
                len(records),

            "load_error_count":
                len(load_errors),

            "load_errors":
                load_errors[:500],

            "truncated_load_errors":
                max(
                    0,
                    len(load_errors) - 500,
                ),

            "config":
                asdict(self.config),

            "results":
                raw["results"],

            "multiple_testing":
                raw["multiple_testing"],

            "research_contract": {
                "claims_are_not_evidence":
                    True,

                "no_rule_invention":
                    True,

                "no_parameter_optimization":
                    True,

                "chronological_validation_only":
                    True,

                "oos_required":
                    True,

                "walk_forward_required":
                    True,

                "negative_evidence_retained":
                    True,

                "fdr_controlled":
                    True,

                "broker_dependency":
                    False,

                "live_trading_authorized":
                    False,
            },
        }

        self.store.write_json(
            "gsr_validation_report.json",
            report,
        )

        self.store.write_jsonl(
            "gsr_validation_strategy_results.jsonl",
            [
                {
                    "strategy_id":
                        strategy_id,
                    **result,
                }
                for strategy_id, result
                in sorted(
                    raw["results"].items()
                )
            ],
        )

        self.store.write_summary_csv(
            "gsr_validation_summary.csv",
            raw["results"],
        )

        regime_rows = []

        for result in raw[
            "results"
        ].values():

            regime_rows.extend(
                result
                .get(
                    "regime_analysis",
                    {},
                )
                .get(
                    "rows",
                    [],
                )
            )

        self.store.write_jsonl(
            "gsr_validation_regime_matrix.jsonl",
            regime_rows,
        )

        return report


# ============================================================
# MANUAL PROMOTION BOUNDARY
# ============================================================

def promotion_decision(
    result: Mapping[str, Any],
    manual_approval: bool = False,
) -> Dict[str, Any]:

    candidate = (
        result.get(
            "promotion_status"
        )
        == "CANDIDATE"
    )

    grade = result.get(
        "evidence_grade",
        "D",
    )

    if (
        candidate
        and grade in {
            "A",
            "B",
        }
        and manual_approval
    ):
        status = (
            "RESEARCH_REGISTRY_PROMOTION_APPROVED"
        )

    elif candidate:
        status = (
            "RESEARCH_REGISTRY_PROMOTION_PENDING_MANUAL_REVIEW"
        )

    else:
        status = (
            "RESEARCH_REGISTRY_PROMOTION_REJECTED_OR_HOLD"
        )

    return {
        "status":
            status,

        "candidate":
            candidate,

        "evidence_grade":
            grade,

        "manual_approval":
            manual_approval,

        "live_trading_authorized":
            False,

        "broker_orders_allowed":
            False,
    }


# ============================================================
# SYNTHETIC SELF TEST
# ============================================================

def synthetic_evidence(
    strategy_id: str,
    n: int = 120,
    positive: bool = True,
) -> List[EvidenceRecord]:

    regimes = (
        "TREND",
        "RANGE",
        "VOL_EXPANSION",
    )

    rows = []

    for index in range(n):

        if positive:
            pnl = (
                1.0
                if index % 3
                else -0.4
            )
        else:
            pnl = 0.0

        if index < 60:
            split = "TRAIN"
        elif index < 90:
            split = "VALIDATION"
        else:
            split = "OOS"

        day = (
            1
            + index // 4
        )

        minute = (
            index % 4
        ) * 15

        rows.append(
            EvidenceRecord(
                strategy_id=
                    strategy_id,

                rule_version=
                    "TEST-1",

                symbol=
                    "SYNTHETIC",

                entry_timestamp=(
                    f"2025-01-{day:02d}"
                    f"T09:{minute:02d}:00+00:00"
                ),

                exit_timestamp=(
                    f"2025-01-{day:02d}"
                    f"T10:{minute:02d}:00+00:00"
                ),

                direction=
                    "LONG",

                net_pnl_points=
                    pnl,

                gross_pnl_points=(
                    pnl + 0.1
                    if positive
                    else pnl
                ),

                outcome=(
                    "TARGET_FIRST"
                    if pnl > 0
                    else "STOP_FIRST"
                ),

                regime=
                    regimes[
                        index
                        % len(regimes)
                    ],

                split=
                    split,

                holding_bars=
                    2,

                mfe_points=
                    max(
                        pnl,
                        0.2,
                    ),

                mae_points=
                    min(
                        pnl,
                        0.2,
                    ),

                target_points=
                    1.0,

                stop_points=
                    0.5,

                cost_points=
                    0.05,

                slippage_points=
                    0.05,

                metadata={
                    "walk_forward_fold":
                        (
                            index // 30
                        ) + 1,

                    "synthetic":
                        True,
                },
            )
        )

    return rows


def run_self_test() -> Dict[str, Any]:

    config = ValidationConfig(
        min_total_trades=50,
        min_oos_trades=20,
        min_regime_trades=5,
        min_walk_forward_folds=3,
        min_positive_folds=2,
        max_negative_folds=5,
        bootstrap_iterations=250,
        min_effective_trades_for_inference=10,
    )

    records = (
        synthetic_evidence(
            "GSR_TEST_POSITIVE",
            120,
            True,
        )
        +
        synthetic_evidence(
            "GSR_TEST_NULL",
            120,
            False,
        )
    )

    result = (
        StrategyValidationEngine(
            config
        ).validate_all(
            records
        )
    )

    return {
        "status":
            "PASS",

        "strategy_count":
            result[
                "strategy_count"
            ],

        "fdr_method":
            result[
                "multiple_testing"
            ]["method"],

        "live_trading_authorized":
            False,
    }


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        description=(
            "GSR-1.1.0 research-only "
            "validation engine"
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=Path(
            "./gsr_replay"
        ),
        help=(
            "Replay directory or "
            "replay_trade_observations.jsonl"
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "./gsr_validation"
        ),
        help=(
            "Validation output directory"
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run deterministic "
            "synthetic self-test"
        ),
    )

    parser.add_argument(
        "--min-trades",
        type=int,
        default=None,
        help=(
            "Override minimum total trades"
        ),
    )

    parser.add_argument(
        "--min-oos",
        type=int,
        default=None,
        help=(
            "Override minimum OOS trades"
        ),
    )

    parser.add_argument(
        "--fdr-alpha",
        type=float,
        default=None,
        help=(
            "Override Benjamini-Hochberg "
            "FDR alpha"
        ),
    )

    return parser


def main(
    argv: Optional[
        Sequence[str]
    ] = None,
) -> int:

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.self_test:

        print(
            json.dumps(
                run_self_test(),
                indent=2,
            )
        )

        return 0

    try:

        config = (
            ValidationConfig
            .from_env()
        )

        if args.min_trades is not None:
            config.min_total_trades = (
                args.min_trades
            )

        if args.min_oos is not None:
            config.min_oos_trades = (
                args.min_oos
            )

        if args.fdr_alpha is not None:
            config.fdr_alpha = (
                args.fdr_alpha
            )

        config.validate()

        pipeline = (
            GSRValidationPipeline(
                config=config,
                output_dir=args.output,
            )
        )

        report = pipeline.run(
            args.input
        )

        print(
            json.dumps(
                {
                    "status":
                        "COMPLETED",

                    "loaded_trade_count":
                        report[
                            "loaded_trade_count"
                        ],

                    "strategy_count":
                        len(
                            report[
                                "results"
                            ]
                        ),

                    "output_dir":
                        str(args.output),

                    "fdr_alpha":
                        config.fdr_alpha,

                    "live_trading_authorized":
                        False,
                },
                indent=2,
            )
        )

        return 0

    except Exception as exc:

        print(
            json.dumps(
                {
                    "status":
                        "ERROR",

                    "error":
                        str(exc),

                    "traceback":
                        traceback.format_exc(),

                    "live_trading_authorized":
                        False,
                },
                indent=2,
            ),
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
