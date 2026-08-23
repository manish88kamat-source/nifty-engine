"""
GSR V1.2.1 â€” Deterministic Behavioral Regression Suite

TEST HARNESS ONLY. It imports the repository's gsr_engine.py as source-of-truth
and does not modify production code or production data.

Run from repository root:
    python test_gsr_v121_regression.py
"""
from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import gsr_engine as gsr


def bar(timestamp, *, symbol="REGTEST", close=100.0, volume=100.0,
        high=None, low=None, bid=None, ask=None, oi=None):
    high = close + 1.0 if high is None else high
    low = close - 1.0 if low is None else low
    row = {"timestamp": timestamp, "symbol": symbol, "open": close,
           "high": high, "low": low, "close": close}
    if volume is not None: row["volume"] = volume
    if bid is not None: row["bid"] = bid
    if ask is not None: row["ask"] = ask
    if oi is not None: row["oi"] = oi
    return row


class GSRV121RegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gsr_v121_regression_")
        self.addCleanup(self.tmp.cleanup)
        self.cfg = gsr.GSRConfig(
            data_dir=Path(self.tmp.name), max_bars_per_symbol=5000,
            regime_min_history=5, purge_bars=2, embargo_bars=2,
            timezone_name="Asia/Kolkata", decay_window=10,
            decay_baseline_window=20,
        )
        self.engine = gsr.GSREngine(self.cfg)

    # Session / timezone / VWAP
    def test_01_timezone_session_key(self):
        self.assertEqual(
            gsr.session_key("2026-01-02T03:45:00+00:00",
                            timezone_name="Asia/Kolkata"),
            "2026-01-02|09:15-15:30")

    def test_02_out_of_session_not_in_vwap(self):
        self.engine.ingest_snapshot(bar("2026-01-02T08:00:00+05:30", close=500, volume=1000))
        second = self.engine.ingest_snapshot(bar("2026-01-02T09:15:00+05:30", close=100, volume=100))
        self.assertAlmostEqual(second["features"]["session_vwap"], 100.0, places=9)

    def test_03_session_vwap_resets_across_days(self):
        self.engine.ingest_snapshot(bar("2026-01-01T09:15:00+05:30", close=100, volume=100))
        self.engine.ingest_snapshot(bar("2026-01-01T09:18:00+05:30", close=110, volume=100))
        day2 = self.engine.ingest_snapshot(bar("2026-01-02T09:15:00+05:30", close=200, volume=100))
        self.assertAlmostEqual(day2["features"]["session_vwap"], 200.0, places=9)

    def test_04_5000_bar_history_cannot_contaminate_vwap(self):
        # 5000 historical bars are enough to stress the rolling history.
        start = date(2012, 1, 1)
        for i in range(5000):
            d = start + timedelta(days=i)
            ts = f"{d.isoformat()}T08:00:00+05:30"
            self.engine.ingest_snapshot(bar(ts, symbol="LONG_HISTORY", close=1000, volume=1000))
        current = self.engine.ingest_snapshot(
            bar("2026-01-02T09:15:00+05:30", symbol="LONG_HISTORY", close=200, volume=100)
        )
        self.assertAlmostEqual(current["features"]["session_vwap"], 200.0, places=9)

    # Probabilistic regime
    def test_05_regime_probabilities_normalized(self):
        last = None
        for i in range(10):
            last = self.engine.ingest_snapshot(
                bar(f"2026-01-02T09:{15+i:02d}:00+05:30", close=100+i))
        regime = last["regime"]
        self.assertEqual(set(regime["probabilities"]), {
            "TREND_UP", "TREND_DOWN", "RANGE", "HIGH_VOL_TRANSITION", "UNKNOWN"})
        self.assertLess(abs(sum(regime["probabilities"].values()) - 1.0), 2e-6)

    def test_06_regime_persistence_is_session_scoped(self):
        for i in range(6):
            self.engine.ingest_snapshot(bar(f"2026-01-01T09:{15+i:02d}:00+05:30", symbol="R", close=100+i))
        for i in range(6):
            self.engine.ingest_snapshot(bar(f"2026-01-02T09:{15+i:02d}:00+05:30", symbol="R", close=200+i))
        keys = self.engine.regime_engine.prev_probs.keys()
        self.assertTrue(keys)
        self.assertTrue(all(isinstance(k, tuple) and len(k) == 2 for k in keys))
        self.assertGreaterEqual(len(keys), 2)

    # Dynamic execution cost / impact
    def test_07_oi_is_not_executable_liquidity(self):
        snap = gsr.MarketSnapshot.from_mapping(
            bar("2026-01-02T09:18:00+05:30", volume=None, oi=100000))
        cost = self.engine.cost_model.estimate(snap, {"atr": 1.0, "atr_pct": 0.005}, order_size=100)
        self.assertEqual(cost["liquidity_source"], "UNAVAILABLE")
        self.assertEqual(cost["participation_rate"], 0.0)

    def test_08_impact_increases_with_order_size(self):
        snap = gsr.MarketSnapshot.from_mapping(
            bar("2026-01-02T09:18:00+05:30", close=200, volume=1000, bid=199.9, ask=200.1))
        features = {"atr": 1.0, "atr_pct": 0.005}
        small = self.engine.cost_model.estimate(snap, features, order_size=10)
        large = self.engine.cost_model.estimate(snap, features, order_size=500)
        self.assertGreaterEqual(large["market_impact_points"], small["market_impact_points"])
        self.assertGreaterEqual(large["participation_rate"], small["participation_rate"])

    def test_09_cost_increases_with_volatility(self):
        snap = gsr.MarketSnapshot.from_mapping(
            bar("2026-01-02T09:18:00+05:30", close=200, volume=1000, bid=199.9, ask=200.1))
        normal = self.engine.cost_model.estimate(snap, {"atr": 1.0, "atr_pct": 0.005}, order_size=100)
        stressed = self.engine.cost_model.estimate(snap, {"atr": 4.0, "atr_pct": 0.020}, order_size=100)
        self.assertGreaterEqual(stressed["estimated_slippage_points"], normal["estimated_slippage_points"])

    def test_10_missing_liquidity_is_degraded(self):
        snap = gsr.MarketSnapshot.from_mapping(
            bar("2026-01-02T09:18:00+05:30", volume=None))
        cost = self.engine.cost_model.estimate(snap, {"atr": 1.0, "atr_pct": 0.005}, order_size=100)
        self.assertEqual(cost["quality"], "DEGRADED")

    # Intrabar realism
    def test_11_both_stop_target_touch_is_ambiguous(self):
        result = self.engine.intrabar.evaluate(
            {"high": 110, "low": 90}, 100, 95, 105, "LONG", policy="CONSERVATIVE")
        self.assertTrue(result["ambiguous"])
        self.assertEqual(result["outcome"], "STOP_FIRST")

    def test_12_lower_timeframe_can_resolve_path(self):
        result = self.engine.intrabar.evaluate(
            {"high": 110, "low": 90}, 100, 95, 105, "LONG",
            lower_bars=[
                {"timestamp": "2026-01-02T09:15:00+05:30", "high": 106, "low": 100},
                {"timestamp": "2026-01-02T09:16:00+05:30", "high": 104, "low": 99},
            ],
            parent_start="2026-01-02T09:15:00+05:30",
            parent_end="2026-01-02T09:18:00+05:30")
        self.assertEqual(result["outcome"], "TARGET_FIRST")
        self.assertFalse(result["ambiguous"])

    def test_13_invalid_lower_timeframe_is_rejected(self):
        with self.assertRaises(ValueError):
            self.engine.intrabar.evaluate(
                {"high": 110, "low": 90}, 100, 95, 105, "LONG",
                lower_bars=[{"timestamp": "2026-01-02T09:19:00+05:30", "high": 106, "low": 94}],
                parent_start="2026-01-02T09:15:00+05:30",
                parent_end="2026-01-02T09:18:00+05:30")

    # Chronology / purge / embargo
    def test_14_out_of_order_ingestion_rejected(self):
        self.engine.ingest_snapshot(bar("2026-01-02T09:18:00+05:30", close=101))
        with self.assertRaises(ValueError):
            self.engine.ingest_snapshot(bar("2026-01-02T09:15:00+05:30", close=100))

    def test_15_duplicate_timestamp_rejected(self):
        ts = "2026-01-02T09:18:00+05:30"
        self.engine.ingest_snapshot(bar(ts, close=101))
        with self.assertRaises(ValueError):
            self.engine.ingest_snapshot(bar(ts, close=102))

    def test_16_event_aware_purge_and_embargo(self):
        rows = []
        for i in range(10):
            minute = 15 + i
            rows.append({
                "timestamp": f"2026-01-02T09:{minute:02d}:00+05:30",
                "event_end_time": f"2026-01-02T09:{minute:02d}:00+05:30"})
        rows[6]["event_end_time"] = "2026-01-02T09:30:00+05:30"
        split = self.engine.validation_engine.chronological_split(rows, train_fraction=0.70)
        self.assertEqual(len(split["embargo"]), 2)
        self.assertEqual(split["purge_mode"], "EVENT_AWARE_PLUS_FIXED_BAR_GAP")
        self.assertTrue(len(split["purged"]) >= 2)
        self.assertTrue(len(split["train"]) < 7)

    # Portfolio correlation
    def test_17_portfolio_correlation_is_session_synchronous(self):
        for i, day in enumerate(["2026-01-01", "2026-01-02", "2026-01-03"]):
            self.engine.validation_store.append({
                "strategy_id": "A", "entry_time": day+"T09:15:00+05:30", "net_pnl_points": float(i+1)})
            self.engine.validation_store.append({
                "strategy_id": "B", "entry_time": day+"T10:15:00+05:30", "net_pnl_points": float(2*(i+1))})
        matrix = self.engine.portfolio.correlation_matrix()
        corr = matrix["correlation_matrix"]["A"]["B"]
        self.assertIsNotNone(corr)
        self.assertGreater(corr, 0.99)

    def test_18_insufficient_correlation_is_none(self):
        self.engine.validation_store.append({
            "strategy_id": "A", "entry_time": "2026-01-01T09:15:00+05:30", "net_pnl_points": 1.0})
        self.engine.validation_store.append({
            "strategy_id": "B", "entry_time": "2026-01-01T10:15:00+05:30", "net_pnl_points": 1.0})
        matrix = self.engine.portfolio.correlation_matrix()
        self.assertIsNone(matrix["correlation_matrix"]["A"]["B"])

    # Decay monitor
    def test_19_decay_baseline_excludes_short_window(self):
        for i in range(30):
            pnl = 1.0 if i < 20 else -1.0
            self.engine.validation_store.append({
                "strategy_id": "DECAY",
                "exit_time": f"2026-02-01T{9 + (15+i)//60:02d}:{(15+i)%60:02d}:00+05:30",
                "net_pnl_points": pnl})
        report = self.engine.decay_monitor.evaluate("DECAY")
        self.assertEqual(report["sample_short"], 10)
        self.assertEqual(report["sample_baseline"], 20)
        self.assertAlmostEqual(report["baseline_mean"], 1.0, places=9)
        self.assertTrue(report["decay_detected"])

    def test_20_decay_insufficient_data_no_alert(self):
        for i in range(15):
            self.engine.validation_store.append({
                "strategy_id": "SHORT_SAMPLE",
                "exit_time": f"2026-02-01T09:{15+i:02d}:00+05:30",
                "net_pnl_points": -1.0})
        report = self.engine.decay_monitor.evaluate("SHORT_SAMPLE")
        self.assertEqual(report["status"], "INSUFFICIENT_DATA")
        self.assertFalse(report["decay_detected"])

    # Isolation / no invented rules
    def test_21_external_opinion_fields_rejected(self):
        forbidden = [
            "alpha", "alpha_score", "confidence", "prediction", "signal",
            "external_regime", "regime_label", "position", "weight",
            "decision", "trade_decision", "target", "stop", "forecast"]
        for field in forbidden:
            payload = bar("2026-01-02T09:15:00+05:30", symbol="ISO_"+field)
            payload[field] = 1.0
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    self.engine.ingest_snapshot(payload)

    def test_22_validation_requires_registered_rule_spec(self):
        with self.assertRaises(ValueError):
            self.engine.register_validation_outcome(
                strategy_id=self.engine.registry[0]["atomic_strategy_id"],
                symbol="NIFTY", entry_time="2026-01-02T09:15:00+05:30",
                exit_time="2026-01-02T09:18:00+05:30", direction="LONG",
                entry_price=100.0, exit_price=101.0, regime="TREND_UP")

    # Registry / health
    def test_23_registry_audit_clean(self):
        audit = self.engine.registry_audit()
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["duplicate_ids"], [])
        self.assertEqual(audit["missing_ids"], [])
        self.assertEqual(audit["missing_dna_hashes"], [])
        self.assertEqual(audit["strategy_count"], 113)

    def test_24_health_research_only(self):
        health = self.engine.health()
        self.assertEqual(health["engine_version"], gsr.ENGINE_VERSION)
        self.assertFalse(health["execution_enabled"])
        self.assertTrue(health["isolation_ok"])
        for key in (
            "session_local_vwap", "dynamic_impact_slippage",
            "probabilistic_regime", "intrabar_realism",
            "portfolio_correlation", "decay_monitor",
            "purge_embargo_validation"):
            self.assertTrue(health["features"][key], key)

    # Engine's own regression suite
    def test_25_engine_internal_regression_suite(self):
        result = gsr.regression_tests()
        self.assertTrue(result["ok"], msg=repr(result))
        self.assertTrue(all(result["tests"].values()), msg=repr(result["tests"]))


def main():
    print("=" * 72)
    print("GSR V1.2.1 â€” DETERMINISTIC BEHAVIORAL REGRESSION SUITE")
    print("=" * 72)
    print("Source of truth: gsr_engine.py")
    print("Production data touched: NO")
    print("Broker/network access: NO")
    print()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GSRV121RegressionTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print()
    print("-" * 72)
    print(f"TESTS RUN : {result.testsRun}")
    print(f"FAILURES  : {len(result.failures)}")
    print(f"ERRORS    : {len(result.errors)}")
    print(f"SKIPPED   : {len(result.skipped)}")
    print("-" * 72)
    if result.wasSuccessful():
        print("GSR V1.2.1 BEHAVIORAL REGRESSION: PASS")
        return 0
    print("GSR V1.2.1 BEHAVIORAL REGRESSION: FAIL")
    print("Do NOT patch blindly. Inspect the first failing test and source path.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
