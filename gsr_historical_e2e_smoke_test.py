"""GSR historical path end-to-end smoke test.

Validates the repository wiring without broker/network access:
CSV -> historical replay loader -> normalization -> GSR engine.
Also verifies the explicit LIVE_SHADOW/HISTORICAL_REPLAY isolation boundary.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from gsr_engine import GSRConfig, GSREngine
from gsr_historical_replay import HistoricalReplayEngine, ReplayConfig
from gsr_live_bridge import BridgeConfig, GSRLiveBridge
from gsr_data_adapter import GSRDataAdapter
from gsr_data_store import GSRRawDataStore


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="gsr_hist_e2e_") as td:
        root = Path(td)
        source = root / "sample.csv"
        source.write_text(
            "timestamp,symbol,open,high,low,close,volume\n"
            "2026-01-02T09:15:00+05:30,NIFTY,100,101,99,100.5,1000\n"
            "2026-01-02T09:18:00+05:30,NIFTY,100.5,102,100,101.5,1200\n"
            "2026-01-02T09:21:00+05:30,NIFTY,101.5,103,101,102.5,1300\n",
            encoding="utf-8",
        )

        # 1) Explicit historical replay path.
        replay = HistoricalReplayEngine(ReplayConfig(replay_dir=root / "replay"))
        loaded_count = replay.load(source)
        loaded = list(replay.market_rows)
        if loaded_count != 3 or len(loaded) != 3:
            raise AssertionError(
                f"expected 3 historical rows, got count={loaded_count}, rows={len(loaded)}"
            )

        # 2) Direct adapter -> durable raw store -> engine path.
        engine = GSREngine(GSRConfig(data_dir=root / "engine_data"))
        adapter = GSRDataAdapter(engine)
        store = GSRRawDataStore(root / "raw.sqlite")
        bridge = GSRLiveBridge(
            engine=engine,
            adapter=adapter,
            store=store,
            config=BridgeConfig(mode="HISTORICAL_REPLAY"),
        )
        results = bridge.replay(loaded, stop_on_error=True)
        if results["accepted"] != 3:
            raise AssertionError(f"expected 3 accepted rows, got {results['accepted']}")

        # 3) The same source must not silently enter live mode.
        live_engine = GSREngine(GSRConfig(data_dir=root / "live_data"))
        live_bridge = GSRLiveBridge(
            engine=live_engine,
            adapter=GSRDataAdapter(live_engine),
            store=GSRRawDataStore(root / "live.sqlite"),
            config=BridgeConfig(mode="LIVE_SHADOW_RESEARCH"),
        )
        try:
            live_bridge.replay(loaded, stop_on_error=True)
        except RuntimeError as exc:
            if "HISTORICAL_REPLAY" not in str(exc):
                raise AssertionError(
                    f"unexpected live replay rejection: {exc}"
                ) from exc
        else:
            raise AssertionError("historical replay entered LIVE_SHADOW_RESEARCH mode")

        print("GSR HISTORICAL E2E: PASS")
        print(f"historical_rows={len(loaded)}")
        print(f"accepted={results['accepted']}")
        print(f"rejected={results['rejected']}")
        print("mode=HISTORICAL_REPLAY")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
