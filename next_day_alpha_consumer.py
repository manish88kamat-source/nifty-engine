#!/usr/bin/env python3
"""
Next-Day Alpha Engine Consumer (Supabase Bridge)
- Pulls raw observations from Supabase `raw_observations` (Kotak live + Yahoo macro).
- Writes/syncs them to the local `SHARED_RAW_CACHE_DIR` jsonl files in the exact format
  expected by the Next-Day Alpha Engine.
- Maintains strict cross-engine isolation (no scores or opinions cross this boundary).
"""

import time
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    from supabase import create_client, Client
except ImportError:
    create_client = None

# Shared raw cache directory utilized by Next-Day Alpha engine
SHARED_RAW_CACHE_DIR = Path(os.getenv("SHARED_RAW_CACHE_DIR", "./shared_raw_cache"))
SHARED_RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def shared_raw_path(symbol: str, date_string: Optional[str] = None) -> Path:
    date_string = date_string or datetime.now().strftime("%Y%m%d")
    safe = str(symbol).replace("/", "_").replace("&", "_").replace(" ", "_").upper()
    return SHARED_RAW_CACHE_DIR / f"{safe}_{date_string}_raw.jsonl"


class SupabaseNextDayAlphaBridge:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL", "")
        self.key = os.getenv("SUPABASE_KEY", "")
        self.client: Optional[Client] = create_client(self.url, self.key) if (create_client and self.url and self.key) else None
        self.last_processed_id = 0
        self.active = False

    def start_bridge(self):
        if not self.client:
            raise RuntimeError("Supabase client not initialized. Check Supabase credentials.")
        
        self.active = True
        print("[NEXT-DAY ALPHA BRIDGE] Started, syncing Supabase raw bus to shared raw cache...")
        
        while self.active:
            try:
                # Fetch latest raw observations from Supabase incrementally
                response = self.client.table("raw_observations") \
                    .select("*") \
                    .order("id", desc=False) \
                    .gt("id", self.last_processed_id) \
                    .limit(100) \
                    .execute()
                
                rows = response.data
                if rows:
                    for row in rows:
                        row_id = row.get("id", 0)
                        self._sync_to_shared_cache(row)
                        self.last_processed_id = row_id
                                    
            except Exception as e:
                print(f"[NEXT-DAY ALPHA BRIDGE] Polling error: {e}")
                
            time.sleep(2.0)

    def _sync_to_shared_cache(self, row: dict):
        symbol = row.get("symbol")
        raw = row.get("raw", {})
        obs_timestamp = row.get("observation_timestamp")
        source = row.get("source")

        if not symbol or not obs_timestamp:
            return

        # Normalize raw fields to match what Next-Day engine's shared raw boundary expects
        normalized = self._normalize_raw(source, symbol, raw, obs_timestamp)
        if not normalized:
            return

        # Partition by date string (YYYYMMDD) derived from timestamp
        date_str = obs_timestamp[:10].replace("-", "")
        path = shared_raw_path(symbol, date_str)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(normalized, ensure_ascii=False) + "\n")
            print(f"[NEXT-DAY ALPHA BRIDGE] Synced raw observation for {symbol} from {source}")
        except Exception as e:
            print(f"[NEXT-DAY ALPHA BRIDGE] Failed to write cache for {symbol}: {e}")

    def _normalize_raw(self, source: str, symbol: str, raw: dict, timestamp: str) -> dict:
        """
        Strips out any forbidden opinion/score fields to comply with the 
        Next-Day Alpha engine's strict raw-only isolation contract.
        """
        if source == "yahoo_macro":
            return {
                "timestamp": timestamp,
                "symbol": symbol,
                "ltp": raw.get("Close") or raw.get("close"),
                "open": raw.get("Open") or raw.get("open"),
                "high": raw.get("High") or raw.get("high"),
                "low": raw.get("Low") or raw.get("low"),
                "close": raw.get("Close") or raw.get("close"),
                "volume": raw.get("Volume") or raw.get("volume"),
            }
        elif source == "kotak_live":
            return {
                "timestamp": timestamp,
                "symbol": symbol,
                "ltp": raw.get("lp") or raw.get("ltp") or raw.get("close") or raw.get("c"),
                "open": raw.get("open") or raw.get("o"),
                "high": raw.get("high") or raw.get("h"),
                "low": raw.get("low") or raw.get("l"),
                "close": raw.get("ltp") or raw.get("close") or raw.get("c"),
                "volume": raw.get("volume") or raw.get("v") or raw.get("last_volume"),
                "oi": raw.get("oi") or raw.get("open_interest"),
                "upper_circuit": raw.get("upper_circuit") or raw.get("upper_price_band"),
                "lower_circuit": raw.get("lower_circuit") or raw.get("lower_price_band"),
            }
        return {}


if __name__ == "__main__":
    bridge = SupabaseNextDayAlphaBridge()
    bridge.start_bridge()
