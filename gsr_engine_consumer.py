#!/usr/bin/env python3
"""
GSR Engine Consumer (Decoupled Global Strategy Research Runner)
- Pulls raw observations from Supabase `raw_observations` (Kotak live + Yahoo macro).
- Safely maps raw bus payloads into MarketSnapshots.
- Feeds them into GSREngine (gsr_engine_v1_2.py) while preserving strict isolation contracts.
"""

import time
import os
from datetime import datetime
from typing import Optional

from gsr_engine_v1_2 import GSREngine, GSRConfig, MarketSnapshot

try:
    from supabase import create_client, Client
except ImportError:
    create_client = None


class SupabaseGSRConsumer:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL", "")
        self.key = os.getenv("SUPABASE_KEY", "")
        self.client: Optional[Client] = create_client(self.url, self.key) if (create_client and self.url and self.key) else None
        
        # Initialize the isolated GSR Research Engine
        self.gsr_engine = GSREngine()
        self.last_processed_id = 0
        self.active = False

    def start_consumer(self):
        if not self.client:
            raise RuntimeError("Supabase client not initialized. Check Supabase credentials.")
        
        self.active = True
        print("[GSR ENGINE] Research consumer started, listening to Supabase raw bus...")
        
        while self.active:
            try:
                # Fetch latest raw observations from Supabase published by the raw producer
                response = self.client.table("raw_observations") \
                    .select("*") \
                    .order("id", desc=False) \
                    .gt("id", self.last_processed_id) \
                    .limit(50) \
                    .execute()
                
                rows = response.data
                if rows:
                    for row in rows:
                        row_id = row.get("id", 0)
                        self._process_observation(row)
                        self.last_processed_id = row_id
                                
            except Exception as e:
                print(f"[GSR ENGINE] Polling/Ingestion error: {e}")
                
            time.sleep(3.0)

    def _process_observation(self, row: dict):
        source = row.get("source")
        symbol = row.get("symbol")
        raw = row.get("raw", {})
        obs_timestamp = row.get("observation_timestamp")

        # Map database raw payload into GSR MarketSnapshot dictionary
        mapped_data = self._map_to_snapshot_format(source, symbol, raw, obs_timestamp)
        if not mapped_data or not mapped_data.get("close"):
            return

        try:
            # Ingest into the GSR Research Engine safely (respects FORBIDDEN_EXTERNAL_OPINION_FIELDS)
            result = self.gsr_engine.ingest_snapshot(mapped_data)
            regime_name = result.get("regime", {}).get("regime", "UNKNOWN")
            print(f"[GSR ENGINE] Ingested [{source}] {symbol} | Close: {mapped_data['close']} | Regime: {regime_name}")
        except Exception as exc:
            print(f"[GSR ENGINE] Snapshot rejection for {symbol}: {exc}")

    def _map_to_snapshot_format(self, source: str, symbol: str, raw: dict, timestamp: str) -> dict:
        """
        Maps raw database payloads into strict MarketSnapshot attributes required by GSR-1.2.1.
        Strips out any external decision or opinion fields to fully comply with GSR isolation.
        """
        if not timestamp:
            return {}

        if source == "yahoo_macro":
            op = float(raw.get("Open") or raw.get("open") or raw.get("Close") or raw.get("close") or 0.0)
            hi = float(raw.get("High") or raw.get("high") or op)
            lo = float(raw.get("Low") or raw.get("low") or op)
            cl = float(raw.get("Close") or raw.get("close") or op)
            vol = float(raw.get("Volume") or raw.get("volume") or 0.0)
            
            return {
                "timestamp": timestamp,
                "symbol": symbol,
                "open": op,
                "high": hi,
                "low": lo,
                "close": cl,
                "volume": vol if vol > 0 else None
            }

        elif source == "kotak_live":
            ltp = float(raw.get("lp") or raw.get("ltp") or raw.get("close") or raw.get("c") or 0.0)
            if ltp <= 0:
                return {}
            op = float(raw.get("open") or raw.get("o") or ltp)
            hi = float(raw.get("high") or raw.get("h") or ltp)
            lo = float(raw.get("low") or raw.get("l") or ltp)
            vol = float(raw.get("volume") or raw.get("v") or raw.get("last_volume") or 0.0)
            oi = float(raw.get("oi") or raw.get("open_interest") or 0.0)

            return {
                "timestamp": timestamp,
                "symbol": symbol,
                "open": op,
                "high": hi,
                "low": lo,
                "close": ltp,
                "volume": vol if vol > 0 else None,
                "oi": oi if oi > 0 else None
            }

        return {}


if __name__ == "__main__":
    consumer = SupabaseGSRConsumer()
    consumer.start_consumer()
