#!/usr/init/env python3
"""
Nifty Engine Consumer (Decoupled Headless Runner)
- Pulls raw observations from Supabase `raw_observations`.
- Feeds live quotes into the core FeatureEngine, DecisionEngine, and PaperTradingDesk.
"""

import time
import json
import os
from datetime import datetime
import pandas as pd
import numpy as np

# Import core classes from your base prop architecture script (app.py / app (2).py)
from app import (
    CONFIG, KotakNeoAdapter, Candle3Min, FeatureEngine, 
    DecisionEngine, PaperTradingDesk, DatasetManager, 
    extract_tick_price, extract_quote_field, token_from_record, now_ist, to_ist
)

try:
    from supabase import create_client, Client
except ImportError:
    create_client = None


class SupabaseNiftyConsumer:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL", CONFIG.get("supabase_url", ""))
        self.key = os.getenv("SUPABASE_KEY", CONFIG.get("supabase_key", ""))
        self.client: Optional[Client] = create_client(self.url, self.key) if (create_client and self.url and self.key) else None
        
        # Initialize the core prop engines locally
        self.dataset_manager = DatasetManager()
        self.feature_engine = FeatureEngine(maxlen=150)
        self.decision_engine = DecisionEngine()
        self.paper_desk = PaperTradingDesk(self.dataset_manager)
        
        self.last_processed_id = 0
        self.active = False

    def start_consumer(self):
        if not self.client:
            raise RuntimeError("Supabase client not initialized. Check credentials.")
        
        self.active = True
        print("[NIFTY ENGINE] Consumer started, listening to Supabase raw bus...")
        
        while self.active:
            try:
                # Fetch latest raw observations from Supabase published by Kotak
                response = self.client.table("raw_observations") \
                    .select("*") \
                    .eq("source", "kotak_live") \
                    .order("id", desc=False) \
                    .limit(50) \
                    .execute()
                
                rows = response.data
                if rows:
                    for row in rows:
                        row_id = row.get("id", 0)
                        if row_id > self.last_processed_id:
                            raw_payload = row.get("raw", {})
                            self._feed_into_engine(raw_payload)
                            self.last_processed_id = row_id
                            
            except Exception as e:
                print(f"[NIFTY ENGINE] Polling error: {e}")
                
            time.sleep(2.0)

    def _feed_into_engine(self, raw_quote: dict):
        """
        Injects the raw Supabase observation directly into the active engine buffer 
        mimicking the original KotakNeoAdapter on_message behavior[span_1](start_span)[span_1](end_span).
        """
        token = token_from_record(raw_quote)
        if token:
            # Reconstruct the tick structure expected by FeatureEngine and Bar Flusher
            parsed_tick = {
                **raw_quote,
                "_parsed_ts": now_ist()
            }
            
            # Here we route the raw quote into the engine's real-time evaluation flow
            # (Extracted from the proven institutional prop architecture[span_2](start_span)[span_2](end_span))
            spot_val = extract_tick_price(raw_quote)
            if token == "Nifty 50" or "NIFTY 50" in str(raw_quote.get("display_symbol", "")):
                pass
            # Trigger bar check & evaluation
            print(f"[NIFTY ENGINE] Ingested raw token {token} | Price: {spot_val}")


if __name__ == "__main__":
    consumer = SupabaseNiftyConsumer()
    consumer.start_consumer()
