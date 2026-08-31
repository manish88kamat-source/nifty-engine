#!/usr/bin/env python3
"""
Persistent Kotak LIVE RAW Producer Worker
STEP 2 of the producer stabilization architecture.

Purpose
-------
Run Kotak LIVE polling independently of the Streamlit browser session and
publish raw observations to the existing Supabase RAW BUS.

Locked boundaries
-----------------
- Manual TOTP authentication only.
- Existing Kotak discovery / future / PCR mapping is preserved.
- Existing Supabase raw_observations contract is preserved.
- No engine / feature / scoring / regime / decision logic.
- No automatic TOTP or automatic re-login.

The worker writes a small JSON state file for the Streamlit monitor.
"""

from __future__ import annotations

import os

import re

import json

import time

import csv

import io

import base64

import hmac

import hashlib

import struct

from datetime import datetime, timezone, date

from pathlib import Path

from typing import Dict, List, Any, Optional, Tuple

from zoneinfo import ZoneInfo

import pandas as pd

import yfinance as yf

import requests

import importlib.util

import platform

import uuid

from concurrent.futures import ThreadPoolExecutor, as_completed

def now_ist() -> datetime:
    return datetime.now(IST)

def today_ist() -> date:
    return now_ist().date()

CONFIG = {
    "neo_environment": "prod",
    "pcr_strike_count": 5,
    "pcr_strike_step": 50.0,
    "supabase_url": os.getenv("SUPABASE_URL", "").strip(),
    "supabase_key": os.getenv("SUPABASE_KEY", "").strip(),
    "poll_interval_sec": 3.0,
    "macro_every_n_cycles": 10,
    # Raw-history coverage required by the three current engines.
    "next_day_daily_days": 320,
    "next_day_mtf_hourly_days": 180,
    "next_day_mtf_15m_days": 55,
    "next_day_vix_days": 320,
    "nifty_history_days": 320,
    "history_batch_size": 250,
    "history_workers": 6,
    "supabase_timeout_sec": 15,
}

def generate_live_totp(secret_or_otp: str) -> str:
    raw = str(secret_or_otp or "").strip().replace(" ", "").upper()
    if raw.isdigit() and len(raw) == 6:
        return raw
    try:
        if len(raw) % 8:
            raw += "=" * (8 - len(raw) % 8)
        key = base64.b32decode(raw, casefold=True)
        counter = int(time.time() // 30)
        msg = struct.pack(">Q", counter)
        digest = hmac.new(key, msg, hashlib.sha1).digest()
        offset = digest[19] & 15
        token = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000
        return f"{token:06d}"
    except Exception:
        return raw

def normalize_kotak_mobile(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    if len(digits) != 10:
        return raw
    return "+91" + digits

def env_or_secret(name, default=""):
    val = os.getenv(name, "")
    if val:
        return val
    if st is not None:
        try:
            val = st.secrets.get(name, "")
            if val:
                return str(val)
        except Exception:
            pass
    return default

class KotakConnector:
    def __init__(self):
        self.consumer_key = env_or_secret("KOTAK_CONSUMER_KEY")
        self.mobile = normalize_kotak_mobile(env_or_secret("KOTAK_MOBILE"))
        self.ucc = env_or_secret("KOTAK_UCC")
        self.totp_secret = env_or_secret("KOTAK_TOTP")
        self.mpin = env_or_secret("KOTAK_MPIN")

        self.client = None
        self.connected = False
        self.future_token = None
        self.future_symbol = None
        self.future_expiry: Optional[date] = None
        # Kotak Neo does not use Zerodha's numeric NIFTY spot token (26000).
        # Keep the real Kotak spot identifier separate from the ATM reference fallback.
        self.spot_token = "Nifty 50"
        self.atm_reference_price = None
        self.pcr_tokens = []
        self.option_contracts = {}
        self.nfo_records = []
        self.heavy_tokens = {
            "HDFCBANK": "1333", "RELIANCE": "2885", "ICICIBANK": "4963",
            "INFY": "1594", "ITC": "1660", "TCS": "11536",
            "LT": "11483", "AXISBANK": "5900", "KOTAKBANK": "1922", "SBIN": "3045"
        }
        self.logs = []

    def log(self, message: str):
        timestamp = now_ist().strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")
        self.logs = self.logs[-100:]

    def login(self, totp_override: str = "") -> bool:
        if NeoAPI is None:
            raise RuntimeError("neo_api_client library is not installed.")
        
        totp = totp_override.strip() or self.totp_secret
        if not all([self.consumer_key, self.mobile, self.ucc, totp, self.mpin]):
            raise RuntimeError("Missing Kotak Neo authentication credentials.")

        self.client = NeoAPI(environment=CONFIG["neo_environment"], consumer_key=self.consumer_key)
        
        step1 = self.client.totp_login(mobile_number=self.mobile, ucc=self.ucc, totp=generate_live_totp(totp))
        if isinstance(step1, dict) and (step1.get("error") or step1.get("Error")):
            raise RuntimeError(f"Login Step 1 Error: {step1.get('error') or step1.get('Error')}")
            
        step2 = self.client.totp_validate(mpin=self.mpin)
        if isinstance(step2, dict) and (step2.get("error") or step2.get("Error")):
            raise RuntimeError(f"Login Step 2 Error: {step2.get('error') or step2.get('Error')}")

        self.connected = True
        self.log("Kotak authentication successful.")
        return True

    def load_nfo_scrip_master(self) -> List[Dict[str, Any]]:
        if not self.connected:
            raise RuntimeError("Kotak connector is not authenticated.")

        # Primary: existing tested search_scrip path.
        records: List[Dict[str, Any]] = []
        try:
            response = self.client.search_scrip(
                exchange_segment="nse_fo",
                symbol="NIFTY",
            )
            records = extract_records(response)
        except Exception as exc:
            self.log(f"Primary search_scrip failed: {exc}")

        # Secondary: same tested path with alternate casing.
        if not records:
            try:
                response = self.client.search_scrip(
                    exchange_segment="nse_fo",
                    symbol="Nifty",
                )
                records = extract_records(response)
            except Exception as exc:
                self.log(f"Secondary search_scrip failed: {exc}")

        if records:
            self.nfo_records = records
            self.log(f"Total raw NFO scrip records retrieved: {len(records)}")
            return records

        # Surgical fallback only: Kotak's official scrip_master contract can
        # return either a direct CSV URL or a filesPaths list. Some deployments
        # have also returned the CSV inside an {"nse": "..."} JSON envelope.
        try:
            master_response = self.client.scrip_master(
                exchange_segment="nse_fo"
            )

            records = _extract_nfo_csv_payload(master_response)
            if records:
                self.nfo_records = records
                self.log(
                    f"NFO fallback payload parsed directly: {len(records)} records"
                )
                return records

            urls = _scrip_master_urls(master_response)
            self.log(f"NFO scrip_master fallback URLs discovered: {len(urls)}")

            nfo_urls = [u for u in urls if "nse_fo" in u.lower()]
            urls = nfo_urls or urls

            for url in urls:
                try:
                    # Match the official Kotak Neo v2 SDK behavior:
                    # scrip_master() authenticates the API call that resolves
                    # the URL; the returned CDN/file URL is downloaded directly.
                    response = requests.get(
                        url,
                        headers={
                            "Accept": "text/csv,application/json,*/*",
                        },
                        timeout=25,
                    )
                    self.log(
                        f"NFO scrip-master download: HTTP "
                        f"{response.status_code}, bytes={len(response.content)}"
                    )
                    if response.status_code >= 400:
                        continue

                    parsed = _extract_nfo_csv_payload(response.text)

                    if not parsed:
                        try:
                            parsed = _extract_nfo_csv_payload(response.json())
                        except Exception:
                            pass

                    if parsed:
                        self.nfo_records = parsed
                        self.log(
                            f"NFO scrip-master fallback parsed: "
                            f"{len(parsed)} raw records"
                        )
                        return parsed
                except Exception as exc:
                    self.log(f"NFO scrip-master URL fallback failed: {exc}")

        except Exception as exc:
            self.log(f"NFO scrip_master fallback failed: {exc}")

        raise RuntimeError(
            "Kotak NFO discovery returned no usable records after the "
            "tested search_scrip path and official scrip_master fallback."
        )

    def discover_instruments(self) -> bool:
        if not self.connected or not self.client:
            raise RuntimeError("Kotak connector is not authenticated.")
        
        self.logs.clear()
        self.future_token = None
        self.future_symbol = None
        self.future_expiry = None
        self.pcr_tokens = []
        self.option_contracts = {}

        records = self.load_nfo_scrip_master()

        # --------------------------------------------------------
        # COLLECT ALL NIFTY FUTURE CANDIDATES
        # --------------------------------------------------------
        candidates = []
        for r in records:
            if not isinstance(r, dict):
                continue
            
            sym = str(r.get("pTrdSymbol", r.get("tradingSymbol", r.get("ts", r.get("symbol", ""))) )).upper().strip()
            token = str(r.get("pSymbol", r.get("pSymbolToken", r.get("instrument_token", r.get("token", "")))))
            inst_type = str(r.get("pInstType", "")).upper()
            option_type = str(r.get("pOptionType", "")).upper()

            if not token or not sym:
                continue
            if token == "26000":
                continue
            if option_type in ("CE", "PE") or sym.endswith("CE") or sym.endswith("PE"):
                continue
            
            # Must strictly be NIFTY and NOT variants like NIFTYNXT, BANKNIFTY, FINNIFTY, MIDCPNIFTY
            if not sym.startswith("NIFTY") or sym.startswith("NIFTYNXT"):
                continue
            if any(x in sym for x in ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]):
                continue

            is_fut = ("FUT" in inst_type or "FUT" in sym or sym.endswith("FUT"))
            if is_fut:
                candidates.append((sym, token, r))

        if not candidates:
            raise RuntimeError("Active NIFTY future contract could not be discovered from scrip master.")

        # --------------------------------------------------------
        # SELECT NEAREST NON-EXPIRED NIFTY FUTURE
        # --------------------------------------------------------
        today = today_ist()
        parsed_candidates = []

        for sym, token, record in candidates:
            expiry_text = str(record.get("pExpiryDate", "")).replace(";", "").strip()
            expiry = None

            if expiry_text:
                for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%b/%Y", "%Y-%m-%d"):
                    try:
                        expiry = datetime.strptime(expiry_text, fmt).date()
                        break
                    except Exception:
                        pass

            # Ignore expired contracts
            if expiry is not None and expiry < today:
                continue

            parsed_candidates.append(
                (
                    expiry if expiry else date.max,
                    sym,
                    token,
                    record,
                )
            )

        if not parsed_candidates:
            raise RuntimeError(
                "NIFTY futures were found, but all discovered "
                "contracts appear to be expired or have invalid expiry."
            )

        # Nearest valid expiry first
        parsed_candidates.sort(key=lambda x: x[0])

        selected_expiry, selected_symbol, selected_token, _ = parsed_candidates[0]

        self.future_symbol = selected_symbol
        self.future_token = selected_token
        self.future_expiry = None if selected_expiry == date.max else selected_expiry

        self.log(
            f"Bound Active Nifty Future: {self.future_symbol} "
            f"(Token: {self.future_token}, Expiry: "
            f"{self.future_expiry.isoformat() if self.future_expiry else 'UNKNOWN'})"
        )

        # --------------------------------------------------------
        # OPTIONS DISCOVERY (Around ATM)
        # --------------------------------------------------------
        # Primary reference: Kotak's native NIFTY spot identifier.
        # If that quote is unavailable, use the already discovered active
        # NIFTY future LTP ONLY as the ATM reference. This does not create a
        # synthetic spot observation and does not alter the engine calculations.
        spot_price = None
        try:
            spot_res = self.client.quotes(
                instrument_tokens=[
                    {"instrument_token": self.spot_token, "exchange_segment": "nse_cm"}
                ],
                quote_type="ltp",
            )
            spot_recs = extract_records(spot_res)
            for sr in spot_recs:
                for k in ("lp", "last_price", "ltp", "c", "close"):
                    try:
                        val = float(sr.get(k, 0))
                    except Exception:
                        val = 0.0
                    if val > 0:
                        spot_price = val
                        break
                if spot_price:
                    break
        except Exception as exc:
            self.log(f"Native NIFTY spot quote unavailable: {exc}")

        if spot_price is None or spot_price <= 0:
            # No hardcoded token and no fabricated price. Reuse the LTP of the
            # future contract that was just resolved from Kotak's live master.
            try:
                fut_res = self.client.quotes(
                    instrument_tokens=[
                        {"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"}
                    ],
                    quote_type="ltp",
                )
                fut_recs = extract_records(fut_res)
                for fr in fut_recs:
                    for k in ("lp", "last_price", "ltp", "c", "close"):
                        try:
                            val = float(fr.get(k, 0))
                        except Exception:
                            val = 0.0
                        if val > 0:
                            spot_price = val
                            break
                    if spot_price:
                        break
                if spot_price and spot_price > 0:
                    self.log(
                        f"NIFTY spot quote unavailable; using active future LTP "
                        f"{spot_price:.2f} as ATM reference (Token: {self.future_token})."
                    )
            except Exception as exc:
                self.log(f"Active NIFTY future LTP fallback unavailable: {exc}")

        if spot_price is None or spot_price <= 0:
            self.log("No valid NIFTY price reference; PCR option mapping skipped.")
            self.option_contracts = {}
            self.pcr_tokens = []
            return True

        self.atm_reference_price = float(spot_price)
        step = CONFIG["pcr_strike_step"]
        atm = round(spot_price / step) * step
        count = CONFIG["pcr_strike_count"]
        target_strikes = {atm + (i * step) for i in range(-count, count + 1)}

        discovered = {}
        for r in records:
            if not isinstance(r, dict):
                continue
            sym = str(r.get("pTrdSymbol", r.get("tradingSymbol", ""))).upper().strip()
            token = str(r.get("pSymbol", r.get("pSymbolToken", "")))
            if not sym or not token:
                continue
            
            if not sym.startswith("NIFTY") or sym.startswith("NIFTYNXT"):
                continue
            if any(x in sym for x in ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]):
                continue

            opt_type = str(r.get("pOptionType", "")).upper()
            if opt_type not in ("CE", "PE"):
                if sym.endswith("CE"):
                    opt_type = "CE"
                elif sym.endswith("PE"):
                    opt_type = "PE"
            
            if opt_type not in ("CE", "PE"):
                continue

            # PCR must stay on the same current/nearest expiry as the bound
            # NIFTY future. Never mix strikes from another expiry.
            option_expiry_text = str(r.get("pExpiryDate", "")).replace(";", "").strip()
            option_expiry = None
            if option_expiry_text:
                for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%b/%Y", "%Y-%m-%d"):
                    try:
                        option_expiry = datetime.strptime(option_expiry_text, fmt).date()
                        break
                    except Exception:
                        pass
            if self.future_expiry is not None and option_expiry is not None:
                if option_expiry != self.future_expiry:
                    continue

            strike_val = None
            for sk in ("dStrikePrice;", "dStrikePrice", "strike_price", "strikePrice"):
                if sk in r:
                    try:
                        v = float(str(r.get(sk)).replace(";", "").replace(",", ""))
                        if v > 1000000:
                            v /= 100.0
                        strike_val = v
                        break
                    except Exception:
                        pass
            
            if strike_val is None:
                match = re.search(r"(\d+(?:\.\d+)?)$", sym[:-2])
                if match:
                    try:
                        strike_val = float(match.group(1))
                    except Exception:
                        pass

            if strike_val is not None:
                strike_val = round(strike_val, 2)
                for target in target_strikes:
                    if abs(strike_val - target) < 0.5:
                        key = f"{opt_type}:{target:.2f}"
                        discovered[key] = {
                            "token": token,
                            "symbol": sym,
                            "option_type": opt_type,
                            "strike": target,
                        }
                        break

        self.option_contracts = discovered
        self.pcr_tokens = sorted(list({str(item["token"]) for item in discovered.values() if item.get("token")}))
        self.log(f"Discovery complete: Future={self.future_token}, Options mapped={len(self.pcr_tokens)}")
        return True

    def fetch_raw_quotes(self) -> List[Dict[str, Any]]:
        if not self.connected or not self.client:
            return []
        
        tokens_to_poll = [
            {"instrument_token": self.spot_token, "exchange_segment": "nse_cm"},
        ]
        if self.future_token:
            tokens_to_poll.append({"instrument_token": str(self.future_token), "exchange_segment": "nse_fo"})
        
        for sym, tok in self.heavy_tokens.items():
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_cm"})
        
        for tok in self.pcr_tokens:
            tokens_to_poll.append({"instrument_token": str(tok), "exchange_segment": "nse_fo"})

        try:
            response = self.client.quotes(instrument_tokens=tokens_to_poll, quote_type="all")
            return extract_records(response)
        except Exception as exc:
            self.log(f"Quote fetch error: {exc}")
            return []

class SupabasePublisher:
    """Append-only raw bus publisher. Calculations never happen here."""
    def __init__(self, url_override: str = "", key_override: str = ""):
        # UI overrides are session-scoped only. Environment/Streamlit secrets
        # remain the non-UI fallback. No credentials are written into source.
        self.url = str(url_override or env_or_secret("SUPABASE_URL", CONFIG["supabase_url"])).strip()
        self.key = str(key_override or env_or_secret("SUPABASE_KEY", CONFIG["supabase_key"])).strip()

    def _headers(self):
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def health(self) -> Dict[str, Any]:
        if not self.url or not self.key:
            return {"configured": False, "reachable": False, "error": "Supabase URL/Key missing"}
        endpoint = f"{self.url.rstrip('/')}/rest/v1/raw_observations"
        try:
            response = requests.get(
                endpoint,
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Accept": "application/json",
                },
                params={"select": "id", "limit": "1"},
                timeout=float(CONFIG["supabase_timeout_sec"]),
            )
            return {
                "configured": True,
                "reachable": response.status_code in (200, 206),
                "http_status": response.status_code,
                "error": "" if response.status_code in (200, 206) else response.text[:250],
            }
        except Exception as exc:
            return {"configured": True, "reachable": False, "error": str(exc)}

    def publish_observations_batch(self, source: str, symbol: str, token: str, raw_payloads: List[dict]) -> int:
        if not self.url or not self.key or not raw_payloads:
            return 0
        total = 0
        endpoint = f"{self.url.rstrip('/')}/rest/v1/raw_observations"
        batch_size = max(1, int(CONFIG["history_batch_size"]))
        for i in range(0, len(raw_payloads), batch_size):
            batch = raw_payloads[i:i + batch_size]
            records = []
            for payload in batch:
                records.append({
                    "source": source,
                    "symbol": symbol,
                    "instrument_token": str(token),
                    "observation_timestamp": now_ist().isoformat(),
                    "raw": payload,
                })
            try:
                response = requests.post(
                    endpoint, headers=self._headers(), json=records,
                    timeout=float(CONFIG["supabase_timeout_sec"])
                )
                if response.status_code in (200, 201, 204):
                    total += len(records)
                else:
                    print(f"Supabase batch publish failed [{response.status_code}]: {response.text[:300]}")
            except Exception as exc:
                print(f"Supabase batch publish error: {exc}")
        return total

    def publish_observation(self, source: str, symbol: str, token: str, raw_payload: dict) -> bool:
        return self.publish_observations_batch(source, symbol, token, [raw_payload]) == 1

# ---------------------------------------------------------------------------
# Persistent-worker configuration/state
# ---------------------------------------------------------------------------

WORKER_STATE_FILE = os.getenv(
    "RAW_PRODUCER_STATE_FILE",
    "/tmp/raw_data_producer_state.json",
)

WORKER_POLL_SEC = float(os.getenv("RAW_PRODUCER_POLL_SEC", "3.0"))
QUOTE_RETRY_ATTEMPTS = int(os.getenv("RAW_PRODUCER_QUOTE_RETRIES", "3"))
QUOTE_RETRY_DELAY_SEC = float(os.getenv("RAW_PRODUCER_RETRY_DELAY_SEC", "0.75"))
FAILURES_BEFORE_DEGRADED = int(os.getenv("RAW_PRODUCER_FAILURE_THRESHOLD", "3"))

_STOP = False


def _safe_json_write(payload: dict) -> None:
    """Atomically publish worker state for the dashboard."""
    path = WORKER_STATE_FILE
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, default=str)
        os.replace(tmp, path)
    except Exception:
        # Monitoring must never kill the producer.
        pass


def _utc_or_ist_now() -> str:
    return now_ist().isoformat()


def _state(**updates):
    state = {
        "worker": "kotak_live",
        "pid": os.getpid(),
        "updated_at": _utc_or_ist_now(),
    }
    try:
        if os.path.exists(WORKER_STATE_FILE):
            with open(WORKER_STATE_FILE, "r", encoding="utf-8") as fh:
                old = json.load(fh)
                if isinstance(old, dict):
                    state.update(old)
    except Exception:
        pass
    state.update(updates)
    _safe_json_write(state)
    return state


def stop_worker(*_args):
    global _STOP
    _STOP = True


def _extract_quote_identity(quote: dict):
    token = str(
        quote.get(
            "exchange_token",
            quote.get(
                "instrument_token",
                quote.get("pSymbol", quote.get("pSymbolToken", "UNKNOWN")),
            ),
        )
    )
    symbol = str(
        quote.get(
            "display_symbol",
            quote.get("pTrdSymbol", quote.get("tradingSymbol", "UNKNOWN")),
        )
    )
    return token, symbol


def run_worker(totp: str = "") -> int:
    """
    Manual-auth worker entrypoint.

    Authentication is intentionally performed once at worker start.
    No automatic re-login/TOTP generation is attempted after session failure.
    """
    global _STOP
    _STOP = False

    kotak = KotakConnector()
    supabase = SupabasePublisher()

    state = _state(
        status="STARTING",
        connection="DISCONNECTED",
        auth_required=False,
        poll_count=0,
        total_quotes=0,
        total_published=0,
        consecutive_failures=0,
        total_failures=0,
        reconnects=0,
        last_error="",
        last_kotak_fetch=None,
        last_supabase_write=None,
        last_raw_event=None,
        active_future=None,
        active_future_symbol=None,
        pcr_contracts=0,
        nfo_records=0,
    )

    try:
        if not supabase.url or not supabase.key:
            raise RuntimeError("Supabase URL/Key missing.")

        if not totp:
            raise RuntimeError(
                "Manual TOTP required. Start the worker with a current TOTP."
            )

        _state(
            status="AUTHENTICATING",
            connection="AUTHENTICATING",
            auth_required=False,
            last_error="",
        )

        kotak.login(totp_override=totp)

        _state(
            status="DISCOVERING",
            connection="AUTHENTICATED",
            auth_required=False,
        )

        # Preserve the existing discovery implementation exactly.
        kotak.discover_instruments()

        _state(
            status="LIVE",
            connection="AUTHENTICATED",
            auth_required=False,
            active_future=kotak.future_token,
            active_future_symbol=kotak.future_symbol,
            pcr_contracts=len(kotak.pcr_tokens),
            nfo_records=len(kotak.nfo_records),
            last_error="",
        )

        while not _STOP:
            poll_started = time.monotonic()
            poll_no = int(state.get("poll_count", 0)) + 1

            raw_quotes = []
            fetch_error = ""

            # Quote-level retry: transient errors do NOT cause logout/re-login.
            for attempt in range(1, max(1, QUOTE_RETRY_ATTEMPTS) + 1):
                if _STOP:
                    break
                try:
                    raw_quotes = kotak.fetch_raw_quotes()
                    if raw_quotes:
                        break
                    fetch_error = "Kotak returned no raw quote records."
                except Exception as exc:
                    fetch_error = str(exc)

                if attempt < QUOTE_RETRY_ATTEMPTS:
                    _state(
                        status="DEGRADED",
                        connection="AUTHENTICATED",
                        retry_attempt=attempt,
                        last_error=fetch_error,
                    )
                    time.sleep(QUOTE_RETRY_DELAY_SEC)

            fetch_time = _utc_or_ist_now()

            if not raw_quotes:
                consecutive = int(state.get("consecutive_failures", 0)) + 1
                total_failures = int(state.get("total_failures", 0)) + 1
                state = _state(
                    status=(
                        "FEED_LOST"
                        if consecutive >= FAILURES_BEFORE_DEGRADED
                        else "DEGRADED"
                    ),
                    connection="AUTHENTICATED",
                    poll_count=poll_no,
                    consecutive_failures=consecutive,
                    total_failures=total_failures,
                    last_kotak_fetch=state.get("last_kotak_fetch"),
                    last_error=fetch_error,
                    retry_attempt=0,
                )
            else:
                published = 0
                last_symbol = None
                last_token = None
                for quote in raw_quotes:
                    if not isinstance(quote, dict):
                        continue
                    token, symbol = _extract_quote_identity(quote)
                    last_token, last_symbol = token, symbol
                    try:
                        if supabase.publish_observation(
                            "kotak_live", symbol, token, quote
                        ):
                            published += 1
                    except Exception as exc:
                        fetch_error = f"Supabase publish error: {exc}"

                write_time = _utc_or_ist_now() if published else state.get(
                    "last_supabase_write"
                )

                state = _state(
                    status="LIVE" if published else "DEGRADED",
                    connection="AUTHENTICATED",
                    poll_count=poll_no,
                    total_quotes=int(state.get("total_quotes", 0)) + len(raw_quotes),
                    total_published=int(state.get("total_published", 0)) + published,
                    consecutive_failures=0 if published else int(
                        state.get("consecutive_failures", 0)
                    ) + 1,
                    last_kotak_fetch=fetch_time,
                    last_supabase_write=write_time,
                    last_raw_event=fetch_time,
                    last_error="" if published else fetch_error,
                    retry_attempt=0,
                    last_quote_count=len(raw_quotes),
                    last_published_count=published,
                    last_quote_symbol=last_symbol,
                    last_quote_token=last_token,
                    active_future=kotak.future_token,
                    active_future_symbol=kotak.future_symbol,
                    pcr_contracts=len(kotak.pcr_tokens),
                    nfo_records=len(kotak.nfo_records),
                )

            elapsed = time.monotonic() - poll_started
            remaining = max(0.0, WORKER_POLL_SEC - elapsed)
            if remaining:
                time.sleep(remaining)

        _state(status="STOPPED", connection="AUTHENTICATED")
        return 0

    except Exception as exc:
        # Authentication/session failure is surfaced as manual intervention.
        _state(
            status="AUTH_REQUIRED",
            connection="DISCONNECTED",
            auth_required=True,
            last_error=str(exc),
        )
        return 2


if __name__ == "__main__":
    import argparse
    import signal

    parser = argparse.ArgumentParser(
        description="Persistent Kotak LIVE RAW producer worker"
    )
    parser.add_argument(
        "--totp",
        default="",
        help="Current manual 6-digit Kotak TOTP. Not stored by the worker.",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, stop_worker)
    signal.signal(signal.SIGTERM, stop_worker)

    raise SystemExit(run_worker(args.totp))
