#!/usr/bin/env python3
"""
NIFTY Raw Bus — Kotak Neo LIVE Worker

Scope:
    Kotak Neo LIVE raw observations -> Supabase raw_observations only.

Isolation contract:
    - NO yfinance dependency/import.
    - NO historical-data fetching.
    - NO indicators, features, scoring, labels, regime or strategy logic.
    - PCR/nearest-expiry discovery is preserved from the locked producer.
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
from typing import Dict, List, Any, Optional
from zoneinfo import ZoneInfo

import requests

try:
    import streamlit as st
except ImportError:
    st = None

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None

IST = ZoneInfo("Asia/Kolkata")

CONFIG = {
    "neo_environment": "prod",
    "pcr_strike_count": 5,
    "pcr_strike_step": 50.0,
    "supabase_url": os.getenv("SUPABASE_URL", "").strip(),
    "supabase_key": os.getenv("SUPABASE_KEY", "").strip(),
    "poll_interval_sec": 3.0,
    "supabase_timeout_sec": 15,
    "live_batch_size": 250,
    # Connection-resilience controls. These do not alter market/data logic.
    "quote_failure_threshold": 2,
    "reconnect_initial_delay_sec": 2.0,
    "reconnect_max_delay_sec": 30.0,
    "reconnect_max_attempts": 5,
    "feed_stale_after_sec": 20.0,
    "reconnect_cooldown_sec": 5.0,
}

def now_ist() -> datetime:
    return datetime.now(IST)

def today_ist() -> date:
    return now_ist().date()

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


def _normalize_scrip_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize only scrip-master column spelling/whitespace."""
    out: Dict[str, Any] = {}
    for key, value in record.items():
        clean_key = str(key).strip().lstrip("\ufeff").strip()
        if clean_key.endswith(";"):
            clean_key = clean_key[:-1]
        out[clean_key] = value.strip() if isinstance(value, str) else value
    return out


def _normalize_kotak_nfo_expiry(record: Dict[str, Any]) -> Dict[str, Any]:
    """Mirror Kotak Neo v2 SDK's NFO expiry normalization for fallback rows."""
    key = "pExpiryDate"
    value = record.get(key)
    if value is None:
        return record

    raw = str(value).strip().replace(";", "")
    if not raw:
        return record

    # Already normalized by a server/CSV variant.
    for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%b/%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
            record[key] = parsed.strftime("%d%b%Y").upper()
            return record
        except Exception:
            pass

    # Kotak Neo v2 converts NSE-FO epoch seconds using this offset before
    # formatting as DDMMMYYYY. Keep the exact SDK convention.
    try:
        epoch = float(raw)
        if epoch > 0:
            epoch_seconds = epoch + 315511200
            parsed = datetime.fromtimestamp(epoch_seconds)
            record[key] = parsed.strftime("%d%b%Y").upper()
    except Exception:
        pass

    return record


def _csv_text_to_records(csv_text: str) -> List[Dict[str, Any]]:
    """Parse Kotak scrip-master CSV, including its JSON-envelope variant."""
    if not isinstance(csv_text, str):
        return []
    text_value = csv_text.lstrip("\ufeff\r\n\t ")
    if not text_value:
        return []

    if text_value.startswith("{"):
        try:
            envelope = json.loads(text_value)
            if isinstance(envelope, dict):
                for key in ("nse", "NSE", "nse_fo", "NSE_FO"):
                    payload = envelope.get(key)
                    if isinstance(payload, str) and payload.strip():
                        csv_text = payload
                        break
        except Exception:
            pass

    try:
        reader = csv.DictReader(io.StringIO(str(csv_text).lstrip("\ufeff")))
        records: List[Dict[str, Any]] = []
        for row in reader:
            if not row:
                continue
            normalized = _normalize_scrip_record(dict(row))
            if (
                str(normalized.get("pSymbol", "")).strip()
                and str(normalized.get("pTrdSymbol", "")).strip()
            ):
                records.append(_normalize_kotak_nfo_expiry(normalized))
        return records
    except Exception:
        return []


def _scrip_master_urls(response: Any) -> List[str]:
    """Accept both current documented URL and filesPaths response shapes."""
    urls: List[str] = []

    def add(value: Any) -> None:
        if isinstance(value, str):
            value = value.strip()
            if value.startswith(("http://", "https://")) and value not in urls:
                urls.append(value)
        elif isinstance(value, (list, tuple)):
            for item in value:
                add(item)

    if isinstance(response, str):
        add(response)
    elif isinstance(response, dict):
        add(response.get("filesPaths"))
        add(response.get("filePath"))
        add(response.get("url"))
        add(response.get("nse_fo"))
        add(response.get("NSE_FO"))
    return urls


def _extract_nfo_csv_payload(response: Any) -> List[Dict[str, Any]]:
    """Parse an already-returned CSV or JSON-envelope payload."""
    if isinstance(response, str):
        return _csv_text_to_records(response)
    if isinstance(response, dict):
        for key in ("nse", "NSE", "nse_fo", "NSE_FO"):
            value = response.get(key)
            if isinstance(value, str):
                parsed = _csv_text_to_records(value)
                if parsed:
                    return parsed
    return []


def extract_records(response: Any) -> List[Dict[str, Any]]:
    if response is None:
        return []
    if isinstance(response, list):
        return [x for x in response if isinstance(x, dict)]
    if isinstance(response, dict):
        for key in ("result", "data", "values", "records", "scrips"):
            value = response.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                nested = extract_records(value)
                if nested:
                    return nested
        return _extract_nfo_csv_payload(response)
    if isinstance(response, str):
        return _csv_text_to_records(response)
    return []


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

        # LIVE connection/feed health state. These fields are operational only;
        # they do not participate in instrument mapping or market calculations.
        self.last_quote_success_at: Optional[datetime] = None
        self.last_quote_error_at: Optional[datetime] = None
        self.last_supabase_write_at: Optional[datetime] = None
        self.connection_started_at: Optional[datetime] = None
        self.last_quote_count = 0
        self.consecutive_quote_failures = 0
        self.reconnect_count = 0
        self.reconnect_in_progress = False
        self.last_reconnect_at: Optional[datetime] = None
        self.last_connection_error = ""

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
        self.connection_started_at = now_ist()
        self.consecutive_quote_failures = 0
        self.last_connection_error = ""
        self.log("Kotak authentication successful.")
        return True

    def _mark_quote_success(self, count: int) -> None:
        """Record a healthy live quote response."""
        self.last_quote_success_at = now_ist()
        self.last_quote_count = int(count)
        self.consecutive_quote_failures = 0
        self.last_connection_error = ""
        self.connected = True

    def _mark_quote_failure(self, error: Any) -> None:
        """Record a quote failure without changing any market/instrument state."""
        self.last_quote_error_at = now_ist()
        self.consecutive_quote_failures += 1
        self.last_connection_error = str(error)

    def feed_age_sec(self) -> Optional[float]:
        """Age of the last successful Kotak quote response."""
        if self.last_quote_success_at is None:
            return None
        return max(0.0, (now_ist() - self.last_quote_success_at).total_seconds())

    def connection_age_sec(self) -> Optional[float]:
        """Age of the current authenticated Kotak session."""
        if self.connection_started_at is None:
            return None
        return max(0.0, (now_ist() - self.connection_started_at).total_seconds())

    def mark_supabase_write(self, count: int) -> None:
        """Record latest successful RAW BUS write; operational UI only."""
        if int(count) > 0:
            self.last_supabase_write_at = now_ist()

    def is_live_healthy(self) -> bool:
        """Authenticated + recent successful quote response."""
        age = self.feed_age_sec()
        return bool(self.connected and age is not None and age <= float(CONFIG["feed_stale_after_sec"]))

    def reconnect_and_restore(self) -> bool:
        """
        Recover a broken Kotak session and restore the already-locked
        instrument selection. No strategy, feature, mapping, or data-contract
        logic is changed here.
        """
        if self.reconnect_in_progress:
            return False

        now = now_ist()
        if self.last_reconnect_at is not None:
            elapsed = (now - self.last_reconnect_at).total_seconds()
            if elapsed < float(CONFIG["reconnect_cooldown_sec"]):
                return False

        self.reconnect_in_progress = True
        self.last_reconnect_at = now
        self.connected = False

        try:
            self.log(
                f"LIVE connection lost/stale. Starting automatic recovery "
                f"(attempts={CONFIG['reconnect_max_attempts']})."
            )

            delay = float(CONFIG["reconnect_initial_delay_sec"])
            max_delay = float(CONFIG["reconnect_max_delay_sec"])
            max_attempts = max(1, int(CONFIG["reconnect_max_attempts"]))

            for attempt in range(1, max_attempts + 1):
                try:
                    self.log(f"Reconnect attempt {attempt}/{max_attempts}...")

                    # Drop the old client object before creating a fresh
                    # authenticated session. This avoids reusing a dead session.
                    self.client = None
                    self.login()

                    # Re-run the existing discovery contract after login so an
                    # expired/changed contract cannot leave stale subscriptions.
                    self.discover_instruments()

                    # Validate the newly restored session immediately.
                    quotes = self.fetch_raw_quotes(allow_reconnect=False)
                    if quotes:
                        self.reconnect_count += 1
                        self.log(
                            f"Automatic recovery successful. Quotes restored: {len(quotes)}."
                        )
                        return True

                    raise RuntimeError("Re-authenticated session returned no live quotes.")
                except Exception as exc:
                    self.connected = False
                    self.last_connection_error = str(exc)
                    self.log(f"Reconnect attempt {attempt} failed: {exc}")
                    if attempt < max_attempts:
                        time.sleep(delay)
                        delay = min(max_delay, delay * 2.0)

            self.log("Automatic recovery exhausted; Kotak remains disconnected.")
            return False
        finally:
            self.reconnect_in_progress = False

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

    def fetch_raw_quotes(self, allow_reconnect: bool = True) -> List[Dict[str, Any]]:
        """
        Fetch the same locked raw quote set, with connection-health recovery.

        The token list and quote payload remain exactly the same. The only
        addition is operational recovery when the authenticated Kotak session
        stops responding.
        """
        if not self.connected or not self.client:
            if allow_reconnect and not self.reconnect_in_progress:
                self.reconnect_and_restore()
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
            records = extract_records(response)

            if records:
                self._mark_quote_success(len(records))
                return records

            # An authenticated client returning no records is treated as a
            # possible dead/stale session after the configured failure count.
            self._mark_quote_failure("Kotak returned no quote records.")
            self.log(
                f"Quote fetch returned no records "
                f"(failure {self.consecutive_quote_failures}/"
                f"{CONFIG['quote_failure_threshold']})."
            )

            if (
                allow_reconnect
                and self.consecutive_quote_failures >= int(CONFIG["quote_failure_threshold"])
                and not self.reconnect_in_progress
            ):
                self.reconnect_and_restore()
            return []
        except Exception as exc:
            self._mark_quote_failure(exc)
            self.log(
                f"Quote fetch error: {exc} "
                f"(failure {self.consecutive_quote_failures}/"
                f"{CONFIG['quote_failure_threshold']})."
            )

            if (
                allow_reconnect
                and self.consecutive_quote_failures >= int(CONFIG["quote_failure_threshold"])
                and not self.reconnect_in_progress
            ):
                self.reconnect_and_restore()
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
        batch_size = max(1, int(CONFIG["live_batch_size"]))
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

def _fmt_ist(value: Optional[datetime]) -> str:
    """Compact IST timestamp for the mobile dashboard."""
    return value.strftime("%H:%M:%S") if value else "—"


def _fmt_age(seconds: Optional[float]) -> str:
    """Human-readable duration for the mobile dashboard."""
    if seconds is None:
        return "—"
    total = max(0, int(seconds))
    h, rem = divmod(total, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _render_mobile_dashboard(kotak, supabase, sup_health: Optional[dict] = None) -> None:
    """Responsive operations dashboard. Contains no market/strategy logic."""
    feed_age = kotak.feed_age_sec()
    connection_age = kotak.connection_age_sec()
    if kotak.is_live_healthy():
        status, icon = "CONNECTED", "🟢"
    elif kotak.reconnect_in_progress:
        status, icon = "RECONNECTING", "🟡"
    elif kotak.connected and feed_age is not None:
        status, icon = "STALE FEED", "🟠"
    else:
        status, icon = "DISCONNECTED", "🔴"

    sup_ready = bool(supabase.url and supabase.key)
    sup_reachable = bool((sup_health or {}).get("reachable"))
    sup_label = "READY" if sup_reachable else ("CONFIGURED" if sup_ready else "NOT CONFIGURED")
    sup_icon = "🟢" if sup_reachable else ("🟡" if sup_ready else "🔴")
    sup_detail = "REACHABLE" if sup_reachable else (str((sup_health or {}).get("error", "Not checked"))[:70] if sup_ready else "Configure Supabase")
    error_text = (kotak.last_connection_error or "None").replace("<", "&lt;").replace(">", "&gt;")[:90]

    st.markdown("""
<style>
.rawbus-wrap{max-width:760px;margin:0 auto;padding:0 2px 14px}
.rawbus-hero{border:1px solid rgba(128,128,128,.25);border-radius:20px;padding:20px 18px 18px;margin:6px 0 12px;text-align:center;box-shadow:0 3px 16px rgba(0,0,0,.07)}
.rawbus-title{font-size:clamp(25px,7vw,42px);font-weight:800;line-height:1.05}
.rawbus-subtitle{font-size:clamp(14px,4vw,18px);opacity:.72;margin-top:6px}
.rawbus-status{margin-top:17px;font-size:clamp(20px,6vw,31px);font-weight:850;letter-spacing:.02em}
.rawbus-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:10px 0}
.rawbus-card{border:1px solid rgba(128,128,128,.23);border-radius:16px;padding:14px 15px;min-height:72px}
.rawbus-label{font-size:12px;text-transform:uppercase;letter-spacing:.07em;opacity:.62;font-weight:700}
.rawbus-value{font-size:20px;font-weight:800;margin-top:5px;overflow-wrap:anywhere}
.rawbus-small{font-size:13px;opacity:.70;margin-top:3px;overflow-wrap:anywhere}
.rawbus-section{font-size:17px;font-weight:800;margin:15px 2px 8px}
@media(max-width:560px){.rawbus-grid{grid-template-columns:1fr}.rawbus-card{min-height:64px}.rawbus-wrap{padding-left:0;padding-right:0}}
</style>
""", unsafe_allow_html=True)
    html = f'''<div class="rawbus-wrap">
  <div class="rawbus-hero">
    <div class="rawbus-title">📡 NIFTY RAW BUS</div>
    <div class="rawbus-subtitle">KOTAK NEO LIVE · RAW DATA PRODUCER</div>
    <div class="rawbus-status">{icon} LIVE {status}</div>
  </div>
  <div class="rawbus-section">LIVE CONNECTION</div>
  <div class="rawbus-grid">
    <div class="rawbus-card"><div class="rawbus-label">Connection Age</div><div class="rawbus-value">{_fmt_age(connection_age)}</div></div>
    <div class="rawbus-card"><div class="rawbus-label">Feed Age</div><div class="rawbus-value">{_fmt_age(feed_age)}</div></div>
    <div class="rawbus-card"><div class="rawbus-label">Last Successful Quote</div><div class="rawbus-value">{_fmt_ist(kotak.last_quote_success_at)}</div><div class="rawbus-small">Quotes: {kotak.last_quote_count}</div></div>
    <div class="rawbus-card"><div class="rawbus-label">Last Supabase Write</div><div class="rawbus-value">{_fmt_ist(kotak.last_supabase_write_at)}</div><div class="rawbus-small">RAW BUS: {sup_label}</div></div>
    <div class="rawbus-card"><div class="rawbus-label">Automatic Reconnects</div><div class="rawbus-value">{kotak.reconnect_count}</div><div class="rawbus-small">Failures: {kotak.consecutive_quote_failures}</div></div>
    <div class="rawbus-card"><div class="rawbus-label">Supabase</div><div class="rawbus-value">{sup_icon} {sup_label}</div><div class="rawbus-small">{sup_detail}</div></div>
  </div>
  <div class="rawbus-section">ACTIVE CONTRACTS</div>
  <div class="rawbus-grid">
    <div class="rawbus-card"><div class="rawbus-label">Active Future</div><div class="rawbus-value">{kotak.future_symbol or "NOT DISCOVERED"}</div><div class="rawbus-small">Token: {kotak.future_token or "—"}</div></div>
    <div class="rawbus-card"><div class="rawbus-label">PCR Contracts</div><div class="rawbus-value">{len(kotak.pcr_tokens)}</div><div class="rawbus-small">Nearest-expiry mapping</div></div>
    <div class="rawbus-card"><div class="rawbus-label">NFO Master Records</div><div class="rawbus-value">{len(kotak.nfo_records)}</div></div>
    <div class="rawbus-card"><div class="rawbus-label">Last Error</div><div class="rawbus-value">{error_text}</div></div>
  </div>
</div>'''
    st.markdown(html, unsafe_allow_html=True)


def main():
    if st is None:
        print("Streamlit not available.")
        return

    st.set_page_config(
        page_title="NIFTY Raw Bus LIVE",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # Mobile-first shell. The old Streamlit sidebar is intentionally not used:
    # controls live in the main page so the phone view opens directly on the
    # operational dashboard instead of showing a large sidebar overlay.
    st.markdown("""
<style>
/* ===== MOBILE-FIRST RAW BUS APP SHELL ===== */
#MainMenu, footer {visibility:hidden;}
header[data-testid="stHeader"] {background:transparent;}
/* HARD DISABLE THE STREAMLIT SIDEBAR. Controls are in the main-page CONTROL CENTER. */
section[data-testid="stSidebar"],
div[data-testid="stSidebar"],
[data-testid="stSidebarNav"],
button[data-testid="stSidebarCollapseButton"] {display:none !important; visibility:hidden !important; width:0 !important; min-width:0 !important;}
.block-container {max-width:980px !important; padding-top:1.0rem !important; padding-bottom:2rem !important;}
.rawbus-app {max-width:900px;margin:0 auto;}
.rawbus-top {text-align:center;margin:2px auto 14px;}
.rawbus-brand {font-size:clamp(30px,8vw,48px);font-weight:900;line-height:1.0;letter-spacing:-.025em;}
.rawbus-sub {font-size:clamp(13px,3.8vw,17px);opacity:.68;margin-top:7px;font-weight:650;}
.rawbus-divider {height:1px;background:rgba(128,128,128,.22);margin:14px 0;}
.rawbus-control-title {font-size:18px;font-weight:850;margin:4px 0 8px;}
.rawbus-note {font-size:12px;opacity:.62;}
/* Keep Streamlit controls comfortable on phones. */
.stButton > button {min-height:46px;border-radius:12px;font-weight:750;}
.stTextInput input {min-height:44px;border-radius:12px;}
@media(max-width:560px){
  .block-container {padding-left:.72rem !important;padding-right:.72rem !important;}
  .rawbus-brand {font-size:31px;}
  .rawbus-sub {font-size:13px;}
  .stButton > button {width:100%;}
}
</style>
""", unsafe_allow_html=True)

    if "kotak" not in st.session_state:
        st.session_state.kotak = KotakConnector()
    if "producer_running" not in st.session_state:
        st.session_state.producer_running = False

    kotak: KotakConnector = st.session_state.kotak

    # Supabase credentials can still come from Streamlit secrets/env. The
    # controls below are deliberately in the main page, not the sidebar.
    supabase_url_input = st.session_state.get(
        "supabase_url_input", env_or_secret("SUPABASE_URL", "")
    )
    supabase_key_input = st.session_state.get(
        "supabase_key_input", env_or_secret("SUPABASE_KEY", "")
    )
    supabase = SupabasePublisher(
        url_override=supabase_url_input,
        key_override=supabase_key_input,
    )

    # Main app header: dashboard is the first thing visible on a phone.
    st.markdown(
        '<div class="rawbus-app"><div class="rawbus-top">'
        '<div class="rawbus-brand">📡 NIFTY RAW BUS</div>'
        '<div class="rawbus-sub">KOTAK NEO LIVE · INSTITUTIONAL RAW DATA PRODUCER</div>'
        '</div></div>',
        unsafe_allow_html=True,
    )

    sup_health = supabase.health() if supabase.url and supabase.key else {
        "reachable": False,
        "error": "Supabase not configured",
    }
    dashboard_placeholder = st.empty()
    with dashboard_placeholder.container():
        _render_mobile_dashboard(kotak, supabase, sup_health)

    # All controls are below the dashboard and collapsed by default. This is
    # the key mobile UX change: no sidebar overlay on first load.
    with st.expander("⚙️ CONTROL CENTER", expanded=False):
        st.markdown('<div class="rawbus-control-title">🔑 Kotak Neo Authentication</div>', unsafe_allow_html=True)
        totp_input = st.text_input(
            "Live TOTP Code",
            type="password",
            key="main_totp_input",
            placeholder="Enter 6-digit TOTP",
        )

        st.markdown('<div class="rawbus-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="rawbus-control-title">🗄️ Supabase RAW BUS</div>', unsafe_allow_html=True)
        supabase_url_input = st.text_input(
            "Supabase URL",
            value=supabase_url_input,
            key="supabase_url_input",
            placeholder="https://your-project.supabase.co",
        )
        supabase_key_input = st.text_input(
            "Supabase Key",
            value=supabase_key_input,
            type="password",
            key="supabase_key_input",
            placeholder="Supabase anon/service key",
        )
        supabase = SupabasePublisher(
            url_override=supabase_url_input,
            key_override=supabase_key_input,
        )

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Test Supabase RAW BUS", use_container_width=True):
                health = supabase.health()
                if health.get("reachable"):
                    st.success("Supabase RAW BUS reachable.")
                else:
                    st.error(health.get("error", "Supabase connection failed."))
        with c2:
            if st.button("Connect Kotak", use_container_width=True):
                try:
                    kotak.login(totp_override=totp_input)
                    st.success("Authenticated Successfully!")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

        c3, c4 = st.columns(2)
        with c3:
            if st.button("Discover Instruments", disabled=not kotak.connected, use_container_width=True):
                try:
                    kotak.discover_instruments()
                    st.success("Discovery Complete!")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        with c4:
            can_start = bool(kotak.connected and kotak.future_token and supabase.url and supabase.key)
            if not st.session_state.producer_running:
                if st.button("▶ START RAW PRODUCER", type="primary", disabled=not can_start, use_container_width=True):
                    st.session_state.producer_running = True
                    st.rerun()
            else:
                if st.button("■ STOP PRODUCER", use_container_width=True):
                    st.session_state.producer_running = False
                    st.rerun()

        if st.button("Test Live Raw → Supabase", disabled=not can_start, use_container_width=True):
            try:
                raw_quotes = kotak.fetch_raw_quotes()
                published = 0
                for quote in raw_quotes:
                    if not isinstance(quote, dict):
                        continue
                    token = str(quote.get(
                        "exchange_token",
                        quote.get("instrument_token", quote.get("pSymbol", quote.get("pSymbolToken", "UNKNOWN")))
                    ))
                    symbol = str(quote.get(
                        "display_symbol", quote.get("pTrdSymbol", quote.get("tradingSymbol", "UNKNOWN"))
                    ))
                    if supabase.publish_observation("kotak_live", symbol, token, quote):
                        published += 1
                kotak.mark_supabase_write(published)
                st.session_state["last_live_test"] = {
                    "kotak_quotes_received": len(raw_quotes),
                    "supabase_rows_published": published,
                    "active_future": kotak.future_token,
                    "pcr_contracts": len(kotak.pcr_tokens),
                    "status": "PASS" if raw_quotes and published else "PARTIAL/NO_DATA",
                }
                st.rerun()
            except Exception as exc:
                st.session_state["last_live_test"] = {"status": "ERROR", "error": str(exc)}

        if st.session_state.get("last_live_test"):
            st.json(st.session_state["last_live_test"])

        st.markdown(
            '<div class="rawbus-note">Credentials remain session/env controlled. '
            'No credential values are written into the source code.</div>',
            unsafe_allow_html=True,
        )

    # Refresh the dashboard once after control interactions so the main view
    # remains the single source of truth for operational status.
    with dashboard_placeholder.container():
        _render_mobile_dashboard(kotak, supabase, supabase.health() if supabase.url and supabase.key else sup_health)

    if kotak.logs:
        with st.expander("📋 Discovery & Execution Logs", expanded=False):
            for log in kotak.logs[-30:]:
                st.text(log)

    with st.expander("🔒 RAW DATA CONTRACT", expanded=False):
        st.code(
            "Kotak Neo → LIVE RAW → Supabase → all 3 engines\n"
            "No yfinance in this worker.\n"
            "No features / scores / labels / regime / decisions cross the bus.",
            language="text",
        )

    if st.session_state.producer_running:
        st.success("🟢 Kotak LIVE Raw Producer is active. Raw quotes are being published to Supabase `raw_observations`.")
        status_container = st.empty()
        log_container = st.empty()
        while st.session_state.producer_running:
            try:
                raw_quotes = kotak.fetch_raw_quotes()
                published_count = 0
                for quote in raw_quotes:
                    if not isinstance(quote, dict):
                        continue
                    token = str(quote.get(
                        "exchange_token",
                        quote.get("instrument_token", quote.get("pSymbol", quote.get("pSymbolToken", "UNKNOWN")))
                    ))
                    symbol = str(quote.get(
                        "display_symbol", quote.get("pTrdSymbol", quote.get("tradingSymbol", "UNKNOWN"))
                    ))
                    if supabase.publish_observation("kotak_live", symbol, token, quote):
                        published_count += 1

                kotak.mark_supabase_write(published_count)
                current_feed_age = kotak.feed_age_sec()
                current_status = (
                    "CONNECTED" if kotak.is_live_healthy()
                    else "RECONNECTING" if kotak.reconnect_in_progress
                    else "STALE FEED" if kotak.connected
                    else "DISCONNECTED"
                )
                with dashboard_placeholder.container():
                    _render_mobile_dashboard(kotak, supabase, supabase.health())

                status_container.info(
                    f"Last Poll: {now_ist().strftime('%H:%M:%S')} | "
                    f"Kotak: {current_status} | "
                    f"Received {len(raw_quotes)} raw quotes | "
                    f"Published {published_count} | "
                    f"Feed Age: {current_feed_age:.1f}s | "
                    f"Reconnects: {kotak.reconnect_count} | "
                    f"Active Future: {kotak.future_token} | "
                    f"Options mapped: {len(kotak.pcr_tokens)}"
                )
            except Exception as exc:
                kotak._mark_quote_failure(exc)
                log_container.error(f"Producer loop exception: {exc}")
                if (
                    kotak.consecutive_quote_failures >= int(CONFIG["quote_failure_threshold"])
                    and not kotak.reconnect_in_progress
                ):
                    kotak.reconnect_and_restore()
                with dashboard_placeholder.container():
                    _render_mobile_dashboard(kotak, supabase, supabase.health())
            time.sleep(float(CONFIG["poll_interval_sec"]))


if __name__ == "__main__":
    main()
