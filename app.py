SURGICAL FIX â€” app.py ONLY
================================

ROOT CAUSE FOUND
----------------
The Kotak producer is already publishing the quote's own symbol + instrument_token
to Supabase.

The NIFTY consumer is the part making unsafe assumptions:

1) It hardcodes the future token:
   CONFIG["nifty_future_token"] -> default "68407"

2) It hardcodes future symbol/expiry:
   NIFTY26SEPFUT / 2026-09-29

3) It creates dummy PCR tokens (70000+), instead of using the real Supabase
   option rows.

4) Most importantly, fetch_market_snapshot() reads the latest 150 rows without
   instrument filtering and then aliases a row to "Nifty 50" using:
       if tok == "Nifty 50" or "NIFTY 50" in sym_name:
           self.latest["Nifty 50"] = raw

The fix is therefore NOT to touch Kotak producer, yFinance producer, Next-Day
Alpha, or GSR.

Only app.py gets a surgical routing fix:
Supabase row's symbol + instrument_token are authoritative.

------------------------------------------------------------
PATCH 1 â€” replace discover_nifty_instruments()
------------------------------------------------------------

Replace the existing future/PCR portion inside discover_nifty_instruments()
with this:

        # Spot is identified ONLY by the exact canonical index symbol.
        self.spot_token = "Nifty 50"
        self.token_to_symbol[self.spot_token] = "NIFTY_SPOT"
        self.discovery_log.append(
            "OK Spot identity locked: exact symbol 'Nifty 50'"
        )

        # Do NOT invent a future token here.
        # The active future will be resolved from Supabase RAW rows.
        self.future_token = ""
        self.future_symbol = ""
        self.future_expiry = None

        self.pcr_tokens = []
        self.pcr_records = {}

        self.discovery_log.append(
            "OK Future + Options will be resolved from authoritative "
            "Supabase symbol/token rows"
        )

        return True


------------------------------------------------------------
PATCH 2 â€” replace fetch_market_snapshot()
------------------------------------------------------------

Replace the complete existing fetch_market_snapshot() method with:

    def fetch_market_snapshot(self):
        if not self.url or not self.key:
            return

        try:
            endpoint = (
                f"{self.url.rstrip('/')}/rest/v1/raw_observations"
                "?select=source,symbol,instrument_token,observation_timestamp,raw"
                "&order=id.desc&limit=300"
            )

            headers = {
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
            }

            resp = requests.get(endpoint, headers=headers, timeout=5)

            if resp.status_code != 200:
                self.last_error = (
                    f"Supabase HTTP {resp.status_code}: "
                    f"{resp.text[:250]}"
                )
                return

            rows = resp.json()
            if not isinstance(rows, list):
                self.last_error = "Supabase RAW BUS returned non-list payload."
                return

            now_ts = now_ist()

            with self.lock:
                for row in reversed(rows):

                    raw = row.get("raw", {})
                    if not isinstance(raw, dict):
                        continue

                    # -------------------------------------------------
                    # AUTHORITATIVE IDENTITY
                    # Supabase row fields win.
                    # Never infer an option as Spot.
                    # -------------------------------------------------
                    row_symbol = str(
                        row.get("symbol")
                        or raw.get("display_symbol")
                        or raw.get("pTrdSymbol")
                        or raw.get("tradingSymbol")
                        or ""
                    ).strip()

                    row_token = str(
                        row.get("instrument_token")
                        or raw.get("exchange_token")
                        or raw.get("pSymbol")
                        or raw.get("pSymbolToken")
                        or raw.get("instrument_token")
                        or raw.get("instrumentToken")
                        or ""
                    ).strip()

                    if not row_symbol or not row_token:
                        continue

                    sym = row_symbol.upper()

                    raw["_parsed_ts"] = now_ts
                    raw["_bus_symbol"] = row_symbol
                    raw["_bus_instrument_token"] = row_token

                    oi_val = self._extract_oi(raw)
                    if is_valid_number(oi_val):
                        raw["oi"] = oi_val
                        raw["open_interest"] = oi_val

                    # -------------------------------------------------
                    # 1. EXACT SPOT ONLY
                    # -------------------------------------------------
                    is_spot = sym == "NIFTY 50"

                    # -------------------------------------------------
                    # 2. FUTURE ONLY
                    #    NIFTY...FUT is never Spot.
                    # -------------------------------------------------
                    is_future = (
                        sym.startswith("NIFTY")
                        and sym.endswith("FUT")
                        and "CE" not in sym
                        and "PE" not in sym
                    )

                    # -------------------------------------------------
                    # 3. OPTION ONLY
                    # -------------------------------------------------
                    is_option = (
                        sym.startswith("NIFTY")
                        and (sym.endswith("CE") or sym.endswith("PE"))
                    )

                    # -------------------------------------------------
                    # 4. HEAVYWEIGHT CASH
                    # -------------------------------------------------
                    heavy_symbol = None
                    for hw in self.heavy_tokens:
                        if sym == hw:
                            heavy_symbol = hw
                            break

                    # Store by the ACTUAL Supabase instrument token.
                    self.latest[row_token] = raw
                    self.tick_buffer.append(raw)

                    # -------------------------------------------------
                    # SPOT ROUTING
                    # An option can NEVER overwrite Spot.
                    # -------------------------------------------------
                    if is_spot:
                        self.latest["Nifty 50"] = raw
                        self.spot_token = row_token
                        self.token_to_symbol[row_token] = "NIFTY_SPOT"

                    # -------------------------------------------------
                    # FUTURE ROUTING
                    # Resolve active future directly from Supabase.
                    # -------------------------------------------------
                    elif is_future:
                        self.future_token = row_token
                        self.future_symbol = row_symbol

                        # Parse expiry from YYYY-MM-DD if available in
                        # the raw row; otherwise leave None.
                        expiry_val = (
                            raw.get("expiry")
                            or raw.get("expiry_date")
                            or raw.get("pExpiryDate")
                        )

                        parsed_expiry = None
                        if expiry_val:
                            try:
                                parsed_expiry = datetime.fromisoformat(
                                    str(expiry_val).replace("Z", "+00:00")
                                )
                                if parsed_expiry.tzinfo is None:
                                    parsed_expiry = parsed_expiry.replace(tzinfo=IST)
                                else:
                                    parsed_expiry = parsed_expiry.astimezone(IST)
                            except Exception:
                                parsed_expiry = None

                        if parsed_expiry is not None:
                            self.future_expiry = parsed_expiry

                        self.token_to_symbol[row_token] = "NIFTY_FUT"

                        pdc = safe_float(
                            raw.get("c")
                            or raw.get("close")
                            or raw.get("pdc")
                        )
                        pdh = safe_float(
                            raw.get("h")
                            or raw.get("high")
                            or raw.get("pdh")
                        )
                        pdl = safe_float(
                            raw.get("l")
                            or raw.get("low")
                            or raw.get("pdl")
                        )
                        open_p = safe_float(
                            raw.get("o")
                            or raw.get("open")
                        )

                        if (
                            is_valid_number(pdc)
                            and self.feature_engine.sess.prev_close is None
                        ):
                            self.feature_engine.set_previous_day(
                                pdc, pdh, pdl
                            )

                        if is_valid_number(open_p):
                            self.feature_engine.set_today_open(open_p)

                    # -------------------------------------------------
                    # OPTIONS
                    # Keep their real token/symbol untouched.
                    # NEVER map CE/PE to Spot.
                    # -------------------------------------------------
                    elif is_option:
                        self.token_to_symbol[row_token] = row_symbol

                    # -------------------------------------------------
                    # HEAVYWEIGHTS
                    # -------------------------------------------------
                    elif heavy_symbol:
                        self.token_to_symbol[row_token] = heavy_symbol

            self.last_error = ""

        except Exception as exc:
            self.last_error = f"Supabase poll error: {exc}"


------------------------------------------------------------
PATCH 3 â€” make UI future lookup safe
------------------------------------------------------------

The existing UI does:

    f = adapter.latest.get(str(adapter.future_token), {})

Keep that line, BUT it now works because future_token is dynamically
resolved from the actual Supabase row.

Do NOT restore:
    "68407"
as a permanent future identity.

------------------------------------------------------------
PATCH 4 â€” remove hardcoded PCR dummy identity
------------------------------------------------------------

Do NOT use this old block anymore:

    dummy_tok_counter = 70000
    ...
    self.pcr_records[tok] = ...

Those are fake tokens and cannot match real Supabase Kotak option rows.

For this surgical stage, leave PCR records empty until the real option rows
are read from Supabase. The raw option observations remain untouched in
self.latest using their real instrument_token + symbol.

------------------------------------------------------------
IMPORTANT â€” DO NOT TOUCH
------------------------------------------------------------

DO NOT MODIFY:
- raw_data_producer_kotak_live.py
- raw_data_producer_yfinance_history.py
- next_day_alpha_engine.py
- next_day_alpha_ui.py
- next_day_alpha_consumer.py
- GSR files
- Supabase schema
- Kotak login/TOTP
- yFinance producer

------------------------------------------------------------
EXPECTED RESULT AFTER DEPLOY
------------------------------------------------------------

If Supabase contains:

    symbol = Nifty 50
    instrument_token = <spot token>
        -> NIFTY SPOT

    symbol = NIFTY26SEPFUT
    instrument_token = 68407
        -> NIFTY FUT

    symbol = NIFTY26SEP24150PE
    instrument_token = <PE token>
        -> OPTION PE

then the consumer will keep these as THREE different instruments.

The option row can no longer overwrite the Spot slot.

The active future is no longer selected from a hardcoded 68407.
It is selected from the actual Supabase row.

This is the surgical fix.
