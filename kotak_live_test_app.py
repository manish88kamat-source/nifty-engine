import re
import time

import streamlit as st

from market_data_hub import MarketDataHub
from kotak_market_data_source import (
    KotakMarketDataSource,
)


st.set_page_config(
    page_title="Kotak → Market Data Hub Live Test",
    layout="wide",
)


st.title(
    "Kotak Neo → Market Data Hub Live Test"
)

st.write(
    "This test verifies the real Kotak → Market Data Hub path."
)

st.markdown(
    """
### Test sequence

1. Streamlit Secrets
2. Kotak authentication
3. Live subscription
4. Real Kotak tick
5. Market Data Hub ingestion
6. Market Data Hub persistence
"""
)


st.divider()


st.subheader(
    "Current Kotak TOTP"
)

totp = st.text_input(
    "Enter the CURRENT 6-digit code from your Authenticator app",
    type="password",
    max_chars=6,
    placeholder="123456",
    help=(
        "Do not save the OTP in Streamlit Secrets. "
        "Enter the currently displayed 6-digit code here."
    ),
)


start_test = st.button(
    "Start Kotak Live Test",
    type="primary",
)


if start_test:

    # --------------------------------------------------------
    # Validate TOTP locally
    # --------------------------------------------------------

    clean_totp = (
        str(totp or "")
        .strip()
    )

    if not re.fullmatch(
        r"\d{6}",
        clean_totp,
    ):

        st.error(
            "Please enter the current 6-digit Authenticator code."
        )

        st.stop()


    # --------------------------------------------------------
    # Create independent Hub
    # --------------------------------------------------------

    hub = MarketDataHub()

    source = KotakMarketDataSource(
        hub=hub
    )


    # --------------------------------------------------------
    # Authentication
    # --------------------------------------------------------

    auth_box = st.empty()

    try:

        auth_box.info(
            "AUTHENTICATION : connecting to Kotak Neo..."
        )

        source.authenticate(
            totp_override=clean_totp
        )

        auth_box.success(
            "AUTHENTICATION : PASS"
        )


        # ----------------------------------------------------
        # Minimal real subscription
        # ----------------------------------------------------

        instruments = [
            {
                "instrument_token": "Nifty 50",
                "exchange_segment": "nse_cm",
            }
        ]


        sub_box = st.empty()

        sub_box.info(
            "SUBSCRIPTION : subscribing to NIFTY..."
        )

        source.subscribe(
            instruments=instruments,
            is_index=True,
        )

        sub_box.success(
            "SUBSCRIPTION : PASS"
        )


        # ----------------------------------------------------
        # Wait for REAL tick
        # ----------------------------------------------------

        st.info(
            "Waiting for REAL Kotak tick..."
        )


        progress = st.progress(
            0
        )

        status_area = st.empty()

        started = time.monotonic()

        timeout_seconds = 30.0


        while (
            time.monotonic()
            - started
            < timeout_seconds
        ):

            elapsed = (
                time.monotonic()
                - started
            )

            pct = min(
                1.0,
                elapsed / timeout_seconds,
            )

            progress.progress(
                pct
            )


            health = source.health(
                max_age_seconds=30.0
            )

            hub_health = hub.health(
                max_age_seconds=30.0
            )


            status_area.json(
                {
                    "kotak": {
                        "stream_state": health.get(
                            "stream_state"
                        ),
                        "ticks_received": health.get(
                            "ticks_received"
                        ),
                        "ticks_accepted": health.get(
                            "ticks_accepted"
                        ),
                        "last_symbol": health.get(
                            "last_symbol"
                        ),
                        "last_ltp": health.get(
                            "last_ltp"
                        ),
                        "data_age_seconds": health.get(
                            "data_age_seconds"
                        ),
                    },
                    "hub": {
                        "received_count": hub_health.get(
                            "received_count"
                        ),
                        "persisted_count": hub_health.get(
                            "persisted_count"
                        ),
                        "status": hub_health.get(
                            "status"
                        ),
                    },
                }
            )


            if (
                health.get(
                    "ticks_accepted",
                    0,
                )
                > 0
            ):
                break


            time.sleep(
                1.0
            )


        progress.progress(
            1.0
        )


        # ----------------------------------------------------
        # Final verification
        # ----------------------------------------------------

        final_source = source.health(
            max_age_seconds=30.0
        )

        final_hub = hub.health(
            max_age_seconds=30.0
        )


        st.divider()

        st.subheader(
            "Final Result"
        )


        col1, col2, col3 = st.columns(
            3
        )


        with col1:

            st.metric(
                "Kotak Ticks Received",
                final_source.get(
                    "ticks_received",
                    0,
                ),
            )


        with col2:

            st.metric(
                "Ticks Accepted",
                final_source.get(
                    "ticks_accepted",
                    0,
                ),
            )


        with col3:

            st.metric(
                "Hub Persisted",
                final_hub.get(
                    "persisted_count",
                    0,
                ),
            )


        # ----------------------------------------------------
        # PASS / FAIL
        # ----------------------------------------------------

        ticks_ok = (
            final_source.get(
                "ticks_accepted",
                0,
            )
            > 0
        )

        hub_ok = (
            final_hub.get(
                "received_count",
                0,
            )
            > 0
            and
            final_hub.get(
                "persisted_count",
                0,
            )
            > 0
        )


        if ticks_ok and hub_ok:

            st.success(
                "LIVE DATA TEST : PASS"
            )

            st.write(
                "Real Kotak data successfully reached "
                "the independent Market Data Hub."
            )


            st.json(
                {
                    "source": "KOTAK_NEO",
                    "symbol": final_source.get(
                        "last_symbol"
                    ),
                    "ltp": final_source.get(
                        "last_ltp"
                    ),
                    "ticks_received": final_source.get(
                        "ticks_received"
                    ),
                    "ticks_accepted": final_source.get(
                        "ticks_accepted"
                    ),
                    "hub_received": final_hub.get(
                        "received_count"
                    ),
                    "hub_persisted": final_hub.get(
                        "persisted_count"
                    ),
                }
            )


        else:

            st.error(
                "LIVE DATA TEST : FAIL"
            )


            st.write(
                "No complete Kotak → Hub path was confirmed."
            )


            st.json(
                {
                    "kotak": final_source,
                    "hub": final_hub,
                }
            )


    except Exception as exc:

        st.error(
            "LIVE DATA TEST : FAIL"
        )

        st.write(
            f"{type(exc).__name__}: {exc}"
        )

    finally:

        source.stop()
