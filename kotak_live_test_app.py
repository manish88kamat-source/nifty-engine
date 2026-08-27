import streamlit as st
import time

from market_data_hub import MarketDataHub
from kotak_market_data_source import (
    KotakMarketDataSource,
)


st.set_page_config(
    page_title="Kotak → Market Data Hub Test",
    layout="wide",
)


st.title(
    "Kotak Neo → Market Data Hub Live Test"
)


st.write(
    "This test checks:"
)

st.write(
    """
    1. Streamlit Secrets loading
    2. Kotak authentication
    3. Live subscription
    4. Real tick ingestion
    5. Market Data Hub persistence
    """
)


if st.button(
    "Start Kotak Live Test"
):

    hub = MarketDataHub()

    source = KotakMarketDataSource(
        hub=hub
    )

    status_box = st.empty()

    try:

        status_box.info(
            "Connecting to Kotak Neo..."
        )

        source.authenticate()

        status_box.success(
            "AUTHENTICATION : PASS"
        )


        instruments = [
            {
                "instrument_token": "Nifty 50",
                "exchange_segment": "nse_cm",
            }
        ]


        source.subscribe(
            instruments
        )


        status_box.success(
            "SUBSCRIPTION : PASS"
        )


        st.write(
            "Waiting for live tick..."
        )


        start = time.time()


        while time.time() - start < 30:

            health = source.health()

            hub_health = hub.health()


            st.json(
                {
                    "Kotak": health,
                    "Hub": hub_health,
                }
            )


            if (
                health["ticks_accepted"]
                > 0
            ):
                break


            time.sleep(2)


        final_source = source.health()

        final_hub = hub.health()


        if (
            final_source["ticks_accepted"]
            > 0
            and
            final_hub["persisted_count"]
            > 0
        ):

            st.success(
                "LIVE DATA TEST : PASS"
            )

        else:

            st.error(
                "LIVE DATA TEST : FAIL"
            )


    except Exception as e:

        st.error(
            f"FAILED: {e}"
        )
