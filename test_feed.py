import os
import sys
from neo_api_client import NeoAPI

consumer_key = os.getenv("KOTAK_CONSUMER_KEY")
mobile = os.getenv("KOTAK_MOBILE")
ucc = os.getenv("KOTAK_UCC")
totp = os.getenv("KOTAK_TOTP")
mpin = os.getenv("KOTAK_MPIN")

print("--- CREDENTIALS CHECK ---")
print(f"Consumer Key present: {bool(consumer_key)}")
print(f"Mobile present: {bool(mobile)}")
print(f"UCC present: {bool(ucc)}")

try:
    client = NeoAPI(environment="prod", consumer_key=consumer_key)
    l1 = client.totp_login(mobile_number=mobile, ucc=ucc, totp=totp)
    print("Step 1 Login:", l1)
    
    l2 = client.totp_validate(mpin=mpin)
    print("Step 2 Validate:", l2)

    # Test 1: Equity Quote (Reliance)
    q_eq = client.quotes(instrument_tokens=[{"instrument_token": "2885", "exchange_segment": "nse_cm"}])
    print("\n--- RELIANCE QUOTE ---")
    print(q_eq)

    # Test 2: Nifty Spot Index
    q_idx = client.quotes(instrument_tokens=[{"instrument_token": "26000", "exchange_segment": "nse_cm"}])
    print("\n--- NIFTY SPOT QUOTE ---")
    print(q_idx)

except Exception as e:
    print(f"\nERROR OCCURRED: {e}")
    sys.exit(1)
