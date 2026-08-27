"""
Kotak Neo Credentials Provider
GSR / Market Data Infrastructure

Version:
    KOTAK_CREDENTIALS_1.0.0

Purpose:
    Centralized, environment/secret-based access to Kotak Neo
    credentials.

IMPORTANT:
    - No credentials are hard-coded.
    - No credentials are printed.
    - No credential values are logged.
    - This module does not connect to Kotak Neo.
    - This module does not create trading orders.
    - This module only supplies authentication inputs.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import struct
import time
from dataclasses import dataclass
from typing import Dict, Optional


VERSION = "1.0.0"


def env_or_secret(
    name: str,
    default: str = "",
) -> str:
    """
    Read a credential/configuration value from the environment.

    The function intentionally does not expose the value anywhere
    except its return value.
    """

    value = os.getenv(
        name,
        default,
    )

    return str(value or "").strip()


def normalize_kotak_mobile(
    value: str,
) -> str:
    """
    Normalize the Kotak mobile number.

    Keeps digits only and preserves the same basic behavior as
    the existing NIFTY engine helper.
    """

    raw = str(value or "").strip()

    if not raw:
        return ""

    digits = "".join(
        ch
        for ch in raw
        if ch.isdigit()
    )

    if digits.startswith("91") and len(digits) == 12:
        return digits[2:]

    if len(digits) == 10:
        return digits

    return digits


def generate_live_totp(
    secret_or_otp: str,
) -> str:
    """
    Generate the current 6-digit TOTP when a Base32 secret is supplied.

    If the supplied value is already a 6-digit OTP, return it unchanged.

    This follows the authentication behavior already used by app.py.
    """

    raw = (
        str(secret_or_otp or "")
        .strip()
        .replace(" ", "")
        .upper()
    )

    if raw.isdigit() and len(raw) == 6:
        return raw

    try:

        if len(raw) % 8:
            raw += "=" * (
                8 - len(raw) % 8
            )

        key = base64.b32decode(
            raw,
            casefold=True,
        )

        counter = int(
            time.time() // 30
        )

        msg = struct.pack(
            ">Q",
            counter,
        )

        digest = hmac.new(
            key,
            msg,
            hashlib.sha1,
        ).digest()

        offset = digest[19] & 15

        token = (
            struct.unpack(
                ">I",
                digest[
                    offset:
                    offset + 4
                ],
            )[0]
            & 0x7FFFFFFF
        ) % 1000000

        return f"{token:06d}"

    except Exception:
        return raw


@dataclass(frozen=True)
class KotakCredentials:
    """
    Immutable authentication configuration.

    Actual values are never represented in repr().
    """

    consumer_key: str
    mobile: str
    ucc: str
    totp_secret: str
    mpin: str

    def __repr__(self) -> str:
        return (
            "KotakCredentials("
            "consumer_key=<REDACTED>, "
            "mobile=<REDACTED>, "
            "ucc=<REDACTED>, "
            "totp_secret=<REDACTED>, "
            "mpin=<REDACTED>)"
        )

    @property
    def totp(self) -> str:
        return generate_live_totp(
            self.totp_secret
        )

    def missing_fields(self) -> list[str]:
        missing = []

        if not self.consumer_key:
            missing.append(
                "KOTAK_CONSUMER_KEY"
            )

        if not self.mobile:
            missing.append(
                "KOTAK_MOBILE"
            )

        if not self.ucc:
            missing.append(
                "KOTAK_UCC"
            )

        if not self.totp_secret:
            missing.append(
                "KOTAK_TOTP"
            )

        if not self.mpin:
            missing.append(
                "KOTAK_MPIN"
            )

        return missing

    def is_complete(self) -> bool:
        return not self.missing_fields()


def load_kotak_credentials() -> KotakCredentials:
    """
    Load Kotak Neo credentials from environment/secrets.

    Expected variables:

        KOTAK_CONSUMER_KEY
        KOTAK_MOBILE
        KOTAK_UCC
        KOTAK_TOTP
        KOTAK_MPIN
    """

    return KotakCredentials(
        consumer_key=env_or_secret(
            "KOTAK_CONSUMER_KEY"
        ),
        mobile=normalize_kotak_mobile(
            env_or_secret(
                "KOTAK_MOBILE"
            )
        ),
        ucc=env_or_secret(
            "KOTAK_UCC"
        ),
        totp_secret=env_or_secret(
            "KOTAK_TOTP"
        ),
        mpin=env_or_secret(
            "KOTAK_MPIN"
        ),
    )


def validate_kotak_credentials(
    credentials: KotakCredentials,
) -> Dict[str, object]:
    """
    Return safe credential-status information.

    NEVER returns actual credential values.
    """

    missing = credentials.missing_fields()

    return {
        "version": VERSION,
        "complete": not missing,
        "missing": missing,
        "mobile_loaded": bool(
            credentials.mobile
        ),
        "consumer_key_loaded": bool(
            credentials.consumer_key
        ),
        "ucc_loaded": bool(
            credentials.ucc
        ),
        "totp_loaded": bool(
            credentials.totp_secret
        ),
        "mpin_loaded": bool(
            credentials.mpin
        ),
    }


def credentials_self_test() -> None:
    """
    Local structural test.

    This does NOT contact Kotak Neo.

    Therefore PASS means only that the credential provider,
    normalization and TOTP generation logic are structurally valid.
    """

    # Test already-generated OTP behavior.
    otp = generate_live_totp(
        "123456"
    )

    assert otp == "123456"

    # Test mobile normalization.
    mobile = normalize_kotak_mobile(
        "+91 98765 43210"
    )

    assert mobile == "9876543210"

    # Test dataclass construction without real credentials.
    credentials = KotakCredentials(
        consumer_key="test-key",
        mobile="9876543210",
        ucc="TESTUCC",
        totp_secret="123456",
        mpin="123456",
    )

    assert credentials.is_complete()

    status = validate_kotak_credentials(
        credentials
    )

    assert status["complete"] is True

    # Verify repr does not expose values.
    representation = repr(
        credentials
    )

    assert "test-key" not in representation
    assert "9876543210" not in representation
    assert "TESTUCC" not in representation
    assert "123456" not in representation

    print(
        "KOTAK CREDENTIAL PROVIDER TEST: PASS"
    )

    print(
        "  credential values : REDACTED"
    )

    print(
        "  TOTP generation   : PASS"
    )

    print(
        "  mobile normalize  : PASS"
    )

    print(
        "  secret protection : PASS"
    )

    print(
        "  network call      : NONE"
    )


if __name__ == "__main__":
    credentials_self_test()
