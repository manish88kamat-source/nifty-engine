"""
GSR Configuration
Version: GSR_1.0.0_IMPLEMENTATION

Central configuration layer.
No strategy logic.
No market opinion.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class GSRConfig:

    architecture_version: str = "GSR_1.0.0"
    implementation_version: str = "1.0.0"

    # Data contracts
    data_contract_version: str = "1.0.0"
    feature_version: str = "1.0.0"
    regime_version: str = "1.0.0"

    # Research safety
    lookahead_bias_forbidden: bool = True
    source_traceability_required: bool = True
    immutable_records: bool = True

    # Feature settings
    ema_fast: int = 20
    ema_medium: int = 50
    ema_slow: int = 200

    atr_period: int = 14
    rsi_period: int = 14

    # Storage
    storage_format: str = "JSON"

    # Runtime
    environment: str = "research"


DEFAULT_CONFIG = GSRConfig()


def get_config() -> GSRConfig:
    return DEFAULT_CONFIG


def config_test():

    cfg = get_config()

    assert cfg.architecture_version == "GSR_1.0.0"
    assert cfg.lookahead_bias_forbidden is True
    assert cfg.source_traceability_required is True

    print("GSR CONFIG TEST: PASS")


if __name__ == "__main__":
    config_test()
