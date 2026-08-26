"""
GSR Version Manager
Version: GSR_1.0.0_IMPLEMENTATION

Purpose:
Manage version lineage of GSR research components.

Rules:
- No strategy logic
- No execution logic
- Only version tracking
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass(frozen=True)
class VersionRecord:
    component: str
    version: str
    status: str
    description: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class GSRVersionManager:

    def __init__(self):
        self.registry: Dict[str, VersionRecord] = {}

    def register(
        self,
        component: str,
        version: str,
        description: str,
        status: str = "ACTIVE"
    ) -> VersionRecord:

        if component in self.registry:
            raise ValueError(
                f"Component already registered: {component}"
            )

        record = VersionRecord(
            component=component,
            version=version,
            status=status,
            description=description
        )

        self.registry[component] = record

        return record

    def get_version(
        self,
        component: str
    ) -> str:

        if component not in self.registry:
            raise KeyError(
                f"Unknown component: {component}"
            )

        return self.registry[component].version


    def compatibility_check(
        self,
        component: str,
        required_version: str
    ) -> bool:

        current = self.get_version(component)

        return current == required_version


    def export_registry(self) -> Dict[str, Any]:

        return {
            key: value.to_dict()
            for key, value in self.registry.items()
        }


def create_version_manager():
    return GSRVersionManager()


def version_manager_test():

    manager = GSRVersionManager()

    manager.register(
        component="gsr_config",
        version="1.0.0",
        description="Core configuration layer"
    )

    manager.register(
        component="gsr_models",
        version="1.0.0",
        description="Core research models"
    )

    assert manager.get_version(
        "gsr_config"
    ) == "1.0.0"

    assert manager.compatibility_check(
        "gsr_models",
        "1.0.0"
    )

    print("GSR VERSION MANAGER TEST: PASS")


if __name__ == "__main__":
    version_manager_test()
