#!/usr/bin/env python3
"""Create or upgrade the Git-managed Kest NiFi flow."""

from kest_nifi.client import NifiClient
from kest_nifi.flow import ensure_flow
from kest_nifi.model import FlowConfig


def main() -> None:
    config = FlowConfig.from_env()
    action = ensure_flow(NifiClient(), config)
    messages = {
        "created": "Created and started NiFi flow",
        "upgraded": "Upgraded NiFi flow",
        "unchanged": "NiFi flow already exists",
    }
    print(f"{messages[action]}: {config.flow_name}")


if __name__ == "__main__":
    main()
