#!/usr/bin/env python3
"""Create or upgrade the Git-managed Kest NiFi flow."""

from kest_nifi.client import NifiClient
from kest_nifi.flow import ensure_flow
from kest_nifi.model import FLOW_NAME, FlowConfig


def main() -> None:
    action = ensure_flow(NifiClient(), FlowConfig.from_env())
    messages = {
        "created": "Created and started NiFi flow",
        "upgraded": "Upgraded NiFi flow",
        "unchanged": "NiFi flow already exists",
    }
    print(f"{messages[action]}: {FLOW_NAME}")


if __name__ == "__main__":
    main()
