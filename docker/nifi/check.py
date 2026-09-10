#!/usr/bin/env python3
"""Check the Git-managed NiFi flow without changing it."""

from kest_nifi.client import NifiClient
from kest_nifi.flow import find_flow
from kest_nifi.model import FAILURE, FLOW_NAME, RUNNING_PROCESSORS


def main():
    api = NifiClient()
    group = find_flow(api)
    if group is None:
        raise RuntimeError(f"NiFi flow is missing: {FLOW_NAME}")

    flow = api.get(f"/flow/process-groups/{group['id']}")["processGroupFlow"]["flow"]
    by_name = {item["component"]["name"]: item for item in flow["processors"]}
    expected = RUNNING_PROCESSORS | {FAILURE}
    missing = expected - by_name.keys()
    if missing:
        raise RuntimeError(f"NiFi processors are missing: {sorted(missing)}")

    bad_processors = []
    for name in RUNNING_PROCESSORS:
        component = by_name[name]["component"]
        if component["state"] != "RUNNING" or component.get("validationErrors"):
            bad_processors.append(name)
    failure = by_name[FAILURE]["component"]
    if failure["state"] != "STOPPED" or failure.get("validationErrors"):
        bad_processors.append(FAILURE)
    if bad_processors:
        raise RuntimeError(f"NiFi processors are not ready: {bad_processors}")

    services = api.get(f"/flow/process-groups/{group['id']}/controller-services")[
        "controllerServices"
    ]
    bad_services = [
        item["component"]["name"]
        for item in services
        if item["component"]["state"] != "ENABLED"
        or item["component"].get("validationErrors")
    ]
    if bad_services:
        raise RuntimeError(f"NiFi controller services are not ready: {bad_services}")

    print(
        f"NiFi flow ready: {FLOW_NAME} "
        f"({len(flow['processors'])} processors, {len(services)} services, "
        "failure queue retained)"
    )


if __name__ == "__main__":
    main()
