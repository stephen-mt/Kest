#!/usr/bin/env python3
"""Check the Git-managed NiFi flow without changing it."""

from bootstrap import FAILURE_PROCESSOR_NAME, FLOW_NAME, Nifi

RUNNING_PROCESSORS = {
    "Receive Debezium HTTP events",
    "Hash raw envelope",
    "Extract CDC metadata",
    "Set storage attributes",
    "Write immutable raw envelope",
    "Build stable Bronze event",
    "Append Bronze Iceberg event",
}


def main():
    api = Nifi()
    root = api.request("GET", "/flow/process-groups/root")["processGroupFlow"]
    group = next(
        (
            item
            for item in root["flow"]["processGroups"]
            if item["component"]["name"] == FLOW_NAME
        ),
        None,
    )
    if group is None:
        raise RuntimeError(f"NiFi flow is missing: {FLOW_NAME}")

    flow = api.request("GET", f"/flow/process-groups/{group['id']}")[
        "processGroupFlow"
    ]["flow"]
    by_name = {item["component"]["name"]: item for item in flow["processors"]}
    expected = RUNNING_PROCESSORS | {FAILURE_PROCESSOR_NAME}
    missing = expected - by_name.keys()
    if missing:
        raise RuntimeError(f"NiFi processors are missing: {sorted(missing)}")

    bad_processors = []
    for name in RUNNING_PROCESSORS:
        component = by_name[name]["component"]
        if component["state"] != "RUNNING" or component.get("validationErrors"):
            bad_processors.append(name)
    failure = by_name[FAILURE_PROCESSOR_NAME]["component"]
    if failure["state"] != "STOPPED" or failure.get("validationErrors"):
        bad_processors.append(FAILURE_PROCESSOR_NAME)
    if bad_processors:
        raise RuntimeError(f"NiFi processors are not ready: {bad_processors}")

    services = api.request(
        "GET", f"/flow/process-groups/{group['id']}/controller-services"
    )["controllerServices"]
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
