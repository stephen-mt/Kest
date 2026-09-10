"""Create and manage NiFi resources used by the Kest flow."""

from __future__ import annotations

import time

from .client import JsonObject, NifiClient
from .model import ProcessorSpec


def bundle_for(type_name: str, available: list[JsonObject]) -> JsonObject:
    try:
        return next(item["bundle"] for item in available if item["type"] == type_name)
    except StopIteration as error:
        raise RuntimeError(f"NiFi component is unavailable: {type_name}") from error


def delete_group(api: NifiClient, group: JsonObject) -> None:
    group_id = group["id"]
    for entity in api.get(f"/process-groups/{group_id}/processors")["processors"]:
        api.delete(
            f"/processors/{entity['id']}?version={entity['revision']['version']}"
        )
    services = api.get(f"/flow/process-groups/{group_id}/controller-services")
    for entity in services["controllerServices"]:
        api.delete(
            f"/controller-services/{entity['id']}?version="
            f"{entity['revision']['version']}"
        )
    latest = api.get(f"/process-groups/{group_id}")
    api.delete(f"/process-groups/{group_id}?version={latest['revision']['version']}")


def ensure_parameter_context(
    api: NifiClient,
    name: str,
    parameters: dict[str, tuple[str, bool]],
) -> str:
    contexts = api.get("/flow/parameter-contexts")["parameterContexts"]
    existing = next(
        (item for item in contexts if item["component"]["name"] == name), None
    )
    if existing:
        return existing["id"]

    entity = api.post(
        "/parameter-contexts",
        {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "description": "Runtime values for the Git-managed Kest CDC flow.",
                "parameters": [
                    {
                        "parameter": {
                            "name": parameter_name,
                            "value": value,
                            "sensitive": sensitive,
                        }
                    }
                    for parameter_name, (value, sensitive) in parameters.items()
                ],
            },
        },
    )
    return entity["id"]


class ComponentManager:
    def __init__(self, api: NifiClient, group_id: str) -> None:
        self.api = api
        self.group_id = group_id
        self.processor_types = api.get("/flow/processor-types")["processorTypes"]
        self.service_types = api.get("/flow/controller-service-types")[
            "controllerServiceTypes"
        ]

    def create_service(
        self, type_name: str, name: str, properties: dict[str, str]
    ) -> JsonObject:
        return self.api.post(
            f"/process-groups/{self.group_id}/controller-services",
            {
                "revision": {"version": 0},
                "component": {
                    "type": type_name,
                    "bundle": bundle_for(type_name, self.service_types),
                    "name": name,
                    "properties": properties,
                },
            },
        )

    def enable_service(self, entity: JsonObject) -> None:
        service_id = entity["id"]
        self.api.put(
            f"/controller-services/{service_id}/run-status",
            {
                "revision": {"version": entity["revision"]["version"]},
                "state": "ENABLED",
            },
        )
        for _ in range(60):
            current = self.api.get(f"/controller-services/{service_id}")
            state = current["component"]["state"]
            if state == "ENABLED":
                return
            if state == "DISABLED":
                problems = current["component"].get("validationErrors") or []
                raise RuntimeError(
                    f"Controller service {service_id} is invalid: {problems}"
                )
            time.sleep(0.5)
        raise RuntimeError(f"Controller service {service_id} did not enable")

    def create_processor(self, spec: ProcessorSpec) -> JsonObject:
        entity = self.api.post(
            f"/process-groups/{self.group_id}/processors",
            {
                "revision": {"version": 0},
                "component": {
                    "type": spec.type_name,
                    "bundle": bundle_for(spec.type_name, self.processor_types),
                    "name": spec.name,
                    "position": {"x": spec.position[0], "y": spec.position[1]},
                    "config": {
                        "properties": spec.properties,
                        "schedulingPeriod": "1 sec",
                        "executionNode": "ALL",
                    },
                },
            },
        )
        return self.set_outbound_relationships(entity, spec.outbound)

    def set_outbound_relationships(
        self, entity: JsonObject, outbound: tuple[str, ...]
    ) -> JsonObject:
        latest = self.api.get(f"/processors/{entity['id']}")
        relationships = {item["name"] for item in latest["component"]["relationships"]}
        unknown = set(outbound) - relationships
        if unknown:
            raise RuntimeError(
                f"Unknown relationships for {latest['component']['name']}: "
                f"{sorted(unknown)}"
            )
        return self.api.put(
            f"/processors/{entity['id']}",
            {
                "revision": {"version": latest["revision"]["version"]},
                "component": {
                    "id": entity["id"],
                    "config": {
                        "autoTerminatedRelationships": sorted(
                            relationships - set(outbound)
                        )
                    },
                },
            },
        )

    def connect(
        self,
        source: JsonObject,
        destination: JsonObject,
        relationships: tuple[str, ...],
    ) -> JsonObject:
        return self.api.post(
            f"/process-groups/{self.group_id}/connections",
            {
                "revision": {"version": 0},
                "component": {
                    "name": (
                        f"{source['component']['name']} to "
                        f"{destination['component']['name']}"
                    ),
                    "source": {
                        "id": source["id"],
                        "groupId": self.group_id,
                        "type": "PROCESSOR",
                    },
                    "destination": {
                        "id": destination["id"],
                        "groupId": self.group_id,
                        "type": "PROCESSOR",
                    },
                    "selectedRelationships": list(relationships),
                    "backPressureObjectThreshold": 10000,
                    "backPressureDataSizeThreshold": "1 GB",
                    "flowFileExpiration": "0 sec",
                },
            },
        )

    def start_processor(self, entity: JsonObject) -> None:
        latest = self.api.get(f"/processors/{entity['id']}")
        problems = latest["component"].get("validationErrors") or []
        if problems:
            raise RuntimeError(
                f"Processor {latest['component']['name']} is invalid: {problems}"
            )
        self._set_processor_state(latest, "RUNNING")

    def stop_processor(self, entity: JsonObject) -> JsonObject:
        latest = self.api.get(f"/processors/{entity['id']}")
        if latest["component"]["state"] == "STOPPED":
            return latest
        self._set_processor_state(latest, "STOPPED")
        for _ in range(60):
            latest = self.api.get(f"/processors/{entity['id']}")
            if latest["component"]["state"] == "STOPPED":
                return latest
            time.sleep(0.5)
        raise RuntimeError(f"Processor {latest['component']['name']} did not stop")

    def _set_processor_state(self, entity: JsonObject, state: str) -> None:
        self.api.put(
            f"/processors/{entity['id']}/run-status",
            {
                "revision": {"version": entity["revision"]["version"]},
                "state": state,
            },
        )
