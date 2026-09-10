"""Build and migrate the Kest PostgreSQL CDC flow."""

from __future__ import annotations

from .client import JsonObject, NifiClient
from .model import (
    CONNECTIONS,
    FAILURE,
    ICEBERG,
    INSPECTION_GROUP_NAME,
    RAW,
    FlowConfig,
    ProcessorSpec,
    processor_specs,
)
from .resources import ComponentManager, delete_group, ensure_parameter_context


def root_groups(api: NifiClient) -> tuple[str, list[JsonObject]]:
    root = api.get("/flow/process-groups/root")["processGroupFlow"]
    return root["id"], root["flow"]["processGroups"]


def find_flow(api: NifiClient, name: str) -> JsonObject | None:
    _, groups = root_groups(api)
    return next(
        (group for group in groups if group["component"]["name"] == name),
        None,
    )


def remove_inspection_groups(api: NifiClient, groups: list[JsonObject]) -> None:
    for group in groups:
        if group["component"]["name"] == INSPECTION_GROUP_NAME:
            delete_group(api, group)


def create_controller_services(manager: ComponentManager) -> dict[str, str]:
    services: dict[str, JsonObject] = {}
    services["aws"] = manager.create_service(
        "org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService",
        "MinIO credentials",
        {
            "Access Key ID": "#{s3.access-key}",
            "Secret Access Key": "#{s3.secret-key}",
        },
    )
    services["file_io"] = manager.create_service(
        "org.apache.nifi.services.iceberg.aws.S3IcebergFileIOProvider",
        "MinIO Iceberg FileIO",
        {
            "Authentication Strategy": "BASIC_CREDENTIALS",
            "Access Key ID": "#{s3.access-key}",
            "Secret Access Key": "#{s3.secret-key}",
            "Client Region": "us-east-1",
            "Endpoint URL": "#{s3.endpoint}",
            "Path Style Access": "true",
        },
    )
    services["catalog"] = manager.create_service(
        "org.apache.nifi.services.iceberg.catalog.RESTIcebergCatalog",
        "Lakekeeper REST catalog",
        {
            "Catalog URI": "#{catalog.uri}",
            "Warehouse Location": "#{catalog.warehouse}",
            "File IO Provider": services["file_io"]["id"],
            "Access Delegation Strategy": "disabled",
            "Authentication Strategy": "BEARER",
            "Bearer Token": "local-auth-disabled",
        },
    )
    services["reader"] = manager.create_service(
        "org.apache.nifi.json.JsonTreeReader",
        "Bronze JSON reader",
        {"Schema Access Strategy": "infer-schema"},
    )
    services["writer"] = manager.create_service(
        "org.apache.nifi.services.iceberg.parquet.ParquetIcebergWriter",
        "Bronze Parquet writer",
        {},
    )
    for service in services.values():
        manager.enable_service(service)
    return {name: service["id"] for name, service in services.items()}


def create_process_group(
    api: NifiClient,
    root_id: str,
    parameter_context_id: str,
    flow_name: str,
) -> JsonObject:
    return api.post(
        f"/process-groups/{root_id}/process-groups",
        {
            "revision": {"version": 0},
            "component": {
                "name": flow_name,
                "comments": "Git-managed flow: docker/nifi/bootstrap.py",
                "position": {"x": 0, "y": 0},
                "parameterContext": {"id": parameter_context_id},
            },
        },
    )


def create_flow(api: NifiClient, config: FlowConfig) -> None:
    root_id, groups = root_groups(api)
    remove_inspection_groups(api, groups)
    parameter_context_id = ensure_parameter_context(
        api, config.parameter_context_name, config.parameters()
    )
    group = create_process_group(api, root_id, parameter_context_id, config.flow_name)
    manager = ComponentManager(api, group["id"])
    specs = processor_specs(create_controller_services(manager))
    processors = {spec.name: manager.create_processor(spec) for spec in specs}

    for connection in CONNECTIONS:
        manager.connect(
            processors[connection.source],
            processors[connection.destination],
            connection.relationships,
        )
    for spec in specs:
        if spec.running:
            manager.start_processor(processors[spec.name])


def upgrade_flow(api: NifiClient, group: JsonObject) -> bool:
    group_id = group["id"]
    processors = api.get(f"/process-groups/{group_id}/processors")["processors"]
    by_name = {item["component"]["name"]: item for item in processors}
    if FAILURE in by_name:
        return False

    missing = {RAW, ICEBERG} - by_name.keys()
    if missing:
        raise RuntimeError(
            f"Cannot upgrade incomplete NiFi flow; missing {sorted(missing)}"
        )

    manager = ComponentManager(api, group_id)
    raw = manager.stop_processor(by_name[RAW])
    iceberg = manager.stop_processor(by_name[ICEBERG])
    raw = manager.set_outbound_relationships(raw, ("failure",))
    iceberg = manager.set_outbound_relationships(iceberg, ("failure",))
    failure = manager.create_processor(
        ProcessorSpec(
            "org.apache.nifi.processors.standard.LogAttribute",
            FAILURE,
            (2400, 0),
            {},
            running=False,
        )
    )
    manager.connect(raw, failure, ("failure",))
    manager.connect(iceberg, failure, ("failure",))
    manager.start_processor(raw)
    manager.start_processor(iceberg)
    return True


def ensure_flow(api: NifiClient, config: FlowConfig) -> str:
    _, groups = root_groups(api)
    remove_inspection_groups(api, groups)
    ensure_parameter_context(api, config.parameter_context_name, config.parameters())
    existing = next(
        (group for group in groups if group["component"]["name"] == config.flow_name),
        None,
    )
    if existing:
        return "upgraded" if upgrade_flow(api, existing) else "unchanged"
    create_flow(api, config)
    return "created"
