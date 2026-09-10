#!/usr/bin/env python3
"""Create the versioned Kest PostgreSQL CDC flow in Apache NiFi."""

from __future__ import annotations

import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://127.0.0.1:8443/nifi-api"
FLOW_NAME = "Kest PostgreSQL CDC v1"
PARAMETER_CONTEXT_NAME = "Kest PostgreSQL CDC local"
FAILURE_PROCESSOR_NAME = "FAILED - inspect and replay"
TLS = ssl._create_unverified_context()


class Nifi:
    def __init__(self):
        self.token = self._wait_for_login()

    def _wait_for_login(self):
        last_error = None
        for _ in range(60):
            try:
                return self._login()
            except urllib.error.URLError as error:
                last_error = error
                time.sleep(2)
        raise RuntimeError(
            "NiFi API did not become ready within 120 seconds"
        ) from last_error

    def _login(self):
        body = urllib.parse.urlencode(
            {
                "username": os.environ["SINGLE_USER_CREDENTIALS_USERNAME"],
                "password": os.environ["SINGLE_USER_CREDENTIALS_PASSWORD"],
            }
        ).encode()
        request = urllib.request.Request(
            BASE_URL + "/access/token",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(request, context=TLS, timeout=10) as response:
            return response.read().decode()

    def request(self, method, path, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            BASE_URL + path,
            data=data,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, context=TLS, timeout=30) as response:
                if response.status == 204:
                    return None
                return json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read().decode()
            raise RuntimeError(
                f"NiFi {method} {path}: HTTP {error.code}: {detail}"
            ) from error


def bundle_for(type_name, available):
    try:
        return next(item["bundle"] for item in available if item["type"] == type_name)
    except StopIteration as error:
        raise RuntimeError(f"NiFi component is unavailable: {type_name}") from error


def delete_group(api, group):
    group_id = group["id"]
    for entity in api.request("GET", f"/process-groups/{group_id}/processors")[
        "processors"
    ]:
        api.request(
            "DELETE",
            f"/processors/{entity['id']}?version={entity['revision']['version']}",
        )
    for entity in api.request(
        "GET", f"/flow/process-groups/{group_id}/controller-services"
    )["controllerServices"]:
        api.request(
            "DELETE",
            f"/controller-services/{entity['id']}?version={entity['revision']['version']}",
        )
    latest = api.request("GET", f"/process-groups/{group_id}")
    api.request(
        "DELETE",
        f"/process-groups/{group_id}?version={latest['revision']['version']}",
    )


def create_parameter_context(api):
    contexts = api.request("GET", "/flow/parameter-contexts")["parameterContexts"]
    for entity in contexts:
        if entity["component"]["name"] == PARAMETER_CONTEXT_NAME:
            return entity["id"]

    parameters = {
        "source.name": (
            os.environ.get("INGESTION_SOURCE_NAME", "postgres-source"),
            False,
        ),
        "listener.port": (os.environ.get("NIFI_LISTENER_PORT", "9090"), False),
        "listener.path": (
            os.environ.get("INGESTION_HTTP_PATH", "cdc/postgres-source"),
            False,
        ),
        "s3.endpoint": ("http://minio:9000", False),
        "s3.bucket": (os.environ["MINIO_BUCKET"], False),
        "s3.access-key": (os.environ["MINIO_ROOT_USER"], True),
        "s3.secret-key": (os.environ["MINIO_ROOT_PASSWORD"], True),
        "catalog.uri": ("http://lakekeeper:8181/catalog", False),
        "catalog.warehouse": (os.environ["LAKEKEEPER_WAREHOUSE"], False),
        "bronze.namespace": ("bronze_ingestion", False),
        "bronze.table": ("postgres_cdc_events", False),
    }
    entity = api.request(
        "POST",
        "/parameter-contexts",
        {
            "revision": {"version": 0},
            "component": {
                "name": PARAMETER_CONTEXT_NAME,
                "description": "Runtime values for the Git-managed Kest PostgreSQL CDC flow.",
                "parameters": [
                    {
                        "parameter": {
                            "name": name,
                            "value": value,
                            "sensitive": sensitive,
                        }
                    }
                    for name, (value, sensitive) in parameters.items()
                ],
            },
        },
    )
    return entity["id"]


def create_service(api, group_id, available, type_name, name, properties):
    entity = api.request(
        "POST",
        f"/process-groups/{group_id}/controller-services",
        {
            "revision": {"version": 0},
            "component": {
                "type": type_name,
                "bundle": bundle_for(type_name, available),
                "name": name,
                "properties": properties,
            },
        },
    )
    return entity


def enable_service(api, entity):
    service_id = entity["id"]
    revision = entity["revision"]["version"]
    api.request(
        "PUT",
        f"/controller-services/{service_id}/run-status",
        {"revision": {"version": revision}, "state": "ENABLED"},
    )
    for _ in range(60):
        current = api.request("GET", f"/controller-services/{service_id}")
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


def create_processor(
    api, group_id, available, type_name, name, position, properties, outbound
):
    entity = api.request(
        "POST",
        f"/process-groups/{group_id}/processors",
        {
            "revision": {"version": 0},
            "component": {
                "type": type_name,
                "bundle": bundle_for(type_name, available),
                "name": name,
                "position": {"x": position[0], "y": position[1]},
                "config": {
                    "properties": properties,
                    "schedulingPeriod": "1 sec",
                    "executionNode": "ALL",
                },
            },
        },
    )
    relationships = {item["name"] for item in entity["component"]["relationships"]}
    unknown = set(outbound) - relationships
    if unknown:
        raise RuntimeError(f"Unknown relationships for {name}: {sorted(unknown)}")
    auto_terminated = sorted(relationships - set(outbound))
    entity = api.request(
        "PUT",
        f"/processors/{entity['id']}",
        {
            "revision": {"version": entity["revision"]["version"]},
            "component": {
                "id": entity["id"],
                "config": {"autoTerminatedRelationships": auto_terminated},
            },
        },
    )
    return entity


def connect(api, group_id, source, destination, relationships):
    return api.request(
        "POST",
        f"/process-groups/{group_id}/connections",
        {
            "revision": {"version": 0},
            "component": {
                "name": f"{source['component']['name']} to {destination['component']['name']}",
                "source": {
                    "id": source["id"],
                    "groupId": group_id,
                    "type": "PROCESSOR",
                },
                "destination": {
                    "id": destination["id"],
                    "groupId": group_id,
                    "type": "PROCESSOR",
                },
                "selectedRelationships": relationships,
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB",
                "flowFileExpiration": "0 sec",
            },
        },
    )


def start_processor(api, entity):
    latest = api.request("GET", f"/processors/{entity['id']}")
    problems = latest["component"].get("validationErrors") or []
    if problems:
        raise RuntimeError(
            f"Processor {latest['component']['name']} is invalid: {problems}"
        )
    api.request(
        "PUT",
        f"/processors/{entity['id']}/run-status",
        {
            "revision": {"version": latest["revision"]["version"]},
            "state": "RUNNING",
        },
    )


def stop_processor(api, entity):
    latest = api.request("GET", f"/processors/{entity['id']}")
    if latest["component"]["state"] == "STOPPED":
        return latest
    api.request(
        "PUT",
        f"/processors/{entity['id']}/run-status",
        {
            "revision": {"version": latest["revision"]["version"]},
            "state": "STOPPED",
        },
    )
    for _ in range(60):
        latest = api.request("GET", f"/processors/{entity['id']}")
        if latest["component"]["state"] == "STOPPED":
            return latest
        time.sleep(0.5)
    raise RuntimeError(f"Processor {latest['component']['name']} did not stop")


def set_outbound_relationships(api, entity, outbound):
    latest = api.request("GET", f"/processors/{entity['id']}")
    relationships = {item["name"] for item in latest["component"]["relationships"]}
    unknown = set(outbound) - relationships
    if unknown:
        raise RuntimeError(
            f"Unknown relationships for {latest['component']['name']}: "
            f"{sorted(unknown)}"
        )
    return api.request(
        "PUT",
        f"/processors/{entity['id']}",
        {
            "revision": {"version": latest["revision"]["version"]},
            "component": {
                "id": entity["id"],
                "config": {
                    "autoTerminatedRelationships": sorted(relationships - set(outbound))
                },
            },
        },
    )


def add_failure_queue(api, group_id, processor_types, raw, iceberg):
    raw = stop_processor(api, raw)
    iceberg = stop_processor(api, iceberg)
    raw = set_outbound_relationships(api, raw, ["failure"])
    iceberg = set_outbound_relationships(api, iceberg, ["failure"])
    failure = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.standard.LogAttribute",
        FAILURE_PROCESSOR_NAME,
        (2400, 0),
        {},
        [],
    )
    connect(api, group_id, raw, failure, ["failure"])
    connect(api, group_id, iceberg, failure, ["failure"])
    start_processor(api, raw)
    start_processor(api, iceberg)
    return failure


def upgrade_existing_flow(api, group):
    group_id = group["id"]
    processors = api.request("GET", f"/process-groups/{group_id}/processors")[
        "processors"
    ]
    by_name = {item["component"]["name"]: item for item in processors}
    if FAILURE_PROCESSOR_NAME in by_name:
        return False

    required = {"Write immutable raw envelope", "Append Bronze Iceberg event"}
    missing = required - by_name.keys()
    if missing:
        raise RuntimeError(
            f"Cannot upgrade incomplete NiFi flow; missing {sorted(missing)}"
        )
    processor_types = api.request("GET", "/flow/processor-types")["processorTypes"]
    add_failure_queue(
        api,
        group_id,
        processor_types,
        by_name["Write immutable raw envelope"],
        by_name["Append Bronze Iceberg event"],
    )
    return True


def main():
    api = Nifi()
    root_flow = api.request("GET", "/flow/process-groups/root")["processGroupFlow"]
    root_id = root_flow["id"]
    for group in root_flow["flow"]["processGroups"]:
        if group["component"]["name"] == "Kest descriptor inspection":
            delete_group(api, group)
            continue
        if group["component"]["name"] == FLOW_NAME:
            upgraded = upgrade_existing_flow(api, group)
            action = "Upgraded" if upgraded else "NiFi flow already exists:"
            print(f"{action} {FLOW_NAME}")
            return

    parameter_context_id = create_parameter_context(api)
    group = api.request(
        "POST",
        f"/process-groups/{root_id}/process-groups",
        {
            "revision": {"version": 0},
            "component": {
                "name": FLOW_NAME,
                "comments": "Git-managed flow: docker/nifi/bootstrap.py",
                "position": {"x": 0, "y": 0},
                "parameterContext": {"id": parameter_context_id},
            },
        },
    )
    group_id = group["id"]
    processor_types = api.request("GET", "/flow/processor-types")["processorTypes"]
    service_types = api.request("GET", "/flow/controller-service-types")[
        "controllerServiceTypes"
    ]

    aws = create_service(
        api,
        group_id,
        service_types,
        "org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService",
        "MinIO credentials",
        {
            "Access Key ID": "#{s3.access-key}",
            "Secret Access Key": "#{s3.secret-key}",
        },
    )
    file_io = create_service(
        api,
        group_id,
        service_types,
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
    catalog = create_service(
        api,
        group_id,
        service_types,
        "org.apache.nifi.services.iceberg.catalog.RESTIcebergCatalog",
        "Lakekeeper REST catalog",
        {
            "Catalog URI": "#{catalog.uri}",
            "Warehouse Location": "#{catalog.warehouse}",
            "File IO Provider": file_io["id"],
            "Access Delegation Strategy": "disabled",
            "Authentication Strategy": "BEARER",
            "Bearer Token": "local-auth-disabled",
        },
    )
    reader = create_service(
        api,
        group_id,
        service_types,
        "org.apache.nifi.json.JsonTreeReader",
        "Bronze JSON reader",
        {"Schema Access Strategy": "infer-schema"},
    )
    writer = create_service(
        api,
        group_id,
        service_types,
        "org.apache.nifi.services.iceberg.parquet.ParquetIcebergWriter",
        "Bronze Parquet writer",
        {},
    )
    for service in (aws, file_io, catalog, reader, writer):
        enable_service(api, service)

    listen = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.standard.ListenHTTP",
        "Receive Debezium HTTP events",
        (0, 0),
        {
            "Base Path": "#{listener.path}",
            "Listening Port": "#{listener.port}",
            "Return Code": "202",
            "Maximum Thread Pool Size": "8",
        },
        ["success"],
    )
    content_hash = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.standard.CryptographicHashContent",
        "Hash raw envelope",
        (400, 0),
        {"Hash Algorithm": "SHA-256", "Fail When Content Empty": "true"},
        ["success"],
    )
    extract = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "Extract CDC metadata",
        (800, 0),
        {
            "Destination": "flowfile-attribute",
            "Return Type": "json",
            "Path Not Found Behavior": "ignore",
            "Null Value Representation": "the string 'null'",
            "source_schema": "$.source.schema",
            "source_table": "$.source.table",
            "operation": "$.op",
            "source_lsn": "$.source.lsn",
            "source_ts_ms": "$.source.ts_ms",
            "before_json": "$.before",
            "after_json": "$.after",
            "transaction_json": "$.transaction",
        },
        ["matched"],
    )
    attributes = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "Set storage attributes",
        (1200, 0),
        {
            "event_id": "${content_SHA-256}",
            "source_name": "#{source.name}",
            "captured_at": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}",
            "s3_object_key": "landing/nifi/#{source.name}/data/${source_table}/${content_SHA-256}.json",
        },
        ["success"],
    )
    raw = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.aws.s3.PutS3Object",
        "Write immutable raw envelope",
        (1600, -200),
        {
            "Bucket": "#{s3.bucket}",
            "Object Key": "${s3_object_key}",
            "Region": "us-east-1",
            "AWS Credentials Provider Service": aws["id"],
            "Endpoint Override URL": "#{s3.endpoint}",
            "Use Path Style Access": "true",
            "Content Type": "application/json",
        },
        ["failure"],
    )
    to_json = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.standard.AttributesToJSON",
        "Build stable Bronze event",
        (1600, 200),
        {
            "Attributes List": "event_id,source_name,source_schema,source_table,operation,source_lsn,source_ts_ms,captured_at,before_json,after_json,transaction_json",
            "Destination": "flowfile-content",
            "Include Core Attributes": "false",
            "Null Value": "true",
            "JSON Handling Strategy": "ESCAPED",
        },
        ["success"],
    )
    iceberg = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.iceberg.PutIcebergRecord",
        "Append Bronze Iceberg event",
        (2000, 200),
        {
            "Iceberg Catalog": catalog["id"],
            "Iceberg Writer": writer["id"],
            "Record Reader": reader["id"],
            "Namespace": "#{bronze.namespace}",
            "Table Name": "#{bronze.table}",
        },
        ["failure"],
    )
    failure = create_processor(
        api,
        group_id,
        processor_types,
        "org.apache.nifi.processors.standard.LogAttribute",
        FAILURE_PROCESSOR_NAME,
        (2400, 0),
        {},
        [],
    )

    connect(api, group_id, listen, content_hash, ["success"])
    connect(api, group_id, content_hash, extract, ["success"])
    connect(api, group_id, extract, attributes, ["matched"])
    connect(api, group_id, attributes, raw, ["success"])
    connect(api, group_id, attributes, to_json, ["success"])
    connect(api, group_id, to_json, iceberg, ["success"])
    connect(api, group_id, raw, failure, ["failure"])
    connect(api, group_id, iceberg, failure, ["failure"])

    for processor in (raw, iceberg, to_json, attributes, extract, content_hash, listen):
        start_processor(api, processor)
    print(f"Created and started NiFi flow: {FLOW_NAME}")


if __name__ == "__main__":
    main()
