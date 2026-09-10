"""Configuration and declarative topology for the Kest NiFi flow."""

from __future__ import annotations

import os
from dataclasses import dataclass

FLOW_NAME = "Kest PostgreSQL CDC v1"
PARAMETER_CONTEXT_NAME = "Kest PostgreSQL CDC local"
INSPECTION_GROUP_NAME = "Kest descriptor inspection"

RECEIVE = "Receive Debezium HTTP events"
HASH = "Hash raw envelope"
EXTRACT = "Extract CDC metadata"
ATTRIBUTES = "Set storage attributes"
RAW = "Write immutable raw envelope"
TO_JSON = "Build stable Bronze event"
ICEBERG = "Append Bronze Iceberg event"
FAILURE = "FAILED - inspect and replay"

RUNNING_PROCESSORS = frozenset(
    {RECEIVE, HASH, EXTRACT, ATTRIBUTES, RAW, TO_JSON, ICEBERG}
)


@dataclass(frozen=True)
class FlowConfig:
    flow_name: str
    parameter_context_name: str
    source_name: str
    listener_port: str
    listener_path: str
    bucket: str
    access_key: str
    secret_key: str
    warehouse: str
    bronze_namespace: str
    bronze_table: str

    @classmethod
    def from_env(cls) -> FlowConfig:
        return cls(
            flow_name=os.environ.get("NIFI_FLOW_NAME", FLOW_NAME),
            parameter_context_name=os.environ.get(
                "NIFI_PARAMETER_CONTEXT_NAME", PARAMETER_CONTEXT_NAME
            ),
            source_name=os.environ.get("INGESTION_SOURCE_NAME", "postgres-source"),
            listener_port=os.environ.get("NIFI_LISTENER_PORT", "9090"),
            listener_path=os.environ.get("INGESTION_HTTP_PATH", "cdc/postgres-source"),
            bucket=os.environ["MINIO_BUCKET"],
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            warehouse=os.environ["LAKEKEEPER_WAREHOUSE"],
            bronze_namespace=os.environ.get(
                "NIFI_BRONZE_NAMESPACE", "bronze_ingestion"
            ),
            bronze_table=os.environ.get("NIFI_BRONZE_TABLE", "postgres_cdc_events"),
        )

    def parameters(self) -> dict[str, tuple[str, bool]]:
        return {
            "source.name": (self.source_name, False),
            "listener.port": (self.listener_port, False),
            "listener.path": (self.listener_path, False),
            "s3.endpoint": ("http://minio:9000", False),
            "s3.bucket": (self.bucket, False),
            "s3.access-key": (self.access_key, True),
            "s3.secret-key": (self.secret_key, True),
            "catalog.uri": ("http://lakekeeper:8181/catalog", False),
            "catalog.warehouse": (self.warehouse, False),
            "bronze.namespace": (self.bronze_namespace, False),
            "bronze.table": (self.bronze_table, False),
        }


@dataclass(frozen=True)
class ProcessorSpec:
    type_name: str
    name: str
    position: tuple[int, int]
    properties: dict[str, str]
    outbound: tuple[str, ...] = ()
    running: bool = True


@dataclass(frozen=True)
class ConnectionSpec:
    source: str
    destination: str
    relationships: tuple[str, ...]


def processor_specs(service_ids: dict[str, str]) -> tuple[ProcessorSpec, ...]:
    return (
        ProcessorSpec(
            "org.apache.nifi.processors.standard.ListenHTTP",
            RECEIVE,
            (0, 0),
            {
                "Base Path": "#{listener.path}",
                "Listening Port": "#{listener.port}",
                "Return Code": "202",
                "Maximum Thread Pool Size": "8",
            },
            ("success",),
        ),
        ProcessorSpec(
            "org.apache.nifi.processors.standard.CryptographicHashContent",
            HASH,
            (400, 0),
            {"Hash Algorithm": "SHA-256", "Fail When Content Empty": "true"},
            ("success",),
        ),
        ProcessorSpec(
            "org.apache.nifi.processors.standard.EvaluateJsonPath",
            EXTRACT,
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
            ("matched",),
        ),
        ProcessorSpec(
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            ATTRIBUTES,
            (1200, 0),
            {
                "event_id": "${content_SHA-256}",
                "source_name": "#{source.name}",
                "captured_at": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}",
                "s3_object_key": "landing/nifi/#{source.name}/data/${source_table}/${content_SHA-256}.json",
            },
            ("success",),
        ),
        ProcessorSpec(
            "org.apache.nifi.processors.aws.s3.PutS3Object",
            RAW,
            (1600, -200),
            {
                "Bucket": "#{s3.bucket}",
                "Object Key": "${s3_object_key}",
                "Region": "us-east-1",
                "AWS Credentials Provider Service": service_ids["aws"],
                "Endpoint Override URL": "#{s3.endpoint}",
                "Use Path Style Access": "true",
                "Content Type": "application/json",
            },
            ("failure",),
        ),
        ProcessorSpec(
            "org.apache.nifi.processors.standard.AttributesToJSON",
            TO_JSON,
            (1600, 200),
            {
                "Attributes List": (
                    "event_id,source_name,source_schema,source_table,operation,"
                    "source_lsn,source_ts_ms,captured_at,before_json,after_json,"
                    "transaction_json"
                ),
                "Destination": "flowfile-content",
                "Include Core Attributes": "false",
                "Null Value": "true",
                "JSON Handling Strategy": "ESCAPED",
            },
            ("success",),
        ),
        ProcessorSpec(
            "org.apache.nifi.processors.iceberg.PutIcebergRecord",
            ICEBERG,
            (2000, 200),
            {
                "Iceberg Catalog": service_ids["catalog"],
                "Iceberg Writer": service_ids["writer"],
                "Record Reader": service_ids["reader"],
                "Namespace": "#{bronze.namespace}",
                "Table Name": "#{bronze.table}",
            },
            ("failure",),
        ),
        ProcessorSpec(
            "org.apache.nifi.processors.standard.LogAttribute",
            FAILURE,
            (2400, 0),
            {},
            running=False,
        ),
    )


CONNECTIONS = (
    ConnectionSpec(RECEIVE, HASH, ("success",)),
    ConnectionSpec(HASH, EXTRACT, ("success",)),
    ConnectionSpec(EXTRACT, ATTRIBUTES, ("matched",)),
    ConnectionSpec(ATTRIBUTES, RAW, ("success",)),
    ConnectionSpec(ATTRIBUTES, TO_JSON, ("success",)),
    ConnectionSpec(TO_JSON, ICEBERG, ("success",)),
    ConnectionSpec(RAW, FAILURE, ("failure",)),
    ConnectionSpec(ICEBERG, FAILURE, ("failure",)),
)
