#!/usr/bin/env python3
"""Create catalog-only dbt artifacts that describe the published Gold tables.

Production dbt builds deliberately write marts to ``gold_candidate``.  The
publisher then atomically exchanges each validated table into ``gold``.  This
copy keeps the original run evidence intact while teaching the catalog about
the consumer-facing relation.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

SOURCE = Path(os.environ.get("DBT_TARGET_PATH", "/opt/kest/.runtime/dbt/target"))
DESTINATION = Path(
    os.environ.get("OPENMETADATA_DBT_ARTIFACT_DIR", "/opt/kest/.runtime/dbt/metadata")
)


def remap_relation(node: dict[str, object]) -> None:
    if node.get("resource_type") != "model" or node.get("schema") != "gold_candidate":
        return
    node["database"] = "default"
    node["schema"] = "gold"
    alias = str(node.get("alias") or node.get("name"))
    node["relation_name"] = f"`default`.`gold`.`{alias}`"


def main() -> None:
    DESTINATION.mkdir(parents=True, exist_ok=True)
    for name in ("manifest.json", "catalog.json", "run_results.json"):
        source = SOURCE / name
        if name == "run_results.json" and not source.exists():
            source = Path("/opt/kest/.runtime/dbt/target/run_results.json")
        if not source.exists():
            raise RuntimeError(f"Missing dbt artifact: {source}")
        payload = json.loads(source.read_text())
        payload = copy.deepcopy(payload)
        if name == "manifest.json":
            for node in payload.get("nodes", {}).values():
                remap_relation(node)
        elif name == "catalog.json":
            for node in payload.get("nodes", {}).values():
                remap_relation(node.get("metadata", {}))
        (DESTINATION / name).write_text(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
