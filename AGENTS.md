# Kest engineering conventions

- Treat correctness and database-enforced permissions as release blockers.
- Keep Airflow DAG files declarative; business logic belongs in `ingestion/`, `transformations/`, or `ops/`.
- Never pass records through XCom. Pass only batch IDs, object paths, and run identifiers.
- Do not write production Gold tables outside `publish_service` and the controlled publish command.
- Do not add unpinned container images or Python dependencies.
- Never mark a scenario PASS in `docs/implementation-status.md` without a command, timestamp, git SHA, and retained output.
- Preserve the optional nature of the `catalog` Compose profile.
- New sources must use dlt resources and the source contract conventions; do not create a parallel ingestion framework.
