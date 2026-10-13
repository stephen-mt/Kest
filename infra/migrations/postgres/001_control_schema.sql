CREATE TABLE IF NOT EXISTS batch_manifest (
    batch_id uuid PRIMARY KEY,
    source text NOT NULL,
    object_prefix text NOT NULL,
    checksum text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    watermark jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL CHECK (status IN ('STORED', 'LOADING', 'LOADED', 'FAILED', 'QUARANTINED')),
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS batch_manifest_source_status_idx
    ON batch_manifest(source, status, created_at);

CREATE TABLE IF NOT EXISTS batch_objects (
    batch_id uuid NOT NULL REFERENCES batch_manifest(batch_id) ON DELETE RESTRICT,
    table_name text NOT NULL,
    object_path text NOT NULL,
    checksum text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    PRIMARY KEY (batch_id, object_path)
);

CREATE TABLE IF NOT EXISTS source_checkpoint (
    source text PRIMARY KEY,
    watermark jsonb NOT NULL,
    batch_id uuid REFERENCES batch_manifest(batch_id),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    error text
);

CREATE TABLE IF NOT EXISTS publication_manifest (
    publication_id uuid PRIMARY KEY,
    run_id uuid REFERENCES pipeline_run(run_id),
    dataset text NOT NULL,
    candidate_checksum text NOT NULL,
    previous_checksum text,
    published_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, dataset)
);

CREATE TABLE IF NOT EXISTS backup_manifest (
    backup_id uuid PRIMARY KEY,
    path text NOT NULL,
    checksum text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    verified_at timestamptz
);
