CREATE TABLE IF NOT EXISTS dataset_release (
  release_id uuid PRIMARY KEY,
  run_id uuid NOT NULL UNIQUE REFERENCES pipeline_run(run_id) ON DELETE RESTRICT,
  status text NOT NULL CHECK (status IN ('PREPARING', 'PARTIAL', 'COMMITTED', 'FAILED', 'ROLLED_BACK')),
  requested_by text NOT NULL,
  environment text NOT NULL,
  git_sha text,
  image_ref text,
  created_at timestamptz NOT NULL DEFAULT now(),
  committed_at timestamptz,
  error text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS release_id uuid REFERENCES dataset_release(release_id);
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'COMMITTED';
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS prepared_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS committed_at timestamptz;
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS production_checksum text;
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS snapshot_table text;
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS error text;
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS requested_by text NOT NULL DEFAULT 'system';
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS rolled_back_at timestamptz;
ALTER TABLE publication_manifest ADD COLUMN IF NOT EXISTS rollback_of uuid REFERENCES publication_manifest(publication_id);

INSERT INTO dataset_release(
  release_id, run_id, status, requested_by, environment, git_sha, image_ref, committed_at
)
SELECT r.run_id, r.run_id, 'COMMITTED', r.requested_by, r.environment, r.git_sha, r.image_ref,
       coalesce(r.finished_at, r.started_at)
FROM pipeline_run r
WHERE EXISTS (SELECT 1 FROM publication_manifest p WHERE p.run_id=r.run_id)
ON CONFLICT (release_id) DO NOTHING;

UPDATE publication_manifest
SET release_id=run_id,
    status='COMMITTED',
    committed_at=coalesce(committed_at, published_at),
    production_checksum=coalesce(production_checksum, candidate_checksum)
WHERE release_id IS NULL;

CREATE TABLE IF NOT EXISTS publication_event (
  event_id uuid PRIMARY KEY,
  publication_id uuid NOT NULL REFERENCES publication_manifest(publication_id) ON DELETE CASCADE,
  event_type text NOT NULL CHECK (event_type IN ('PREPARED', 'COMMITTED', 'FAILED', 'ROLLED_BACK', 'RECONCILED')),
  actor text NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS publication_manifest_dataset_status_idx
  ON publication_manifest(dataset, status, committed_at DESC);
