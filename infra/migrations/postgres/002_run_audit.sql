ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS run_type text NOT NULL DEFAULT 'PIPELINE';
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS environment text NOT NULL DEFAULT 'local';
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS git_sha text;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS image_ref text;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS orchestrator text;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS orchestrator_run_id text;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS requested_by text NOT NULL DEFAULT 'system';
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS reason text;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS source text;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS window_start date;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS window_end date;
ALTER TABLE pipeline_run ADD COLUMN IF NOT EXISTS metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE UNIQUE INDEX IF NOT EXISTS pipeline_run_orchestrator_idx
  ON pipeline_run(orchestrator, orchestrator_run_id)
  WHERE orchestrator IS NOT NULL AND orchestrator_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS pipeline_run_step (
  run_id uuid NOT NULL REFERENCES pipeline_run(run_id) ON DELETE CASCADE,
  step text NOT NULL,
  status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
  attempt integer NOT NULL DEFAULT 1,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (run_id, step, attempt)
);

CREATE TABLE IF NOT EXISTS pipeline_run_batch (
  run_id uuid NOT NULL REFERENCES pipeline_run(run_id) ON DELETE CASCADE,
  batch_id uuid NOT NULL REFERENCES batch_manifest(batch_id) ON DELETE RESTRICT,
  PRIMARY KEY (run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS backfill_request (
  request_id uuid PRIMARY KEY,
  run_id uuid UNIQUE REFERENCES pipeline_run(run_id) ON DELETE SET NULL,
  source text NOT NULL,
  window_start date NOT NULL,
  window_end date NOT NULL,
  reason text NOT NULL,
  requested_by text NOT NULL,
  status text NOT NULL CHECK (status IN ('PLANNED', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
  dry_run boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error text,
  CHECK (window_start <= window_end)
);
