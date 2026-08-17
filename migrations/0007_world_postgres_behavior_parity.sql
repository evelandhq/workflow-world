ALTER TABLE workflow.workflow_runs
  ADD COLUMN IF NOT EXISTS "encryption_public_key" varchar;
