-- Hook token retention.
--
-- A `hook_created` event may carry `tokenRetentionUntil`, asking that the hook's
-- token stay reserved past the end of its run. Without this column the field was
-- accepted and silently discarded, and run termination deleted the hook
-- regardless — so the token became immediately reusable despite the caller
-- having asked otherwise.
--
-- Nullable, and NULL means "no retention": that is the shape every existing row
-- has, and it reads as "delete with the run" in the predicates that consume it.
ALTER TABLE "workflow"."workflow_hooks"
  ADD COLUMN IF NOT EXISTS "token_retention_until" timestamp with time zone;
