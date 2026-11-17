-- Hook resume deduplication (`capabilities.hookResumeDedup`), as upstream
-- world-postgres's 0021 migration adds it.
--
-- `resumeHook()` sends a `resumeId` and a SHA-256 of the payload bytes with its
-- durable `hook_received` write. `events.create` answers a retry of that write
-- with the event it already committed, and this index is what guarantees one
-- event per `(run, resumeId)`. `tenant_id` leads because the table is
-- partitioned by it.
--
-- Both columns are nullable without a default, so adding them rewrites no
-- partition. Every existing row has a NULL `resume_id`, so the partial index
-- starts empty; building it still scans each partition under a SHARE lock,
-- which blocks event writes until this transaction commits.

alter table workflow.workflow_events add column resume_id varchar;
alter table workflow.workflow_events add column resume_payload_digest varchar;

create unique index workflow_events_hook_resume_unique
  on workflow.workflow_events (tenant_id, run_id, resume_id)
  where type = 'hook_received' and resume_id is not null;

-- Postgres has just created this index on every existing tenant partition
-- under a generated, length-dependent name, and a unique violation reports the
-- child's name. Rename each child to the name `resumeDedupIndexName(tenantId)`
-- (src/tenant.ts) derives, which `ensureTenantPartitions` gives the partitions
-- it creates from now on:
--   'wf_events_resume_t_' || lower(id) with [^a-z0-9_] as '_', first 24
--   characters || '_' || first 8 hex digits of sha256(id).
do $migration$
declare
  child record;
  tenant text;
  target text;
begin
  for child in
    select ci.relname as index_name,
           pg_get_expr(ct.relpartbound, ct.oid) as bound
      from pg_inherits inh
      join pg_class ci on ci.oid = inh.inhrelid
      join pg_index i on i.indexrelid = ci.oid
      join pg_class ct on ct.oid = i.indrelid
     where inh.inhparent = 'workflow.workflow_events_hook_resume_unique'::regclass
  loop
    tenant := substring(child.bound from $re$^FOR VALUES IN \('([A-Za-z0-9_-]+)'\)$$re$);
    if tenant is null then
      raise exception 'workflow_events partition with unexpected bound: %', child.bound;
    end if;
    target := 'wf_events_resume_t_'
      || left(regexp_replace(lower(tenant), '[^a-z0-9_]', '_', 'g'), 24)
      || '_'
      || left(encode(sha256(convert_to(tenant, 'UTF8')), 'hex'), 8);
    if child.index_name <> target then
      execute format('alter index workflow.%I rename to %I', child.index_name, target);
    end if;
  end loop;
end
$migration$;
