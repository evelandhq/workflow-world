-- Compatibility shell for Releases baked with @evelandhq/workflow-world <= 0.12.0.
--
-- 0015 dropped workflow.run_quarantines together with the cutover tooling, but
-- a World is a build-time property of each Release: every deployment built
-- against 0.11.x/0.12.x still runs `isRunQuarantined` on its enqueue path
-- (`select 1 from workflow.run_quarantines ...`) and fails every delivery with
-- "relation does not exist" once the newer dispatcher has migrated the shared
-- database. The table therefore stays — empty, never written by this version —
-- until no Release baked with <= 0.12.0 remains. Nothing in this version reads
-- it; the dispatcher's own terminal marker is workflow.dispatch_dead_letters.
create table if not exists workflow.run_quarantines (
  tenant_id text not null,
  run_id text not null,
  operation_id text not null,
  reason text not null,
  created_at timestamptz not null default now(),
  resolved_at timestamptz,
  resolved_by text,
  constraint run_quarantines_pkey primary key (tenant_id, run_id)
);

create index if not exists run_quarantines_unresolved_index
  on workflow.run_quarantines (tenant_id, run_id)
  where resolved_at is null;
