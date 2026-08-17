-- Packed physical rows keep logical chunk ids inside `data`; the first id stays
-- in the existing `id` column so old cursors remain useful after a rewrite.
alter table workflow.workflow_stream_chunks
  add column if not exists codec_version integer,
  add column if not exists chunk_count integer,
  add column if not exists last_chunk_id varchar;

create index if not exists workflow_stream_chunks_block_tail_index
  on workflow.workflow_stream_chunks (tenant_id, stream_id, last_chunk_id)
  where eof = false and last_chunk_id is not null;

-- Once a stream is packed its EOF leaves this partial index, keeping the
-- dispatcher's global candidate scan proportional to the pending backlog.
create index if not exists workflow_stream_chunks_pending_pack_index
  on workflow.workflow_stream_chunks (tenant_id, run_id, stream_id, id)
  where eof = true and codec_version is distinct from 2;

-- Rehydration state is deliberately server-side. Public cursors remain a small
-- logical position instead of carrying accumulated conversation content.
create table if not exists workflow.workflow_stream_checkpoints (
  tenant_id varchar not null,
  stream_id varchar not null,
  run_id varchar,
  chunk_id varchar not null,
  next_index integer not null check (next_index >= 0),
  state jsonb not null,
  created_at timestamptz default now() not null,
  primary key (tenant_id, stream_id, chunk_id)
);

create index if not exists workflow_stream_checkpoints_tenant_run_index
  on workflow.workflow_stream_checkpoints (tenant_id, run_id);
