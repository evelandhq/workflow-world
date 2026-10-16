-- The delivery that started a step, as the runtime stamps it on step_started
-- (`ownerMessageId`), kept on the step so a restart can be refused while that
-- delivery is still in flight (storage.ts, step_started). Null for a step a
-- queued step message started, and for every row an older World wrote.
alter table workflow.workflow_steps add column if not exists owner_message_id varchar;
