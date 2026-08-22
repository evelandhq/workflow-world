-- The one-time external-only cutover is complete and its tooling is gone:
-- nothing writes cutover proofs or run quarantine markers any more, and the
-- dispatcher no longer reads them. Dead letters (0002) remain the only
-- dispatch-terminal marker.
drop table if exists workflow.cutover_proofs;
drop table if exists workflow.run_quarantines;
