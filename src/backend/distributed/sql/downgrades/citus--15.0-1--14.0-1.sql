-- citus--15.0-1--14.0-1
-- downgrade version to 14.0-1

DROP FUNCTION IF EXISTS citus_internal.get_next_colocation_id();
DROP FUNCTION IF EXISTS citus_internal.adjust_identity_column_seq_settings(regclass, bigint);
DROP FUNCTION IF EXISTS pg_catalog.worker_apply_sequence_command(text, regtype, bigint);
