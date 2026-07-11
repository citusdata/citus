-- citus--15.0-1--14.0-1
-- downgrade version to 14.0-1

DROP FUNCTION IF EXISTS citus_internal.get_next_colocation_id();

-- re-create the legacy version that we kept for backward compatibility at Citus 13 and 14
#include "../udfs/worker_adjust_identity_column_seq_ranges/11.3-1.sql"
DROP FUNCTION IF EXISTS citus_internal.adjust_identity_column_seq_settings(regclass, bigint, boolean);

-- re-create the legacy version that we kept for backward compatibility at Citus 13 and 14
-- by using the same definition as in citus--8.3-1--9.0-1.sql
CREATE OR REPLACE FUNCTION worker_apply_sequence_command(create_sequence_command text,
                                                         sequence_type_id regtype DEFAULT 'bigint'::regtype)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_apply_sequence_command$$;
COMMENT ON FUNCTION worker_apply_sequence_command(text,regtype)
    IS 'create a sequence which produces globally unique values';
DROP FUNCTION IF EXISTS pg_catalog.worker_apply_sequence_command(text, regtype, bigint, boolean);

DROP FUNCTION IF EXISTS citus_internal.lock_colocation_id(int, int);

DROP FUNCTION IF EXISTS citus_internal.acquire_placement_colocation_lock(bigint, int);

-- cluster changes block UDFs
DROP FUNCTION IF EXISTS pg_catalog.citus_cluster_changes_block(int);
DROP FUNCTION IF EXISTS pg_catalog.citus_cluster_changes_unblock();
DROP FUNCTION IF EXISTS pg_catalog.citus_cluster_changes_block_status();
