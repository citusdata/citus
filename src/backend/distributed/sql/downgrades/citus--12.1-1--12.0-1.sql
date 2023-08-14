-- citus--12.1-1--12.0-1

-- we have modified the relevant upgrade script to include any_value changes
-- we don't need to upgrade this downgrade path for any_value changes
-- since if we are doing a Citus downgrade, not PG downgrade, then it would be no-op.

DROP FUNCTION pg_catalog.citus_internal_update_none_dist_table_metadata(
    relation_id oid, replication_model "char", colocation_id bigint,
    auto_converted boolean
);

DROP FUNCTION pg_catalog.worker_copy_table_to_node(
    source_table regclass,
    target_node_id integer,
    exclusive_connection boolean
);

#include "../udfs/worker_copy_table_to_node/11.1-1.sql"
