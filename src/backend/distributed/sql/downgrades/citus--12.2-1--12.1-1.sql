-- citus--12.2-1--12.1-1

DROP FUNCTION citus_internal.database_command(text);
DROP FUNCTION citus_internal.acquire_citus_advisory_object_class_lock(int, cstring);

#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"

DROP FUNCTION citus_internal.start_management_transaction(
    outer_xid xid8
);

DROP FUNCTION citus_internal.execute_command_on_remote_nodes_as_user(
    query text,
    username text
);

DROP FUNCTION citus_internal.mark_object_distributed(
    classId Oid, objectName text, objectId Oid, connectionUser text
);

DROP FUNCTION citus_internal.commit_management_command_2pc();

ALTER TABLE pg_catalog.pg_dist_transaction DROP COLUMN outer_xid;
REVOKE USAGE ON SCHEMA citus_internal FROM PUBLIC;

DROP FUNCTION citus_internal.add_colocation_metadata(int, int, int, regtype, oid);
DROP FUNCTION citus_internal.add_object_metadata(text, text[], text[], integer, integer, boolean);
DROP FUNCTION citus_internal.add_partition_metadata(regclass, "char", text, integer, "char");
DROP FUNCTION citus_internal.add_placement_metadata(bigint, bigint, integer, bigint);
DROP FUNCTION citus_internal.add_shard_metadata(regclass, bigint, "char", text, text);
DROP FUNCTION citus_internal.add_tenant_schema(oid, integer);
DROP FUNCTION citus_internal.adjust_local_clock_to_remote(pg_catalog.cluster_clock);
