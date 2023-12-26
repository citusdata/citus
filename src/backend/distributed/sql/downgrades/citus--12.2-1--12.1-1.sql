-- citus--12.2-1--12.1-1

DROP FUNCTION pg_catalog.citus_internal_database_command(text);

#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"

DROP FUNCTION citus_internal.start_management_transaction(
    outer_xid xid8
);

DROP FUNCTION citus_internal.execute_command_on_remote_nodes_as_user(
    query text,
    username text
);

DROP FUNCTION citus_internal.mark_object_distributed(
    classId Oid, objectName text, objectId Oid
);

DROP FUNCTION citus_internal.commit_management_command_2pc();

ALTER TABLE pg_catalog.pg_dist_transaction DROP COLUMN outer_xid;
