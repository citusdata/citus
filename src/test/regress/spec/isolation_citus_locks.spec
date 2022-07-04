#include "isolation_mx_common.include.spec"

setup
{
    SELECT citus_add_node('localhost', 57636, groupid:=0);
    SET citus.next_shard_id TO 12345000;
    CREATE TABLE dist_table (a INT, b INT);
    SELECT create_distributed_table('dist_table', 'a', shard_count:=4);
}

teardown
{
    DROP TABLE dist_table, selected_gpid;
    SELECT citus_remove_node('localhost', 57636);
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-alter-dist-table"
{
	ALTER TABLE dist_table ADD COLUMN data text;
}

step "s1-record-gpid"
{
	SELECT citus_backend_gpid() INTO selected_gpid;
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-show-locks"
{
	SELECT relation_name, citus_nodename_for_nodeid(nodeid), citus_nodeport_for_nodeid(nodeid), mode
	FROM citus_locks
	WHERE global_pid IN (SELECT * FROM selected_gpid) AND relation_name LIKE 'dist_table%'
	ORDER BY 1, 2, 3, 4;
}

permutation "s1-record-gpid" "s1-begin" "s2-show-locks" "s1-alter-dist-table" "s2-show-locks" "s1-commit" "s2-show-locks"
