#include "isolation_mx_common.include.spec"

setup
{
	SELECT citus_set_coordinator_host('localhost', 57636);

	SELECT 1 FROM citus_add_node('localhost', 57637);
	SELECT 1 FROM citus_add_node('localhost', 57638);

    SELECT run_command_on_workers($$SET citus.enable_metadata_sync TO off;CREATE OR REPLACE FUNCTION override_backend_data_gpid(bigint)
        RETURNS void
        LANGUAGE C STRICT IMMUTABLE
        AS 'citus'$$);


	-- Consistently place distributed-schema tables in the cluster,
	-- see EmptySingleShardTableColocationDecideNodeId().
	ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 10001;

	SET citus.shard_replication_factor TO 1;

	SET citus.enable_schema_based_sharding TO ON;
	CREATE SCHEMA sc1;
	RESET citus.enable_schema_based_sharding;
}

teardown
{
	DROP SCHEMA IF EXISTS sc1, sc2 CASCADE;
	DROP TABLE IF EXISTS ref;
	SELECT COUNT(*)>=0 FROM (
		SELECT citus_remove_node(nodename, nodeport)
		FROM pg_dist_node
		WHERE nodename = 'localhost' AND nodeport IN (57637, 57638)
	) q;
}

session "coord"

step "coord-begin" { BEGIN; }
step "coord-commit" { COMMIT; }
step "coord-shard-replication-factor" { SET citus.shard_replication_factor TO 1; }
step "coord-create-table-ref" { CREATE TABLE ref (id int PRIMARY KEY); }
step "coord-create-reference-table-ref" { SELECT create_reference_table('ref'); }
step "coord-create-table-sc1-t1" { CREATE TABLE sc1.t1 (a int); }
step "coord-create-table-sc1-t2" { CREATE TABLE sc1.t2 (a int); }
step "coord-add-worker-57638" { SELECT 1 FROM citus_add_node('localhost', 57638); }
step "coord-remove-worker-57638" { SELECT 1 FROM citus_remove_node('localhost', 57638); }
step "coord-query-ref-placements" { SELECT nodeport, success, result FROM run_command_on_placements('ref', 'SELECT count(*) FROM %s') ORDER BY nodeport; }
step "coord-query-sc1-t1-placement" { SELECT nodeport, success, result FROM run_command_on_placements('sc1.t1', 'SELECT count(*) FROM %s') ORDER BY nodeport; }
step "coord-query-sc1-t2-placement" { SELECT nodeport, success, result FROM run_command_on_placements('sc1.t2', 'SELECT count(*) FROM %s') ORDER BY nodeport; }
step "coord-move-shard-sc1-t1"
{
	SELECT citus_move_shard_placement(
		s.shardid,
		src.nodename, src.nodeport,
		dst.nodename, dst.nodeport,
		'block_writes'
	)
	FROM pg_dist_shard s
	JOIN pg_dist_shard_placement src USING (shardid)
	CROSS JOIN (
		SELECT nodename, nodeport
		FROM pg_dist_node
		WHERE noderole = 'primary' AND isactive AND shouldhaveshards AND
			  (nodename, nodeport) NOT IN (
				SELECT p.nodename, p.nodeport
				FROM pg_dist_shard_placement p
				JOIN pg_dist_shard ps USING (shardid)
				WHERE ps.logicalrelid = 'sc1.t1'::regclass
			  )
	    ORDER BY nodeport
		LIMIT 1
	) dst
	WHERE s.logicalrelid = 'sc1.t1'::regclass;
}
step "coord-show-tables-in-schema-sc1"
{
	SELECT nodeport, success, result
	FROM run_command_on_all_nodes($$
		SELECT array_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname = 'sc1' AND tablename IN ('t1', 't2', 't1_renamed')
	$$)
	JOIN pg_dist_node USING (nodeid)
	ORDER BY nodeport;
}

session "worker-57637"

step "worker-57637-start" { SELECT start_session_level_connection_to_node('localhost', 57637); }
step "worker-57637-stop" { SELECT stop_session_level_connection_to_node(); }
step "worker-57637-begin" { SELECT run_commands_on_session_level_connection_to_node('BEGIN'); }
step "worker-57637-commit" { SELECT run_commands_on_session_level_connection_to_node('COMMIT'); }
step "worker-57637-shard-replication-factor" { SELECT run_commands_on_session_level_connection_to_node('SET citus.shard_replication_factor TO 1'); }
step "worker-57637-create-table-sc1-t1" { SELECT run_commands_on_session_level_connection_to_node('CREATE TABLE sc1.t1 (a int)'); }
step "worker-57637-create-table-sc1-t2" { SELECT run_commands_on_session_level_connection_to_node('CREATE TABLE sc1.t2 (a int)'); }
step "worker-57637-drop-table-sc1-t1" { SELECT run_commands_on_session_level_connection_to_node('DROP TABLE sc1.t1'); }
step "worker-57637-alter-table-rename-sc1-t1" { SELECT run_commands_on_session_level_connection_to_node('ALTER TABLE sc1.t1 RENAME TO t1_renamed'); }

session "worker-57638"

step "worker-57638-start" { SELECT start_session_level_connection_to_node('localhost', 57638); }
step "worker-57638-stop" { SELECT stop_session_level_connection_to_node(); }
step "worker-57638-begin" { SELECT run_commands_on_session_level_connection_to_node('BEGIN'); }
step "worker-57638-commit" { SELECT run_commands_on_session_level_connection_to_node('COMMIT'); }
step "worker-57638-shard-replication-factor" { SELECT run_commands_on_session_level_connection_to_node('SET citus.shard_replication_factor TO 1'); }
step "worker-57638-create-table-sc1-t1" { SELECT run_commands_on_session_level_connection_to_node('CREATE TABLE sc1.t1 (a int)'); }
step "worker-57638-create-table-sc1-t2" { SELECT run_commands_on_session_level_connection_to_node('CREATE TABLE sc1.t2 (a int)'); }
step "worker-57638-drop-table-sc1-t1" { SELECT run_commands_on_session_level_connection_to_node('DROP TABLE sc1.t1'); }
step "worker-57638-alter-table-rename-sc1-t1" { SELECT run_commands_on_session_level_connection_to_node('ALTER TABLE sc1.t1 RENAME TO t1_renamed'); }

// Create a distributed-schema table via worker-57637 that causes ensuring that the reference tables are replicated to all nodes while adding a new worker node via the coordinator.
// worker-57637-create-table-sc1-t2 won't replicate the reference table to the new node, so the first call to coord-query-ref-placements won't show a placement on the new node, but the second call will do.
permutation "coord-create-table-ref" "coord-create-reference-table-ref" "coord-remove-worker-57638" "coord-begin" "coord-add-worker-57638" "worker-57637-start" "worker-57637-shard-replication-factor" "worker-57637-create-table-sc1-t2" "coord-commit" "coord-query-ref-placements" "coord-query-sc1-t2-placement" "worker-57637-shard-replication-factor" "worker-57637-create-table-sc1-t1" "worker-57637-stop" "coord-query-ref-placements" "coord-query-sc1-t1-placement"

// create a distributed-schema table via worker-57637 while moving a shard of another table under the same schema via the coordinator
permutation "coord-shard-replication-factor" "coord-create-table-sc1-t1" "coord-query-sc1-t1-placement" "coord-begin" "coord-move-shard-sc1-t1" "worker-57637-start" "worker-57637-shard-replication-factor" "worker-57637-create-table-sc1-t2" "coord-commit" "worker-57637-stop" "coord-query-sc1-t1-placement"

// create a distributed-schema table via worker-57637 while dropping the **only** another table under the same schema via worker-57638
permutation "coord-shard-replication-factor" "coord-create-table-sc1-t1" "worker-57638-start" "worker-57638-begin" "worker-57638-drop-table-sc1-t1" "worker-57637-start" "worker-57637-shard-replication-factor" "worker-57637-create-table-sc1-t2" "worker-57638-commit" "worker-57638-stop" "worker-57637-stop" "coord-show-tables-in-schema-sc1"

// move a shard via the coordinator while creating a distributed-schema table under the same schema via worker-57637
permutation "coord-shard-replication-factor" "coord-create-table-sc1-t1" "coord-query-sc1-t1-placement" "worker-57637-start" "worker-57637-shard-replication-factor" "worker-57637-begin" "worker-57637-create-table-sc1-t2" "coord-move-shard-sc1-t1" "worker-57637-commit" "worker-57637-stop" "coord-query-sc1-t1-placement"

// drop the **only** table of a distributed-schema via worker-57638 while creating another table under the same schema via worker-57637
permutation "coord-shard-replication-factor" "coord-create-table-sc1-t1" "worker-57637-start" "worker-57637-shard-replication-factor" "worker-57638-start" "worker-57637-begin" "worker-57637-create-table-sc1-t2" "worker-57638-drop-table-sc1-t1" "worker-57637-commit" "worker-57638-stop" "worker-57637-stop" "coord-show-tables-in-schema-sc1"

// create a distributed-schema table via worker-57638 while creating another table under the same schema via worker-57637
permutation "worker-57637-start" "worker-57637-shard-replication-factor" "worker-57637-begin" "worker-57637-create-table-sc1-t1" "worker-57638-start" "worker-57638-shard-replication-factor" "worker-57638-create-table-sc1-t2" "worker-57637-commit" "worker-57637-stop" "worker-57638-commit" "worker-57638-stop" "coord-show-tables-in-schema-sc1"

// create a distributed-schema table via worker-57637 that causes ensuring that the reference tables are replicated to all nodes while doing the same the coordinator
permutation "coord-remove-worker-57638" "coord-create-table-ref" "coord-create-reference-table-ref" "coord-add-worker-57638" "worker-57637-start" "worker-57637-begin" "worker-57637-shard-replication-factor" "worker-57637-create-table-sc1-t1" "coord-begin" "coord-shard-replication-factor" "coord-create-table-sc1-t2" "worker-57637-commit" "worker-57637-stop" "coord-commit" "coord-query-ref-placements" "coord-query-sc1-t1-placement" "coord-query-sc1-t2-placement"

// create the same distributed-schema table from different workers
permutation "worker-57637-start" "worker-57637-shard-replication-factor" "worker-57637-begin" "worker-57637-create-table-sc1-t1" "worker-57638-start" "worker-57638-shard-replication-factor" "worker-57638-create-table-sc1-t1" "worker-57637-commit" "worker-57637-stop" "worker-57638-stop" "coord-show-tables-in-schema-sc1"

// drop the same distributed-schema table from different workers
permutation "coord-shard-replication-factor" "coord-create-table-sc1-t1" "worker-57637-start" "worker-57637-begin" "worker-57637-drop-table-sc1-t1" "worker-57638-start" "worker-57638-drop-table-sc1-t1" "worker-57637-commit" "worker-57637-stop" "worker-57638-stop" "coord-show-tables-in-schema-sc1"

// rename the same distributed-schema table from different workers
permutation "coord-shard-replication-factor" "coord-create-table-sc1-t1" "worker-57637-start" "worker-57637-begin" "worker-57637-alter-table-rename-sc1-t1" "worker-57638-start" "worker-57638-alter-table-rename-sc1-t1" "worker-57637-commit" "worker-57637-stop" "worker-57638-stop" "coord-show-tables-in-schema-sc1"
