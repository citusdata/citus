setup
{
	SET citus.shard_replication_factor to 1;

	ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 123000;
	ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 123000;
	ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 123000;
	ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 123000;

	-- Create the necessary test utility function
	CREATE OR REPLACE FUNCTION activate_node_snapshot()
		RETURNS text[]
		LANGUAGE C STRICT
		AS 'citus';
	SELECT create_distributed_function('activate_node_snapshot()');

	-- Create distributed tables
	CREATE TABLE ref_table (test_id integer, y int unique);
	SELECT create_reference_table('ref_table');

	CREATE TABLE dist_table (x int, y int);
	SELECT create_distributed_table('dist_table', 'x');

	CREATE TABLE dist_partitioned_table (x int, y int) PARTITION BY RANGE(y);
	SELECT create_distributed_table('dist_partitioned_table', 'x');

	CREATE TABLE dist_partitioned_table_p1(x int, y int);
}

teardown
{
	DROP TABLE IF EXISTS ref_table,
						 dist_table,
						 dist_partitioned_table,
						 dist_partitioned_table_p1,
						 dist_partitioned_table_p2,
						 new_dist_table,
						 new_ref_table;


	DROP FUNCTION activate_node_snapshot();
	DROP FUNCTION IF EXISTS squares(int);
	DROP TYPE IF EXISTS my_type;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-commit"
{
    COMMIT;
}

step "s1-start-metadata-sync"
{
	SELECT start_metadata_sync_to_node('localhost', 57638);
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-commit"
{
	COMMIT;
}

step "s2-start-metadata-sync-to-same-node"
{
	SELECT start_metadata_sync_to_node('localhost', 57638);
}

step "s2-start-metadata-sync-to-another-node"
{
	SELECT start_metadata_sync_to_node('localhost', 57637);
}

step "s2-alter-table"
{
	ALTER TABLE dist_table ADD COLUMN z int;
}

step "s2-add-fk"
{
	ALTER TABLE dist_table ADD CONSTRAINT y_fk FOREIGN KEY (y) REFERENCES ref_table(y);
}

step "s2-drop-fk"
{
	ALTER TABLE dist_table DROP CONSTRAINT y_fk;
}

step "s2-drop-table"
{
	DROP TABLE dist_table;
}

step "s2-create-dist-table"
{
	CREATE TABLE new_dist_table(id int, data int);
	SELECT create_distributed_table('new_dist_table', 'id');
}

step "s2-create-schema"
{
	CREATE SCHEMA dist_schema;
	CREATE TABLE dist_schema.dist_table_in_schema(id int, data int);

	SELECT create_distributed_table('dist_schema.dist_table_in_schema', 'id');
}

step "s2-drop-schema"
{
	DROP SCHEMA dist_schema CASCADE;
}

step "s2-create-ref-table"
{
	CREATE TABLE new_ref_table(id int, data int);
	SELECT create_reference_table('new_ref_table');
}

step "s2-attach-partition"
{
	ALTER TABLE dist_partitioned_table ATTACH PARTITION dist_partitioned_table_p1 FOR VALUES FROM (1) TO (9);
}

step "s2-detach-partition"
{
	ALTER TABLE dist_partitioned_table DETACH PARTITION dist_partitioned_table_p1;
}

step "s2-create-partition-of"
{
	CREATE TABLE dist_partitioned_table_p2 PARTITION OF dist_partitioned_table FOR VALUES FROM (10) TO (20);
}

step "s2-create-type"
{
	CREATE TYPE my_type AS (a int, b int);
}

step "s2-drop-type"
{
	DROP TYPE my_type;
}

step "s2-alter-type"
{
	ALTER TYPE my_type ADD ATTRIBUTE x int;
}

step "s2-create-dist-func"
{
	CREATE FUNCTION squares(int) RETURNS SETOF RECORD
    AS $$ SELECT i, i * i FROM generate_series(1, $1) i $$
    LANGUAGE SQL;

	SELECT create_distributed_function('squares(int)');
}

step "s2-drop-dist-func"
{
	DROP FUNCTION squares(int);
}

session "s3"

step "s3-compare-snapshot"
{
	SELECT count(*) = 0 AS same_metadata_in_workers
	FROM
	(
		(
			SELECT unnest(activate_node_snapshot())
				EXCEPT
			SELECT unnest(result::text[]) AS unnested_result
			FROM run_command_on_workers($$SELECT activate_node_snapshot()$$)
		)
	UNION
		(
			SELECT unnest(result::text[]) AS unnested_result
			FROM run_command_on_workers($$SELECT activate_node_snapshot()$$)
				EXCEPT
			SELECT unnest(activate_node_snapshot())
		)
	) AS foo;
}

step "s3-compare-type-definition"
{
	SELECT run_command_on_workers($$SELECT '(1,1,1)'::my_type$$);
}

step "s3-debug"
{
	SELECT unnest(activate_node_snapshot());

	SELECT unnest(result::text[])
	FROM run_command_on_workers('SELECT activate_node_snapshot()');
}

// before running any updates to metadata, make sure all nodes have same metadata in the cluster
permutation "s3-compare-snapshot"

// concurrent metadata syncing operations get blocked
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-start-metadata-sync-to-same-node" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-start-metadata-sync-to-another-node" "s1-commit" "s2-commit" "s3-compare-snapshot"

// the following operations get blocked when a concurrent metadata sync is in progress
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-alter-table" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-drop-table" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-create-schema" "s1-commit" "s2-commit" "s3-compare-snapshot" "s2-drop-schema"
permutation "s2-create-schema" "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-drop-schema" "s1-commit" "s2-commit" "s3-compare-snapshot"
// currently snapshot comparison is omitted because independent pg_dist_object records do not have a deterministic order
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-create-dist-table" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-create-ref-table" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-attach-partition" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s2-attach-partition" "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-detach-partition" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s2-attach-partition" "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-create-partition-of" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-add-fk" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s2-add-fk" "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-drop-fk" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s2-create-type" "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-drop-type" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s2-create-dist-func" "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-drop-dist-func" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s2-create-type" "s1-begin" "s1-start-metadata-sync" "s2-alter-type" "s1-commit" "s3-compare-snapshot" "s3-compare-type-definition"

// the following operations block concurrent metadata sync calls
permutation "s1-begin" "s2-begin" "s2-create-dist-table" "s1-start-metadata-sync" "s2-commit" "s1-commit" "s3-compare-snapshot"
permutation "s2-create-dist-func" "s1-begin" "s2-begin" "s2-drop-dist-func" "s1-start-metadata-sync" "s2-commit" "s1-commit" "s3-compare-snapshot"
permutation "s2-create-schema" "s1-begin" "s2-begin" "s2-drop-schema" "s1-start-metadata-sync" "s2-commit" "s1-commit" "s3-compare-snapshot"
permutation "s2-create-type" "s1-begin" "s2-begin" "s2-alter-type" "s1-start-metadata-sync" "s2-commit" "s1-commit" "s3-compare-snapshot" "s3-compare-type-definition"

// the following operations do not get blocked
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-create-type" "s1-commit" "s2-commit" "s3-compare-snapshot"
permutation "s1-begin" "s2-begin" "s1-start-metadata-sync" "s2-create-dist-func" "s1-commit" "s2-commit" "s3-compare-snapshot"
