#include "isolation_mx_common.include.spec"

// Test scenario for nonblocking split and concurrent INSERT/UPDATE/DELETE
//  session s1 - Executes create_distributed_table_concurrently after dropping a column on tables with replica identities
//  session s2 - Does concurrent inserts/update/delete
//  session s3 - Holds advisory locks

setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE observations_with_pk (
  	tenant_id text not null,
  	dummy int,
  	measurement_id bigserial not null,
  	payload jsonb not null,
  	observation_time timestamptz not null default '03/11/2018 02:00:00'::TIMESTAMP,
	PRIMARY KEY (tenant_id, measurement_id)
	);

	CREATE TABLE observations_with_full_replica_identity (
  	tenant_id text not null,
  	dummy int,
  	measurement_id bigserial not null,
  	payload jsonb not null,
  	observation_time timestamptz not null default '03/11/2018 02:00:00'::TIMESTAMP
	);
	ALTER TABLE observations_with_full_replica_identity REPLICA IDENTITY FULL;
}

teardown
{
    DROP TABLE observations_with_pk;
	DROP TABLE observations_with_full_replica_identity;
}

session "s1"

step "s1-alter-table"
{
	ALTER TABLE observations_with_pk DROP COLUMN dummy;
	ALTER TABLE observations_with_full_replica_identity DROP COLUMN dummy;
}

step "s1-set-factor-1"
{
	SET citus.shard_replication_factor TO 1;
	SELECT citus_set_coordinator_host('localhost');
}

step "s1-create-distributed-table-observations_with_pk-concurrently"
{
	SELECT create_distributed_table_concurrently('observations_with_pk','tenant_id');
}

step "s1-create-distributed-table-observations-2-concurrently"
{
	SELECT create_distributed_table_concurrently('observations_with_full_replica_identity','tenant_id');
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-insert-observations_with_pk"
{
	INSERT INTO observations_with_pk(tenant_id, payload) SELECT 'tenant_id', jsonb_build_object('name', 29.3);
	INSERT INTO observations_with_pk(tenant_id, payload) SELECT 'tenant_id', jsonb_build_object('name', 29.3);
	INSERT INTO observations_with_pk(tenant_id, payload) SELECT 'tenant_id', jsonb_build_object('name', 29.3);
	INSERT INTO observations_with_pk(tenant_id, payload) SELECT 'tenant_id', jsonb_build_object('name', 29.3);
}

step "s2-insert-observations_with_full_replica_identity"
{
	INSERT INTO observations_with_full_replica_identity(tenant_id, payload) SELECT 'tenant_id', jsonb_build_object('name', 29.3);
	INSERT INTO observations_with_full_replica_identity(tenant_id, payload) SELECT 'tenant_id', jsonb_build_object('name', 29.3);
	INSERT INTO observations_with_full_replica_identity(tenant_id, payload) SELECT 'tenant_id', jsonb_build_object('name', 29.3);
}

step "s2-update-observations_with_pk"
{
	UPDATE observations_with_pk set observation_time='03/11/2019 02:00:00'::TIMESTAMP  where tenant_id = 'tenant_id' and measurement_id = 3;
}

step "s2-update-primary-key-observations_with_pk"
{
	UPDATE observations_with_pk set measurement_id=100 where tenant_id = 'tenant_id'  and measurement_id = 4 ;
}

step "s2-update-observations_with_full_replica_identity"
{
	UPDATE observations_with_full_replica_identity set observation_time='03/11/2019 02:00:00'::TIMESTAMP  where tenant_id = 'tenant_id' and measurement_id = 3;
}

step "s2-delete-observations_with_pk"
{
	DELETE FROM observations_with_pk where tenant_id = 'tenant_id' and measurement_id = 3	;
}

step "s2-delete-observations_with_full_replica_identity"
{
	DELETE FROM observations_with_full_replica_identity where tenant_id = 'tenant_id' and measurement_id = 3	;
}

step "s2-end"
{
	  COMMIT;
}

step "s2-print-cluster-1"
{
	-- row count per shard
	SELECT
		nodeport, shardid, success, result
	FROM
		run_command_on_placements('observations_with_pk', 'select count(*) from %s')
	ORDER BY
		nodeport, shardid;

	SELECT *
	FROM
		observations_with_pk
	ORDER BY
	measurement_id;
}

step "s2-print-cluster-2"
{
	-- row count per shard
	SELECT
		nodeport, shardid, success, result
	FROM
		run_command_on_placements('observations_with_full_replica_identity', 'select count(*) from %s')
	ORDER BY
		nodeport, shardid;

	SELECT *
	FROM
		observations_with_full_replica_identity
	ORDER BY
	measurement_id;
}


session "s3"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s3-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

// Concurrent Insert/Update with create_distributed_table_concurrently(with primary key as replica identity) after dropping a column:
// s3 holds advisory lock -> s1 starts create_distributed_table_concurrently and waits for advisory lock ->
// s2 concurrently inserts/deletes/updates rows -> s3 releases the advisory lock
// -> s1 complete create_distributed_table_concurrently -> result is reflected in new shards
permutation "s2-print-cluster-1" "s3-acquire-advisory-lock" "s2-begin" "s1-alter-table" "s1-set-factor-1" "s1-create-distributed-table-observations_with_pk-concurrently" "s2-insert-observations_with_pk" "s2-update-observations_with_pk" "s2-end" "s2-print-cluster-1" "s3-release-advisory-lock" "s2-print-cluster-1"
permutation "s2-print-cluster-1" "s3-acquire-advisory-lock" "s2-begin" "s1-alter-table" "s1-set-factor-1" "s1-create-distributed-table-observations_with_pk-concurrently" "s2-insert-observations_with_pk" "s2-update-primary-key-observations_with_pk" "s2-end" "s2-print-cluster-1" "s3-release-advisory-lock" "s2-print-cluster-1"
permutation "s2-print-cluster-1" "s3-acquire-advisory-lock" "s2-begin" "s1-alter-table" "s1-set-factor-1" "s1-create-distributed-table-observations_with_pk-concurrently" "s2-insert-observations_with_pk" "s2-update-observations_with_pk" "s2-delete-observations_with_pk" "s2-end" "s2-print-cluster-1" "s3-release-advisory-lock" "s2-print-cluster-1"


// Concurrent Insert/Update with create_distributed_table_concurrently(with replica identity full) after dropping a column:
// s3 holds advisory lock -> s1 starts create_distributed_table_concurrently and waits for advisory lock ->
// s2 concurrently inserts/deletes/updates rows -> s3 releases the advisory lock
// -> s1 complete create_distributed_table_concurrently -> result is reflected in new shards
permutation "s2-print-cluster-2" "s3-acquire-advisory-lock" "s2-begin" "s1-alter-table" "s1-set-factor-1" "s1-create-distributed-table-observations-2-concurrently" "s2-insert-observations_with_full_replica_identity" "s2-update-observations_with_full_replica_identity" "s2-end" "s2-print-cluster-2" "s3-release-advisory-lock" "s2-print-cluster-2"
permutation "s2-print-cluster-2" "s3-acquire-advisory-lock" "s2-begin" "s1-alter-table" "s1-set-factor-1" "s1-create-distributed-table-observations-2-concurrently" "s2-insert-observations_with_full_replica_identity" "s2-update-observations_with_full_replica_identity" "s2-delete-observations_with_full_replica_identity" "s2-end" "s2-print-cluster-2" "s3-release-advisory-lock" "s2-print-cluster-2"
