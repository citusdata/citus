setup
{
	SET citus.shard_count TO 2;
	SET citus.shard_replication_factor TO 1;
	ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1490100;

	CREATE TABLE detach_parent (a int) PARTITION BY RANGE (a);
	CREATE TABLE detach_child PARTITION OF detach_parent
		FOR VALUES FROM (0) TO (10);
	SELECT create_distributed_table('detach_parent', 'a');
}

teardown
{
	DROP TABLE detach_child;
	DROP TABLE detach_parent;
}

session "s1"
step "s1_begin" { BEGIN; }
step "s1_read" { SELECT * FROM detach_parent; }
step "s1_commit" { COMMIT; }

session "s2"
step "s2_detach"
{
	ALTER TABLE detach_parent DETACH PARTITION detach_child CONCURRENTLY;
}

session "s3"
step "s3_cancel"
{
	SELECT pg_cancel_backend(pid)
	FROM pg_stat_activity
	WHERE pid <> pg_backend_pid()
	  AND query LIKE
		'%ALTER TABLE detach_parent DETACH PARTITION detach_child CONCURRENTLY%';
}
step "s3_pending"
{
	SELECT inhdetachpending
	FROM pg_inherits
	WHERE inhparent = 'detach_parent'::regclass;
}
step "s3_workers_attached"
{
	SELECT result
	FROM run_command_on_workers($$
		SELECT count(*)
		FROM pg_inherits i
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname LIKE 'detach_parent\_%'
	$$)
	ORDER BY result;
}
step "s3_finalize"
{
	ALTER TABLE detach_parent DETACH PARTITION detach_child FINALIZE;
}
step "s3_done"
{
	SELECT relispartition
	FROM pg_class
	WHERE oid = 'detach_child'::regclass;
}
step "s3_workers_done"
{
	SELECT result
	FROM run_command_on_workers($$
		SELECT count(*)
		FROM pg_inherits i
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname LIKE 'detach_parent\_%'
	$$)
	ORDER BY result;
}

permutation
	s1_begin
	s1_read
	s2_detach(s3_cancel)
	s3_cancel
	s3_pending
	s3_workers_attached
	s1_commit
	s3_finalize
	s3_done
	s3_workers_done
