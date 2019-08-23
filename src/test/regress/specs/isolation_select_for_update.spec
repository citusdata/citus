
setup
{
	SELECT citus_internal.replace_isolation_tester_func();
	SELECT citus_internal.refresh_isolation_tester_prepared_statement();

	SET citus.shard_replication_factor to 1;

	CREATE TABLE test_table_1_rf1(id int, val_1 int);
	SELECT create_distributed_table('test_table_1_rf1','id');
	INSERT INTO test_table_1_rf1 values(1,2),(2,3),(3,4);

	CREATE VIEW test_1 AS SELECT * FROM test_table_1_rf1 WHERE val_1 = 2;

	CREATE TABLE test_table_2_rf1(id int, val_1 int);
	SELECT create_distributed_table('test_table_2_rf1','id');
	INSERT INTO test_table_2_rf1 values(1,2),(2,3),(3,4);

	CREATE TABLE ref_table(id int, val_1 int);
	SELECT create_reference_table('ref_table');
	INSERT INTO ref_table values(1,2),(3,4),(5,6);
}

teardown
{
	DROP TABLE test_table_1_rf1 CASCADE;
	DROP TABLE test_table_2_rf1;
	DROP TABLE ref_table;

	SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-select-from-t1-t2-for-update"
{
 	SELECT * FROM
		test_table_1_rf1 as tt1 INNER JOIN test_table_2_rf1 as tt2 on tt1.id = tt2.id
		WHERE tt1.id = 1 
		ORDER BY 1
		FOR UPDATE;
}

step "s1-select-from-t1-t2-for-share"
{
	SELECT * FROM
		test_table_1_rf1 as tt1 INNER JOIN test_table_2_rf1 as tt2 on tt1.id = tt2.id
		WHERE tt1.id = 1 
		ORDER BY 1
		FOR SHARE;
}

step "s1-select-from-t1-rt-for-update"
{
	SELECT * FROM
		test_table_1_rf1 as tt1 INNER JOIN ref_table as rt1 on tt1.id = rt1.id
		WHERE tt1.id = 1
		ORDER BY 1
		FOR UPDATE;
}

step "s1-select-from-t1-rt-with-lc-for-update"
{
	SELECT * FROM
		test_table_1_rf1 as tt1 INNER JOIN ref_table as rt1 on tt1.id = rt1.id
		WHERE tt1.id = 1
		ORDER BY 1
		FOR UPDATE
		OF rt1;
}

step "s1-select-from-t1-within-cte"
{
	WITH first_value AS ( SELECT val_1 FROM test_table_1_rf1 WHERE id = 1 FOR UPDATE)
	SELECT * FROM first_value;
}

step "s1-update-rt-with-cte-select-from-rt"
{
	WITH foo AS (SELECT * FROM ref_table FOR UPDATE)
	UPDATE ref_table SET val_1 = 4 FROM foo WHERE ref_table.id = foo.id;
}

step "s1-select-from-t1-with-subquery"
{
	SELECT * FROM (SELECT * FROM test_table_1_rf1 FOR UPDATE) foo WHERE id = 1;
}

step "s1-select-from-rt-with-subquery"
{
	SELECT * FROM (SELECT * FROM ref_table FOR UPDATE) foo WHERE id = 1;
}

step "s1-select-from-t1-with-view"
{
	SELECT * FROM test_1 WHERE id = 1 FOR UPDATE;
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-update-t1"
{
	UPDATE test_table_1_rf1 SET val_1 = 5 WHERE id = 1;
}

step "s2-update-rt"
{
	UPDATE ref_table SET val_1 = 5 WHERE id = 1;
}

step "s2-delete-t1"
{
	DELETE FROM test_table_1_rf1 WHERE id = 1;
}

step "s2-select-from-t1-t2-for-share"
{
	SELECT * FROM
		test_table_1_rf1 as tt1 INNER JOIN test_table_1_rf1 as tt2 on tt1.id = tt2.id
		WHERE tt1.id = 1 
		ORDER BY 1
		FOR SHARE;
}

step "s2-select-from-t1-t2-for-update"
{
 	SELECT * FROM
		test_table_1_rf1 as tt1 INNER JOIN test_table_1_rf1 as tt2 on tt1.id = tt2.id
		WHERE tt1.id = 1 
		ORDER BY 1
		FOR UPDATE;
}

step "s2-commit"
{
	COMMIT;
}

permutation "s1-begin" "s1-select-from-t1-t2-for-update" "s2-begin" "s2-update-t1" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-t2-for-share" "s2-begin" "s2-delete-t1" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-rt-for-update" "s2-begin" "s2-update-t1" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-rt-with-lc-for-update" "s2-begin" "s2-update-rt" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-rt-with-lc-for-update" "s2-begin" "s2-update-t1" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-t2-for-share" "s2-begin" "s2-select-from-t1-t2-for-share" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-rt-for-update" "s2-begin" "s2-select-from-t1-t2-for-update" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-within-cte" "s2-begin" "s2-select-from-t1-t2-for-update" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-within-cte" "s2-begin" "s2-update-t1" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-with-subquery" "s2-begin" "s2-update-t1" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-rt-with-subquery" "s2-begin" "s2-update-rt" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-from-t1-with-view" "s2-begin" "s2-update-t1" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-update-rt-with-cte-select-from-rt" "s2-begin" "s2-update-rt" "s1-commit" "s2-commit"
