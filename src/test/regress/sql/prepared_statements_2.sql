SET search_path TO "prepared statements";


-- test parameterized inserts
PREPARE prepared_insert(varchar(20)) AS
	INSERT INTO router_executor_table VALUES (1, $1, $2);

EXECUTE prepared_insert('comment-1', '(1, 10)');
EXECUTE prepared_insert('comment-2', '(2, 20)');
EXECUTE prepared_insert('comment-3', '(3, 30)');
EXECUTE prepared_insert('comment-4', '(4, 40)');
EXECUTE prepared_insert('comment-5', '(5, 50)');
EXECUTE prepared_insert('comment-6', '(6, 60)');
EXECUTE prepared_insert('comment-7', '(7, 67)');


-- to make this work, Citus adds the type casting for composite keys
-- during the deparsing
PREPARE prepared_custom_type_select(test_composite_type) AS
	SELECT count(*) FROM router_executor_table WHERE id = 1 AND stats = $1;

EXECUTE prepared_custom_type_select('(1,10)');
EXECUTE prepared_custom_type_select('(2,20)');
EXECUTE prepared_custom_type_select('(3,30)');
EXECUTE prepared_custom_type_select('(4,40)');
EXECUTE prepared_custom_type_select('(5,50)');
EXECUTE prepared_custom_type_select('(6,60)');
EXECUTE prepared_custom_type_select('(7,67)');
EXECUTE prepared_custom_type_select('(7,67)');


-- test parameterized selects
PREPARE prepared_select(integer, integer) AS
	SELECT count(*) FROM router_executor_table
		WHERE id = 1 AND stats = ROW($1, $2)::test_composite_type;

EXECUTE prepared_select(1, 10);
EXECUTE prepared_select(2, 20);
EXECUTE prepared_select(3, 30);
EXECUTE prepared_select(4, 40);
EXECUTE prepared_select(5, 50);
EXECUTE prepared_select(6, 60);
EXECUTE prepared_select(7, 67);
EXECUTE prepared_select(7, 67);

-- Test that parameterized partition column for an insert is supported

PREPARE prepared_partition_column_insert(bigint) AS
INSERT INTO router_executor_table VALUES ($1, 'arsenous', '(1,10)');

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_partition_column_insert(1);
EXECUTE prepared_partition_column_insert(2);
EXECUTE prepared_partition_column_insert(3);
EXECUTE prepared_partition_column_insert(4);
EXECUTE prepared_partition_column_insert(5);
EXECUTE prepared_partition_column_insert(6);
EXECUTE prepared_partition_column_insert(7);


PREPARE prepared_no_parameter_insert AS
	INSERT INTO prepare_table (key) VALUES (0);

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_no_parameter_insert;
EXECUTE prepared_no_parameter_insert;
EXECUTE prepared_no_parameter_insert;
EXECUTE prepared_no_parameter_insert;
EXECUTE prepared_no_parameter_insert;
EXECUTE prepared_no_parameter_insert;
EXECUTE prepared_no_parameter_insert;

PREPARE prepared_single_parameter_insert(int) AS
	INSERT INTO prepare_table (key) VALUES ($1);

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_single_parameter_insert(1);
EXECUTE prepared_single_parameter_insert(2);
EXECUTE prepared_single_parameter_insert(3);
EXECUTE prepared_single_parameter_insert(4);
EXECUTE prepared_single_parameter_insert(5);
EXECUTE prepared_single_parameter_insert(6);
EXECUTE prepared_single_parameter_insert(7);

PREPARE prepared_double_parameter_insert(int, int) AS
	INSERT INTO prepare_table (key, value) VALUES ($1, $2);

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_double_parameter_insert(1, 10);
EXECUTE prepared_double_parameter_insert(2, 20);
EXECUTE prepared_double_parameter_insert(3, 30);
EXECUTE prepared_double_parameter_insert(4, 40);
EXECUTE prepared_double_parameter_insert(5, 50);
EXECUTE prepared_double_parameter_insert(6, 60);
EXECUTE prepared_double_parameter_insert(7, 70);

PREPARE prepared_multi_insert(int, int) AS
	INSERT INTO prepare_table (key, value) VALUES ($1, $2), ($1 + 1, $2 + 10);

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_multi_insert( 7,  70);
EXECUTE prepared_multi_insert( 9,  90);
EXECUTE prepared_multi_insert(11, 110);
EXECUTE prepared_multi_insert(13, 130);
EXECUTE prepared_multi_insert(15, 150);
EXECUTE prepared_multi_insert(17, 170);
EXECUTE prepared_multi_insert(19, 190);

PREPARE prepared_non_partition_parameter_insert(int) AS
	INSERT INTO prepare_table (key, value) VALUES (0, $1);

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_non_partition_parameter_insert(10);
EXECUTE prepared_non_partition_parameter_insert(20);
EXECUTE prepared_non_partition_parameter_insert(30);
EXECUTE prepared_non_partition_parameter_insert(40);
EXECUTE prepared_non_partition_parameter_insert(50);
EXECUTE prepared_non_partition_parameter_insert(60);
EXECUTE prepared_non_partition_parameter_insert(70);

-- check inserted values
SELECT count(*) FROM prepare_table;

DELETE FROM prepare_table WHERE value >= 70;

-- check router executor select
PREPARE prepared_router_partition_column_select(int) AS
	SELECT
		prepare_table.key,
		prepare_table.value
	FROM
		prepare_table
	WHERE
		prepare_table.key = $1
	ORDER BY
		key,
		value;

EXECUTE prepared_router_partition_column_select(1);
EXECUTE prepared_router_partition_column_select(2);
EXECUTE prepared_router_partition_column_select(3);
EXECUTE prepared_router_partition_column_select(4);
EXECUTE prepared_router_partition_column_select(5);
EXECUTE prepared_router_partition_column_select(6);
EXECUTE prepared_router_partition_column_select(7);

PREPARE prepared_router_non_partition_column_select(int) AS
	SELECT
		prepare_table.key,
		prepare_table.value
	FROM
		prepare_table
	WHERE
		prepare_table.key = 0 AND
		prepare_table.value = $1
	ORDER BY
		key,
		value;

EXECUTE prepared_router_non_partition_column_select(10);
EXECUTE prepared_router_non_partition_column_select(20);
EXECUTE prepared_router_non_partition_column_select(30);
EXECUTE prepared_router_non_partition_column_select(40);
EXECUTE prepared_router_non_partition_column_select(50);
EXECUTE prepared_router_non_partition_column_select(60);
EXECUTE prepared_router_non_partition_column_select(67);

-- check real-time executor
PREPARE prepared_real_time_non_partition_column_select(int) AS
	SELECT
		prepare_table.key,
		prepare_table.value
	FROM
		prepare_table
	WHERE
		prepare_table.value = $1
	ORDER BY
		key,
		value;

EXECUTE prepared_real_time_non_partition_column_select(10);
EXECUTE prepared_real_time_non_partition_column_select(20);
EXECUTE prepared_real_time_non_partition_column_select(30);
EXECUTE prepared_real_time_non_partition_column_select(40);
EXECUTE prepared_real_time_non_partition_column_select(50);
EXECUTE prepared_real_time_non_partition_column_select(60);
EXECUTE prepared_real_time_non_partition_column_select(70);

PREPARE prepared_real_time_partition_column_select(int) AS
	SELECT
		prepare_table.key,
		prepare_table.value
	FROM
		prepare_table
	WHERE
		prepare_table.key = $1 OR
		prepare_table.value = 10
	ORDER BY
		key,
		value;

EXECUTE prepared_real_time_partition_column_select(1);
EXECUTE prepared_real_time_partition_column_select(2);
EXECUTE prepared_real_time_partition_column_select(3);
EXECUTE prepared_real_time_partition_column_select(4);
EXECUTE prepared_real_time_partition_column_select(5);
EXECUTE prepared_real_time_partition_column_select(6);
EXECUTE prepared_real_time_partition_column_select(7);

PREPARE prepared_task_tracker_non_partition_column_select(int) AS
	SELECT
		prepare_table.key,
		prepare_table.value
	FROM
		prepare_table
	WHERE
		prepare_table.value = $1
	ORDER BY
		key,
		value;

EXECUTE prepared_task_tracker_non_partition_column_select(10);
EXECUTE prepared_task_tracker_non_partition_column_select(20);
EXECUTE prepared_task_tracker_non_partition_column_select(30);
EXECUTE prepared_task_tracker_non_partition_column_select(40);
EXECUTE prepared_task_tracker_non_partition_column_select(50);
EXECUTE prepared_task_tracker_non_partition_column_select(60);
EXECUTE prepared_task_tracker_non_partition_column_select(67);

PREPARE prepared_task_tracker_partition_column_select(int) AS
	SELECT
		prepare_table.key,
		prepare_table.value
	FROM
		prepare_table
	WHERE
		prepare_table.key = $1 OR
		prepare_table.value = 10
	ORDER BY
		key,
		value;

EXECUTE prepared_task_tracker_partition_column_select(1);
EXECUTE prepared_task_tracker_partition_column_select(2);
EXECUTE prepared_task_tracker_partition_column_select(3);
EXECUTE prepared_task_tracker_partition_column_select(4);
EXECUTE prepared_task_tracker_partition_column_select(5);
EXECUTE prepared_task_tracker_partition_column_select(6);
EXECUTE prepared_task_tracker_partition_column_select(7);


-- check updates
PREPARE prepared_partition_parameter_update(int, int) AS
	UPDATE prepare_table SET value = $2 WHERE key = $1;

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_partition_parameter_update(1, 11);
EXECUTE prepared_partition_parameter_update(2, 21);
EXECUTE prepared_partition_parameter_update(3, 31);
EXECUTE prepared_partition_parameter_update(4, 41);
EXECUTE prepared_partition_parameter_update(5, 51);
EXECUTE prepared_partition_parameter_update(6, 61);
EXECUTE prepared_partition_parameter_update(7, 71);

PREPARE prepared_non_partition_parameter_update(int, int) AS
	UPDATE prepare_table SET value = $2 WHERE key = 0 AND value = $1;

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_non_partition_parameter_update(10, 12);
EXECUTE prepared_non_partition_parameter_update(20, 22);
EXECUTE prepared_non_partition_parameter_update(30, 32);
EXECUTE prepared_non_partition_parameter_update(40, 42);
EXECUTE prepared_non_partition_parameter_update(50, 52);
EXECUTE prepared_non_partition_parameter_update(60, 62);
EXECUTE prepared_non_partition_parameter_update(70, 72);

-- check after updates
SELECT * FROM prepare_table ORDER BY key, value;

-- check deletes
PREPARE prepared_partition_parameter_delete(int, int) AS
	DELETE FROM prepare_table WHERE key = $1 AND value = $2;

EXECUTE prepared_partition_parameter_delete(1, 11);
EXECUTE prepared_partition_parameter_delete(2, 21);
EXECUTE prepared_partition_parameter_delete(3, 31);
EXECUTE prepared_partition_parameter_delete(4, 41);
EXECUTE prepared_partition_parameter_delete(5, 51);
EXECUTE prepared_partition_parameter_delete(6, 61);
EXECUTE prepared_partition_parameter_delete(7, 71);

PREPARE prepared_non_partition_parameter_delete(int) AS
	DELETE FROM prepare_table WHERE key = 0 AND value = $1;

-- execute 6 times to trigger prepared statement usage
EXECUTE prepared_non_partition_parameter_delete(12);
EXECUTE prepared_non_partition_parameter_delete(22);
EXECUTE prepared_non_partition_parameter_delete(32);
EXECUTE prepared_non_partition_parameter_delete(42);
EXECUTE prepared_non_partition_parameter_delete(52);
EXECUTE prepared_non_partition_parameter_delete(62);
EXECUTE prepared_non_partition_parameter_delete(72);

-- check after deletes
SELECT * FROM prepare_table ORDER BY key, value;

