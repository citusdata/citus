--
-- MULTI_CROSS_SHARD
--
-- Tests to log cross shard queries according to error log level
--

SET citus.enable_repartition_joins to ON;

-- Create a distributed table and add data to it
CREATE TABLE multi_task_table
(
	id int,
	name varchar(20)
);
SELECT create_distributed_table('multi_task_table', 'id');

INSERT INTO multi_task_table VALUES(1, 'elem_1');
INSERT INTO multi_task_table VALUES(2, 'elem_2');
INSERT INTO multi_task_table VALUES(3, 'elem_3');

-- Shouldn't log anything when the log level is 'off'
SHOW citus.multi_task_query_log_level;
SELECT * FROM multi_task_table ORDER BY 1;

-- Get messages with the log level 'notice'
SET citus.multi_task_query_log_level TO notice;
SELECT * FROM multi_task_table ORDER BY 1;
SELECT AVG(id) AS avg_id FROM multi_task_table;

-- Get messages with the log level 'error'
SET citus.multi_task_query_log_level TO error;
SELECT * FROM multi_task_table;

-- Check the log message with INSERT INTO ... SELECT
CREATE TABLE raw_table
(
    id int,
    order_count int
);

CREATE TABLE summary_table
(
    id int,
    order_sum BIGINT
);

SELECT create_distributed_table('raw_table', 'id');
SELECT create_distributed_table('summary_table', 'id');

INSERT INTO raw_table VALUES(1, '15');
INSERT INTO raw_table VALUES(2, '15');
INSERT INTO raw_table VALUES(3, '15');
INSERT INTO raw_table VALUES(1, '20');
INSERT INTO raw_table VALUES(2, '25');
INSERT INTO raw_table VALUES(3, '35');

--  Should notice user that the query is multi-task one
SET citus.multi_task_query_log_level TO notice;
INSERT INTO summary_table SELECT id, SUM(order_count) FROM raw_table GROUP BY id;

-- Should error out since the query is multi-task one
SET citus.multi_task_query_log_level TO error;
INSERT INTO summary_table SELECT id, SUM(order_count) FROM raw_table GROUP BY id;

-- Shouldn't error out since it is a single task insert-into select query
INSERT INTO summary_table SELECT id, SUM(order_count) FROM raw_table WHERE id = 1 GROUP BY id;

-- Should have four rows (three rows from the query without where and the one from with where)
SET citus.multi_task_query_log_level to DEFAULT;
SELECT * FROM summary_table ORDER BY 1,2;

-- Set log-level to different levels inside the transaction
BEGIN;
--  Should notice user that the query is multi-task one
SET citus.multi_task_query_log_level TO notice;
INSERT INTO summary_table SELECT id, SUM(order_count) FROM raw_table GROUP BY id;

-- Should error out since the query is multi-task one
SET citus.multi_task_query_log_level TO error;
INSERT INTO summary_table SELECT id, SUM(order_count) FROM raw_table GROUP BY id;
ROLLBACK;

-- Should have only four rows since the transaction is rollbacked.
SET citus.multi_task_query_log_level to DEFAULT;
SELECT * FROM summary_table ORDER BY 1,2;

-- Test router-select query
SET citus.multi_task_query_log_level TO notice;

-- Shouldn't log since it is a router select query
SELECT * FROM raw_table WHERE ID = 1;

CREATE TABLE tt1
(
	id int,
	name varchar(20)
);

CREATE TABLE tt2
(
	id int,
	name varchar(20),
	count bigint
);

SELECT create_distributed_table('tt1', 'id');
SELECT create_distributed_table('tt2', 'name');

INSERT INTO tt1 VALUES(1, 'Ahmet');
INSERT INTO tt1 VALUES(2, 'Mehmet');

INSERT INTO tt2 VALUES(1, 'Ahmet', 5);
INSERT INTO tt2 VALUES(2, 'Mehmet', 15);

SELECT tt1.id, tt2.count from tt1,tt2 where tt1.id = tt2.id ORDER BY 1;

SET citus.task_executor_type to DEFAULT;

DROP TABLE tt2;
DROP TABLE tt1;
DROP TABLE multi_task_table;
DROP TABLE raw_table;
DROP TABLE summary_table;
