

SET citus.next_shard_id TO 100400;

-- =================================================================
-- test modification functionality on reference tables from MX nodes
-- =================================================================

CREATE SCHEMA mx_modify_reference_table;
SET search_path TO 'mx_modify_reference_table';

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

SET citus.replication_model TO 'streaming';
SELECT start_metadata_sync_to_node(:'worker_1_host', :worker_1_port);
SELECT start_metadata_sync_to_node(:'worker_2_host', :worker_2_port);

CREATE TABlE ref_table(id int, value_1 int);
SELECT create_reference_table('ref_table');

CREATE TABlE ref_table_2(id int, value_1 int);
SELECT create_reference_table('ref_table_2');

CREATE TABLE test_table_1(id int, value_1 int);
SELECT create_distributed_table('test_table_1', 'id');
INSERT INTO test_table_1 VALUES(5,5),(6,6);

\c - - - :worker_1_port
SET search_path TO 'mx_modify_reference_table';

-- Simple DML operations from the first worker node
INSERT INTO ref_table VALUES(1,1),(2,2);
SELECT SUM(value_1) FROM ref_table;

UPDATE ref_table SET value_1 = 1 WHERE id = 2;
SELECT SUM(value_1) FROM ref_table;

DELETE FROM ref_table;
SELECT SUM(value_1) FROM ref_table;

COPY ref_table FROM STDIN DELIMITER ',';
1,1
2,2
\.
SELECT SUM(value_1) FROM ref_table;

-- Select For Update also follows the same logic with modification.
-- It has been started to be supported on MX nodes with DML operations.
SELECT * FROM ref_table FOR UPDATE;

-- Both distributed and non-distributed INSERT INTO ... SELECT
-- queries are also supported on MX nodes.
INSERT INTO ref_table SELECT * FROM test_table_1;
SELECT SUM(value_1) FROM ref_table;

INSERT INTO ref_table_2 SELECT * FROM ref_table;
SELECT SUM(value_1) FROM ref_table_2;

-- Now connect to the second worker and observe the results as well
\c - - - :worker_2_port
SET search_path TO 'mx_modify_reference_table';

SELECT SUM(value_1) FROM ref_table;
SELECT SUM(value_1) FROM ref_table_2;

-- Run basic queries from second worker node. These tests have been added
-- since locking logic is slightly different between running these commands
-- from first worker node and the second one
INSERT INTO ref_table VALUES(1,1),(2,2);
SELECT SUM(value_1) FROM ref_table;

UPDATE ref_table SET value_1 = 1 WHERE id = 2;
SELECT SUM(value_1) FROM ref_table;

COPY ref_table FROM STDIN DELIMITER ',';
1,1
2,2
\.
SELECT SUM(value_1) FROM ref_table;

INSERT INTO ref_table SELECT * FROM test_table_1;
SELECT SUM(value_1) FROM ref_table;

INSERT INTO ref_table_2 SELECT * FROM ref_table;
SELECT SUM(value_1) FROM ref_table_2;

\c - - - :master_port

SET search_path TO 'public';
DROP SCHEMA mx_modify_reference_table CASCADE;
