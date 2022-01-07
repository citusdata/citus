\set VERBOSITY terse

SET citus.next_shard_id TO 1511000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA multi_row_router_insert;
SET search_path TO multi_row_router_insert;

SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- when using local execution, multi-row & router inserts works fine
-- even when not specifying some default columns

CREATE TABLE reference_table(column1 INT DEFAULT 1111, column2 INT DEFAULT 2222);
SELECT create_reference_table('reference_table');

INSERT INTO reference_table VALUES (5), (6);
-- note that first column is specified in below INSERT
INSERT INTO reference_table VALUES (DEFAULT), (7);
INSERT INTO reference_table (column2) VALUES (8), (9);

PREPARE prepared_statement(int) AS INSERT INTO reference_table (column2) VALUES ($1), ($1 * 500);
EXECUTE prepared_statement(1);
EXECUTE prepared_statement(1);
EXECUTE prepared_statement(1);
EXECUTE prepared_statement(2);
EXECUTE prepared_statement(3);
EXECUTE prepared_statement(3);

SELECT * FROM reference_table ORDER BY 1,2;

CREATE OR REPLACE FUNCTION square(a INT) RETURNS INT AS $$
BEGIN
    RETURN a*a;
END; $$ LANGUAGE PLPGSQL STABLE;
SELECT create_distributed_function('square(int)');
CREATE TABLE citus_local_table(a int, b int DEFAULT square(10));
SELECT citus_add_local_table_to_metadata('citus_local_table');

INSERT INTO citus_local_table VALUES (10), (11);
INSERT INTO citus_local_table (a) VALUES (12), (13);

ALTER TABLE citus_local_table ADD COLUMN c INT DEFAULT to_number('5', '91');
ALTER TABLE citus_local_table ADD COLUMN d INT;

INSERT INTO citus_local_table (d, a, b) VALUES (13, 14, 15), (16, 17, 18), (19, 20, 21);

SELECT * FROM citus_local_table ORDER BY 1,2,3,4;

-- cleanup at exit
DROP SCHEMA multi_row_router_insert CASCADE;
