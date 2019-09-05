CREATE SCHEMA upgrade_distributed_table_before;
SET search_path TO upgrade_distributed_table_before, public;

CREATE TABLE t(a int);
SELECT create_distributed_table('t', 'a');
INSERT INTO t SELECT * FROM generate_series(1, 5);
