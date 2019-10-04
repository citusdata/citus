CREATE SCHEMA before_citus_upgrade_coord;
SET search_path TO before_citus_upgrade_coord, public;

CREATE TABLE t(a int);
SELECT create_distributed_table('t', 'a');
INSERT INTO t SELECT * FROM generate_series(1, 5);
