
CREATE SCHEMA insert_select_repartition;
SET search_path TO 'insert_select_repartition';

SET citus.shard_count TO 8;
CREATE TABLE src_table (a int);
SELECT create_distributed_table('src_table', 'a');
INSERT INTO src_table SELECT i FROM generate_series(1, 100) i;

SET citus.shard_count TO 3;
CREATE TABLE trg_table (a int);
SELECT create_distributed_table('trg_table', 'a');
INSERT INTO trg_table SELECT * FROM src_table;

DROP SCHEMA insert_select_repartition;
