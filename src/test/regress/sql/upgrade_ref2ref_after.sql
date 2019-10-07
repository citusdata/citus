SET search_path TO upgrade_ref2ref, public;
BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

SELECT * FROM ref_table_1 ORDER BY id;
SELECT * FROM ref_table_2 ORDER BY id;
SELECT * FROM ref_table_3 ORDER BY id;
SELECT * FROM dist_table ORDER BY id;

UPDATE ref_table_1 SET id = 10 where id = 1;

SELECT * FROM ref_table_1 ORDER BY id;
SELECT * FROM ref_table_2 ORDER BY id;
SELECT * FROM ref_table_3 ORDER BY id;
SELECT * FROM dist_table ORDER BY id;

DELETE FROM ref_table_1 WHERE id = 4;

SELECT * FROM ref_table_1 ORDER BY id;
SELECT * FROM ref_table_2 ORDER BY id;
SELECT * FROM ref_table_3 ORDER BY id;
SELECT * FROM dist_table ORDER BY id;

ROLLBACK;
