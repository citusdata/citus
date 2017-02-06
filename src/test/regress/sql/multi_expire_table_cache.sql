---
--- MULTI_EXPIRE_TABLE_CACHE
---


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1220000;

-- create test table
CREATE TABLE large_table(a int, b int);
SELECT master_create_distributed_table('large_table', 'a', 'hash');
SELECT master_create_worker_shards('large_table', 8, 1);

CREATE TABLE broadcast_table(a int, b int);
SELECT master_create_distributed_table('broadcast_table', 'a', 'hash');
SELECT master_create_worker_shards('broadcast_table', 2, 1);

-- verify only small tables are supported
SELECT master_expire_table_cache('large_table');
SELECT master_expire_table_cache('broadcast_table');

-- run a join so that broadcast tables are cached on other workers
SELECT * from large_table l, broadcast_table b where l.a = b.b;

-- insert some data
INSERT INTO large_table VALUES(1, 1);
INSERT INTO large_table VALUES(1, 2);
INSERT INTO large_table VALUES(2, 1);
INSERT INTO large_table VALUES(2, 2);
INSERT INTO large_table VALUES(3, 1);
INSERT INTO large_table VALUES(3, 2);

INSERT INTO broadcast_table VALUES(1, 1);

-- verify returned results are wrong
SELECT * from large_table l, broadcast_table b WHERE l.b = b.b ORDER BY l.a, l.b;

-- expire cache and re-run, results should be correct this time
SELECT master_expire_table_cache('broadcast_table');

SELECT * from large_table l, broadcast_table b WHERE l.b = b.b ORDER BY l.a, l.b;

-- insert some more data into broadcast table
INSERT INTO broadcast_table VALUES(2, 2);

-- run the same query, get wrong results
SELECT * from large_table l, broadcast_table b WHERE l.b = b.b ORDER BY l.a, l.b;

-- expire cache and re-run, results should be correct this time
SELECT master_expire_table_cache('broadcast_table');
SELECT * from large_table l, broadcast_table b WHERE l.b = b.b ORDER BY l.a, l.b;


DROP TABLE large_table, broadcast_table;
