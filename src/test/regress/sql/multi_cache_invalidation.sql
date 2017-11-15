SET citus.next_shard_id TO 1601000;
CREATE TABLE tab9 (test_id integer NOT NULL, data int);
CREATE TABLE tab10 (test_id integer NOT NULL, data int);
SELECT master_create_distributed_table('tab9', 'test_id', 'hash');
SELECT master_create_distributed_table('tab10', 'test_id', 'hash');
SELECT master_create_worker_shards('tab9', 1, 1);
TRUNCATE tab9;
UPDATE pg_dist_shard SET logicalrelid = 'tab10'::regclass WHERE logicalrelid = 'tab9'::regclass;
TRUNCATE tab10;

DROP TABLE tab9;
DROP TABLE tab10;
