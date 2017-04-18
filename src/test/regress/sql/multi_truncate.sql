--
-- MULTI_TRUNCATE
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1210000;

--
-- truncate for append distribution
-- expect all shards to be dropped
--
CREATE TABLE test_truncate_append(a int);
SELECT master_create_distributed_table('test_truncate_append', 'a', 'append');

-- verify no error is thrown when no shards are present
TRUNCATE TABLE test_truncate_append;

SELECT master_create_empty_shard('test_truncate_append') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 1, shardmaxvalue = 500
WHERE shardid = :new_shard_id;

SELECT count(*) FROM test_truncate_append;

INSERT INTO test_truncate_append values (1);

SELECT count(*) FROM test_truncate_append;

-- create some more shards
SELECT master_create_empty_shard('test_truncate_append');
SELECT master_create_empty_shard('test_truncate_append');

-- verify 3 shards are presents
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_append'::regclass ORDER BY shardid;

TRUNCATE TABLE test_truncate_append;

-- verify data is truncated from the table
SELECT count(*) FROM test_truncate_append;

-- verify no shard exists anymore
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_append'::regclass;

-- command can run inside transaction
BEGIN; TRUNCATE TABLE test_truncate_append; COMMIT;

DROP TABLE test_truncate_append;

--
-- truncate for range distribution
-- expect shard to be present, data to be truncated
--
CREATE TABLE test_truncate_range(a int);
SELECT master_create_distributed_table('test_truncate_range', 'a', 'range');

-- verify no error is thrown when no shards are present
TRUNCATE TABLE test_truncate_range;

SELECT master_create_empty_shard('test_truncate_range') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 1, shardmaxvalue = 500
WHERE shardid = :new_shard_id;

SELECT master_create_empty_shard('test_truncate_range') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 501, shardmaxvalue = 1500
WHERE shardid = :new_shard_id;

SELECT master_create_empty_shard('test_truncate_range') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 1501, shardmaxvalue = 2500
WHERE shardid = :new_shard_id;


SELECT count(*) FROM test_truncate_range;

INSERT INTO test_truncate_range values (1);
INSERT INTO test_truncate_range values (1001);
INSERT INTO test_truncate_range values (2000);
INSERT INTO test_truncate_range values (100);

SELECT count(*) FROM test_truncate_range;

-- verify 3 shards are presents
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_range'::regclass ORDER BY shardid;

TRUNCATE TABLE test_truncate_range;

-- verify data is truncated from the table
SELECT count(*) FROM test_truncate_range;

-- verify 3 shards are still present
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_range'::regclass ORDER BY shardid;

-- verify that truncate can be aborted
INSERT INTO test_truncate_range VALUES (1);
BEGIN; TRUNCATE TABLE test_truncate_range; ROLLBACK;
SELECT count(*) FROM test_truncate_range;

DROP TABLE test_truncate_range;


--
-- truncate for hash distribution.
-- expect shard to be present, data to be truncated
--
CREATE TABLE test_truncate_hash(a int);
SELECT master_create_distributed_table('test_truncate_hash', 'a', 'hash');

-- verify no error is thrown when no shards are present
TRUNCATE TABLE test_truncate_hash;

SELECT count(*) FROM test_truncate_hash;

INSERT INTO test_truncate_hash values (1);
INSERT INTO test_truncate_hash values (1001);
INSERT INTO test_truncate_hash values (2000);
INSERT INTO test_truncate_hash values (100);

SELECT count(*) FROM test_truncate_hash;

-- verify 4 shards are present
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_hash'::regclass ORDER BY shardid;

TRUNCATE TABLE test_truncate_hash;

SELECT master_create_worker_shards('test_truncate_hash', 4, 1);

INSERT INTO test_truncate_hash values (1);
INSERT INTO test_truncate_hash values (1001);
INSERT INTO test_truncate_hash values (2000);
INSERT INTO test_truncate_hash values (100);

SELECT count(*) FROM test_truncate_hash;

TRUNCATE TABLE test_truncate_hash;

-- verify data is truncated from the table
SELECT count(*) FROM test_truncate_hash;

-- verify 4 shards are still presents
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_hash'::regclass ORDER BY shardid;

-- verify that truncate can be aborted
INSERT INTO test_truncate_hash VALUES (1);
BEGIN; TRUNCATE TABLE test_truncate_hash; ROLLBACK;
SELECT count(*) FROM test_truncate_hash;

DROP TABLE test_truncate_hash;

-- test with table with spaces in it
CREATE TABLE "a b hash" (a int, b int);
SELECT master_create_distributed_table('"a b hash"', 'a', 'hash');
SELECT master_create_worker_shards('"a b hash"', 4, 1);
INSERT INTO "a b hash" values (1, 0);
SELECT * from "a b hash";
TRUNCATE TABLE "a b hash";
SELECT * from "a b hash";

DROP TABLE "a b hash";

-- now with append
CREATE TABLE "a b append" (a int, b int);
SELECT master_create_distributed_table('"a b append"', 'a', 'append');
SELECT master_create_empty_shard('"a b append"') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 1, shardmaxvalue = 500
WHERE shardid = :new_shard_id;

SELECT master_create_empty_shard('"a b append"') AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 501, shardmaxvalue = 1000
WHERE shardid = :new_shard_id;

INSERT INTO "a b append" values (1, 1);
INSERT INTO "a b append" values (600, 600);

SELECT * FROM "a b append" ORDER BY a;

TRUNCATE TABLE "a b append";

-- verify all shards are dropped
SELECT shardid FROM pg_dist_shard where logicalrelid = '"a b append"'::regclass;

DROP TABLE "a b append";
