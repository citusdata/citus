--
-- MULTI_TRUNCATE
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1210000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1210000;

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
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_append'::regclass;

TRUNCATE TABLE test_truncate_append;

-- verify data is truncated from the table
SELECT count(*) FROM test_truncate_append;

-- verify no shard exists anymore
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_append'::regclass;

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
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_range'::regclass;

TRUNCATE TABLE test_truncate_range;

-- verify data is truncated from the table
SELECT count(*) FROM test_truncate_range;

-- verify 3 shards are still present
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_range'::regclass;

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
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_hash'::regclass;

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
SELECT shardid FROM pg_dist_shard where logicalrelid = 'test_truncate_hash'::regclass;

DROP TABLE test_truncate_hash;
