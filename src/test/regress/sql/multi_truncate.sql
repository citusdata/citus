--
-- MULTI_TRUNCATE
--


SET citus.next_shard_id TO 1210000;
CREATE SCHEMA multi_truncate;
SET search_path TO multi_truncate;

-- helper view that prints out local table names and sizes in the schema
CREATE VIEW table_sizes AS
SELECT
  c.relname as name,
  pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as size
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
      AND n.nspname = 'multi_truncate'
ORDER BY 1;

--
-- truncate for append distribution
-- expect all shards to be dropped
--
CREATE TABLE test_truncate_append(a int);
SELECT create_distributed_table('test_truncate_append', 'a', 'append');

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
SELECT create_distributed_table('test_truncate_range', 'a', 'range');

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
SET citus.shard_replication_factor TO 1;
CREATE TABLE "a b hash" (a int, b int);
SELECT create_distributed_table('"a b hash"', 'a', 'hash');
INSERT INTO "a b hash" values (1, 0);
SELECT * from "a b hash";
TRUNCATE TABLE "a b hash";
SELECT * from "a b hash";

DROP TABLE "a b hash";

-- now with append
CREATE TABLE "a b append" (a int, b int);
SELECT create_distributed_table('"a b append"', 'a', 'append');
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

-- Truncate local data only
CREATE TABLE test_local_truncate (x int, y int);
INSERT INTO test_local_truncate VALUES (1,2);
SELECT create_distributed_table('test_local_truncate', 'x', colocate_with => 'none');

BEGIN;
SET LOCAL citus.enable_ddl_propagation TO off;
TRUNCATE test_local_truncate;
COMMIT;

-- Ensure distributed data is not truncated
SELECT * FROM test_local_truncate;

-- Undistribute table
SELECT citus_drop_all_shards('test_local_truncate', 'public', 'test_local_truncate');
DELETE FROM pg_dist_partition WHERE logicalrelid = 'test_local_truncate'::regclass;

-- Ensure local data is truncated
SELECT * FROM test_local_truncate;

DROP TABLE test_local_truncate;

-- Truncate local data, but roll back
CREATE TABLE test_local_truncate (x int, y int);
INSERT INTO test_local_truncate VALUES (1,2);
SELECT create_distributed_table('test_local_truncate', 'x', colocate_with => 'none');

BEGIN;
SET LOCAL citus.enable_ddl_propagation TO off;
TRUNCATE test_local_truncate;
ROLLBACK;

-- Ensure distributed data is not truncated
SELECT * FROM test_local_truncate;

-- Undistribute table
SELECT citus_drop_all_shards('test_local_truncate', 'public', 'test_local_truncate');
DELETE FROM pg_dist_partition WHERE logicalrelid = 'test_local_truncate'::regclass;

-- Ensure local data is not truncated
SELECT * FROM test_local_truncate;

DROP TABLE test_local_truncate;

-- Test truncate_local_data_after_distributing_table UDF
CREATE TABLE referenced_table(id int UNIQUE, test_column int);
INSERT INTO referenced_table SELECT x,x FROM generate_series(1,10000) x;

CREATE TABLE referencing_table(id int, ref_id int REFERENCES referenced_table(id));
INSERT INTO referencing_table SELECT * FROM referenced_table;

-- The following will fail as the table is not distributed
SELECT truncate_local_data_after_distributing_table('referenced_table');

-- Test foreign keys from local tables to distributed tables
-- We can not truncate local tables until all the local foreign keys are removed.
SELECT create_distributed_table('referenced_table', 'id');

-- The following will fail, as local referencing_table has fk references
SELECT truncate_local_data_after_distributing_table('referenced_table');

-- Test foreign keys between distributed tables
SELECT create_distributed_table('referencing_table', 'ref_id');

BEGIN;
-- The following will no longer fail as the referencing table is now distributed
SELECT truncate_local_data_after_distributing_table('referenced_table');
SELECT * FROM table_sizes;
ROLLBACK;

-- observe that none of the tables are truncated
SELECT * FROM table_sizes;

-- test that if we truncate the referencing table, only said table is affected
BEGIN;
SELECT truncate_local_data_after_distributing_table('referencing_table');
SELECT * FROM table_sizes;
ROLLBACK;

-- however if we truncate referenced table, both of the tables get truncated
-- because we supply the CASCADE option
-- test that if we truncate the referencing table, only said table is affected
BEGIN;
SELECT truncate_local_data_after_distributing_table('referenced_table');
SELECT * FROM table_sizes;
ROLLBACK;

DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test truncating reference tables
CREATE TABLE ref(id int UNIQUE, data int);
INSERT INTO ref SELECT x,x FROM generate_series(1,10000) x;
SELECT create_reference_table('ref');

CREATE TABLE dist(id int, ref_id int REFERENCES ref(id));
INSERT INTO dist SELECT x,x FROM generate_series(1,10000) x;

-- test that we do not cascade truncates to local referencing tables
SELECT truncate_local_data_after_distributing_table('ref');

-- distribute the table and start testing allowed truncation queries
SELECT create_distributed_table('dist','id');

-- the following should truncate ref and dist
BEGIN;
SELECT truncate_local_data_after_distributing_table('ref');
SELECT * FROM table_sizes;
ROLLBACK;

-- the following should truncate dist table only
BEGIN;
SELECT truncate_local_data_after_distributing_table('dist');
SELECT * FROM table_sizes;
ROLLBACK;

DROP TABLE ref, dist;

-- tests for issue 1770
CREATE TABLE t1(a int, b int);
INSERT INTO t1 VALUES(1,1);
SELECT create_distributed_table('t1', 'a');
ALTER TABLE t1 ADD CONSTRAINT t1_a_check CHECK(a > 2) NOT VALID;

-- will error out with "ERROR:  CHECK CONSTRAINT "t1_a_check" is violated by some row"
ALTER TABLE t1 VALIDATE CONSTRAINT t1_a_check;
-- remove violating row
DELETE FROM t1 where a = 1;
-- verify no rows in t1
SELECT * FROM t1;
-- this will still error out
ALTER TABLE t1 VALIDATE CONSTRAINT t1_a_check;

-- The check will pass when the local copies are truncated
SELECT truncate_local_data_after_distributing_table('t1');
ALTER TABLE t1 VALIDATE CONSTRAINT t1_a_check;

DROP VIEW table_sizes;
DROP TABLE t1;
DROP SCHEMA multi_truncate CASCADE;
