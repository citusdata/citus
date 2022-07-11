CREATE SCHEMA index_create;
SET search_path TO index_create;

CREATE TABLE test_tbl (a INT NOT NULL PRIMARY KEY, b text, c BIGINT);
CREATE UNIQUE INDEX CONCURRENTLY a_index ON test_tbl (a);
SELECT create_distributed_table('test_tbl','a');

-- suppress the WARNING message: not propagating CLUSTER command to worker nodes
SET client_min_messages TO ERROR;
CLUSTER test_tbl USING test_tbl_pkey;
RESET client_min_messages;

BEGIN;
    CREATE INDEX idx1 ON test_tbl (a) INCLUDE (b, c);
    DROP TABLE test_tbl;
ROLLBACK;

CREATE INDEX idx1 ON test_tbl (a) INCLUDE (b, c) WHERE a > 10;
CREATE INDEX idx2 ON test_tbl (lower(b));

CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
-- create its partitions
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

-- create indexes on the parent
CREATE INDEX IF NOT EXISTS partitioned_idx_1 ON ONLY partitioning_test (id);
CREATE INDEX IF NOT EXISTS partitioned_idx_2 ON partitioning_test (id, time NULLS FIRST);

SELECT create_distributed_table('partitioning_test', 'id');

-- create hash index on distributed partitioned table
CREATE INDEX partition_idx_hash ON partitioning_test USING hash (id);

-- change statistics of index
ALTER INDEX idx2 ALTER COLUMN 1 SET STATISTICS 1000;

-- test reindex
REINDEX INDEX idx1;

ALTER TABLE test_tbl REPLICA IDENTITY USING INDEX a_index;

-- postgres allows ALTER INDEX rename on tables, and so Citus..
-- and, also ALTER TABLE rename on indexes..
CREATE TABLE alter_idx_rename_test (a INT);
CREATE INDEX alter_idx_rename_test_idx ON alter_idx_rename_test (a);
CREATE TABLE alter_idx_rename_test_parted (a INT) PARTITION BY LIST (a);
CREATE INDEX alter_idx_rename_test_parted_idx ON alter_idx_rename_test_parted (a);
BEGIN;
-- rename index/table with weird syntax
ALTER INDEX alter_idx_rename_test RENAME TO alter_idx_rename_test_2;
ALTER TABLE alter_idx_rename_test_idx RENAME TO alter_idx_rename_test_idx_2;
ALTER INDEX alter_idx_rename_test_parted RENAME TO alter_idx_rename_test_parted_2;
ALTER TABLE alter_idx_rename_test_parted_idx RENAME TO alter_idx_rename_test_parted_idx_2;

-- also, rename index/table with proper syntax
ALTER INDEX alter_idx_rename_test_idx_2 RENAME TO alter_idx_rename_test_idx_3;
ALTER TABLE alter_idx_rename_test_2 RENAME TO alter_idx_rename_test_3;
ALTER INDEX alter_idx_rename_test_parted_idx_2 RENAME TO alter_idx_rename_test_parted_idx_3;
ALTER TABLE alter_idx_rename_test_parted_2 RENAME TO alter_idx_rename_test_parted_3;

SELECT 'alter_idx_rename_test_3'::regclass, 'alter_idx_rename_test_idx_3'::regclass;
SELECT 'alter_idx_rename_test_parted_3'::regclass, 'alter_idx_rename_test_parted_idx_3'::regclass;

ROLLBACK;

-- now, on distributed tables
SELECT create_distributed_table('alter_idx_rename_test', 'a');
SELECT create_distributed_table('alter_idx_rename_test_parted', 'a');

-- rename index/table with weird syntax
ALTER INDEX alter_idx_rename_test RENAME TO alter_idx_rename_test_2;
ALTER TABLE alter_idx_rename_test_idx RENAME TO alter_idx_rename_test_idx_2;
ALTER INDEX alter_idx_rename_test_parted RENAME TO alter_idx_rename_test_parted_2;
ALTER TABLE alter_idx_rename_test_parted_idx RENAME TO alter_idx_rename_test_parted_idx_2;

-- also, rename index/table with proper syntax
ALTER INDEX alter_idx_rename_test_idx_2 RENAME TO alter_idx_rename_test_idx_3;
ALTER TABLE alter_idx_rename_test_2 RENAME TO alter_idx_rename_test_3;
ALTER INDEX alter_idx_rename_test_parted_idx_2 RENAME TO alter_idx_rename_test_parted_idx_3;
ALTER TABLE alter_idx_rename_test_parted_2 RENAME TO alter_idx_rename_test_parted_3;

SELECT 'alter_idx_rename_test_3'::regclass, 'alter_idx_rename_test_idx_3'::regclass;
SELECT 'alter_idx_rename_test_parted_3'::regclass, 'alter_idx_rename_test_parted_idx_3'::regclass;

ALTER INDEX alter_idx_rename_test_idx_3 RENAME TO alter_idx_rename_test_idx_4;
DROP INDEX alter_idx_rename_test_idx_4;
DROP TABLE alter_idx_rename_test_3;

DROP INDEX alter_idx_rename_test_parted_idx_3;
DROP TABLE alter_idx_rename_test_parted_3;
