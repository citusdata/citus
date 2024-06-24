--
-- MULTI_SIZE_QUERIES
--
-- Test checks whether size of distributed tables can be obtained with citus_table_size.
-- To find the relation size and total relation size citus_relation_size and
-- citus_total_relation_size are also tested.

SET citus.next_shard_id TO 1390000;

-- Tests with invalid relation IDs
SELECT citus_table_size(1);
SELECT citus_relation_size(1);
SELECT citus_total_relation_size(1);

-- Tests with non-distributed table
CREATE TABLE non_distributed_table (x int primary key);

SELECT citus_table_size('non_distributed_table');
SELECT citus_relation_size('non_distributed_table');
SELECT citus_total_relation_size('non_distributed_table');

SELECT citus_table_size('non_distributed_table_pkey');
SELECT citus_relation_size('non_distributed_table_pkey');
SELECT citus_total_relation_size('non_distributed_table_pkey');
DROP TABLE non_distributed_table;

-- fix broken placements via disabling the node
SET client_min_messages TO ERROR;
SELECT replicate_table_shards('lineitem_hash_part', shard_replication_factor:=2, shard_transfer_mode:='block_writes');

-- Tests on distributed table with replication factor > 1
VACUUM (FULL) lineitem_hash_part;

SELECT citus_relation_size('lineitem_hash_part') <= citus_table_size('lineitem_hash_part');
SELECT citus_table_size('lineitem_hash_part') <= citus_total_relation_size('lineitem_hash_part');
SELECT citus_relation_size('lineitem_hash_part') > 0;

CREATE INDEX lineitem_hash_part_idx ON lineitem_hash_part(l_orderkey);
VACUUM (FULL) lineitem_hash_part;

SELECT citus_relation_size('lineitem_hash_part') <= citus_table_size('lineitem_hash_part');
SELECT citus_table_size('lineitem_hash_part') <= citus_total_relation_size('lineitem_hash_part');
SELECT citus_relation_size('lineitem_hash_part') > 0;

SELECT citus_relation_size('lineitem_hash_part_idx') <= citus_table_size('lineitem_hash_part_idx');
SELECT citus_table_size('lineitem_hash_part_idx') <= citus_total_relation_size('lineitem_hash_part_idx');
SELECT citus_relation_size('lineitem_hash_part_idx') > 0;

SELECT citus_total_relation_size('lineitem_hash_part') >=
       citus_table_size('lineitem_hash_part') + citus_table_size('lineitem_hash_part_idx');

DROP INDEX lineitem_hash_part_idx;

VACUUM (FULL) customer_copy_hash;

-- Tests on distributed tables with streaming replication.
SELECT citus_table_size('customer_copy_hash');
SELECT citus_relation_size('customer_copy_hash');
SELECT citus_total_relation_size('customer_copy_hash');

-- Make sure we can get multiple sizes in a single query
SELECT citus_table_size('customer_copy_hash'),
       citus_table_size('customer_copy_hash'),
       citus_table_size('customer_copy_hash');

CREATE INDEX index_1 on customer_copy_hash(c_custkey);
VACUUM (FULL) customer_copy_hash;

-- Tests on distributed table with index.
SELECT citus_table_size('customer_copy_hash');
SELECT citus_relation_size('customer_copy_hash');
SELECT citus_total_relation_size('customer_copy_hash');

SELECT citus_table_size('index_1');
SELECT citus_relation_size('index_1');
SELECT citus_total_relation_size('index_1');

-- Tests on reference table
VACUUM (FULL) supplier;

SELECT citus_table_size('supplier');
SELECT citus_relation_size('supplier');
SELECT citus_total_relation_size('supplier');

CREATE INDEX index_2 on supplier(s_suppkey);
VACUUM (FULL) supplier;

SELECT citus_table_size('supplier');
SELECT citus_relation_size('supplier');
SELECT citus_total_relation_size('supplier');

SELECT citus_table_size('index_2');
SELECT citus_relation_size('index_2');
SELECT citus_total_relation_size('index_2');

-- Test on partitioned table
CREATE TABLE split_me (dist_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
CREATE INDEX ON split_me(dist_col);

-- create 2 partitions
CREATE TABLE m PARTITION OF split_me FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE e PARTITION OF split_me FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

INSERT INTO split_me SELECT 1, '2018-01-01'::timestamp + i * interval '1 day' FROM generate_series(1, 360) i;
INSERT INTO split_me SELECT 2, '2019-01-01'::timestamp + i * interval '1 day' FROM generate_series(1, 180) i;

-- before citus
SELECT citus_relation_size('split_me');
SELECT citus_relation_size('split_me_dist_col_idx');
SELECT citus_relation_size('m');
SELECT citus_relation_size('m_dist_col_idx');

-- distribute the table(s)
SELECT create_distributed_table('split_me', 'dist_col');

-- after citus
SELECT citus_relation_size('split_me');
SELECT citus_relation_size('split_me_dist_col_idx');
SELECT citus_relation_size('m');
SELECT citus_relation_size('m_dist_col_idx');

DROP TABLE split_me;

-- Test inside the transaction
BEGIN;
ALTER TABLE supplier ALTER COLUMN s_suppkey SET NOT NULL;
select citus_table_size('supplier');
END;

show citus.node_conninfo;
ALTER SYSTEM SET citus.node_conninfo = 'sslmode=require';
SELECT pg_reload_conf();

-- make sure that any invalidation to the connection info
-- wouldn't prevent future commands to fail
SELECT citus_total_relation_size('customer_copy_hash');
SELECT pg_reload_conf();
SELECT citus_total_relation_size('customer_copy_hash');

-- reset back to the original node_conninfo
ALTER SYSTEM RESET citus.node_conninfo;
SELECT pg_reload_conf();

DROP INDEX index_1;
DROP INDEX index_2;
