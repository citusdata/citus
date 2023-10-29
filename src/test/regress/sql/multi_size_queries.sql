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
-- table
SELECT citus_table_size('non_distributed_table');
SELECT citus_relation_size('non_distributed_table');
SELECT citus_total_relation_size('non_distributed_table');
-- index
SELECT citus_table_size('non_distributed_table_pkey');
SELECT citus_relation_size('non_distributed_table_pkey');
SELECT citus_total_relation_size('non_distributed_table_pkey');
DROP TABLE non_distributed_table;

-- fix broken placements via disabling the node
SET client_min_messages TO ERROR;
SELECT replicate_table_shards('lineitem_hash_part', shard_replication_factor:=2, shard_transfer_mode:='block_writes');

CREATE INDEX lineitem_hash_part_idx ON lineitem_hash_part(l_orderkey);
-- Tests on distributed table with replication factor > 1
VACUUM (FULL) lineitem_hash_part;

-- table
SELECT citus_table_size('lineitem_hash_part');
SELECT citus_relation_size('lineitem_hash_part');
SELECT citus_total_relation_size('lineitem_hash_part');
-- index
SELECT citus_table_size('lineitem_hash_part_idx');
SELECT citus_relation_size('lineitem_hash_part_idx');
SELECT citus_total_relation_size('lineitem_hash_part_idx');

DROP INDEX lineitem_hash_part_idx;

CREATE INDEX customer_copy_hash_idx on customer_copy_hash(c_custkey);
VACUUM (FULL) customer_copy_hash;

-- Tests on distributed tables with streaming replication.
-- table
SELECT citus_table_size('customer_copy_hash');
SELECT citus_relation_size('customer_copy_hash');
SELECT citus_total_relation_size('customer_copy_hash');
-- index
SELECT citus_table_size('customer_copy_hash_idx');
SELECT citus_relation_size('customer_copy_hash_idx');
SELECT citus_total_relation_size('customer_copy_hash_idx');

VACUUM (FULL) supplier;

-- Make sure we can get multiple sizes in a single query
SELECT citus_table_size('customer_copy_hash'),
       citus_table_size('customer_copy_hash'),
       citus_table_size('supplier');

-- Tests on reference table
-- table
SELECT citus_table_size('supplier');
SELECT citus_relation_size('supplier');
SELECT citus_total_relation_size('supplier');

CREATE INDEX supplier_idx on supplier(s_suppkey);

-- table
SELECT citus_table_size('supplier');
SELECT citus_relation_size('supplier');
SELECT citus_total_relation_size('supplier');
-- index
SELECT citus_table_size('supplier_idx');
SELECT citus_relation_size('supplier_idx');
SELECT citus_total_relation_size('supplier_idx');

-- Test on partitioned table
CREATE TABLE split_me (dist_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
CREATE INDEX ON split_me(dist_col);

-- create 2 partitions
CREATE TABLE m PARTITION OF split_me FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE e PARTITION OF split_me FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

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

-- And we should make sure that following always returns true:
SELECT citus_relation_size('split_me_dist_col_idx')
    < citus_relation_size('split_me_dist_col_idx')
    + citus_relation_size('m_dist_col_idx')
    + citus_relation_size('e_dist_col_idx');

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

DROP INDEX customer_copy_hash_idx;
DROP INDEX supplier_idx;
