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
CREATE TABLE non_distributed_table (x int);
SELECT citus_table_size('non_distributed_table');
SELECT citus_relation_size('non_distributed_table');
SELECT citus_total_relation_size('non_distributed_table');
DROP TABLE non_distributed_table;

-- Tests on distributed table with replication factor > 1
VACUUM (FULL) lineitem_hash_part;

SELECT citus_table_size('lineitem_hash_part');
SELECT citus_relation_size('lineitem_hash_part');
SELECT citus_total_relation_size('lineitem_hash_part');

VACUUM (FULL) customer_copy_hash;

-- Tests on distributed tables with streaming replication.
SELECT citus_table_size('customer_copy_hash');
SELECT citus_relation_size('customer_copy_hash');
SELECT citus_total_relation_size('customer_copy_hash');

-- Make sure we can get multiple sizes in a single query
SELECT citus_table_size('customer_copy_hash'),
       citus_table_size('customer_copy_hash'),
       citus_table_size('supplier');

CREATE INDEX index_1 on customer_copy_hash(c_custkey);
VACUUM (FULL) customer_copy_hash;

-- Tests on distributed table with index.
SELECT citus_table_size('customer_copy_hash');
SELECT citus_relation_size('customer_copy_hash');
SELECT citus_total_relation_size('customer_copy_hash');

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
