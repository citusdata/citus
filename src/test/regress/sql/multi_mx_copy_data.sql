--
-- MULTI_MX_COPY_DATA
--

-- We truncate them to make this test runnable multiple times.
-- Note that we cannot do that at the end of the test because
-- we need to keep the data for the other tests.
TRUNCATE lineitem_mx, orders_mx;

\set nation_data_file :abs_srcdir '/data/nation.data'
\set client_side_copy_command '\\copy nation_hash FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command

SET search_path TO citus_mx_test_schema;
\set client_side_copy_command '\\copy nation_hash FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy citus_mx_test_schema_join_1.nation_hash FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy citus_mx_test_schema_join_1.nation_hash_2 FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy citus_mx_test_schema_join_2.nation_hash FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command

SET citus.shard_replication_factor TO 2;
CREATE TABLE citus_mx_test_schema.nation_hash_replicated AS SELECT * FROM citus_mx_test_schema.nation_hash;
SELECT create_distributed_table('citus_mx_test_schema.nation_hash_replicated', 'n_nationkey');
\set client_side_copy_command '\\copy nation_hash_replicated FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command

-- now try loading data from worker node
\c - - - :worker_1_port
SET search_path TO public;

\set lineitem_1_data_file :abs_srcdir '/data/lineitem.1.data'
\set lineitem_2_data_file :abs_srcdir '/data/lineitem.2.data'
\set client_side_copy_command '\\copy lineitem_mx FROM ' :'lineitem_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy lineitem_mx FROM ' :'lineitem_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command

\set nation_data_file :abs_srcdir '/data/nation.data'
\set client_side_copy_command '\\copy citus_mx_test_schema.nation_hash_replicated FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command

\c - - - :worker_2_port
-- and use second worker as well
\set orders_1_data_file :abs_srcdir '/data/orders.1.data'
\set orders_2_data_file :abs_srcdir '/data/orders.2.data'
\set nation_data_file :abs_srcdir '/data/nation.data'
\set client_side_copy_command '\\copy orders_mx FROM ' :'orders_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy orders_mx FROM ' :'orders_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy citus_mx_test_schema.nation_hash_replicated FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command

-- get ready for the next test
TRUNCATE orders_mx;

\c - - - :worker_2_port
SET citus.log_local_commands TO ON;
-- simulate the case where there is no connection slots available
ALTER SYSTEM SET citus.local_shared_pool_size TO -1;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
show citus.local_shared_pool_size;

\set orders_1_data_file :abs_srcdir '/data/orders.1.data'
\set orders_2_data_file :abs_srcdir '/data/orders.2.data'
\set client_side_copy_command '\\copy orders_mx FROM ' :'orders_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy orders_mx FROM ' :'orders_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command

\set nation_data_file :abs_srcdir '/data/nation.data'
\set client_side_copy_command '\\copy citus_mx_test_schema.nation_hash_replicated FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command

-- set it back
ALTER SYSTEM RESET citus.local_shared_pool_size;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
show citus.local_shared_pool_size;

-- These copies were intended to test copying data to single sharded table from
-- worker nodes, yet in order to remove broadcast logic related codes we change
-- the table to reference table and copy data from master. Should be updated
-- when worker nodes gain capability to run dml commands on reference tables.
\c - - - :master_port
SET search_path TO public;

\set customer_1_data_file :abs_srcdir '/data/customer.1.data'
\set nation_data_file :abs_srcdir '/data/nation.data'
\set part_data_file :abs_srcdir '/data/part.data'
\set supplier_data_file :abs_srcdir '/data/supplier.data'
\set client_side_copy_command '\\copy customer_mx FROM ' :'customer_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy nation_mx FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy part_mx FROM ' :'part_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy supplier_mx FROM ' :'supplier_data_file' ' with delimiter '''|''';'
:client_side_copy_command

DROP TABLE citus_mx_test_schema.nation_hash_replicated;
