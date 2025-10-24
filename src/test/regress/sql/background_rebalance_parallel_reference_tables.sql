--
-- BACKGROUND_REBALANCE_PARALLEL_REFERENCE_TABLES
--
-- Test to check if the background tasks scheduled for moving reference tables
-- shards in parallel by the background rebalancer have the correct dependencies
--
CREATE SCHEMA background_rebalance_parallel;
SET search_path TO background_rebalance_parallel;
SET citus.next_shard_id TO 85674000;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO ERROR;

ALTER SEQUENCE pg_dist_background_job_job_id_seq RESTART 17777;
ALTER SEQUENCE pg_dist_background_task_task_id_seq RESTART 1000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 50050;

SELECT nextval('pg_catalog.pg_dist_groupid_seq') AS last_group_id_cls \gset
SELECT nextval('pg_catalog.pg_dist_node_nodeid_seq') AS last_node_id_cls \gset
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 50;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 50;

SELECT 1 FROM master_remove_node('localhost', :worker_1_port);
SELECT 1 FROM master_remove_node('localhost', :worker_2_port);

SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
ALTER SYSTEM SET citus.max_background_task_executors_per_node = 5;
SELECT pg_reload_conf();

-- Colocation group 1: create two tables table1_colg1, table2_colg1 and in a colocation group
CREATE TABLE table1_colg1 (a int PRIMARY KEY);
SELECT create_distributed_table('table1_colg1', 'a', shard_count => 4, colocate_with => 'none');

CREATE TABLE table2_colg1 (b int PRIMARY KEY);

SELECT create_distributed_table('table2_colg1', 'b', colocate_with => 'table1_colg1');

-- Colocation group 2: create two tables table1_colg2, table2_colg2 and in a colocation group
CREATE TABLE table1_colg2 (a int PRIMARY KEY);

SELECT create_distributed_table('table1_colg2', 'a', shard_count => 4, colocate_with => 'none');

CREATE TABLE  table2_colg2 (b int primary key);

SELECT create_distributed_table('table2_colg2', 'b', colocate_with => 'table1_colg2');

-- Colocation group 3: create two tables table1_colg3, table2_colg3 and in a colocation group
CREATE TABLE table1_colg3 (a int PRIMARY KEY);

SELECT create_distributed_table('table1_colg3', 'a', shard_count => 4, colocate_with => 'none');

CREATE TABLE  table2_colg3 (b int primary key);

SELECT create_distributed_table('table2_colg3', 'b', colocate_with => 'table1_colg3');

-- Create reference tables with primary-foreign key relationships

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    order_date DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

SELECT create_reference_table('customers');
SELECT create_reference_table('orders');
SELECT create_reference_table('order_items');

-- INSERT SOME DATA
-- Insert 10 customers
INSERT INTO customers (name, email)
SELECT
    'Customer ' || i,
    'customer' || i || '@example.com'
FROM generate_series(1, 10) AS i;

-- Insert 30 orders: each customer gets 3 orders
INSERT INTO orders (customer_id, order_date)
SELECT
    (i % 10) + 1,  -- customer_id between 1 and 10
    CURRENT_DATE - (i % 7)
FROM generate_series(1, 30) AS i;

-- Insert 90 order_items: each order has 3 items
INSERT INTO order_items (order_id, product_name, quantity, price)
SELECT
    (i % 30) + 1,  -- order_id between 1 and 30
    'Product ' || (i % 5 + 1),
    (i % 10) + 1,
    round((random() * 100 + 10)::numeric, 2)
FROM generate_series(1, 90) AS i;



SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    COUNT(oi.id) AS total_order_items
FROM customers c
JOIN orders o
    ON c.id = o.customer_id
JOIN order_items oi
    ON o.id = oi.order_id
GROUP BY c.id, c.name, c.email
ORDER BY c.id;

-- Add two new nodes so that we can rebalance
SELECT 1 FROM citus_add_node('localhost', :worker_3_port);
SELECT 1 FROM citus_add_node('localhost', :worker_4_port);

SELECT * FROM get_rebalance_table_shards_plan() ORDER BY shardid;

SET client_min_messages TO DEBUG1;

SELECT citus_rebalance_start AS job_id from citus_rebalance_start(
    shard_transfer_mode := 'force_logical',
    parallel_transfer_colocated_shards := true,
    parallel_transfer_reference_tables := true) \gset

SET client_min_messages TO ERROR;

SELECT citus_rebalance_wait();

SELECT citus_rebalance_wait();

-- see the dependencies of the tasks scheduled by the background rebalancer
SELECT * from pg_dist_background_task_depend ORDER BY job_id, task_id, depends_on;

-- Temporary hack to eliminate SET application name from command until we get the
-- background job enhancement done.
SELECT D.task_id,
       (SELECT
        CASE
            WHEN T.command LIKE '%citus_internal.citus_internal_copy_single_shard_placement%' THEN
                SUBSTRING(T.command FROM 'citus_internal\.citus_internal_copy_single_shard_placement\((\d+)')
            WHEN T.command LIKE '%pg_catalog.citus_move_shard_placement%' THEN
                SUBSTRING(T.command FROM 'pg_catalog\.citus_move_shard_placement\((\d+)')
            ELSE
                T.command
        END
       FROM pg_dist_background_task T WHERE T.task_id = D.task_id),
       D.depends_on,
       (SELECT
       CASE
            WHEN T.command LIKE '%citus_internal.citus_internal_copy_single_shard_placement%' THEN
                SUBSTRING(T.command FROM 'citus_internal\.citus_internal_copy_single_shard_placement\((\d+)')
            WHEN T.command LIKE '%pg_catalog.citus_move_shard_placement%' THEN
                SUBSTRING(T.command FROM 'pg_catalog\.citus_move_shard_placement\((\d+)')
        ELSE
            T.command
       END
       FROM pg_dist_background_task T WHERE T.task_id = D.depends_on)
FROM pg_dist_background_task_depend D  WHERE job_id in (:job_id) ORDER BY D.task_id, D.depends_on ASC;


TRUNCATE pg_dist_background_job CASCADE;
TRUNCATE pg_dist_background_task CASCADE;
TRUNCATE pg_dist_background_task_depend;

-- Drain worker_3 so that we can move only one colocation group to worker_3
-- to create an unbalance that would cause parallel rebalancing.
SELECT 1 FROM citus_drain_node('localhost',:worker_3_port);
SELECT citus_set_node_property('localhost', :worker_3_port, 'shouldhaveshards', true);

CALL citus_cleanup_orphaned_resources();

-- Move all the shards of Colocation group 3 to worker_3.
SELECT
master_move_shard_placement(shardid, 'localhost', nodeport, 'localhost', :worker_3_port, 'block_writes')
FROM
        pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
        logicalrelid = 'table1_colg3'::regclass AND nodeport <> :worker_3_port
ORDER BY
      shardid;

CALL citus_cleanup_orphaned_resources();

SELECT 1 FROM citus_activate_node('localhost', :worker_4_port);
SELECT citus_set_node_property('localhost', :worker_4_port, 'shouldhaveshards', true);

SELECT 1 FROM citus_add_node('localhost', :worker_5_port);
SELECT 1 FROM citus_add_node('localhost', :worker_6_port);

SELECT * FROM get_rebalance_table_shards_plan() ORDER BY shardid;

SET client_min_messages TO DEBUG1;

SELECT citus_rebalance_start AS job_id from citus_rebalance_start(
    shard_transfer_mode := 'block_writes',
    parallel_transfer_colocated_shards := true,
    parallel_transfer_reference_tables := true) \gset


SET client_min_messages TO ERROR;

SELECT citus_rebalance_wait();

-- see the dependencies of the tasks scheduled by the background rebalancer
SELECT * from pg_dist_background_task_depend ORDER BY job_id, task_id, depends_on;
-- Temporary hack to eliminate SET application name from command until we get the
-- background job enhancement done.
SELECT D.task_id,
       (SELECT
        CASE
            WHEN T.command LIKE '%citus_internal.citus_internal_copy_single_shard_placement%' THEN
                SUBSTRING(T.command FROM 'citus_internal\.citus_internal_copy_single_shard_placement\((\d+)')
            WHEN T.command LIKE '%pg_catalog.citus_move_shard_placement%' THEN
                SUBSTRING(T.command FROM 'pg_catalog\.citus_move_shard_placement\((\d+)')
            ELSE
                T.command
        END
       FROM pg_dist_background_task T WHERE T.task_id = D.task_id),
       D.depends_on,
       (SELECT
       CASE
            WHEN T.command LIKE '%citus_internal.citus_internal_copy_single_shard_placement%' THEN
                SUBSTRING(T.command FROM 'citus_internal\.citus_internal_copy_single_shard_placement\((\d+)')
            WHEN T.command LIKE '%pg_catalog.citus_move_shard_placement%' THEN
                SUBSTRING(T.command FROM 'pg_catalog\.citus_move_shard_placement\((\d+)')
        ELSE
            T.command
       END
       FROM pg_dist_background_task T WHERE T.task_id = D.depends_on)
FROM pg_dist_background_task_depend D  WHERE job_id in (:job_id) ORDER BY D.task_id, D.depends_on ASC;

SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    COUNT(oi.id) AS total_order_items
FROM customers c
JOIN orders o
    ON c.id = o.customer_id
JOIN order_items oi
    ON o.id = oi.order_id
GROUP BY c.id, c.name, c.email
ORDER BY c.id;

DROP SCHEMA background_rebalance_parallel CASCADE;
TRUNCATE pg_dist_background_job CASCADE;
TRUNCATE pg_dist_background_task CASCADE;
TRUNCATE pg_dist_background_task_depend;
SELECT public.wait_for_resource_cleanup();
select citus_remove_node('localhost', :worker_3_port);
select citus_remove_node('localhost', :worker_4_port);
select citus_remove_node('localhost', :worker_5_port);
select citus_remove_node('localhost', :worker_6_port);

ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id_cls;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id_cls;
