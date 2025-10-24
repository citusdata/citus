--
-- Test for adding a worker node from a backup
--

-- setup cluster
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

SELECT * from pg_dist_node;


-- create a distributed table and load data
CREATE TABLE backup_test(id int, value text);
SELECT create_distributed_table('backup_test', 'id', 'hash');
INSERT INTO backup_test SELECT g, 'test' || g FROM generate_series(1, 10) g;

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
     email TEXT UNIQUE NOT NULL );

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    order_date DATE NOT NULL DEFAULT CURRENT_DATE);

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

SELECT count(*) from customers;
SELECT count(*) from orders;
SELECT count(*) from order_items;

-- verify initial shard placement
SELECT nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;
-- wait for the new node to be ready
SELECT pg_sleep(5);

-- register the new node as a clone
-- the function returns the new node id
SELECT citus_add_clone_node('localhost', :follower_worker_1_port, 'localhost', :worker_1_port) AS clone_node_id \gset

SELECT * from pg_dist_node ORDER by nodeid;

SELECT :clone_node_id ;

SELECT shardid, nodename, 'PRIMARY' as node_type FROM pg_dist_shard_placement WHERE nodeport = :worker_1_port ORDER BY shardid;
SELECT shardid, nodename, 'CLONE' as node_type FROM pg_dist_shard_placement WHERE nodeport = :follower_worker_1_port ORDER BY shardid;

SELECT * from get_snapshot_based_node_split_plan('localhost', :worker_1_port, 'localhost', :follower_worker_1_port);

-- promote the clone and rebalance the shards
SET client_min_messages to 'LOG';
SELECT citus_promote_clone_and_rebalance(:clone_node_id);
SET client_min_messages to DEFAULT;

SELECT shardid, nodename, 'PRIMARY' as node_type FROM pg_dist_shard_placement WHERE nodeport = :worker_1_port ORDER BY shardid;
SELECT shardid, nodename, 'CLONE' as node_type FROM pg_dist_shard_placement WHERE nodeport = :follower_worker_1_port ORDER BY shardid;

\c - - - :worker_1_port
SELECT 'WORKER' as node_type,* from pg_dist_node;
SELECT 'WORKER' as node_type, nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;
SELECT * from citus_tables;
SELECT id, value FROM backup_test ORDER BY id;
SELECT count(*) from customers;
SELECT count(*) from orders;
SELECT count(*) from order_items;


\c - - - :follower_worker_1_port
SELECT 'CLONE' as node_type ,* from pg_dist_node;
SELECT 'CLONE' as node_type, nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;
SELECT * from citus_tables;
SELECT id, value FROM backup_test ORDER BY id;
SELECT count(*) from customers;
SELECT count(*) from orders;
SELECT count(*) from order_items;


\c - - - :master_port
SELECT 'MASTER' as node_type, * from pg_dist_node;
SELECT 'MASTER' as node_type, nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;
SELECT * from citus_tables;
SELECT id, value FROM backup_test ORDER BY id;
SELECT count(*) from customers;
SELECT count(*) from orders;
SELECT count(*) from order_items;

-- verify data
SELECT count(*) FROM backup_test;
SELECT id, value FROM backup_test ORDER BY id;

-- cleanup
DROP TABLE backup_test;

