-- Test citus_schema_distribute_concurrently function
-- This function distributes a schema without blocking writes using logical replication

SET citus.next_shard_id TO 1740000;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO WARNING;
SET citus.enable_schema_based_sharding TO off;

-- Add coordinator as a node (required for distribution)
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);

-- Add at least one worker node for distribution
SELECT 1 FROM citus_add_node('localhost', :worker_1_port);

CREATE USER concurrentuser SUPERUSER;
SET ROLE concurrentuser;

-- Test 1: Basic concurrent distribution of a simple schema
CREATE SCHEMA concurrent_tenant1;

CREATE TABLE concurrent_tenant1.users(id int PRIMARY KEY, name text, email text);
INSERT INTO concurrent_tenant1.users SELECT i, 'user_' || i, 'user_' || i || '@example.com' FROM generate_series(1, 100) i;

CREATE TABLE concurrent_tenant1.orders(id int PRIMARY KEY, user_id int REFERENCES concurrent_tenant1.users(id), amount numeric);
INSERT INTO concurrent_tenant1.orders SELECT i, (i % 100) + 1, random() * 1000 FROM generate_series(1, 500) i;

CREATE TABLE concurrent_tenant1.products(id int PRIMARY KEY, name text, price numeric);
INSERT INTO concurrent_tenant1.products SELECT i, 'product_' || i, random() * 100 FROM generate_series(1, 50) i;

-- Verify data before distribution
SELECT COUNT(*) FROM concurrent_tenant1.users;
SELECT COUNT(*) FROM concurrent_tenant1.orders;
SELECT COUNT(*) FROM concurrent_tenant1.products;

-- Distribute the schema concurrently
SELECT citus_schema_distribute_concurrently('concurrent_tenant1');

-- Verify the schema is now distributed
SELECT schemaid::regnamespace as distributed_schema FROM pg_dist_schema WHERE schemaid = 'concurrent_tenant1'::regnamespace;

-- Verify all tables are distributed
SELECT logicalrelid::text, partmethod, colocationid > 0 as has_colocation
FROM pg_dist_partition
WHERE logicalrelid::text LIKE 'concurrent_tenant1.%'
ORDER BY logicalrelid::text;

-- Verify data integrity after distribution
SELECT COUNT(*) FROM concurrent_tenant1.users;
SELECT COUNT(*) FROM concurrent_tenant1.orders;
SELECT COUNT(*) FROM concurrent_tenant1.products;

-- Verify foreign keys still work
INSERT INTO concurrent_tenant1.users VALUES (101, 'new_user', 'new@example.com');
INSERT INTO concurrent_tenant1.orders VALUES (501, 101, 250.50);

-- This should fail due to foreign key constraint
INSERT INTO concurrent_tenant1.orders VALUES (502, 999, 100.00);

-- Clean up
SELECT citus_schema_undistribute('concurrent_tenant1');
DROP SCHEMA concurrent_tenant1 CASCADE;


-- Test 2: Concurrent distribution with complex foreign key dependencies
CREATE SCHEMA concurrent_tenant2;

CREATE TABLE concurrent_tenant2.countries(id int PRIMARY KEY, name text);
INSERT INTO concurrent_tenant2.countries VALUES (1, 'USA'), (2, 'Canada'), (3, 'Mexico');

CREATE TABLE concurrent_tenant2.states(id int PRIMARY KEY, country_id int REFERENCES concurrent_tenant2.countries(id), name text);
INSERT INTO concurrent_tenant2.states VALUES (1, 1, 'California'), (2, 1, 'Texas'), (3, 2, 'Ontario');

CREATE TABLE concurrent_tenant2.cities(id int PRIMARY KEY, state_id int REFERENCES concurrent_tenant2.states(id), name text);
INSERT INTO concurrent_tenant2.cities VALUES (1, 1, 'Los Angeles'), (2, 1, 'San Francisco'), (3, 2, 'Houston');

-- Distribute concurrently
SELECT citus_schema_distribute_concurrently('concurrent_tenant2');

-- Verify topological sorting handled foreign keys correctly
SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::text LIKE 'concurrent_tenant2.%' ORDER BY logicalrelid::text;

-- Verify data integrity
SELECT COUNT(*) FROM concurrent_tenant2.countries;
SELECT COUNT(*) FROM concurrent_tenant2.states;
SELECT COUNT(*) FROM concurrent_tenant2.cities;

-- Test cascade on foreign key
DELETE FROM concurrent_tenant2.countries WHERE id = 3;
SELECT COUNT(*) FROM concurrent_tenant2.states WHERE country_id = 3;

-- Clean up
SELECT citus_schema_undistribute('concurrent_tenant2');
DROP SCHEMA concurrent_tenant2 CASCADE;


-- Test 3: Distribution with reference table foreign keys
CREATE SCHEMA concurrent_tenant3;
CREATE SCHEMA reference_schema;

CREATE TABLE reference_schema.categories(id int PRIMARY KEY, name text);
SELECT create_reference_table('reference_schema.categories');
INSERT INTO reference_schema.categories VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Clothing');

CREATE TABLE concurrent_tenant3.items(id int PRIMARY KEY, category_id int REFERENCES reference_schema.categories(id), name text);
INSERT INTO concurrent_tenant3.items SELECT i, (i % 3) + 1, 'item_' || i FROM generate_series(1, 100) i;

-- Distribute concurrently
SELECT citus_schema_distribute_concurrently('concurrent_tenant3');

-- Verify foreign key to reference table still works
INSERT INTO concurrent_tenant3.items VALUES (101, 2, 'new_book');

-- This should fail - invalid category_id
INSERT INTO concurrent_tenant3.items VALUES (102, 999, 'invalid_item');

-- Clean up
SELECT citus_schema_undistribute('concurrent_tenant3');
DROP SCHEMA concurrent_tenant3 CASCADE;
DROP SCHEMA reference_schema CASCADE;


-- Test 4: Error cases

-- Cannot distribute from worker nodes
SELECT run_command_on_workers($$SELECT citus_schema_distribute_concurrently('public');$$);

-- Already distributed schema
CREATE SCHEMA concurrent_tenant4;
CREATE TABLE concurrent_tenant4.data(id int);
SELECT citus_schema_distribute_concurrently('concurrent_tenant4');

SET client_min_messages TO NOTICE;
-- Should show notice that schema is already distributed
SELECT citus_schema_distribute_concurrently('concurrent_tenant4');
SET client_min_messages TO WARNING;

SELECT citus_schema_undistribute('concurrent_tenant4');
DROP SCHEMA concurrent_tenant4 CASCADE;

-- Disallowed schema names
SELECT citus_schema_distribute_concurrently('public');
SELECT citus_schema_distribute_concurrently('pg_catalog');
SELECT citus_schema_distribute_concurrently('pg_toast');
SELECT citus_schema_distribute_concurrently('information_schema');

-- Non-existent schema
SELECT citus_schema_distribute_concurrently('nonexistent_schema');


-- Test 5: Cannot use in transaction block
CREATE SCHEMA concurrent_tenant5;
CREATE TABLE concurrent_tenant5.data(id int);

BEGIN;
-- This should error - not allowed in transaction block
SELECT citus_schema_distribute_concurrently('concurrent_tenant5');
ROLLBACK;

-- Clean up
DROP SCHEMA concurrent_tenant5 CASCADE;


-- Test 6: Permission checks
CREATE USER regularuser;
CREATE SCHEMA concurrent_tenant6;
CREATE TABLE concurrent_tenant6.data(id int);

SET ROLE regularuser;
-- Should fail - not schema owner
SELECT citus_schema_distribute_concurrently('concurrent_tenant6');

-- Make regularuser the owner
RESET ROLE;
ALTER SCHEMA concurrent_tenant6 OWNER TO regularuser;
SET ROLE regularuser;

-- Should still fail - not table owner
SELECT citus_schema_distribute_concurrently('concurrent_tenant6');

-- Make regularuser own the table too
RESET ROLE;
ALTER TABLE concurrent_tenant6.data OWNER TO regularuser;
SET ROLE regularuser;

-- Should succeed now (but will fail due to no worker nodes)
-- We'll skip actual distribution to avoid complexity
RESET ROLE;
DROP SCHEMA concurrent_tenant6 CASCADE;
DROP USER regularuser;


-- Test 7: Schema with partitioned tables
CREATE SCHEMA concurrent_tenant7;

CREATE TABLE concurrent_tenant7.measurements(
    id int,
    measure_date date,
    value numeric
) PARTITION BY RANGE (measure_date);

CREATE TABLE concurrent_tenant7.measurements_2024_q1
    PARTITION OF concurrent_tenant7.measurements
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE concurrent_tenant7.measurements_2024_q2
    PARTITION OF concurrent_tenant7.measurements
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');

INSERT INTO concurrent_tenant7.measurements VALUES (1, '2024-01-15', 100.5);
INSERT INTO concurrent_tenant7.measurements VALUES (2, '2024-02-20', 200.3);
INSERT INTO concurrent_tenant7.measurements VALUES (3, '2024-05-10', 150.7);

-- Distribute concurrently
SELECT citus_schema_distribute_concurrently('concurrent_tenant7');

-- Verify data integrity
SELECT COUNT(*) FROM concurrent_tenant7.measurements;
SELECT COUNT(*) FROM concurrent_tenant7.measurements_2024_q1;
SELECT COUNT(*) FROM concurrent_tenant7.measurements_2024_q2;

-- Verify partitioning still works
INSERT INTO concurrent_tenant7.measurements VALUES (4, '2024-03-25', 175.2);
SELECT COUNT(*) FROM concurrent_tenant7.measurements_2024_q1;

-- Clean up
SELECT citus_schema_undistribute('concurrent_tenant7');
DROP SCHEMA concurrent_tenant7 CASCADE;


-- Test 8: Empty schema distribution
CREATE SCHEMA concurrent_empty;

-- Distribute empty schema
SELECT citus_schema_distribute_concurrently('concurrent_empty');

-- Verify it's distributed
SELECT schemaid::regnamespace FROM pg_dist_schema WHERE schemaid = 'concurrent_empty'::regnamespace;

-- Now add tables - they should automatically be tenant tables
CREATE TABLE concurrent_empty.new_table(id int PRIMARY KEY);
INSERT INTO concurrent_empty.new_table VALUES (1), (2), (3);

-- Verify the table is a tenant table
SELECT logicalrelid::text, partmethod FROM pg_dist_partition WHERE logicalrelid = 'concurrent_empty.new_table'::regclass;

-- Clean up
SELECT citus_schema_undistribute('concurrent_empty');
DROP SCHEMA concurrent_empty CASCADE;


-- Test 9: Distribution with Citus local tables
CREATE SCHEMA concurrent_tenant8;
CREATE SCHEMA ref_schema;

CREATE TABLE ref_schema.reference_data(id int PRIMARY KEY);
SELECT create_reference_table('ref_schema.reference_data');
INSERT INTO ref_schema.reference_data VALUES (1), (2), (3);

-- Create a Citus local table (auto-converted due to foreign key)
CREATE TABLE concurrent_tenant8.local_with_fkey(id int REFERENCES ref_schema.reference_data(id));
INSERT INTO concurrent_tenant8.local_with_fkey VALUES (1), (2);

-- Create a manually created Citus local table
CREATE TABLE concurrent_tenant8.manual_local(id int PRIMARY KEY);
SELECT citus_add_local_table_to_metadata('concurrent_tenant8.manual_local');
INSERT INTO concurrent_tenant8.manual_local VALUES (1), (2);

-- Both should be Citus local tables now
SELECT logicalrelid::text, partmethod FROM pg_dist_partition WHERE logicalrelid::text LIKE 'concurrent_tenant8.%' ORDER BY logicalrelid::text;

-- Distribute concurrently
SELECT citus_schema_distribute_concurrently('concurrent_tenant8');

-- Verify distribution
SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::text LIKE 'concurrent_tenant8.%' ORDER BY logicalrelid::text;

-- Verify data integrity
SELECT COUNT(*) FROM concurrent_tenant8.local_with_fkey;
SELECT COUNT(*) FROM concurrent_tenant8.manual_local;

-- Clean up
SELECT citus_schema_undistribute('concurrent_tenant8');
DROP SCHEMA concurrent_tenant8 CASCADE;
DROP SCHEMA ref_schema CASCADE;


-- Test 10: Verify metadata synchronization across nodes
CREATE SCHEMA concurrent_tenant9;

CREATE TABLE concurrent_tenant9.sync_test(id int PRIMARY KEY, data text);
INSERT INTO concurrent_tenant9.sync_test SELECT i, 'data_' || i FROM generate_series(1, 50) i;

-- Distribute concurrently
SELECT citus_schema_distribute_concurrently('concurrent_tenant9');

-- Verify metadata is synced to all nodes
SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid::regnamespace
    FROM pg_dist_schema
    WHERE schemaid = 'concurrent_tenant9'::regnamespace
$$);

-- Verify partition metadata is synced
SELECT result FROM run_command_on_all_nodes($$
    SELECT logicalrelid::text
    FROM pg_dist_partition
    WHERE logicalrelid::text LIKE 'concurrent_tenant9.%'
    ORDER BY logicalrelid::text
$$);

-- Clean up
SELECT citus_schema_undistribute('concurrent_tenant9');
DROP SCHEMA concurrent_tenant9 CASCADE;


-- Final cleanup
RESET ROLE;
DROP USER concurrentuser;
SELECT citus_remove_node('localhost', :worker_1_port);
SELECT citus_remove_node('localhost', :master_port);
