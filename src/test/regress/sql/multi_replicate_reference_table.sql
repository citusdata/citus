--
-- MULTI_REPLICATE_REFERENCE_TABLE
--
-- Tests that check that reference tables are replicated when adding new nodes.

CREATE SCHEMA replicate_reference_table;
SET search_path TO replicate_reference_table;

SET citus.next_shard_id TO 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 1370000;

-- only query shards created in this test
CREATE VIEW pg_dist_shard_placement_view AS
SELECT * FROM pg_dist_shard_placement WHERE shardid BETWEEN 1370000 AND 1380000;

-- remove a node for testing purposes
CREATE TABLE tmp_shard_placement AS SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
DELETE FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT master_remove_node('localhost', :worker_2_port);


-- test adding new node with no reference tables

-- verify there is no node with nodeport = :worker_2_port before adding the node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- verify node is added
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

-- verify nothing is replicated to the new node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port;


-- test adding new node with a reference table which does not have any healthy placement
SELECT master_remove_node('localhost', :worker_2_port);

-- verify there is no node with nodeport = :worker_2_port before adding the node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

CREATE TABLE replicate_reference_table_unhealthy(column1 int);
SELECT create_reference_table('replicate_reference_table_unhealthy');
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1370000;

SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- verify node is not added
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

-- verify nothing is replicated to the new node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port;

DROP TABLE replicate_reference_table_unhealthy;

-- test replicating a reference table when a new node added
CREATE TABLE replicate_reference_table_valid(column1 int);
SELECT create_reference_table('replicate_reference_table_valid');

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);

SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);


-- test add same node twice

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);

SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);

DROP TABLE replicate_reference_table_valid;


-- test replicating a reference table when a new node added in TRANSACTION + ROLLBACK
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_rollback(column1 int);
SELECT create_reference_table('replicate_reference_table_rollback');

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_rollback'::regclass);

BEGIN;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
ROLLBACK;

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_rollback'::regclass);

DROP TABLE replicate_reference_table_rollback;

-- confirm that there is just 1 node
SELECT count(*) FROM pg_dist_node;
-- test whether we can create distributed objects on a single worker node
CREATE TABLE cp_test (a int, b text);
CREATE PROCEDURE ptest1(x text)
LANGUAGE SQL
AS $$
 INSERT INTO cp_test VALUES (1, x);
$$;

-- test replicating a reference table when a new node added in TRANSACTION + COMMIT
CREATE TABLE replicate_reference_table_commit(column1 int);
SELECT create_reference_table('replicate_reference_table_commit');

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_commit'::regclass);

BEGIN;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
COMMIT;

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_commit'::regclass);

DROP TABLE replicate_reference_table_commit;

-- exercise reference table replication in create_distributed_table_concurrently
SELECT citus_remove_node('localhost', :worker_2_port);
CREATE TABLE replicate_reference_table_cdtc(column1 int);
SELECT create_reference_table('replicate_reference_table_cdtc');
SELECT citus_add_node('localhost', :worker_2_port);

-- required for create_distributed_table_concurrently
SET citus.shard_replication_factor TO 1;

CREATE TABLE distributed_table_cdtc(column1 int primary key);
SELECT create_distributed_table_concurrently('distributed_table_cdtc', 'column1');

RESET citus.shard_replication_factor;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;
DROP TABLE replicate_reference_table_cdtc, distributed_table_cdtc;

-- test adding new node + upgrading another hash distributed table to reference table + creating new reference table in TRANSACTION
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_reference_one(column1 int);
SELECT create_reference_table('replicate_reference_table_reference_one');

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;

CREATE TABLE replicate_reference_table_reference_two(column1 int);

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_reference_one'::regclass);

SELECT colocationid AS reference_table_colocationid FROM pg_dist_colocation WHERE distributioncolumntype = 0 \gset

SELECT
    logicalrelid, partmethod, colocationid = :reference_table_colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid IN ('replicate_reference_table_reference_one', 'replicate_reference_table_reference_two')
ORDER BY logicalrelid;

SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
SELECT create_reference_table('replicate_reference_table_reference_two');
RESET client_min_messages;

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_reference_one'::regclass);

SELECT
    logicalrelid, partmethod, colocationid = :reference_table_colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid IN ('replicate_reference_table_reference_one', 'replicate_reference_table_reference_two')
ORDER BY
	logicalrelid;

DROP TABLE replicate_reference_table_reference_one;
DROP TABLE replicate_reference_table_reference_two;


-- test inserting a value then adding a new node in a transaction
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_insert(column1 int);
SELECT create_reference_table('replicate_reference_table_insert');

BEGIN;
INSERT INTO replicate_reference_table_insert VALUES(1);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
ROLLBACK;

DROP TABLE replicate_reference_table_insert;


-- test COPY then adding a new node in a transaction
CREATE TABLE replicate_reference_table_copy(column1 int);
SELECT create_reference_table('replicate_reference_table_copy');

SET citus.enable_local_execution = 'off';
BEGIN;
COPY replicate_reference_table_copy FROM STDIN;
1
2
3
4
5
\.
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
ROLLBACK;

RESET citus.enable_local_execution;

DROP TABLE replicate_reference_table_copy;


-- test executing DDL command then adding a new node in a transaction
CREATE TABLE replicate_reference_table_ddl(column1 int);
SELECT create_reference_table('replicate_reference_table_ddl');

BEGIN;
ALTER TABLE replicate_reference_table_ddl ADD column2 int;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
ROLLBACK;

DROP TABLE replicate_reference_table_ddl;


-- test DROP table after adding new node in a transaction
CREATE TABLE replicate_reference_table_drop(column1 int);
SELECT create_reference_table('replicate_reference_table_drop');

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_drop'::regclass);

BEGIN;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);
DROP TABLE replicate_reference_table_drop;
COMMIT;

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT * FROM pg_dist_colocation WHERE colocationid = 1370009;

-- test adding a node while there is a reference table at another schema
SELECT master_remove_node('localhost', :worker_2_port);

CREATE SCHEMA replicate_reference_table_schema;
CREATE TABLE replicate_reference_table_schema.table1(column1 int);
SELECT create_reference_table('replicate_reference_table_schema.table1');

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_schema.table1'::regclass);

SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_schema.table1'::regclass);

DROP TABLE replicate_reference_table_schema.table1;
DROP SCHEMA replicate_reference_table_schema CASCADE;


-- test adding a node when there are foreign keys between reference tables
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE ref_table_1(id int primary key, v int);
CREATE TABLE ref_table_2(id int primary key, v int references ref_table_1(id));
CREATE TABLE ref_table_3(id int primary key, v int references ref_table_2(id));

SELECT create_reference_table('ref_table_1');
SELECT create_reference_table('ref_table_2');
SELECT create_reference_table('ref_table_3');

-- status before master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    nodeport = :worker_2_port
ORDER BY shardid, nodeport;

-- verify constraints have been created on the new node
SELECT run_command_on_workers('select count(*) from pg_constraint where contype=''f'' AND conname similar to ''ref_table%\d'';');

DROP TABLE ref_table_1, ref_table_2, ref_table_3;

-- do some tests with inactive node
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE initially_not_replicated_reference_table (key int);
SELECT create_reference_table('initially_not_replicated_reference_table');

SELECT 1 FROM master_add_inactive_node('localhost', :worker_2_port);

-- we should see only one shard placements (other than coordinator)
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    shardid IN (SELECT
                    shardid
                FROM
                    pg_dist_shard
                WHERE
                    logicalrelid = 'initially_not_replicated_reference_table'::regclass)
    AND nodeport != :master_port
ORDER BY 1,4,5;

-- we should see the two shard placements after activation
SELECT 1 FROM master_activate_node('localhost', :worker_2_port);

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement_view
WHERE
    shardid IN (SELECT
                    shardid
                FROM
                    pg_dist_shard
                WHERE
                    logicalrelid = 'initially_not_replicated_reference_table'::regclass)
    AND nodeport != :master_port
ORDER BY 1,4,5;

SELECT 1 FROM master_remove_node('localhost', :worker_2_port);

CREATE TABLE ref_table(id bigserial PRIMARY KEY, a int);
CREATE INDEX ON ref_table (a);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table(a) SELECT * FROM generate_series(1, 10);

-- verify direct call to citus_copy_shard_placement errors if target node is not yet added
SELECT citus_copy_shard_placement(
           (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='ref_table'::regclass::oid),
           'localhost', :worker_1_port,
           'localhost', :worker_2_port,
           transfer_mode := 'block_writes');

-- verify direct call to citus_copy_shard_placement errors if target node is secondary
SELECT citus_add_secondary_node('localhost', :worker_2_port, 'localhost', :worker_1_port);
SELECT citus_copy_shard_placement(
           (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='ref_table'::regclass::oid),
           'localhost', :worker_1_port,
           'localhost', :worker_2_port,
           transfer_mode := 'block_writes');
SELECT citus_remove_node('localhost', :worker_2_port);

-- verify direct call to citus_copy_shard_placement errors if target node is inactive
SELECT 1 FROM master_add_inactive_node('localhost', :worker_2_port);
SELECT citus_copy_shard_placement(
           (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='ref_table'::regclass::oid),
           'localhost', :worker_1_port,
           'localhost', :worker_2_port,
           transfer_mode := 'block_writes');

SELECT 1 FROM master_activate_node('localhost', :worker_2_port);

-- verify we cannot replicate reference tables in a transaction modifying pg_dist_node
BEGIN;
SELECT citus_set_coordinator_host('127.0.0.1');
SELECT replicate_reference_tables();
ROLLBACK;

-- verify we cannot replicate reference tables in a transaction which
-- modified reference tables
BEGIN;
DELETE FROM ref_table;
SELECT replicate_reference_tables();
ROLLBACK;

BEGIN;
ALTER TABLE ref_table ADD COLUMN b int;
SELECT replicate_reference_tables();
ROLLBACK;

BEGIN;
CREATE INDEX ref_idx ON ref_table(a);
SELECT replicate_reference_tables();
ROLLBACK;

--
-- read from reference table, then replicate, then write. verify
-- placements are consistent.
--
BEGIN;
SELECT count(*) FROM ref_table;
SELECT replicate_reference_tables('block_writes');
INSERT INTO ref_table(a) VALUES (11);
SELECT count(*), sum(a) FROM ref_table;
UPDATE ref_table SET a = a + 1;
SELECT sum(a) FROM ref_table;
COMMIT;

SELECT min(result) = max(result) AS consistent FROM run_command_on_placements('ref_table', 'SELECT sum(a) FROM %s');

SET client_min_messages TO WARNING;

SELECT shardid AS ref_table_shard FROM pg_dist_shard WHERE logicalrelid = 'ref_table'::regclass \gset

SELECT count(*) AS ref_table_placements FROM pg_dist_shard_placement WHERE shardid = :ref_table_shard \gset

-- remove reference table replica from worker 2
SELECT 1 FROM master_remove_node('localhost', :worker_2_port);

SELECT count(*) - :ref_table_placements FROM pg_dist_shard_placement WHERE shardid = :ref_table_shard;

-- verify that master_create_empty_shard replicates reference table shards
CREATE TABLE range_table(a int);
SELECT create_distributed_table('range_table', 'a', 'range');

SELECT 1 FROM master_add_node('localhost', :worker_2_port);

SELECT count(*) - :ref_table_placements FROM pg_dist_shard_placement WHERE shardid = :ref_table_shard;
SELECT 1 FROM master_create_empty_shard('range_table');
SELECT count(*) - :ref_table_placements FROM pg_dist_shard_placement WHERE shardid = :ref_table_shard;

DROP TABLE range_table;
SELECT 1 FROM master_remove_node('localhost', :worker_2_port);

-- test that metadata is synced when citus_copy_shard_placement replicates
-- reference table shards
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

SET citus.shard_replication_factor TO 1;

SELECT citus_copy_shard_placement(
           :ref_table_shard,
           'localhost', :worker_1_port,
           'localhost', :worker_2_port,
           transfer_mode := 'block_writes');

SELECT result::int - :ref_table_placements
FROM run_command_on_workers('SELECT count(*) FROM pg_dist_placement a, pg_dist_shard b, pg_class c WHERE a.shardid=b.shardid AND b.logicalrelid=c.oid AND c.relname=''ref_table''')
WHERE nodeport=:worker_1_port;

-- test that we can use non-blocking rebalance
SELECT 1 FROM master_remove_node('localhost', :worker_2_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

SELECT rebalance_table_shards(shard_transfer_mode := 'force_logical');

-- test that metadata is synced on replicate_reference_tables
SELECT 1 FROM master_remove_node('localhost', :worker_2_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- detects correctly that referecence table doesn't have replica identity
SELECT replicate_reference_tables();
-- allows force_logical
SELECT replicate_reference_tables('force_logical');

SELECT result::int - :ref_table_placements
FROM run_command_on_workers('SELECT count(*) FROM pg_dist_placement a, pg_dist_shard b, pg_class c WHERE a.shardid=b.shardid AND b.logicalrelid=c.oid AND c.relname=''ref_table''')
WHERE nodeport=:worker_1_port;

-- join the reference table with a distributed table from worker 1
-- to verify that metadata for worker 2 placements have been synced
-- to worker 1.

CREATE TABLE dist_table(a int, b int);
SELECT create_distributed_table('dist_table', 'a');
INSERT INTO dist_table SELECT i, i * i FROM generate_series(1, 20) i;

TRUNCATE ref_table;
INSERT INTO ref_table(a) SELECT 2 * i FROM generate_series(1, 5) i;

\c - - - :worker_1_port

SET search_path TO replicate_reference_table;

SELECT array_agg(dist_table.b ORDER BY ref_table.a)
FROM ref_table, dist_table
WHERE ref_table.a = dist_table.a;

\c - - - :master_port

SET search_path TO replicate_reference_table;

--
-- The following case used to get stuck on create_distributed_table() instead
-- of detecting the distributed deadlock.
--
SET citus.shard_replication_factor TO 1;

SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE ref (a int primary key, b int);
SELECT create_reference_table('ref');
CREATE TABLE test (x int, y int references ref(a));
select 1 FROM master_add_node('localhost', :worker_2_port);
BEGIN;
DROP TABLE test;
CREATE TABLE test (x int, y int references ref(a));
SELECT create_distributed_table('test','x');
END;

-- verify the split fails if we still need to replicate reference tables
SELECT citus_remove_node('localhost', :worker_2_port);
SELECT create_distributed_table('test','x');
SELECT citus_add_node('localhost', :worker_2_port);
SELECT
  citus_split_shard_by_split_points(shardid,
                                    ARRAY[(shardminvalue::int + (shardmaxvalue::int - shardminvalue::int)/2)::text],
                                    ARRAY[nodeid, nodeid],
                                    'force_logical')
FROM
  pg_dist_shard, pg_dist_node
WHERE
  logicalrelid = 'replicate_reference_table.test'::regclass AND nodename = 'localhost' AND nodeport = :worker_2_port
ORDER BY shardid LIMIT 1;

-- test adding an invalid node while we have reference tables to replicate
-- set client message level to ERROR and verbosity to terse to supporess
-- OS-dependent host name resolution warnings
\set VERBOSITY terse
SET client_min_messages to ERROR;
DO $$
DECLARE
        errors_received INTEGER;
BEGIN
errors_received := 0;
        BEGIN
		SELECT master_add_node('invalid-node-name', 9999);
        EXCEPTION WHEN OTHERS THEN
                IF SQLERRM LIKE 'connection to the remote node%%' THEN
                        errors_received := errors_received + 1;
                END IF;
        END;
RAISE '(%/1) failed to add node', errors_received;
END;
$$;

-- drop unnecassary tables
DROP TABLE initially_not_replicated_reference_table;

-- reload pg_dist_shard_placement table
INSERT INTO pg_dist_shard_placement (SELECT * FROM tmp_shard_placement);
DROP TABLE tmp_shard_placement;

DROP SCHEMA replicate_reference_table CASCADE;
