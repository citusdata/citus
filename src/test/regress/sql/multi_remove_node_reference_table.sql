--
-- MULTI_REMOVE_NODE_REFERENCE_TABLE
--
-- Tests that check the metadata after master_remove_node.


SET citus.next_shard_id TO 1380000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1380000;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 1380000;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 1380000;

-- create copy of pg_dist_shard_placement to reload after the test
CREATE TABLE tmp_shard_placement AS SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
DELETE FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;

-- make worker 1 receive metadata changes
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- remove non-existing node
SELECT master_remove_node('localhost', 55555);


-- remove a node with no reference tables

-- verify node exist before removal
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT master_remove_node('localhost', :worker_2_port);

-- verify node is removed
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

-- re-add the node for next tests
SELECT groupid AS worker_2_group FROM master_add_node('localhost', :worker_2_port) \gset
-- add a secondary to check we don't attempt to replicate the table to it
SELECT isactive FROM master_add_node('localhost', 9000, groupid=>:worker_2_group, noderole=>'secondary');

-- remove a node with reference table
CREATE TABLE remove_node_reference_table(column1 int);
SELECT create_reference_table('remove_node_reference_table');

-- make sure when we add a secondary we don't attempt to add placements to it
SELECT isactive FROM master_add_node('localhost', 9001, groupid=>:worker_2_group, noderole=>'secondary');
SELECT count(*) FROM pg_dist_placement WHERE groupid = :worker_2_group;
-- make sure when we disable a secondary we don't remove any placements
SELECT master_disable_node('localhost', 9001);
SELECT isactive FROM pg_dist_node WHERE nodeport = 9001;
SELECT count(*) FROM pg_dist_placement WHERE groupid = :worker_2_group;
-- make sure when we activate a secondary we don't add any placements
SELECT 1 FROM master_activate_node('localhost', 9001);
SELECT count(*) FROM pg_dist_placement WHERE groupid = :worker_2_group;
-- make sure when we remove a secondary we don't remove any placements
SELECT master_remove_node('localhost', 9001);
SELECT count(*) FROM pg_dist_placement WHERE groupid = :worker_2_group;

-- status before master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);
     
\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

SELECT master_remove_node('localhost', :worker_2_port);

-- status after master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

-- remove same node twice
SELECT master_remove_node('localhost', :worker_2_port);

-- re-add the node for next tests
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- try to disable the node before removing it (this used to crash)
SELECT master_disable_node('localhost', :worker_2_port);
SELECT master_remove_node('localhost', :worker_2_port);

-- re-add the node for the next test
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- remove node in a transaction and ROLLBACK

-- status before master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);
     
\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

BEGIN;
SELECT master_remove_node('localhost', :worker_2_port);
ROLLBACK;

-- status after master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

-- remove node in a transaction and COMMIT

-- status before master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);
     
\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

BEGIN;
SELECT master_remove_node('localhost', :worker_2_port);
COMMIT;

-- status after master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);
     
\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

-- re-add the node for next tests
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- test inserting a value then removing a node in a transaction

-- status before master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port     
     
BEGIN;
INSERT INTO remove_node_reference_table VALUES(1);
SELECT master_remove_node('localhost', :worker_2_port);
COMMIT;

-- status after master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

--verify the data is inserted
SELECT * FROM remove_node_reference_table;

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
SELECT * FROM remove_node_reference_table;
    
\c - - - :master_port

-- re-add the node for next tests
SELECT 1 FROM master_add_node('localhost', :worker_2_port);


-- test executing DDL command then removing a node in a transaction

-- status before master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

BEGIN;
ALTER TABLE remove_node_reference_table ADD column2 int;
SELECT master_remove_node('localhost', :worker_2_port);
COMMIT;

-- status after master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);
     
\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

SET citus.next_shard_id TO 1380001;

-- verify table structure is changed
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.remove_node_reference_table'::regclass;

-- re-add the node for next tests
SELECT 1 FROM master_add_node('localhost', :worker_2_port);


-- test DROP table after removing a node in a transaction

-- status before master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

BEGIN;
SELECT master_remove_node('localhost', :worker_2_port);
DROP TABLE remove_node_reference_table;
COMMIT;

-- status after master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT * FROM pg_dist_colocation WHERE colocationid = 1380000;

-- re-add the node for next tests
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- re-create remove_node_reference_table
CREATE TABLE remove_node_reference_table(column1 int);
SELECT create_reference_table('remove_node_reference_table');

-- test removing a node while there is a reference table at another schema
CREATE SCHEMA remove_node_reference_table_schema;
CREATE TABLE remove_node_reference_table_schema.table1(column1 int);
SELECT create_reference_table('remove_node_reference_table_schema.table1');

-- status before master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port
ORDER BY
    shardid;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table_schema.table1'::regclass);
     
\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port
ORDER BY
	shardid;
\c - - - :master_port

SELECT master_remove_node('localhost', :worker_2_port);

-- status after master_remove_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table_schema.table1'::regclass);

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port     
     
-- re-add the node for next tests
SELECT 1 FROM master_add_node('localhost', :worker_2_port);


-- test with master_disable_node

-- status before master_disable_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port
ORDER BY
    shardid;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port
ORDER BY shardid ASC;
    
\c - - - :master_port     
     
SELECT master_disable_node('localhost', :worker_2_port);

-- status after master_disable_node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'remove_node_reference_table'::regclass);

\c - - - :worker_1_port

SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;
    
\c - - - :master_port

-- re-add the node for next tests
SELECT 1 FROM master_activate_node('localhost', :worker_2_port);


-- DROP tables to clean workspace
DROP TABLE remove_node_reference_table;
DROP TABLE remove_node_reference_table_schema.table1;
DROP SCHEMA remove_node_reference_table_schema CASCADE;

SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

-- reload pg_dist_shard_placement table
INSERT INTO pg_dist_shard_placement (SELECT * FROM tmp_shard_placement);
DROP TABLE tmp_shard_placement;
