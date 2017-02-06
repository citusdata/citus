--
-- MULTI_REPLICATE_REFERENCE_TABLE
--
-- Tests that check the metadata returned by the master node.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART 1370000;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART 1370000;


-- remove a node for testing purposes
CREATE TABLE tmp_shard_placement AS SELECT * FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
DELETE FROM pg_dist_shard_placement WHERE nodeport = :worker_2_port;
SELECT master_remove_node('localhost', :worker_2_port);


-- test adding new node with no reference tables

-- verify there is no node with nodeport = :worker_2_port before adding the node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

SELECT master_add_node('localhost', :worker_2_port);

-- verify node is added
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

-- verify nothing is replicated to the new node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;


-- test adding new node with a reference table which does not have any healthy placement
SELECT master_remove_node('localhost', :worker_2_port);

-- verify there is no node with nodeport = :worker_2_port before adding the node
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

CREATE TABLE replicate_reference_table_unhealthy(column1 int);
SELECT create_reference_table('replicate_reference_table_unhealthy');
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1370000;

SELECT master_add_node('localhost', :worker_2_port);

-- verify node is not added
SELECT COUNT(*) FROM pg_dist_node WHERE nodeport = :worker_2_port;

-- verify nothing is replicated to the new node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
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
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);

SELECT master_add_node('localhost', :worker_2_port);

-- status after master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);


-- test add same node twice

-- status before master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_valid'::regclass);

SELECT master_add_node('localhost', :worker_2_port);

-- status after master_add_node
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
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_rollback'::regclass);

BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
ROLLBACK;

-- status after master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_rollback'::regclass);

DROP TABLE replicate_reference_table_rollback;


-- test replicating a reference table when a new node added in TRANSACTION + COMMIT
CREATE TABLE replicate_reference_table_commit(column1 int);
SELECT create_reference_table('replicate_reference_table_commit');

-- status before master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_commit'::regclass);

BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
COMMIT;

-- status after master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_commit'::regclass);

DROP TABLE replicate_reference_table_commit;


-- test adding new node + upgrading another hash distributed table to reference table + creating new reference table in TRANSACTION
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_reference_one(column1 int);
SELECT create_reference_table('replicate_reference_table_reference_one');

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
CREATE TABLE replicate_reference_table_hash(column1 int);
SELECT create_distributed_table('replicate_reference_table_hash', 'column1');

-- update replication model to statement-based replication since streaming replicated tables cannot be upgraded to reference tables
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='replicate_reference_table_hash'::regclass;

CREATE TABLE replicate_reference_table_reference_two(column1 int);

-- status before master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_reference_one'::regclass);

SELECT
    logicalrelid, partmethod, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid IN ('replicate_reference_table_reference_one', 'replicate_reference_table_hash', 'replicate_reference_table_reference_two')
ORDER BY logicalrelid;

BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
SELECT upgrade_to_reference_table('replicate_reference_table_hash');
SELECT create_reference_table('replicate_reference_table_reference_two');
COMMIT;

-- status after master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_reference_one'::regclass);

SELECT
    logicalrelid, partmethod, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid IN ('replicate_reference_table_reference_one', 'replicate_reference_table_hash', 'replicate_reference_table_reference_two')
ORDER BY 
	logicalrelid;

DROP TABLE replicate_reference_table_reference_one;
DROP TABLE replicate_reference_table_hash;
DROP TABLE replicate_reference_table_reference_two;


-- test inserting a value then adding a new node in a transaction
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE replicate_reference_table_insert(column1 int);
SELECT create_reference_table('replicate_reference_table_insert');

BEGIN;
INSERT INTO replicate_reference_table_insert VALUES(1);
SELECT master_add_node('localhost', :worker_2_port);
ROLLBACK;

DROP TABLE replicate_reference_table_insert;


-- test COPY then adding a new node in a transaction
CREATE TABLE replicate_reference_table_copy(column1 int);
SELECT create_reference_table('replicate_reference_table_copy');

BEGIN;
COPY replicate_reference_table_copy FROM STDIN;
1
2
3
4
5
\.
SELECT master_add_node('localhost', :worker_2_port);
ROLLBACK;

DROP TABLE replicate_reference_table_copy;


-- test executing DDL command then adding a new node in a transaction
CREATE TABLE replicate_reference_table_ddl(column1 int);
SELECT create_reference_table('replicate_reference_table_ddl');

BEGIN;
ALTER TABLE replicate_reference_table_ddl ADD column2 int;
SELECT master_add_node('localhost', :worker_2_port);
ROLLBACK;

DROP TABLE replicate_reference_table_ddl;


-- test DROP table after adding new node in a transaction
CREATE TABLE replicate_reference_table_drop(column1 int);
SELECT create_reference_table('replicate_reference_table_drop');

-- status before master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_drop'::regclass);

BEGIN;
SELECT master_add_node('localhost', :worker_2_port);
DROP TABLE replicate_reference_table_drop;
COMMIT;

-- status after master_add_node
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

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
    pg_dist_shard_placement
WHERE
    nodeport = :worker_2_port;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'replicate_reference_table_schema.table1'::regclass);

SELECT master_add_node('localhost', :worker_2_port);

-- status after master_add_node
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
     WHERE logicalrelid = 'replicate_reference_table_schema.table1'::regclass);

DROP TABLE replicate_reference_table_schema.table1;
DROP SCHEMA replicate_reference_table_schema CASCADE;

-- do some tests with inactive node
SELECT master_remove_node('localhost', :worker_2_port);

CREATE TABLE initially_not_replicated_reference_table (key int);
SELECT create_reference_table('initially_not_replicated_reference_table');

SELECT master_add_inactive_node('localhost', :worker_2_port);

-- we should see only one shard placements
SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT 
                    shardid 
                FROM 
                    pg_dist_shard 
                WHERE 
                    logicalrelid = 'initially_not_replicated_reference_table'::regclass)
ORDER BY 1,4,5;

-- we should see the two shard placements after activation
SELECT master_activate_node('localhost', :worker_2_port);

SELECT
    shardid, shardstate, shardlength, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT 
                    shardid 
                FROM 
                    pg_dist_shard 
                WHERE 
                    logicalrelid = 'initially_not_replicated_reference_table'::regclass)
ORDER BY 1,4,5;

-- this should have no effect
SELECT master_add_node('localhost', :worker_2_port);

-- drop unnecassary tables
DROP TABLE initially_not_replicated_reference_table;

-- reload pg_dist_shard_placement table
INSERT INTO pg_dist_shard_placement (SELECT * FROM tmp_shard_placement);
DROP TABLE tmp_shard_placement;

RESET citus.shard_replication_factor;
RESET citus.replication_model;
