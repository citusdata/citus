--
-- MULTI_UPGRADE_REFERENCE_TABLE
--
-- Tests around upgrade_reference_table UDF
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1360000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1360000;

-- We run this twice, once with coordinator node in pg_dist_node and once without.
-- Set client_min_messages to WARNING to discard NOTICE messages by
-- upgrade_to_reference_table() to make the output consistent in both cases.
-- We check that reference table placements were actually replicated by checking
-- pg_dist_placement.
SET client_min_messages TO WARNING;

-- test with not distributed table
CREATE TABLE upgrade_reference_table_local(column1 int);
SELECT upgrade_to_reference_table('upgrade_reference_table_local');
DROP TABLE upgrade_reference_table_local;

-- test with table which has more than one shard
SET citus.shard_count TO 4;
CREATE TABLE upgrade_reference_table_multiple_shard(column1 int);
SELECT create_distributed_table('upgrade_reference_table_multiple_shard', 'column1');
SELECT upgrade_to_reference_table('upgrade_reference_table_multiple_shard');
DROP TABLE upgrade_reference_table_multiple_shard;

-- test with table which has no shard
CREATE TABLE upgrade_reference_table_no_shard(column1 int);
SELECT create_distributed_table('upgrade_reference_table_no_shard', 'column1', 'append');
SELECT upgrade_to_reference_table('upgrade_reference_table_no_shard');
DROP TABLE upgrade_reference_table_no_shard;

-- test with table with foreign keys
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
CREATE TABLE upgrade_reference_table_referenced(column1 int PRIMARY KEY);
SELECT create_distributed_table('upgrade_reference_table_referenced', 'column1');

CREATE TABLE upgrade_reference_table_referencing(column1 int REFERENCES upgrade_reference_table_referenced(column1));
SELECT create_distributed_table('upgrade_reference_table_referencing', 'column1');

-- update replication model to statement-based replication since streaming replicated tables cannot be upgraded to reference tables
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='upgrade_reference_table_referenced'::regclass;
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='upgrade_reference_table_referencing'::regclass;

SELECT upgrade_to_reference_table('upgrade_reference_table_referenced');
SELECT upgrade_to_reference_table('upgrade_reference_table_referencing');

DROP TABLE upgrade_reference_table_referencing;
DROP TABLE upgrade_reference_table_referenced;

-- test with no healthy placements
CREATE TABLE upgrade_reference_table_unhealthy(column1 int);
SELECT create_distributed_table('upgrade_reference_table_unhealthy', 'column1');
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='upgrade_reference_table_unhealthy'::regclass;
UPDATE pg_dist_shard_placement SET shardstate = 3
   WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_unhealthy'::regclass::oid);
SELECT upgrade_to_reference_table('upgrade_reference_table_unhealthy');
DROP TABLE upgrade_reference_table_unhealthy;

-- test with table containing composite type
CREATE TYPE upgrade_test_composite_type AS (key1 text, key2 text);

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
CREATE TABLE upgrade_reference_table_composite(column1 int, column2 upgrade_test_composite_type);
SELECT create_distributed_table('upgrade_reference_table_composite', 'column1');
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='upgrade_reference_table_composite'::regclass;
SELECT upgrade_to_reference_table('upgrade_reference_table_composite');
DROP TABLE upgrade_reference_table_composite;
DROP TYPE upgrade_test_composite_type;

-- test with reference table
CREATE TABLE upgrade_reference_table_reference(column1 int);
SELECT create_reference_table('upgrade_reference_table_reference');
SELECT upgrade_to_reference_table('upgrade_reference_table_reference');
DROP TABLE upgrade_reference_table_reference;

-- test valid cases, append distributed table
CREATE TABLE upgrade_reference_table_append(column1 int);
SELECT create_distributed_table('upgrade_reference_table_append', 'column1', 'append');
COPY upgrade_reference_table_append FROM STDIN;
1
2
3
4
5
\.

-- situation before upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_append'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_append'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_append'::regclass);

SELECT count(*) active_primaries FROM pg_dist_node WHERE isactive AND noderole='primary' \gset

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_append'::regclass)
GROUP BY shardid
ORDER BY shardid;

SELECT upgrade_to_reference_table('upgrade_reference_table_append');

-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_append'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_append'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_append'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_append'::regclass)
GROUP BY shardid
ORDER BY shardid;
    
DROP TABLE upgrade_reference_table_append;

-- test valid cases, shard exists at one worker
CREATE TABLE upgrade_reference_table_one_worker(column1 int);
SELECT create_distributed_table('upgrade_reference_table_one_worker', 'column1');
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='upgrade_reference_table_one_worker'::regclass;

-- situation before upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_one_worker'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_one_worker'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass)
GROUP BY shardid
ORDER BY shardid;

SELECT upgrade_to_reference_table('upgrade_reference_table_one_worker');

-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_one_worker'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_one_worker'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_worker'::regclass)
GROUP BY shardid
ORDER BY shardid;
    
DROP TABLE upgrade_reference_table_one_worker;

-- test valid cases, shard exists at both workers but one is unhealthy
SET citus.shard_replication_factor TO 2;
CREATE TABLE upgrade_reference_table_one_unhealthy(column1 int);
SELECT create_distributed_table('upgrade_reference_table_one_unhealthy', 'column1');
UPDATE pg_dist_shard_placement SET shardstate = 3
WHERE shardid = (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass::oid) AND nodeport = :worker_1_port;

-- situation before upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass)
    AND shardstate = 1
GROUP BY shardid
ORDER BY shardid;

SELECT upgrade_to_reference_table('upgrade_reference_table_one_unhealthy');

-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_one_unhealthy'::regclass)
    AND shardstate = 1
GROUP BY shardid
ORDER BY shardid;
    
DROP TABLE upgrade_reference_table_one_unhealthy;

-- test valid cases, shard exists at both workers and both are healthy
CREATE TABLE upgrade_reference_table_both_healthy(column1 int);
SELECT create_distributed_table('upgrade_reference_table_both_healthy', 'column1');

-- situation before upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass);

SELECT
    shardid
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass)
GROUP BY shardid
ORDER BY shardid;

SELECT upgrade_to_reference_table('upgrade_reference_table_both_healthy');

-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_both_healthy'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_both_healthy'::regclass)
GROUP BY shardid
ORDER BY shardid;
    
DROP TABLE upgrade_reference_table_both_healthy;

-- test valid cases, do it in transaction and ROLLBACK
SET citus.shard_replication_factor TO 1;
CREATE TABLE upgrade_reference_table_transaction_rollback(column1 int);
SELECT create_distributed_table('upgrade_reference_table_transaction_rollback', 'column1');
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='upgrade_reference_table_transaction_rollback'::regclass;

-- situation before upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass)
GROUP BY shardid
ORDER BY shardid;

BEGIN;
SELECT upgrade_to_reference_table('upgrade_reference_table_transaction_rollback');
ROLLBACK;

-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass);


SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_rollback'::regclass)
GROUP BY shardid
ORDER BY shardid;
     
DROP TABLE upgrade_reference_table_transaction_rollback;

-- test valid cases, do it in transaction and COMMIT
SET citus.shard_replication_factor TO 1;
CREATE TABLE upgrade_reference_table_transaction_commit(column1 int);
SELECT create_distributed_table('upgrade_reference_table_transaction_commit', 'column1');
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='upgrade_reference_table_transaction_commit'::regclass;

-- situation before upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass)
GROUP BY shardid
ORDER BY shardid;

BEGIN;
SELECT upgrade_to_reference_table('upgrade_reference_table_transaction_commit');
COMMIT;

-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_transaction_commit'::regclass)
GROUP BY shardid
ORDER BY shardid;

-- verify that shard is replicated to other worker
\c - - - :worker_2_port
\dt upgrade_reference_table_transaction_commit_*
\c - - - :master_port

DROP TABLE upgrade_reference_table_transaction_commit;

-- create an mx table
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
CREATE TABLE upgrade_reference_table_mx(column1 int);
SELECT create_distributed_table('upgrade_reference_table_mx', 'column1');

-- verify that streaming replicated tables cannot be upgraded to reference tables
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass);

SELECT
    shardid
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass)
GROUP BY shardid
ORDER BY shardid;
     

SELECT upgrade_to_reference_table('upgrade_reference_table_mx');

     
-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass)
GROUP BY shardid
ORDER BY shardid;

DROP TABLE upgrade_reference_table_mx;

-- test valid cases, do it with MX
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 2;
RESET citus.replication_model;
CREATE TABLE upgrade_reference_table_mx(column1 int);
SELECT create_distributed_table('upgrade_reference_table_mx', 'column1');
UPDATE pg_dist_shard_placement SET shardstate = 3 
WHERE nodeport = :worker_2_port AND 
	shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='upgrade_reference_table_mx'::regclass);
	
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- situation before upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass);

SELECT
    shardid
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass)
GROUP BY shardid
ORDER BY shardid;
     
SET client_min_messages TO WARNING;
SELECT upgrade_to_reference_table('upgrade_reference_table_mx');

     
-- situation after upgrade_reference_table
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT *
FROM pg_dist_colocation
WHERE colocationid IN
    (SELECT colocationid
     FROM pg_dist_partition
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass);

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass)
GROUP BY shardid
ORDER BY shardid;
     
-- situation on metadata worker
\c - - - :worker_1_port
SELECT
    partmethod, (partkey IS NULL) as partkeyisnull, colocationid, repmodel
FROM
    pg_dist_partition
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT
    shardid, (shardminvalue IS NULL) as shardminvalueisnull, (shardmaxvalue IS NULL) as shardmaxvalueisnull
FROM
    pg_dist_shard
WHERE
    logicalrelid = 'upgrade_reference_table_mx'::regclass;

SELECT
    shardid, count(distinct nodeport) = :active_primaries
FROM pg_dist_shard_placement
WHERE shardid IN
    (SELECT shardid
     FROM pg_dist_shard
     WHERE logicalrelid = 'upgrade_reference_table_mx'::regclass)
GROUP BY shardid
ORDER BY shardid;
     
\c - - - :master_port
DROP TABLE upgrade_reference_table_mx;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
RESET client_min_messages;
