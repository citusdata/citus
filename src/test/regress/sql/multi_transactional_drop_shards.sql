--
-- MULTI_TRANSACTIONAL_DROP_SHARDS
--
-- Tests that check the metadata returned by the master node.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1410000;

SET citus.shard_count TO 4;

-- test DROP TABLE(ergo master_drop_all_shards) in transaction, then ROLLBACK
CREATE TABLE transactional_drop_shards(column1 int);
SELECT create_distributed_table('transactional_drop_shards', 'column1');

BEGIN;
DROP TABLE transactional_drop_shards;
ROLLBACK;

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify table is not dropped
\d transactional_drop_shards;

-- verify shards are not dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;
\c - - - :master_port


-- test DROP TABLE(ergo master_drop_all_shards) in transaction, then COMMIT
BEGIN;
DROP TABLE transactional_drop_shards;
COMMIT;

-- verify metadata is deleted
SELECT shardid FROM pg_dist_shard WHERE shardid IN (1410000, 1410001, 1410002, 1410003) ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (1410000, 1410001, 1410002, 1410003)
ORDER BY
    shardid, nodename, nodeport;

-- verify table is dropped
\d transactional_drop_shards;

-- verify shards are dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;
\c - - - :master_port


-- test master_delete_protocol in transaction, then ROLLBACK
CREATE TABLE transactional_drop_shards(column1 int);
SELECT create_distributed_table('transactional_drop_shards', 'column1', 'append');
SELECT master_create_empty_shard('transactional_drop_shards');

BEGIN;
SELECT master_apply_delete_command('DELETE FROM transactional_drop_shards');
ROLLBACK;

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify shards are not dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;
\c - - - :master_port


-- test master_delete_protocol in transaction, then COMMIT
BEGIN;
SELECT master_apply_delete_command('DELETE FROM transactional_drop_shards');
COMMIT;

-- verify metadata is deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify shards are dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;
\c - - - :master_port


-- test DROP table in a transaction after insertion
SELECT master_create_empty_shard('transactional_drop_shards');

BEGIN;
INSERT INTO transactional_drop_shards VALUES (1);
DROP TABLE transactional_drop_shards;
ROLLBACK;

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify table is not dropped
\d transactional_drop_shards;

-- verify shards are not dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;
\c - - - :master_port


-- test master_apply_delete_command in a transaction after insertion
BEGIN;
INSERT INTO transactional_drop_shards VALUES (1);
SELECT master_apply_delete_command('DELETE FROM transactional_drop_shards');
ROLLBACK;

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify shards are not dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;


-- test DROP table with failing worker
CREATE FUNCTION fail_drop_table() RETURNS event_trigger AS $fdt$
    BEGIN
        RAISE 'illegal value';
    END;
$fdt$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER fail_drop_table ON sql_drop EXECUTE PROCEDURE fail_drop_table();

\c - - - :master_port

\set VERBOSITY terse
DROP TABLE transactional_drop_shards;
\set VERBOSITY default

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify table is not dropped
\d transactional_drop_shards;

-- verify shards are not dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;
\c - - - :master_port


-- test DROP reference table with failing worker
CREATE TABLE transactional_drop_reference(column1 int);
SELECT create_reference_table('transactional_drop_reference');

\set VERBOSITY terse
DROP TABLE transactional_drop_reference;
\set VERBOSITY default

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_reference'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_reference'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify table is not dropped
\d transactional_drop_reference;

-- verify shards are not dropped
\c - - - :worker_1_port
\d transactional_drop_reference*;
\c - - - :master_port


-- test master_apply_delete_command table with failing worker
\set VERBOSITY terse
SELECT master_apply_delete_command('DELETE FROM transactional_drop_shards');
\set VERBOSITY default

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_shards'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify shards are not dropped
\c - - - :worker_1_port
\d transactional_drop_shards_*;
DROP EVENT TRIGGER fail_drop_table;
\c - - - :master_port


-- test with SERIAL column + with more shards
SET citus.shard_count TO 8;
CREATE TABLE transactional_drop_serial(column1 int, column2 SERIAL);
SELECT create_distributed_table('transactional_drop_serial', 'column1');

-- test DROP TABLE(ergo master_drop_all_shards) in transaction, then ROLLBACK
BEGIN;
DROP TABLE transactional_drop_serial;
ROLLBACK;

-- verify metadata is not deleted
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_serial'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_serial'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- verify table is not dropped
\d transactional_drop_serial;

-- verify shards and sequence are not dropped
\c - - - :worker_1_port
\d transactional_drop_serial_1410006;
\ds transactional_drop_serial_column2_seq
\c - - - :master_port


-- test DROP TABLE(ergo master_drop_all_shards) in transaction, then COMMIT
BEGIN;
DROP TABLE transactional_drop_serial;
COMMIT;

-- verify metadata is deleted
SELECT shardid FROM pg_dist_shard WHERE shardid IN (1410007, 1410008, 1410009, 1410010, 1410011, 1410012, 1410013, 1410014) ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (1410007, 1410008, 1410009, 1410010, 1410011, 1410012, 1410013, 1410014)
ORDER BY
    shardid, nodename, nodeport;

-- verify table is dropped
\d transactional_drop_serial;

-- verify shards and sequence are dropped
\c - - - :worker_1_port
\d transactional_drop_serial_1410006;
\ds transactional_drop_serial_column2_seq
\c - - - :master_port


-- test with MX, DROP TABLE, then ROLLBACK
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;

CREATE TABLE transactional_drop_mx(column1 int);
SELECT create_distributed_table('transactional_drop_mx', 'column1');

UPDATE pg_dist_partition SET repmodel='s' WHERE logicalrelid='transactional_drop_mx'::regclass;

-- make worker 1 receive metadata changes
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- see metadata is propogated to the worker
\c - - - :worker_1_port
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_mx'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_mx'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

\c - - - :master_port
BEGIN;
DROP TABLE transactional_drop_mx;
ROLLBACK;

-- verify metadata is not deleted
\c - - - :worker_1_port
SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_mx'::regclass ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'transactional_drop_mx'::regclass ORDER BY shardid)
ORDER BY
    shardid, nodename, nodeport;

-- test with MX, DROP TABLE, then COMMIT
\c - - - :master_port
BEGIN;
DROP TABLE transactional_drop_mx;
COMMIT;

-- verify metadata is deleted
\c - - - :worker_1_port
SELECT shardid FROM pg_dist_shard WHERE shardid IN (1410015, 1410016, 1410017, 1410018) ORDER BY shardid;
SELECT
    shardid, shardstate, nodename, nodeport
FROM
    pg_dist_shard_placement
WHERE
    shardid IN (1410015, 1410016, 1410017, 1410018)
ORDER BY
    shardid, nodename, nodeport;

\c - - - :master_port

-- clean the workspace
DROP TABLE transactional_drop_shards, transactional_drop_reference;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
