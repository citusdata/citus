
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1300000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1300000;

-- ===================================================================
-- create test utility function
-- ===================================================================

CREATE SEQUENCE colocation_test_seq
    MINVALUE 1000
    NO CYCLE;

/* a very simple UDF that only sets the colocation ids the same 
 * DO NOT USE THIS FUNCTION IN PRODUCTION. It manually sets colocationid column of
 * pg_dist_partition and it does not check anything about pyshical state about shards.
 */
CREATE OR REPLACE FUNCTION colocation_test_colocate_tables(source_table regclass, target_table regclass)
    RETURNS BOOL
    LANGUAGE plpgsql
    AS $colocate_tables$
DECLARE nextid INTEGER;
BEGIN
    SELECT nextval('colocation_test_seq') INTO nextid;

    UPDATE pg_dist_partition SET colocationId = nextid
    WHERE logicalrelid IN
    (
        (SELECT p1.logicalrelid
            FROM pg_dist_partition p1, pg_dist_partition p2
            WHERE
                p2.logicalrelid = source_table AND
                (p1.logicalrelid = source_table OR 
                (p1.colocationId = p2.colocationId AND p1.colocationId != 0)))
        UNION
        (SELECT target_table)
    );
    RETURN TRUE;
END;
$colocate_tables$;

-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION get_table_colocation_id(regclass)
    RETURNS INTEGER
    AS 'citus'
    LANGUAGE C STRICT;

CREATE FUNCTION tables_colocated(regclass, regclass)
    RETURNS bool
    AS 'citus'
    LANGUAGE C;

CREATE FUNCTION shards_colocated(bigint, bigint)
    RETURNS bool
    AS 'citus'
    LANGUAGE C STRICT;

CREATE FUNCTION get_colocated_shard_array(bigint)
    RETURNS BIGINT[]
    AS 'citus'
    LANGUAGE C STRICT;

CREATE FUNCTION find_shard_interval_index(bigint)
    RETURNS int
    AS 'citus'
    LANGUAGE C STRICT;

-- ===================================================================
-- test co-location util functions
-- ===================================================================

-- create distributed table observe shard pruning
CREATE TABLE table1_group1 ( id int );
SELECT master_create_distributed_table('table1_group1', 'id', 'hash');
SELECT master_create_worker_shards('table1_group1', 4, 2);

CREATE TABLE table2_group1 ( id int );
SELECT master_create_distributed_table('table2_group1', 'id', 'hash');
SELECT master_create_worker_shards('table2_group1', 4, 2);

CREATE TABLE table3_group2 ( id int );
SELECT master_create_distributed_table('table3_group2', 'id', 'hash');
SELECT master_create_worker_shards('table3_group2', 4, 2);

CREATE TABLE table4_group2 ( id int );
SELECT master_create_distributed_table('table4_group2', 'id', 'hash');
SELECT master_create_worker_shards('table4_group2', 4, 2);

CREATE TABLE table5_groupX ( id int );
SELECT master_create_distributed_table('table5_groupX', 'id', 'hash');
SELECT master_create_worker_shards('table5_groupX', 4, 2);

CREATE TABLE table6_append ( id int );
SELECT master_create_distributed_table('table6_append', 'id', 'append');
SELECT master_create_empty_shard('table6_append');
SELECT master_create_empty_shard('table6_append');

-- make table1_group1 and table2_group1 co-located manually
SELECT colocation_test_colocate_tables('table1_group1', 'table2_group1');

-- check co-location id
SELECT get_table_colocation_id('table1_group1');
SELECT get_table_colocation_id('table5_groupX');
SELECT get_table_colocation_id('table6_append');

-- check self table co-location
SELECT tables_colocated('table1_group1', 'table1_group1');
SELECT tables_colocated('table5_groupX', 'table5_groupX');
SELECT tables_colocated('table6_append', 'table6_append');

-- check table co-location with same co-location group
SELECT tables_colocated('table1_group1', 'table2_group1');

-- check table co-location with different co-location group
SELECT tables_colocated('table1_group1', 'table3_group2');

-- check table co-location with invalid co-location group
SELECT tables_colocated('table1_group1', 'table5_groupX');
SELECT tables_colocated('table1_group1', 'table6_append');

-- check self shard co-location
SELECT shards_colocated(1300000, 1300000);
SELECT shards_colocated(1300016, 1300016);
SELECT shards_colocated(1300020, 1300020);

-- check shard co-location with same co-location group
SELECT shards_colocated(1300000, 1300004);

-- check shard co-location with same table different co-location group
SELECT shards_colocated(1300000, 1300001);

-- check shard co-location with different co-location group
SELECT shards_colocated(1300000, 1300005);

-- check shard co-location with invalid co-location group
SELECT shards_colocated(1300000, 1300016);
SELECT shards_colocated(1300000, 1300020);

-- check co-located table list
SELECT UNNEST(get_colocated_table_array('table1_group1'))::regclass;
SELECT UNNEST(get_colocated_table_array('table5_groupX'))::regclass;
SELECT UNNEST(get_colocated_table_array('table6_append'))::regclass;

-- check co-located shard list
SELECT get_colocated_shard_array(1300000);
SELECT get_colocated_shard_array(1300016);
SELECT get_colocated_shard_array(1300020);

-- check FindShardIntervalIndex function
SELECT find_shard_interval_index(1300000);
SELECT find_shard_interval_index(1300001);
SELECT find_shard_interval_index(1300002);
SELECT find_shard_interval_index(1300003);
SELECT find_shard_interval_index(1300016);


-- check external colocation API

SET citus.shard_count = 2;

CREATE TABLE table1_groupA ( id int );
SELECT create_distributed_table('table1_groupA', 'id');

CREATE TABLE table2_groupA ( id int );
SELECT create_distributed_table('table2_groupA', 'id');

-- change shard replication factor
SET citus.shard_replication_factor = 1;

CREATE TABLE table1_groupB ( id int );
SELECT create_distributed_table('table1_groupB', 'id');

CREATE TABLE table2_groupB ( id int );
SELECT create_distributed_table('table2_groupB', 'id');

-- revert back to default shard replication factor
SET citus.shard_replication_factor to DEFAULT;

-- change partition column type
CREATE TABLE table1_groupC ( id text );
SELECT create_distributed_table('table1_groupC', 'id');

CREATE TABLE table2_groupC ( id text );
SELECT create_distributed_table('table2_groupC', 'id');

-- change shard count
SET citus.shard_count = 4;

CREATE TABLE table1_groupD ( id int );
SELECT create_distributed_table('table1_groupD', 'id');

CREATE TABLE table2_groupD ( id int );
SELECT create_distributed_table('table2_groupD', 'id');

-- try other distribution methods
CREATE TABLE table_append ( id int );
SELECT create_distributed_table('table_append', 'id', 'append');

CREATE TABLE table_range ( id int );
SELECT create_distributed_table('table_range', 'id', 'range');

-- test foreign table creation
CREATE FOREIGN TABLE table3_groupD ( id int ) SERVER fake_fdw_server;
SELECT create_distributed_table('table3_groupD', 'id');

-- check metadata
SELECT * FROM pg_dist_colocation 
    WHERE colocationid >= 1 AND colocationid < 1000 
    ORDER BY colocationid;

SELECT logicalrelid, colocationid FROM pg_dist_partition
    WHERE colocationid >= 1 AND colocationid < 1000 
    ORDER BY colocationid;

-- check effects of dropping tables
DROP TABLE table1_groupA;
SELECT * FROM pg_dist_colocation WHERE colocationid = 1;

-- dropping all tables in a colocation group also deletes the colocation group
DROP TABLE table2_groupA;
SELECT * FROM pg_dist_colocation WHERE colocationid = 1;

-- create dropped colocation group again
SET citus.shard_count = 2;

CREATE TABLE table1_groupE ( id int );
SELECT create_distributed_table('table1_groupE', 'id');

CREATE TABLE table2_groupE ( id int );
SELECT create_distributed_table('table2_groupE', 'id');

-- test different table DDL
CREATE TABLE table3_groupE ( dummy_column text, id int );
SELECT create_distributed_table('table3_groupE', 'id');

-- test different schema
CREATE SCHEMA schema_collocation;

CREATE TABLE schema_collocation.table4_groupE ( id int );
SELECT create_distributed_table('schema_collocation.table4_groupE', 'id');

-- check worker table schemas
\c - - - :worker_1_port
\d table3_groupE_1300050
\d schema_collocation.table4_groupE_1300052

\c - - - :master_port

CREATE TABLE table1_groupF ( id int );
SELECT create_reference_table('table1_groupF');

CREATE TABLE table2_groupF ( id int );
SELECT create_reference_table('table2_groupF');

-- check metadata
SELECT * FROM pg_dist_colocation 
    WHERE colocationid >= 1 AND colocationid < 1000 
    ORDER BY colocationid;

-- cross check with internal colocation API
SELECT 
    p1.logicalrelid::regclass AS table1,
    p2.logicalrelid::regclass AS table2,
    tables_colocated(p1.logicalrelid , p2.logicalrelid) AS colocated
FROM
    pg_dist_partition p1,
    pg_dist_partition p2
WHERE
    p1.logicalrelid < p2.logicalrelid AND
    p1.colocationid != 0 AND
    p2.colocationid != 0 AND
    tables_colocated(p1.logicalrelid , p2.logicalrelid) is TRUE
ORDER BY
    table1,
    table2;

-- check created shards
SELECT
    logicalrelid,
    pg_dist_shard.shardid AS shardid,
    shardstorage,
    nodeport,
    shardminvalue,
    shardmaxvalue
FROM
    pg_dist_shard,
    pg_dist_shard_placement
WHERE
    pg_dist_shard.shardid = pg_dist_shard_placement.shardid AND
    pg_dist_shard.shardid >= 1300026
ORDER BY
    logicalrelid,
    shardmaxvalue::integer,
    shardid,
    placementid;
