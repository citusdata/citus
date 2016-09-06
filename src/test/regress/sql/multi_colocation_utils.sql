
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1300000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1300000;

-- ===================================================================
-- create test utility function
-- ===================================================================

CREATE SEQUENCE colocation_test_seq
    MINVALUE 1
    NO CYCLE;

/* a very simple UDF that only sets the colocation ids the same 
 * DO NOT USE THIS FUNCTION IN PRODUCTION. It manually sets colocationid column of
 * pg_dist_partition and it does not check anything about pyshical state about shards.
 */
CREATE OR REPLACE FUNCTION colocation_test_colocate_tables(source_table regclass, target_table regclass)
    RETURNS BOOL
    LANGUAGE plpgsql
    AS $colocate_tables$
DECLARE nextid BIGINT;
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
    RETURNS BIGINT
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

CREATE FUNCTION get_colocated_table_array(regclass)
    RETURNS regclass[]
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
SELECT master_create_worker_shards('table1_group1', 4, 1);

CREATE TABLE table2_group1 ( id int );
SELECT master_create_distributed_table('table2_group1', 'id', 'hash');
SELECT master_create_worker_shards('table2_group1', 4, 1);

CREATE TABLE table3_group2 ( id int );
SELECT master_create_distributed_table('table3_group2', 'id', 'hash');
SELECT master_create_worker_shards('table3_group2', 4, 1);

CREATE TABLE table4_group2 ( id int );
SELECT master_create_distributed_table('table4_group2', 'id', 'hash');
SELECT master_create_worker_shards('table4_group2', 4, 1);

CREATE TABLE table5_groupX ( id int );
SELECT master_create_distributed_table('table5_groupX', 'id', 'hash');
SELECT master_create_worker_shards('table5_groupX', 4, 1);

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

-- check table  co-location with same co-location group
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
