
SET citus.next_shard_id TO 1300000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 4;

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
SELECT UNNEST(get_colocated_table_array('table1_group1'))::regclass ORDER BY 1;
SELECT UNNEST(get_colocated_table_array('table5_groupX'))::regclass ORDER BY 1;
SELECT UNNEST(get_colocated_table_array('table6_append'))::regclass ORDER BY 1;

-- check co-located shard list
SELECT UNNEST(get_colocated_shard_array(1300000))::regclass ORDER BY 1;
SELECT UNNEST(get_colocated_shard_array(1300016))::regclass ORDER BY 1;
SELECT UNNEST(get_colocated_shard_array(1300020))::regclass ORDER BY 1;

-- check FindShardIntervalIndex function
SELECT find_shard_interval_index(1300000);
SELECT find_shard_interval_index(1300001);
SELECT find_shard_interval_index(1300002);
SELECT find_shard_interval_index(1300003);
SELECT find_shard_interval_index(1300016);

-- check external colocation API

SELECT count(*) FROM pg_dist_partition WHERE colocationid IN (4, 5);
DELETE FROM pg_dist_colocation WHERE colocationid IN (4, 5);

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

UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='table1_groupB'::regclass;
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='table2_groupB'::regclass;

-- revert back to default shard replication factor
SET citus.shard_replication_factor to DEFAULT;

-- change partition column type
CREATE TABLE table1_groupC ( id text );
SELECT create_distributed_table('table1_groupC', 'id');

CREATE TABLE table2_groupC ( id text );
SELECT create_distributed_table('table2_groupC', 'id');

-- change shard count
SET citus.shard_count = 8;

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
    ORDER BY logicalrelid;

-- check effects of dropping tables
DROP TABLE table1_groupA;
SELECT * FROM pg_dist_colocation WHERE colocationid = 4;

-- dropping all tables in a colocation group also deletes the colocation group
DROP TABLE table2_groupA;
SELECT * FROM pg_dist_colocation WHERE colocationid = 4;

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
CREATE SCHEMA schema_colocation;

CREATE TABLE schema_colocation.table4_groupE ( id int );
SELECT create_distributed_table('schema_colocation.table4_groupE', 'id');

-- test colocate_with option
CREATE TABLE table1_group_none_1 ( id int );
SELECT create_distributed_table('table1_group_none_1', 'id', colocate_with => 'none');

CREATE TABLE table2_group_none_1 ( id int );
SELECT create_distributed_table('table2_group_none_1', 'id', colocate_with => 'table1_group_none_1');

CREATE TABLE table1_group_none_2 ( id int );
SELECT create_distributed_table('table1_group_none_2', 'id', colocate_with => 'none');

CREATE TABLE table4_groupE ( id int );
SELECT create_distributed_table('table4_groupE', 'id', colocate_with => 'default');

SET citus.shard_count = 3;

-- check that this new configuration does not have a default group
CREATE TABLE table1_group_none_3 ( id int );
SELECT create_distributed_table('table1_group_none_3', 'id', colocate_with => 'NONE');

-- a new table does not use a non-default group
CREATE TABLE table1_group_default ( id int );
SELECT create_distributed_table('table1_group_default', 'id', colocate_with => 'DEFAULT');

-- check metadata
SELECT * FROM pg_dist_colocation
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid;

SELECT logicalrelid, colocationid FROM pg_dist_partition
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid, logicalrelid;

-- check failing colocate_with options
CREATE TABLE table_postgresql( id int );
CREATE TABLE table_failing ( id int );

SELECT create_distributed_table('table_failing', 'id', colocate_with => 'table_append');
SELECT create_distributed_table('table_failing', 'id', 'append', 'table1_groupE');
SELECT create_distributed_table('table_failing', 'id', colocate_with => 'table_postgresql');
SELECT create_distributed_table('table_failing', 'id', colocate_with => 'no_table');
SELECT create_distributed_table('table_failing', 'id', colocate_with => '');
SELECT create_distributed_table('table_failing', 'id', colocate_with => NULL);

-- check with different distribution column types
CREATE TABLE table_bigint ( id bigint );
SELECT create_distributed_table('table_bigint', 'id', colocate_with => 'table1_groupE');
-- check worker table schemas
\c - - - :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.table3_groupE_1300062'::regclass;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='schema_colocation.table4_groupE_1300064'::regclass;

\c - - - :master_port
SET citus.next_shard_id TO 1300080;

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
    nodeport;

-- reset colocation ids to test update_distributed_table_colocation
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1;
DELETE FROM pg_dist_colocation
    WHERE colocationid >= 1 AND colocationid < 1000;
UPDATE pg_dist_partition SET colocationid = 0
    WHERE colocationid >= 1 AND colocationid < 1000;

-- check metadata
SELECT * FROM pg_dist_colocation
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid;

SELECT logicalrelid, colocationid FROM pg_dist_partition
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid, logicalrelid;

-- first check failing cases
SELECT update_distributed_table_colocation('table1_groupB', colocate_with => 'table1_groupC');
SELECT update_distributed_table_colocation('table1_groupB', colocate_with => 'table1_groupD');
SELECT update_distributed_table_colocation('table1_groupB', colocate_with => 'table1_groupE');
SELECT update_distributed_table_colocation('table1_groupB', colocate_with => 'table1_groupF');
SELECT update_distributed_table_colocation('table1_groupB', colocate_with => 'table1_groupD');

-- check metadata to see failing calls didn't have any side effects
SELECT * FROM pg_dist_colocation
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid;

SELECT logicalrelid, colocationid FROM pg_dist_partition
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid, logicalrelid;

-- check successfully cololated tables
SELECT update_distributed_table_colocation('table1_groupB', colocate_with => 'table2_groupB');
SELECT update_distributed_table_colocation('table1_groupC', colocate_with => 'table2_groupC');
SELECT update_distributed_table_colocation('table1_groupD', colocate_with => 'table2_groupD');
SELECT update_distributed_table_colocation('table1_groupE', colocate_with => 'table2_groupE');
SELECT update_distributed_table_colocation('table1_groupE', colocate_with => 'table3_groupE');

-- check to colocate with itself
SELECT update_distributed_table_colocation('table1_groupB', colocate_with => 'table1_groupB');

SET citus.shard_count = 2;

CREATE TABLE table1_group_none ( id int );
SELECT create_distributed_table('table1_group_none', 'id', colocate_with => 'NONE');

CREATE TABLE table2_group_none ( id int );
SELECT create_distributed_table('table2_group_none', 'id', colocate_with => 'NONE');

-- check metadata to see colocation groups are created successfully
SELECT * FROM pg_dist_colocation
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid;

SELECT logicalrelid, colocationid FROM pg_dist_partition
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid, logicalrelid;

-- move the all tables in colocation group 5 to colocation group 7
SELECT update_distributed_table_colocation('table1_group_none', colocate_with => 'table1_groupE');
SELECT update_distributed_table_colocation('table1_group_none', colocate_with => 'table2_groupE');
SELECT update_distributed_table_colocation('table1_group_none', colocate_with => 'table3_groupE');

-- move a table with a colocation id which is already not in pg_dist_colocation
SELECT update_distributed_table_colocation('table1_group_none', colocate_with => 'table2_group_none');

-- check metadata to see that unused colocation group is deleted
SELECT * FROM pg_dist_colocation
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid;

SELECT logicalrelid, colocationid FROM pg_dist_partition
    WHERE colocationid >= 1 AND colocationid < 1000
    ORDER BY colocationid, logicalrelid;

-- try to colocate different replication models
CREATE TABLE table1_groupG ( id int );
SELECT create_distributed_table('table1_groupG', 'id');

-- update replication model
UPDATE pg_dist_partition SET repmodel = 's' WHERE logicalrelid = 'table1_groupG'::regclass;

CREATE TABLE table2_groupG ( id int );
SELECT create_distributed_table('table2_groupG', 'id', colocate_with => 'table1_groupG');
DROP TABLE table2_groupG;

CREATE TABLE table2_groupG ( id int );
SELECT create_distributed_table('table2_groupG', 'id', colocate_with => 'NONE');

SELECT update_distributed_table_colocation('table1_groupG', colocate_with => 'table2_groupG');

CREATE TABLE d1(a int, b int);
CREATE TABLE d2(a int, b int);
CREATE TABLE d3(a int, b int);
CREATE TABLE d4(a int, b int);
CREATE TABLE different_d1(ch char);
CREATE TABLE append_table(a int, b int);
CREATE TABLE range_table(a int, b int);
-- special keyword none
CREATE TABLE none(a int, b int);
CREATE TABLE ref(a int);
CREATE TABLE local_table(a int);

SELECT create_distributed_table('d1', 'a');
SELECT create_distributed_table('d2', 'a', colocate_with => 'd1');
SELECT create_distributed_table('d3', 'a', colocate_with => 'd2');
SELECT create_distributed_table('d4', 'a', colocate_with => 'd3');
SELECT create_distributed_table('none', 'a', colocate_with => 'd4');
SELECT create_distributed_table('different_d1', 'ch');
SELECT create_distributed_table('append_table', 'a', 'append');
SELECT create_distributed_table('range_table', 'a', 'range');


SELECT create_reference_table('ref');

SELECT tables_colocated('d1', 'd2');
SELECT tables_colocated('d2', 'd3');
SELECT tables_colocated('d2', 'd4');
SELECT tables_colocated('d3', 'd4');
SELECT tables_colocated('d1', 'd3');
SELECT tables_colocated('d1', 'd4');

-- break colocation of d2
SELECT update_distributed_table_colocation('d2', colocate_with => 'none');

SELECT tables_colocated('d1', 'd2');
SELECT tables_colocated('d2', 'd3');
SELECT tables_colocated('d1', 'd3');
SELECT tables_colocated('d1', 'd4');

-- break colocation of d2
-- update colocation should not error if d2 doesn't have any colocated table.
SELECT update_distributed_table_colocation('d2', colocate_with => 'none');

SELECT tables_colocated('d1', 'd2');
SELECT tables_colocated('d2', 'd3');
SELECT tables_colocated('d1', 'd3');
SELECT tables_colocated('d1', 'd4');

SELECT update_distributed_table_colocation('d3', colocate_with => 'd2');

SELECT tables_colocated('d1', 'd2');
SELECT tables_colocated('d2', 'd3');
SELECT tables_colocated('d1', 'd3');
SELECT tables_colocated('d1', 'd4');

-- special case, colocate with a table named "none".
SELECT update_distributed_table_colocation('d3', colocate_with => '"none"');

SELECT tables_colocated('d1', 'd2');
SELECT tables_colocated('d2', 'd3');
SELECT tables_colocated('d1', 'd3');
SELECT tables_colocated('d1', 'd4');
SELECT tables_colocated('d1', 'none');
SELECT tables_colocated('d4', 'none');
SELECT tables_colocated('d3', 'none');
SELECT tables_colocated('d2', 'none');

-- make sure reference and local tables return an error.
SELECT update_distributed_table_colocation('ref', colocate_with => 'none');
SELECT update_distributed_table_colocation('local_table', colocate_with => 'none');

-- make sure that different types cannot be colocated
SELECT update_distributed_table_colocation('different_d1', colocate_with => 'd1');
SELECT update_distributed_table_colocation('d1', colocate_with => 'different_d1');

-- make sure that append distributed tables cannot be colocated
SELECT update_distributed_table_colocation('append_table', colocate_with => 'd1');
SELECT update_distributed_table_colocation('d1', colocate_with => 'append_table');
SELECT update_distributed_table_colocation('range_table', colocate_with => 'd1');
SELECT update_distributed_table_colocation('d1', colocate_with => 'range_table');




-- drop tables to clean test space
DROP TABLE table1_groupb;
DROP TABLE table2_groupb;
DROP TABLE table1_groupc;
DROP TABLE table2_groupc;
DROP TABLE table1_groupd;
DROP TABLE table2_groupd;
DROP TABLE table1_groupf;
DROP TABLE table2_groupf;
DROP TABLE table1_groupg;
DROP TABLE table2_groupg;
DROP TABLE table1_groupe;
DROP TABLE table2_groupe;
DROP TABLE table3_groupe;
DROP TABLE table4_groupe;
DROP TABLE schema_colocation.table4_groupe;
DROP TABLE table1_group_none_1;
DROP TABLE table2_group_none_1;
DROP TABLE table1_group_none_2;
DROP TABLE table1_group_none_3;
DROP TABLE table1_group_none;
DROP TABLE table2_group_none;
DROP TABLE table1_group_default;
DROP TABLE d1;
DROP TABLE d2;
DROP TABLE d3;
DROP TABLE d4;
DROP TABLE different_d1;
DROP TABLE append_table;
DROP TABLE range_table;
DROP TABLE none;
DROP TABLE ref;
DROP TABLE local_table;
DROP FOREIGN TABLE table3_groupD CASCADE;
