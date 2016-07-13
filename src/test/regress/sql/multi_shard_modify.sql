--
-- MULTI_SHARD_MODIFY
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 350000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 350000;


-- Create a new hash partitioned multi_shard_modify_test table and stage data into it.
CREATE TABLE multi_shard_modify_test (
        t_key integer not null,
        t_name varchar(25) not null,
        t_value integer not null);
SELECT master_create_distributed_table('multi_shard_modify_test', 't_key', 'hash');
SELECT master_create_worker_shards('multi_shard_modify_test', 4, 2);

COPY multi_shard_modify_test (t_key, t_name, t_value) FROM STDIN WITH (FORMAT 'csv');
1,san francisco,99
2,istanbul,34
3,paris,46
4,london,91
5,toronto,98
6,london,44
7,stockholm,21
8,tallinn,33
9,helsinki,21
10,ankara,6
11,karabuk,78
12,kastamonu,37
13,samsun,55
14,rome,13
15,madrid,1
16,barcelona,8
17,poznan,12
31,kabul,4
32,dhaka,62
33,iamey,121
34,muscat,77
41,uppsala,-1
42,malmo,-2
101,tokyo,106
102,new delhi,978
201,taipei,556
202,beijing,754
\.

-- Testing master_modify_multiple_shards
-- Verify that master_modify_multiple_shards cannot be called in a transaction block
BEGIN;
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key > 10 AND t_key <= 13');
ROLLBACK;

-- Check that master_modify_multiple_shards cannot be called with non-distributed tables
CREATE TEMPORARY TABLE temporary_nondistributed_table (col_1 integer,col_2 text);
INSERT INTO temporary_nondistributed_table VALUES (37, 'eren'), (31, 'onder');
SELECT master_modify_multiple_shards('DELETE FROM temporary_nondistributed_table WHERE col_1 = 37');

-- commands with volatile functions in their quals
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key = (random() * 1000)');
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_value = (random() * 1000)');

-- commands with immutable functions in their quals
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key = abs(-3)');

-- DELETE with expression in WHERE clause
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key = (3*18-40)');

-- commands with a USING clause are unsupported
CREATE TEMP TABLE temp_nations(name text, key integer);
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test USING temp_nations WHERE multi_shard_modify_test.t_value = temp_nations.key AND temp_nations.name = ''foobar'' ');

-- commands with a RETURNING clause are unsupported
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key = 3 RETURNING *');

-- commands containing a CTE are unsupported
SELECT master_modify_multiple_shards('WITH deleted_stuff AS (INSERT INTO multi_shard_modify_test DEFAULT VALUES RETURNING *) DELETE FROM multi_shard_modify_test');

-- Check that we can successfully delete from multiple shards with 1PC
SET citus.multi_shard_commit_protocol TO '1pc';
SELECT count(*) FROM multi_shard_modify_test;
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key > 200');
SELECT count(*) FROM multi_shard_modify_test;

-- Check that we can successfully delete from multiple shards with 2PC
SET citus.multi_shard_commit_protocol TO '2pc';
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key > 100');
SELECT count(*) FROM multi_shard_modify_test;

-- Check that shard pruning works
SET client_min_messages TO DEBUG2;
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key = 15');
SET client_min_messages TO NOTICE;

-- Check that master_modify_multiple_shards works without partition keys
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_name LIKE ''barce%'' ');


-- Simple, Single Shard Update
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_name=''warsaw'' WHERE t_key=17');
SELECT t_name FROM multi_shard_modify_test WHERE t_key=17;

-- Simple, Multi Shard Update
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_name=''???'' WHERE t_key>30 AND t_key<35');
SELECT t_name FROM multi_shard_modify_test WHERE t_key>30 AND t_key<35;

-- expression UPDATE
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_value=8*37 WHERE t_key>30 AND t_key<35');
SELECT t_value FROM multi_shard_modify_test WHERE t_key>30 AND t_key<35;

-- multi-column UPDATE
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_name=''somename'', t_value=333 WHERE t_key>30 AND t_key<35');
SELECT t_name, t_value FROM multi_shard_modify_test WHERE t_key>30 AND t_key<35;

-- commands with no constraints on the partition key are supported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_name=''nice city'' WHERE t_value < 0');
SELECT t_name FROM multi_shard_modify_test WHERE t_value < 0;

-- attempting to change the partition key is unsupported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_key=3000 WHERE t_key < 10 ');

-- UPDATEs with a FROM clause are unsupported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_name = ''FAIL'' FROM temp_nations WHERE multi_shard_modify_test.t_key = 3 AND multi_shard_modify_test.t_value = temp_nations.key AND temp_nations.name = ''dummy'' ');

-- commands with a RETURNING clause are unsupported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_name=''FAIL'' WHERE t_key=4 RETURNING *');

-- commands containing a CTE are unsupported
SELECT master_modify_multiple_shards('WITH t AS (INSERT INTO multi_shard_modify_test DEFAULT VALUES RETURNING *) UPDATE multi_shard_modify_test SET t_name = ''FAIL'' ');

-- updates referencing just a var are supported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_value=t_key WHERE t_key = 10');
SELECT t_value FROM multi_shard_modify_test WHERE t_key=10;

-- updates referencing a column are supported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_value = t_value + 37 WHERE t_key = 10');
SELECT t_value FROM multi_shard_modify_test WHERE t_key=10;

CREATE FUNCTION temp_stable_func() RETURNS integer AS 'SELECT 10;' LANGUAGE SQL STABLE;

-- updates referencing non-IMMUTABLE functions are unsupported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_name = ''FAIL!'' WHERE t_key = temp_stable_func()');

-- updates referencing IMMUTABLE functions in SET section are supported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_value = abs(-78) WHERE t_key = 10');
SELECT t_value FROM multi_shard_modify_test WHERE t_key=10;

-- updates referencing STABLE functions in SET section are supported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_value = temp_stable_func() * 2 WHERE t_key = 10');

-- updates referencing VOLATILE functions in SET section are not supported
SELECT master_modify_multiple_shards('UPDATE multi_shard_modify_test SET t_value = random() WHERE t_key = 10');

-- commands with stable functions in their quals are allowed
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test WHERE t_key = temp_stable_func()');

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 102046;
