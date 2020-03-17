CREATE SCHEMA local_shard_copy;
SET search_path TO local_shard_copy;

SET client_min_messages TO DEBUG;
SET citus.next_shard_id TO 1570000;

SELECT * FROM master_add_node('localhost', :master_port, groupid := 0);

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';


CREATE TABLE reference_table (key int PRIMARY KEY);
SELECT create_reference_table('reference_table');

CREATE TABLE distributed_table (key int PRIMARY KEY, age bigint CHECK (age >= 10));
SELECT create_distributed_table('distributed_table','key');

INSERT INTO distributed_table SELECT *,* FROM generate_series(20, 40);
INSERT INTO reference_table SELECT * FROM generate_series(1, 10);

CREATE TABLE local_table (key int PRIMARY KEY);
INSERT INTO local_table SELECT * from generate_series(1, 10);

-- partitioned table
CREATE TABLE collections_list (
	key bigserial,
	collection_id integer
) PARTITION BY LIST (collection_id );

SELECT create_distributed_table('collections_list', 'key');

CREATE TABLE collections_list_0
	PARTITION OF collections_list (key, collection_id)
	FOR VALUES IN ( 0 );

CREATE TABLE collections_list_1
	PARTITION OF collections_list (key, collection_id)
	FOR VALUES IN ( 1 );


-- connection worker and get ready for the tests
\c - - - :worker_1_port

SET search_path TO local_shard_copy;
SET citus.log_local_commands TO ON;

-- returns true of the distribution key filter
-- on the distributed tables (e.g., WHERE key = 1), we'll hit a shard
-- placement which is local to this not
CREATE OR REPLACE FUNCTION shard_of_distribution_column_is_local(dist_key int) RETURNS bool AS $$

		DECLARE shard_is_local BOOLEAN := FALSE;

		BEGIN

		  	WITH  local_shard_ids 			  AS (SELECT get_shard_id_for_distribution_column('local_shard_copy.distributed_table', dist_key)),
				  all_local_shard_ids_on_node AS (SELECT shardid FROM pg_dist_placement WHERE groupid IN (SELECT groupid FROM pg_dist_local_group))
		SELECT
			true INTO shard_is_local
		FROM
			local_shard_ids
		WHERE
			get_shard_id_for_distribution_column IN (SELECT * FROM all_local_shard_ids_on_node);

		IF shard_is_local IS NULL THEN
			shard_is_local = FALSE;
		END IF;

		RETURN shard_is_local;
        END;
$$ LANGUAGE plpgsql;

-- pick some example values that reside on the shards locally and remote

-- distribution key values of 1,6, 500 and 701 are LOCAL to shards,
-- we'll use these values in the tests
SELECT shard_of_distribution_column_is_local(1);
SELECT shard_of_distribution_column_is_local(6);
SELECT shard_of_distribution_column_is_local(500);
SELECT shard_of_distribution_column_is_local(701);

-- distribution key values of 11 and 12 are REMOTE to shards
SELECT shard_of_distribution_column_is_local(11);
SELECT shard_of_distribution_column_is_local(12);

BEGIN;
    -- run select with local execution
    SELECT count(*) FROM distributed_table WHERE key = 1;

    SELECT count(*) FROM distributed_table;
    -- the local placements should be executed locally
    COPY distributed_table FROM STDIN WITH delimiter ',';
1, 100
2, 200
3, 300
4, 400
5, 500
\.
    -- verify that the copy is successful.
    SELECT count(*) FROM distributed_table;

ROLLBACK;

BEGIN;
    -- run select with local execution
    SELECT count(*) FROM distributed_table WHERE key = 1;

    SELECT count(*) FROM distributed_table;
    -- the local placements should be executed locally
    COPY distributed_table FROM STDIN WITH delimiter ',';
1, 100
2, 200
3, 300
4, 400
5, 500
\.
    -- verify the put ages.
    SELECT * FROM distributed_table;

ROLLBACK;


BEGIN;
    -- run select with local execution
    SELECT count(*) FROM distributed_table WHERE key = 1;

    SELECT count(*) FROM distributed_table;
    -- the local placements should be executed locally
    COPY distributed_table FROM STDIN WITH delimiter ',';
1, 100
2, 200
3, 300
4, 400
5, 500
\.
    -- verify that the copy is successful.
    SELECT count(*) FROM distributed_table;

ROLLBACK;

BEGIN;
    -- run select with local execution
    SELECT age FROM distributed_table WHERE key = 1;

    SELECT count(*) FROM collections_list;
    -- the local placements should be executed locally
    COPY collections_list FROM STDIN WITH delimiter ',';
1, 0
2, 0
3, 0
4, 1
5, 1
\.
    -- verify that the copy is successful.
    SELECT count(*) FROM collections_list;

ROLLBACK;

BEGIN;
    -- run select with local execution
    SELECT age FROM distributed_table WHERE key = 1;

    SELECT count(*) FROM distributed_table;
    -- the local placements should be executed locally
    COPY distributed_table FROM STDIN WITH delimiter ',';
1, 100
2, 200
3, 300
4, 400
5, 500
\.


    -- verify that the copy is successful.
    SELECT count(*) FROM distributed_table;

ROLLBACK;

BEGIN;
-- Since we are in a transaction, the copy should be locally executed.
COPY distributed_table FROM STDIN WITH delimiter ',';
1, 100
2, 200
3, 300
4, 400
5, 500
\.
ROLLBACK;

-- Since we are not in a transaction, the copy should not be locally executed.
COPY distributed_table FROM STDIN WITH delimiter ',';
1, 100
2, 200
3, 300
4, 400
5, 500
\.

BEGIN;
-- Since we are in a transaction, the copy should be locally executed. But
-- we are putting duplicate key, so it should error.
COPY distributed_table FROM STDIN WITH delimiter ',';
1, 100
2, 200
3, 300
4, 400
5, 500
\.
ROLLBACK;

TRUNCATE distributed_table;

BEGIN;

-- insert a lot of data ( around 8MB),
-- this should use local copy and it will exceed the LOCAL_COPY_FLUSH_THRESHOLD (512KB)
INSERT INTO distributed_table SELECT * , * FROM generate_series(20, 1000000);

ROLLBACK;

COPY distributed_table FROM STDIN WITH delimiter ',';
1, 9
\.

BEGIN;
-- Since we are in a transaction, the execution will be local, however we are putting invalid age.
-- The constaints should give an error
COPY distributed_table FROM STDIN WITH delimiter ',';
1,9
\.
ROLLBACK;

TRUNCATE distributed_table;


-- different delimiters
BEGIN;
-- run select with local execution
SELECT count(*) FROM distributed_table WHERE key = 1;
-- initial size
SELECT count(*) FROM distributed_table;
COPY distributed_table FROM STDIN WITH delimiter '|';
1|10
2|30
3|40
\.
-- new size
SELECT count(*) FROM distributed_table;
ROLLBACK;

BEGIN;
-- run select with local execution
SELECT count(*) FROM distributed_table WHERE key = 1;
-- initial size
SELECT count(*) FROM distributed_table;
COPY distributed_table FROM STDIN WITH delimiter '[';
1[10
2[30
3[40
\.
-- new size
SELECT count(*) FROM distributed_table;
ROLLBACK;


-- multiple local copies
BEGIN;
COPY distributed_table FROM STDIN WITH delimiter ',';
1,15
2,20
3,30
\.
COPY distributed_table FROM STDIN WITH delimiter ',';
10,15
20,20
30,30
\.
COPY distributed_table FROM STDIN WITH delimiter ',';
100,15
200,20
300,30
\.
ROLLBACK;

-- local copy followed by local copy should see the changes
-- and error since it is a duplicate primary key.
BEGIN;
COPY distributed_table FROM STDIN WITH delimiter ',';
1,15
\.
COPY distributed_table FROM STDIN WITH delimiter ',';
1,16
\.
ROLLBACK;


-- local copy followed by local copy should see the changes
BEGIN;
COPY distributed_table FROM STDIN WITH delimiter ',';
1,15
\.
-- select should see the change
SELECT key FROM distributed_table WHERE key = 1;
ROLLBACK;

\c - - - :master_port

SET search_path TO local_shard_copy;
SET citus.log_local_commands TO ON;

TRUNCATE TABLE reference_table;
TRUNCATE TABLE local_table;

SELECT count(*) FROM reference_table, local_table WHERE reference_table.key = local_table.key;

SET citus.enable_local_execution = 'on';

BEGIN;
-- copy should be executed locally
COPY reference_table FROM STDIN;
1
2
3
4
\.
ROLLBACK;

SET citus.enable_local_execution = 'off';

BEGIN;
-- copy should not be executed locally as citus.enable_local_execution = off
COPY reference_table FROM STDIN;
1
2
3
4
\.
ROLLBACK;

SET citus.enable_local_execution = 'on';

CREATE TABLE ref_table(a int);
INSERT INTO ref_table VALUES(1);

BEGIN;
-- trigger local execution
SELECT COUNT(*) FROM reference_table;
-- shard creation should be done locally
SELECT create_reference_table('ref_table');
INSERT INTO ref_table VALUES(2);

-- verify that it worked.
SELECT COUNT(*) FROM ref_table;
ROLLBACK;

SET client_min_messages TO ERROR;
SET search_path TO public;
DROP SCHEMA local_shard_copy CASCADE;
