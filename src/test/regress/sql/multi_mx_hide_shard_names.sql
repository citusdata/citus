--
-- Hide shard names on MX worker nodes
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1130000;


-- make sure that the signature of the citus_table_is_visible
-- and pg_table_is_visible are the same since the logic
-- relies on that
SELECT
	proname, proisstrict, proretset, provolatile,
	proparallel, pronargs, pronargdefaults ,prorettype,
	proargtypes, proacl
FROM
	pg_proc
WHERE
	proname LIKE '%table_is_visible%'
ORDER BY 1;

CREATE SCHEMA mx_hide_shard_names;
SET search_path TO 'mx_hide_shard_names';

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

SET citus.replication_model TO 'streaming';
SELECT start_metadata_sync_to_node(:'worker_1_host', :worker_1_port);
SELECT start_metadata_sync_to_node(:'worker_2_host', :worker_2_port);

CREATE TABLE test_table(id int, time date);
SELECT create_distributed_table('test_table', 'id');

-- first show that the views does not show
-- any shards on the coordinator as expected
SELECT * FROM citus_shards_on_worker;
SELECT * FROM citus_shard_indexes_on_worker;

-- now show that we see the shards, but not the
-- indexes as there are no indexes
\c - - - :worker_1_port
SET search_path TO 'mx_hide_shard_names';
SELECT * FROM citus_shards_on_worker ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker ORDER BY 2;

-- also show that nested calls to pg_table_is_visible works fine
-- if both of the calls to the pg_table_is_visible haven't been
-- replaced, we would get 0 rows in the output
SELECT
	pg_table_is_visible((SELECT
								"t1"."Name"::regclass
						 FROM
						 	citus_shards_on_worker as t1
						 WHERE
						 	NOT pg_table_is_visible("t1"."Name"::regclass)
						 LIMIT
						 	1));

-- now create an index
\c - - - :master_port
SET search_path TO 'mx_hide_shard_names';
CREATE INDEX test_index ON mx_hide_shard_names.test_table(id);

-- now show that we see the shards, and the
-- indexes as well
\c - - - :worker_1_port
SET search_path TO 'mx_hide_shard_names';
SELECT * FROM citus_shards_on_worker ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker ORDER BY 2;

-- we should be able to select from the shards directly if we
-- know the name of the tables
SELECT count(*) FROM test_table_1130000;

-- disable the config so that table becomes visible
SELECT pg_table_is_visible('test_table_1130000'::regclass);
SET citus.override_table_visibility TO FALSE;
SELECT pg_table_is_visible('test_table_1130000'::regclass);

\c - - - :master_port
-- make sure that we're resilient to the edge cases
-- such that the table name includes the shard number
SET search_path TO 'mx_hide_shard_names';
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

SET citus.replication_model TO 'streaming';

-- not existing shard ids appended to the distributed table name
CREATE TABLE test_table_102008(id int, time date);
SELECT create_distributed_table('test_table_102008', 'id');

\c - - - :worker_1_port
SET search_path TO 'mx_hide_shard_names';

-- existing shard ids appended to a local table name
-- note that we cannot create a distributed or local table
-- with the same name since a table with the same
-- name already exists :)
CREATE TABLE test_table_2_1130000(id int, time date);

SELECT * FROM citus_shards_on_worker ORDER BY 2;

\d

\c - - - :master_port
-- make sure that don't mess up with schemas
CREATE SCHEMA mx_hide_shard_names_2;
SET search_path TO 'mx_hide_shard_names_2';
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

SET citus.replication_model TO 'streaming';
CREATE TABLE test_table(id int, time date);
SELECT create_distributed_table('test_table', 'id');
CREATE INDEX test_index ON mx_hide_shard_names_2.test_table(id);

\c - - - :worker_1_port
SET search_path TO 'mx_hide_shard_names';
SELECT * FROM citus_shards_on_worker ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker ORDER BY 2;
SET search_path TO 'mx_hide_shard_names_2';
SELECT * FROM citus_shards_on_worker ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker ORDER BY 2;
SET search_path TO 'mx_hide_shard_names_2, mx_hide_shard_names';
SELECT * FROM citus_shards_on_worker ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker ORDER BY 2;

-- now try very long table names
\c - - - :master_port

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

SET citus.replication_model TO 'streaming';

CREATE SCHEMA mx_hide_shard_names_3;
SET search_path TO 'mx_hide_shard_names_3';

-- Verify that a table name > 56 characters handled properly.
CREATE TABLE too_long_12345678901234567890123456789012345678901234567890 (
        col1 integer not null,
        col2 integer not null);
SELECT create_distributed_table('too_long_12345678901234567890123456789012345678901234567890', 'col1');

\c - - - :worker_1_port
SET search_path TO 'mx_hide_shard_names_3';
SELECT * FROM citus_shards_on_worker ORDER BY 2;
\d



-- now try weird schema names
\c - - - :master_port

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

SET citus.replication_model TO 'streaming';

CREATE SCHEMA "CiTuS.TeeN";
SET search_path TO "CiTuS.TeeN";

CREATE TABLE "TeeNTabLE.1!?!"(id int, "TeNANt_Id" int);

CREATE INDEX "MyTenantIndex" ON  "CiTuS.TeeN"."TeeNTabLE.1!?!"("TeNANt_Id");
-- create distributed table with weird names
SELECT create_distributed_table('"CiTuS.TeeN"."TeeNTabLE.1!?!"', 'TeNANt_Id');

\c - - - :worker_1_port
SET search_path TO "CiTuS.TeeN";
SELECT * FROM citus_shards_on_worker ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker ORDER BY 2;

\d
\di

-- clean-up
\c - - - :master_port

-- show that common psql functions do not show shards
-- including the ones that are not in the current schema
SET search_path TO 'mx_hide_shard_names';
\d
\di

DROP SCHEMA mx_hide_shard_names CASCADE;
DROP SCHEMA mx_hide_shard_names_2 CASCADE;
DROP SCHEMA mx_hide_shard_names_3 CASCADE;
DROP SCHEMA "CiTuS.TeeN" CASCADE;
