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
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);

CREATE TABLE test_table(id int, time date);
SELECT create_distributed_table('test_table', 'id');

-- first show that the views does not show
-- any shards on the coordinator as expected
SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'mx_hide_shard_names';
SELECT * FROM citus_shard_indexes_on_worker WHERE "Schema" = 'mx_hide_shard_names';

-- now show that we see the shards, but not the
-- indexes as there are no indexes
\c postgresql://postgres@localhost::worker_1_port/regression?application_name=psql
SET search_path TO 'mx_hide_shard_names';
SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'mx_hide_shard_names' ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker WHERE "Schema" = 'mx_hide_shard_names' ORDER BY 2;

-- make sure that pg_class queries do not get blocked on table locks
begin;
SET LOCAL citus.enable_ddl_propagation TO OFF;
lock table test_table in access exclusive mode;

prepare transaction 'take-aggressive-lock';

-- shards are hidden when using psql as application_name
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;
-- Even when using subquery and having no existing quals on pg_clcass
SELECT relname FROM (SELECT relname, relnamespace FROM pg_catalog.pg_class) AS q WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- Check that inserts into pg_class don't add the filter
EXPLAIN (COSTS OFF) INSERT INTO pg_class VALUES (1);
-- Unless it's an INSERT SELECT that queries from pg_class;
EXPLAIN (COSTS OFF) INSERT INTO pg_class SELECT * FROM pg_class;

-- Check that query that psql "\d test_table" does gets optimized to an index
-- scan
EXPLAIN (COSTS OFF) SELECT c.oid,
  n.nspname,
  c.relname
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname OPERATOR(pg_catalog.~) '^(test_table)$' COLLATE pg_catalog.default
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 2, 3;

commit prepared 'take-aggressive-lock';

-- now create an index
\c - - - :master_port
SET search_path TO 'mx_hide_shard_names';
CREATE INDEX test_index ON mx_hide_shard_names.test_table(id);

-- now show that we see the shards, and the
-- indexes as well
\c postgresql://postgres@localhost::worker_1_port/regression?application_name=psql
SET search_path TO 'mx_hide_shard_names';
SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'mx_hide_shard_names' ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker WHERE "Schema" = 'mx_hide_shard_names' ORDER BY 2;

-- shards are hidden when using psql as application_name
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- changing application_name reveals the shards
SET application_name TO 'pg_regress';
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;
RESET application_name;

-- shards are hidden again after GUCs are reset
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- changing application_name in transaction reveals the shards
BEGIN;
SET LOCAL application_name TO 'pg_regress';
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;
ROLLBACK;

-- shards are hidden again after GUCs are reset
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- now with session-level GUC, but ROLLBACK;
BEGIN;
SET application_name TO 'pg_regress';
ROLLBACK;

-- shards are hidden again after GUCs are reset
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- we should hide correctly based on application_name with savepoints
BEGIN;
SAVEPOINT s1;
SET application_name TO 'pg_regress';
-- changing application_name reveals the shards
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;
ROLLBACK TO SAVEPOINT s1;
-- shards are hidden again after GUCs are reset
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;
ROLLBACK;

-- changing citus.show_shards_for_app_name_prefix reveals the shards
BEGIN;
SET LOCAL citus.show_shards_for_app_name_prefixes TO 'psql';
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;
ROLLBACK;

-- shards are hidden again after GUCs are reset
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- we should be able to select from the shards directly if we
-- know the name of the tables
SELECT count(*) FROM test_table_1130000;

-- shards on the search_path still match pg_table_is_visible
SELECT pg_table_is_visible('test_table_1130000'::regclass);

-- shards on the search_path do not match citus_table_is_visible
SELECT citus_table_is_visible('test_table_1130000'::regclass);

\c - - - :master_port
-- make sure that we're resilient to the edge cases
-- such that the table name includes the shard number
SET search_path TO 'mx_hide_shard_names';
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

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

SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'mx_hide_shard_names' ORDER BY 2;

\d

\c - - - :master_port
-- make sure that don't mess up with schemas
CREATE SCHEMA mx_hide_shard_names_2;
SET search_path TO 'mx_hide_shard_names_2';
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
CREATE TABLE test_table(id int, time date);
SELECT create_distributed_table('test_table', 'id');
CREATE INDEX test_index ON mx_hide_shard_names_2.test_table(id);

\c - - - :worker_1_port
SET search_path TO 'mx_hide_shard_names';
SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'mx_hide_shard_names' ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker WHERE "Schema" = 'mx_hide_shard_names' ORDER BY 2;
SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'mx_hide_shard_names_2' ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker WHERE "Schema" = 'mx_hide_shard_names_2' ORDER BY 2;

-- now try very long table names
\c - - - :master_port

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA mx_hide_shard_names_3;
SET search_path TO 'mx_hide_shard_names_3';

-- Verify that a table name > 56 characters handled properly.
CREATE TABLE too_long_12345678901234567890123456789012345678901234567890 (
        col1 integer not null,
        col2 integer not null);
SELECT create_distributed_table('too_long_12345678901234567890123456789012345678901234567890', 'col1');

\c - - - :worker_1_port
SET search_path TO 'mx_hide_shard_names_3';
SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'mx_hide_shard_names_3' ORDER BY 2;
\d



-- now try weird schema names
\c - - - :master_port

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA "CiTuS.TeeN";
SET search_path TO "CiTuS.TeeN";

CREATE TABLE "TeeNTabLE.1!?!"(id int, "TeNANt_Id" int);

CREATE INDEX "MyTenantIndex" ON  "CiTuS.TeeN"."TeeNTabLE.1!?!"("TeNANt_Id");
-- create distributed table with weird names
SELECT create_distributed_table('"CiTuS.TeeN"."TeeNTabLE.1!?!"', 'TeNANt_Id');

\c - - - :worker_1_port
SET search_path TO "CiTuS.TeeN";
SELECT * FROM citus_shards_on_worker WHERE "Schema" = 'CiTuS.TeeN' ORDER BY 2;
SELECT * FROM citus_shard_indexes_on_worker WHERE "Schema" = 'CiTuS.TeeN' ORDER BY 2;

\d
\di


\c - - - :worker_1_port
-- re-connect to the worker node and show that only
-- client backends can filter shards
SET search_path TO "CiTuS.TeeN";

-- Create the necessary test utility function
SET citus.enable_metadata_sync TO off;
CREATE OR REPLACE FUNCTION set_backend_type(backend_type int)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';
RESET citus.enable_metadata_sync;

-- the shards and indexes do not show up
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- PG16 added one more backend type B_STANDALONE_BACKEND
-- and also alphabetized the backend types, hence the orders changed
-- Relevant PG16 commit:
-- https://github.com/postgres/postgres/commit/0c679464a837079acc75ff1d45eaa83f79e05690
-- Relevant Pg17 commit:
-- https://github.com/postgres/postgres/commit/067701f57758f9baed5bd9d868539738d77bfa92#diff-afc0ebd67534b71b5b94b29a1387aa6eedffe342a5539f52d686428be323e802
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 17 AS server_version_ge_17 \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16 \gset
\if :server_version_ge_17
  SELECT 1 AS client_backend \gset
  SELECT 4 AS bgworker \gset
  SELECT 5 AS walsender \gset
\elif :server_version_ge_16
  SELECT 4 AS client_backend \gset
  SELECT 5 AS bgworker \gset
  SELECT 12 AS walsender \gset
\else
  SELECT 3 AS client_backend \gset
  SELECT 4 AS bgworker \gset
  SELECT 9 AS walsender \gset
\endif

-- say, we set it to bgworker
-- the shards and indexes do not show up
SELECT set_backend_type(:bgworker);
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- or, we set it to walsender
-- the shards and indexes do not show up
SELECT set_backend_type(:walsender);
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;

-- unless the application name starts with citus_shard
SET application_name = 'citus_shard_move';
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;
RESET application_name;

-- but, client backends to see the shards
SELECT set_backend_type(:client_backend);
SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 'mx_hide_shard_names'::regnamespace ORDER BY relname;


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
