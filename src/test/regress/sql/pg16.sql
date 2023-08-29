--
-- PG16
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16
\gset
\if :server_version_ge_16
\else
\q
\endif

CREATE SCHEMA pg16;
SET search_path TO pg16;
SET citus.next_shard_id TO 950000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1400000;
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;

-- test the new vacuum and analyze options
-- Relevant PG commits:
-- https://github.com/postgres/postgres/commit/1cbbee03385763b066ae3961fc61f2cd01a0d0d7
-- https://github.com/postgres/postgres/commit/4211fbd8413b26e0abedbe4338aa7cda2cd469b4
-- https://github.com/postgres/postgres/commit/a46a7011b27188af526047a111969f257aaf4db8

CREATE TABLE t1 (a int);
SELECT create_distributed_table('t1','a');
SET citus.log_remote_commands TO ON;

VACUUM (PROCESS_MAIN FALSE) t1;
VACUUM (PROCESS_MAIN FALSE, PROCESS_TOAST FALSE) t1;
VACUUM (PROCESS_MAIN TRUE) t1;
VACUUM (PROCESS_MAIN FALSE, FULL) t1;
VACUUM (SKIP_DATABASE_STATS) t1;
VACUUM (ONLY_DATABASE_STATS) t1;
VACUUM (BUFFER_USAGE_LIMIT '512 kB') t1;
VACUUM (BUFFER_USAGE_LIMIT 0) t1;
VACUUM (BUFFER_USAGE_LIMIT 16777220) t1;
VACUUM (BUFFER_USAGE_LIMIT -1) t1;
VACUUM (BUFFER_USAGE_LIMIT 'test') t1;
ANALYZE (BUFFER_USAGE_LIMIT '512 kB') t1;
ANALYZE (BUFFER_USAGE_LIMIT 0) t1;

SET citus.log_remote_commands TO OFF;

-- only verifying it works and not printing log
-- remote commands because it can be flaky
VACUUM (ONLY_DATABASE_STATS);

-- New GENERIC_PLAN option in EXPLAIN
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/3c05284

CREATE TABLE tenk1 (
	unique1 int4,
	unique2 int4,
    thousand int4
);
SELECT create_distributed_table('tenk1', 'unique1');

SET citus.log_remote_commands TO on;
EXPLAIN (GENERIC_PLAN) SELECT unique1 FROM tenk1 WHERE thousand = 1000;
EXPLAIN (GENERIC_PLAN, ANALYZE) SELECT unique1 FROM tenk1 WHERE thousand = 1000;
SET citus.log_remote_commands TO off;

-- Proper error when creating statistics without a name on a Citus table
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/624aa2a13bd02dd584bb0995c883b5b93b2152df

CREATE TABLE test_stats (
    a   int,
    b   int
);

SELECT create_distributed_table('test_stats', 'a');

CREATE STATISTICS (dependencies) ON a, b FROM test_stats;
CREATE STATISTICS (ndistinct, dependencies) on a, b from test_stats;
CREATE STATISTICS (ndistinct, dependencies, mcv) on a, b from test_stats;

-- STORAGE option in CREATE is already propagated by Citus
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/784cedd
CREATE TABLE test_storage (a text, c text STORAGE plain);
SELECT create_distributed_table('test_storage', 'a', shard_count := 2);
SELECT result FROM run_command_on_all_nodes
($$ SELECT array_agg(DISTINCT (attname, attstorage)) FROM pg_attribute
    WHERE attrelid::regclass::text ILIKE 'pg16.test_storage%' AND attnum > 0;$$) ORDER BY 1;

SELECT alter_distributed_table('test_storage', shard_count := 4);
SELECT result FROM run_command_on_all_nodes
($$ SELECT array_agg(DISTINCT (attname, attstorage)) FROM pg_attribute
    WHERE attrelid::regclass::text ILIKE 'pg16.test_storage%' AND attnum > 0;$$) ORDER BY 1;

SELECT undistribute_table('test_storage');
SELECT result FROM run_command_on_all_nodes
($$ SELECT array_agg(DISTINCT (attname, attstorage)) FROM pg_attribute
    WHERE attrelid::regclass::text ILIKE 'pg16.test_storage%' AND attnum > 0;$$) ORDER BY 1;

-- New option to change storage to DEFAULT in PG16
-- ALTER TABLE .. ALTER COLUMN .. SET STORAGE is already
-- not supported by Citus, so this is also not supported
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/b9424d0
SELECT create_distributed_table('test_storage', 'a');
ALTER TABLE test_storage ALTER a SET STORAGE default;

--
-- COPY FROM ... DEFAULT
-- Already supported in Citus, adding all PG tests with a distributed table
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/9f8377f
CREATE TABLE copy_default (
	id integer PRIMARY KEY,
	text_value text NOT NULL DEFAULT 'test',
	ts_value timestamp without time zone NOT NULL DEFAULT '2022-07-05'
);
SELECT create_distributed_table('copy_default', 'id');

-- if DEFAULT is not specified, then the marker will be regular data
COPY copy_default FROM stdin;
1	value	'2022-07-04'
2	\D	'2022-07-05'
\.
SELECT * FROM copy_default ORDER BY id;
TRUNCATE copy_default;

COPY copy_default FROM stdin WITH (format csv);
1,value,2022-07-04
2,\D,2022-07-05
\.
SELECT * FROM copy_default ORDER BY id;
TRUNCATE copy_default;

-- DEFAULT cannot be used in binary mode
COPY copy_default FROM stdin WITH (format binary, default '\D');

-- DEFAULT cannot be new line nor carriage return
COPY copy_default FROM stdin WITH (default E'\n');
COPY copy_default FROM stdin WITH (default E'\r');

-- DELIMITER cannot appear in DEFAULT spec
COPY copy_default FROM stdin WITH (delimiter ';', default 'test;test');

-- CSV quote cannot appear in DEFAULT spec
COPY copy_default FROM stdin WITH (format csv, quote '"', default 'test"test');

-- NULL and DEFAULT spec must be different
COPY copy_default FROM stdin WITH (default '\N');

-- cannot use DEFAULT marker in column that has no DEFAULT value
COPY copy_default FROM stdin WITH (default '\D');
\D	value	'2022-07-04'
2	\D	'2022-07-05'
\.

COPY copy_default FROM stdin WITH (format csv, default '\D');
\D,value,2022-07-04
2,\D,2022-07-05
\.

-- The DEFAULT marker must be unquoted and unescaped or it's not recognized
COPY copy_default FROM stdin WITH (default '\D');
1	\D	'2022-07-04'
2	\\D	'2022-07-04'
3	"\D"	'2022-07-04'
\.
SELECT * FROM copy_default ORDER BY id;
TRUNCATE copy_default;

COPY copy_default FROM stdin WITH (format csv, default '\D');
1,\D,2022-07-04
2,\\D,2022-07-04
3,"\D",2022-07-04
\.
SELECT * FROM copy_default ORDER BY id;
TRUNCATE copy_default;

-- successful usage of DEFAULT option in COPY
COPY copy_default FROM stdin WITH (default '\D');
1	value	'2022-07-04'
2	\D	'2022-07-03'
3	\D	\D
\.
SELECT * FROM copy_default ORDER BY id;
TRUNCATE copy_default;

COPY copy_default FROM stdin WITH (format csv, default '\D');
1,value,2022-07-04
2,\D,2022-07-03
3,\D,\D
\.
SELECT * FROM copy_default ORDER BY id;
TRUNCATE copy_default;

\c - - - :worker_1_port
COPY pg16.copy_default FROM stdin WITH (format csv, default '\D');
1,value,2022-07-04
2,\D,2022-07-03
3,\D,\D
\.
SELECT * FROM pg16.copy_default ORDER BY id;

\c - - - :master_port
TRUNCATE pg16.copy_default;

\c - - - :worker_2_port
COPY pg16.copy_default FROM stdin WITH (format csv, default '\D');
1,value,2022-07-04
2,\D,2022-07-03
3,\D,\D
\.
SELECT * FROM pg16.copy_default ORDER BY id;

\c - - - :master_port
SET search_path TO pg16;
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;

-- DEFAULT cannot be used in COPY TO
COPY (select 1 as test) TO stdout WITH (default '\D');

-- Tests for SQL/JSON: support the IS JSON predicate
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/6ee30209

CREATE TABLE test_is_json (id bigserial, js text);
SELECT create_distributed_table('test_is_json', 'id');

INSERT INTO test_is_json(js) VALUES
 (NULL),
 (''),
 ('123'),
 ('"aaa "'),
 ('true'),
 ('null'),
 ('[]'),
 ('[1, "2", {}]'),
 ('{}'),
 ('{ "a": 1, "b": null }'),
 ('{ "a": 1, "a": null }'),
 ('{ "a": 1, "b": [{ "a": 1 }, { "a": 2 }] }'),
 ('{ "a": 1, "b": [{ "a": 1, "b": 0, "a": 2 }] }'),
 ('aaa'),
 ('{a:1}'),
 ('["a",]');

-- run IS JSON predicate in the worker nodes
SELECT
	js,
	js IS JSON "JSON",
	js IS NOT JSON "NOT JSON",
	js IS JSON VALUE "VALUE",
	js IS JSON OBJECT "OBJECT",
	js IS JSON ARRAY "ARRAY",
	js IS JSON SCALAR "SCALAR",
	js IS JSON WITHOUT UNIQUE KEYS "WITHOUT UNIQUE",
	js IS JSON WITH UNIQUE KEYS "WITH UNIQUE"
FROM
	test_is_json ORDER BY js;

-- pull the data, and run IS JSON predicate in the coordinator
WITH pulled_data as (SELECT js FROM test_is_json OFFSET 0)
SELECT
	js,
	js IS JSON "IS JSON",
	js IS NOT JSON "IS NOT JSON",
	js IS JSON VALUE "IS VALUE",
	js IS JSON OBJECT "IS OBJECT",
	js IS JSON ARRAY "IS ARRAY",
	js IS JSON SCALAR "IS SCALAR",
	js IS JSON WITHOUT UNIQUE KEYS "WITHOUT UNIQUE",
	js IS JSON WITH UNIQUE KEYS "WITH UNIQUE"
FROM
    pulled_data ORDER BY js;

SELECT
	js,
	js IS JSON "IS JSON",
	js IS NOT JSON "IS NOT JSON",
	js IS JSON VALUE "IS VALUE",
	js IS JSON OBJECT "IS OBJECT",
	js IS JSON ARRAY "IS ARRAY",
	js IS JSON SCALAR "IS SCALAR",
	js IS JSON WITHOUT UNIQUE KEYS "WITHOUT UNIQUE",
	js IS JSON WITH UNIQUE KEYS "WITH UNIQUE"
FROM
	(SELECT js::json FROM test_is_json WHERE js IS JSON) foo(js);

SELECT
	js0,
	js IS JSON "IS JSON",
	js IS NOT JSON "IS NOT JSON",
	js IS JSON VALUE "IS VALUE",
	js IS JSON OBJECT "IS OBJECT",
	js IS JSON ARRAY "IS ARRAY",
	js IS JSON SCALAR "IS SCALAR",
	js IS JSON WITHOUT UNIQUE KEYS "WITHOUT UNIQUE",
	js IS JSON WITH UNIQUE KEYS "WITH UNIQUE"
FROM
	(SELECT js, js::bytea FROM test_is_json WHERE js IS JSON) foo(js0, js);

SELECT
	js,
	js IS JSON "IS JSON",
	js IS NOT JSON "IS NOT JSON",
	js IS JSON VALUE "IS VALUE",
	js IS JSON OBJECT "IS OBJECT",
	js IS JSON ARRAY "IS ARRAY",
	js IS JSON SCALAR "IS SCALAR",
	js IS JSON WITHOUT UNIQUE KEYS "WITHOUT UNIQUE",
	js IS JSON WITH UNIQUE KEYS "WITH UNIQUE"
FROM
	(SELECT js::jsonb FROM test_is_json WHERE js IS JSON) foo(js);

-- SYSTEM_USER
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/0823d061

CREATE TABLE table_name_for_view(id int, val_1 text);
SELECT create_distributed_table('table_name_for_view', 'id');
INSERT INTO table_name_for_view VALUES (1, 'test');

-- define a view that uses SYSTEM_USER keyword
CREATE VIEW prop_view_1 AS
    SELECT *, SYSTEM_USER AS su FROM table_name_for_view;
SELECT * FROM prop_view_1;

-- check definition with SYSTEM_USER is correctly propagated to workers
\c - - - :worker_1_port
SELECT pg_get_viewdef('pg16.prop_view_1', true);

\c - - - :master_port
SET search_path TO pg16;

\set VERBOSITY terse
SET client_min_messages TO ERROR;
DROP SCHEMA pg16 CASCADE;

--
-- PG16 allows GRANT WITH ADMIN | INHERIT | SET
--
-- GRANT privileges to a role or roles  
\c - - - :master_port
CREATE ROLE create_role;
CREATE ROLE create_role_2;
CREATE ROLE create_role_3;
CREATE ROLE create_role_4;
CREATE USER create_user;
CREATE USER create_user_2;
CREATE GROUP create_group;
CREATE GROUP create_group_2;

--test grant role
GRANT create_group TO create_role;
GRANT create_group TO create_role_2 WITH ADMIN OPTION;
GRANT create_group TO create_role_3 WITH INHERIT;
GRANT create_group TO create_role_4 WITH SET; 

-- ADMIN role can perfom administrative tasks 
-- role can now access the data and permissions of the table (owner of table)
-- role can change current user to any other user/role that has access 
GRANT ADMIN ON DATABASE db_name TO role_name;
GRANT INHERIT ON TABLE table_name TO role_name;
GRANT SET SESSION AUTHORIZATION TO role_name;

SELECT * FROM table_name WHERE column_name = 'value';

SELECT COUNT(*) FROM table_name WHERE column_name = 'value';