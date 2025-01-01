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
EXPLAIN (GENERIC_PLAN) SELECT unique1 FROM tenk1 WHERE thousand = $1;
EXPLAIN (GENERIC_PLAN, ANALYZE) SELECT unique1 FROM tenk1 WHERE thousand = $1;
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

-- New ICU_RULES option added to CREATE DATABASE
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/30a53b7

CREATE DATABASE test_db WITH LOCALE_PROVIDER = 'icu' LOCALE = '' ICU_RULES = '&a < g' TEMPLATE = 'template0';
SELECT result FROM run_command_on_workers
($$CREATE DATABASE test_db WITH LOCALE_PROVIDER = 'icu' LOCALE = '' ICU_RULES = '&a < g' TEMPLATE = 'template0'$$);

CREATE TABLE test_db_table (a text);
SELECT create_distributed_table('test_db_table', 'a');
INSERT INTO test_db_table VALUES ('Abernathy'), ('apple'), ('bird'), ('Boston'), ('Graham'), ('green');
-- icu default rules order
SELECT * FROM test_db_table ORDER BY a COLLATE "en-x-icu";
-- regression database's default order
SELECT * FROM test_db_table ORDER BY a;

-- now see the order in the new database
\c test_db
CREATE EXTENSION citus;
\c - - - :worker_1_port
CREATE EXTENSION citus;
\c - - - :worker_2_port
CREATE EXTENSION citus;
\c - - - :master_port

SELECT 1 FROM citus_add_node('localhost', :worker_1_port);
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

CREATE TABLE test_db_table (a text);
SELECT create_distributed_table('test_db_table', 'a');
INSERT INTO test_db_table VALUES ('Abernathy'), ('apple'), ('bird'), ('Boston'), ('Graham'), ('green');
-- icu default rules order
SELECT * FROM test_db_table ORDER BY a COLLATE "en-x-icu";
-- test_db database's default order with ICU_RULES = '&a < g'
SELECT * FROM test_db_table ORDER BY a;

\c regression
\c - - - :master_port
DROP DATABASE test_db;
SELECT result FROM run_command_on_workers
($$DROP DATABASE test_db$$);
SET search_path TO pg16;

-- New rules option added to CREATE COLLATION
-- Similar to above test with CREATE DATABASE
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/30a53b7

CREATE COLLATION default_rule (provider = icu, locale = '');
CREATE COLLATION special_rule (provider = icu, locale = '', rules = '&a < g');

CREATE TABLE test_collation_rules (a text);
SELECT create_distributed_table('test_collation_rules', 'a');
INSERT INTO test_collation_rules VALUES ('Abernathy'), ('apple'), ('bird'), ('Boston'), ('Graham'), ('green');

SELECT collname, collprovider, collicurules
FROM pg_collation
WHERE collname like '%_rule%'
ORDER BY 1;

SELECT * FROM test_collation_rules ORDER BY a COLLATE default_rule;
SELECT * FROM test_collation_rules ORDER BY a COLLATE special_rule;

\c - - - :worker_1_port
SET search_path TO pg16;

SELECT collname, collprovider, collicurules
FROM pg_collation
WHERE collname like '%_rule%'
ORDER BY 1;

SELECT * FROM test_collation_rules ORDER BY a COLLATE default_rule;
SELECT * FROM test_collation_rules ORDER BY a COLLATE special_rule;

\c - - - :master_port
SET search_path TO pg16;
SET citus.next_shard_id TO 951000;

-- Foreign table TRUNCATE trigger
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/3b00a94
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);
SET citus.use_citus_managed_tables TO ON;
CREATE TABLE foreign_table_test (id integer NOT NULL, data text, a bigserial);
INSERT INTO foreign_table_test VALUES (1, 'text_test');
CREATE EXTENSION postgres_fdw;
CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', port :'master_port', dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER
        SERVER foreign_server
        OPTIONS (user 'postgres');
CREATE FOREIGN TABLE foreign_table (
        id integer NOT NULL,
        data text,
        a bigserial
)
        SERVER foreign_server
        OPTIONS (schema_name 'pg16', table_name 'foreign_table_test');

-- verify it's a Citus foreign table
SELECT partmethod, repmodel FROM pg_dist_partition
WHERE logicalrelid = 'foreign_table'::regclass ORDER BY logicalrelid;

INSERT INTO foreign_table VALUES (2, 'test_2');
INSERT INTO foreign_table_test VALUES (3, 'test_3');

CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

CREATE FUNCTION trigger_func_on_shard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func_on_shard(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

CREATE TRIGGER trig_stmt_before BEFORE TRUNCATE ON foreign_table
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
SET citus.override_table_visibility TO off;
CREATE TRIGGER trig_stmt_shard_before BEFORE TRUNCATE ON foreign_table_951001
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func_on_shard();
RESET citus.override_table_visibility;

SELECT * FROM foreign_table ORDER BY 1;
TRUNCATE foreign_table;
SELECT * FROM foreign_table ORDER BY 1;

RESET citus.use_citus_managed_tables;

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

-- Tests for SQL/JSON: JSON_ARRAYAGG and JSON_OBJECTAGG aggregates
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/7081ac4
SET citus.next_shard_id TO 952000;

CREATE TABLE agg_test(a int, b serial);
SELECT create_distributed_table('agg_test', 'a');
INSERT INTO agg_test SELECT i FROM generate_series(1, 5) i;

-- JSON_ARRAYAGG with distribution key
SELECT JSON_ARRAYAGG(a ORDER BY a),
JSON_ARRAYAGG(a ORDER BY a RETURNING jsonb)
FROM agg_test;

-- JSON_ARRAYAGG with other column
SELECT JSON_ARRAYAGG(b ORDER BY b),
JSON_ARRAYAGG(b ORDER BY b RETURNING jsonb)
FROM agg_test;

-- JSON_ARRAYAGG with router query
SET citus.log_remote_commands TO on;
SELECT JSON_ARRAYAGG(a ORDER BY a),
JSON_ARRAYAGG(a ORDER BY a RETURNING jsonb)
FROM agg_test WHERE a = 2;
RESET citus.log_remote_commands;

-- JSON_OBJECTAGG with distribution key
SELECT
	JSON_OBJECTAGG(a: a),
    JSON_ARRAYAGG(a ORDER BY a), -- for order
	JSON_OBJECTAGG(a: a RETURNING jsonb)
FROM
	agg_test;

-- JSON_OBJECTAGG with other column
SELECT
	JSON_OBJECTAGG(b: b),
    JSON_ARRAYAGG(b ORDER BY b), -- for order
	JSON_OBJECTAGG(b: b RETURNING jsonb)
FROM
	agg_test;

-- JSON_OBJECTAGG with router query
SET citus.log_remote_commands TO on;
SELECT
	JSON_OBJECTAGG(a: a),
	JSON_OBJECTAGG(a: a RETURNING jsonb)
FROM
	agg_test WHERE a = 3;
RESET citus.log_remote_commands;

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

-- REINDEX DATABASE/SYSTEM name is optional
-- We already don't propagate these commands automatically
-- Testing here with run_command_on_workers
-- Relevant PG commit: https://github.com/postgres/postgres/commit/2cbc3c1

REINDEX DATABASE;
SELECT result FROM run_command_on_workers
($$REINDEX DATABASE$$);

REINDEX SYSTEM;
SELECT result FROM run_command_on_workers
($$REINDEX SYSTEM$$);

--
-- random_normal() to provide normally-distributed random numbers
-- adding here the same tests as the ones with random() in aggregate_support.sql
-- Relevant PG commit: https://github.com/postgres/postgres/commit/38d8176
--

CREATE TABLE dist_table (dist_col int, agg_col numeric);
SELECT create_distributed_table('dist_table', 'dist_col');

CREATE TABLE ref_table (int_col int);
SELECT create_reference_table('ref_table');

-- Test the cases where the worker agg exec. returns no tuples.

SELECT PERCENTILE_DISC(.25) WITHIN GROUP (ORDER BY agg_col)
FROM (SELECT *, random_normal() FROM dist_table) a;

SELECT PERCENTILE_DISC((2 > random_normal(stddev => 1, mean => 0))::int::numeric / 10)
       WITHIN GROUP (ORDER BY agg_col)
FROM dist_table
LEFT JOIN ref_table ON TRUE;

-- run the same queries after loading some data

INSERT INTO dist_table VALUES (2, 11.2), (3, NULL), (6, 3.22), (3, 4.23), (5, 5.25),
                              (4, 63.4), (75, NULL), (80, NULL), (96, NULL), (8, 1078), (0, 1.19);

SELECT PERCENTILE_DISC(.25) WITHIN GROUP (ORDER BY agg_col)
FROM (SELECT *, random_normal() FROM dist_table) a;

SELECT PERCENTILE_DISC((2 > random_normal(stddev => 1, mean => 0))::int::numeric / 10)
       WITHIN GROUP (ORDER BY agg_col)
FROM dist_table
LEFT JOIN ref_table ON TRUE;

--
-- PG16 added WITH ADMIN FALSE option to GRANT ROLE
-- WITH ADMIN FALSE is the default, make sure we propagate correctly in Citus
-- Relevant PG commit: https://github.com/postgres/postgres/commit/e3ce2de
--

CREATE ROLE role1;
CREATE ROLE role2;

SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%GRANT%';
-- default admin option is false
GRANT role1 TO role2;
REVOKE role1 FROM role2;
-- should behave same as default
GRANT role1 TO role2 WITH ADMIN FALSE;
REVOKE role1 FROM role2;
-- with admin option and with admin true are the same
GRANT role1 TO role2 WITH ADMIN OPTION;
REVOKE role1 FROM role2;
GRANT role1 TO role2 WITH ADMIN TRUE;
REVOKE role1 FROM role2;

RESET citus.log_remote_commands;
RESET citus.grep_remote_commands;

--
-- PG16 added new options to GRANT ROLE
-- inherit: https://github.com/postgres/postgres/commit/e3ce2de
-- set: https://github.com/postgres/postgres/commit/3d14e17
-- We don't propagate for now in Citus
--
GRANT role1 TO role2 WITH INHERIT FALSE;
REVOKE role1 FROM role2;
GRANT role1 TO role2 WITH INHERIT TRUE;
REVOKE role1 FROM role2;
GRANT role1 TO role2 WITH INHERIT OPTION;
REVOKE role1 FROM role2;
GRANT role1 TO role2 WITH SET FALSE;
REVOKE role1 FROM role2;
GRANT role1 TO role2 WITH SET TRUE;
REVOKE role1 FROM role2;
GRANT role1 TO role2 WITH SET OPTION;
REVOKE role1 FROM role2;

-- connect to worker node
GRANT role1 TO role2 WITH ADMIN OPTION, INHERIT FALSE, SET FALSE;

SELECT roleid::regrole::text AS role, member::regrole::text,
admin_option, inherit_option, set_option FROM pg_auth_members
WHERE roleid::regrole::text = 'role1' ORDER BY 1, 2;

\c - - - :worker_1_port

SELECT roleid::regrole::text AS role, member::regrole::text,
admin_option, inherit_option, set_option FROM pg_auth_members
WHERE roleid::regrole::text = 'role1' ORDER BY 1, 2;

SET citus.enable_ddl_propagation TO off;
GRANT role1 TO role2 WITH ADMIN OPTION, INHERIT FALSE, SET FALSE;
RESET citus.enable_ddl_propagation;

SELECT roleid::regrole::text AS role, member::regrole::text,
admin_option, inherit_option, set_option FROM pg_auth_members
WHERE roleid::regrole::text = 'role1' ORDER BY 1, 2;

\c - - - :master_port
REVOKE role1 FROM role2;

-- test REVOKES as well
GRANT role1 TO role2;
REVOKE SET OPTION FOR role1 FROM role2;
REVOKE INHERIT OPTION FOR role1 FROM role2;

DROP ROLE role1, role2;

-- test that everything works fine for roles that are not propagated
SET citus.enable_ddl_propagation TO off;
CREATE ROLE role3;
CREATE ROLE role4;
CREATE ROLE role5;
RESET citus.enable_ddl_propagation;
-- by default, admin option is false, inherit is true, set is true
GRANT role3 TO role4;
GRANT role3 TO role5 WITH ADMIN TRUE, INHERIT FALSE, SET FALSE;
SELECT roleid::regrole::text AS role, member::regrole::text, admin_option, inherit_option, set_option FROM pg_auth_members WHERE roleid::regrole::text = 'role3' ORDER BY 1, 2;

DROP ROLE role3, role4, role5;

\set VERBOSITY terse
SET client_min_messages TO ERROR;
DROP EXTENSION postgres_fdw CASCADE;
DROP SCHEMA pg16 CASCADE;
