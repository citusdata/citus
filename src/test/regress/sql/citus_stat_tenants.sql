CREATE SCHEMA citus_stat_tenants;
SET search_path TO citus_stat_tenants;
SET citus.next_shard_id TO 5797500;
SET citus.shard_replication_factor TO 1;

-- make sure that we are tracking the tenant stats
SELECT result FROM run_command_on_all_nodes('SHOW citus.stat_tenants_track');

CREATE OR REPLACE FUNCTION pg_catalog.sleep_until_next_period()
RETURNS VOID
LANGUAGE C
AS 'citus', $$sleep_until_next_period$$;

SELECT citus_stat_tenants_reset();

-- set period to upper limit to prevent stats from being reset
SELECT result FROM run_command_on_all_nodes('ALTER SYSTEM SET citus.stat_tenants_period TO 86400');
SELECT result FROM run_command_on_all_nodes('SELECT pg_reload_conf()');

CREATE TABLE dist_tbl (a INT, b TEXT);
SELECT create_distributed_table('dist_tbl', 'a', shard_count:=4, colocate_with:='none');

CREATE TABLE dist_tbl_2 (a INT, b INT);
SELECT create_distributed_table('dist_tbl_2', 'a', colocate_with:='dist_tbl');

CREATE TABLE dist_tbl_text (a TEXT, b INT);
SELECT create_distributed_table('dist_tbl_text', 'a', shard_count:=4, colocate_with:='none');

CREATE TABLE ref_tbl (a INT, b INT);
SELECT create_reference_table('ref_tbl');

INSERT INTO dist_tbl VALUES (1, 'abcd');
INSERT INTO dist_tbl VALUES (2, 'abcd');
UPDATE dist_tbl SET b = a + 1 WHERE a = 3;
UPDATE dist_tbl SET b = a + 1 WHERE a = 4;
DELETE FROM dist_tbl WHERE a = 5;

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period,
    (cpu_usage_in_this_period>0) AS cpu_is_used_in_this_period, (cpu_usage_in_last_period>0) AS cpu_is_used_in_last_period
FROM citus_stat_tenants(true)
ORDER BY tenant_attribute;

SELECT citus_stat_tenants_reset();

-- queries with multiple tenants should not be counted
SELECT count(*)>=0 FROM dist_tbl WHERE a IN (1, 5);

-- queries with reference tables should not be counted
SELECT count(*)>=0 FROM ref_tbl WHERE a = 1;

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants(true) ORDER BY tenant_attribute;

-- queries with multiple tables but one tenant should be counted
SELECT count(*)>=0 FROM dist_tbl, dist_tbl_2 WHERE dist_tbl.a = 1 AND dist_tbl_2.a = 1;
SELECT count(*)>=0 FROM dist_tbl JOIN dist_tbl_2 ON dist_tbl.a = dist_tbl_2.a WHERE dist_tbl.a = 1;

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants(true) WHERE tenant_attribute = '1';

-- test scoring
-- all of these distribution column values are from second worker
SELECT nodeid AS worker_2_nodeid FROM pg_dist_node WHERE nodeport = :worker_2_port \gset

SELECT count(*)>=0 FROM dist_tbl WHERE a = 2;
SELECT count(*)>=0 FROM dist_tbl WHERE a = 2;
SELECT count(*)>=0 FROM dist_tbl WHERE a = 3;
SELECT count(*)>=0 FROM dist_tbl WHERE a = 3;
SELECT count(*)>=0 FROM dist_tbl WHERE a = 4;
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'abcd';

SELECT tenant_attribute, query_count_in_this_period, score FROM citus_stat_tenants(true) WHERE nodeid = :worker_2_nodeid ORDER BY score DESC, tenant_attribute;

SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'abcd';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'abcd';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'cdef';

SELECT tenant_attribute, query_count_in_this_period, score FROM citus_stat_tenants(true) WHERE nodeid = :worker_2_nodeid ORDER BY score DESC, tenant_attribute;

SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'defg';

SELECT tenant_attribute, query_count_in_this_period, score FROM citus_stat_tenants(true) WHERE nodeid = :worker_2_nodeid ORDER BY score DESC, tenant_attribute;

-- test period passing
\c - - - :worker_1_port

SET search_path TO citus_stat_tenants;
SET citus.stat_tenants_period TO 2;
SELECT citus_stat_tenants_reset();
SELECT sleep_until_next_period();

SELECT count(*)>=0 FROM dist_tbl WHERE a = 1;
INSERT INTO dist_tbl VALUES (5, 'abcd');

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period,
    (cpu_usage_in_this_period>0) AS cpu_is_used_in_this_period, (cpu_usage_in_last_period>0) AS cpu_is_used_in_last_period
FROM citus_stat_tenants_local
ORDER BY tenant_attribute;

-- simulate passing the period
SELECT sleep_until_next_period();
SELECT pg_sleep(1);

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period,
    (cpu_usage_in_this_period>0) AS cpu_is_used_in_this_period, (cpu_usage_in_last_period>0) AS cpu_is_used_in_last_period
FROM citus_stat_tenants_local
ORDER BY tenant_attribute;

SELECT sleep_until_next_period();
SELECT pg_sleep(1);

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period,
    (cpu_usage_in_this_period>0) AS cpu_is_used_in_this_period, (cpu_usage_in_last_period>0) AS cpu_is_used_in_last_period
FROM citus_stat_tenants_local
ORDER BY tenant_attribute;

\c - - - :master_port
SET search_path TO citus_stat_tenants;

-- test logs
SET client_min_messages TO LOG;
SELECT count(*)>=0 FROM citus_stat_tenants;
SET citus.stat_tenants_log_level TO ERROR;
SELECT count(*)>=0 FROM citus_stat_tenants;
SET citus.stat_tenants_log_level TO OFF;
SELECT count(*)>=0 FROM citus_stat_tenants;
SET citus.stat_tenants_log_level TO LOG;
SELECT count(*)>=0 FROM citus_stat_tenants;
SET citus.stat_tenants_log_level TO DEBUG;
SELECT count(*)>=0 FROM citus_stat_tenants;
RESET client_min_messages;

SELECT citus_stat_tenants_reset();

-- test turning monitoring on/off
SET citus.stat_tenants_track TO "NONE";
SELECT count(*)>=0 FROM dist_tbl WHERE a = 1;
INSERT INTO dist_tbl VALUES (1, 1);

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants;

SET citus.stat_tenants_track TO "ALL";

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants;

SELECT count(*)>=0 FROM dist_tbl WHERE a = 1;
INSERT INTO dist_tbl VALUES (1, 1);

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants;

-- test special and multibyte characters in tenant attribute
SELECT citus_stat_tenants_reset();
TRUNCATE TABLE dist_tbl_text;

SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/*bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/b*cde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/b*c/de';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'b/*//cde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/b/*/cde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/b/**/cde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde*';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde*/';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = U&'\0061\0308bc';

\c - - - :worker_1_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants(true) ORDER BY tenant_attribute;
\c - - - :worker_2_port
SET search_path TO citus_stat_tenants;

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants(true) ORDER BY tenant_attribute;

SELECT citus_stat_tenants_reset();

-- test local queries
-- all of these distribution column values are from second worker

SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/b*c/de';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = U&'\0061\0308bc';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde*';

DELETE FROM dist_tbl_text WHERE a = '/b*c/de';
DELETE FROM dist_tbl_text WHERE a = '/bcde';
DELETE FROM dist_tbl_text WHERE a = U&'\0061\0308bc';
DELETE FROM dist_tbl_text WHERE a = 'bcde*';

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants_local(true) ORDER BY tenant_attribute;

-- test local cached queries & prepared statements

PREPARE dist_tbl_text_select_plan (text) AS SELECT count(*)>=0 FROM dist_tbl_text WHERE a = $1;

EXECUTE dist_tbl_text_select_plan('/b*c/de');
EXECUTE dist_tbl_text_select_plan('/bcde');
EXECUTE dist_tbl_text_select_plan(U&'\0061\0308bc');
EXECUTE dist_tbl_text_select_plan('bcde*');
EXECUTE dist_tbl_text_select_plan('/b*c/de');
EXECUTE dist_tbl_text_select_plan('/bcde');
EXECUTE dist_tbl_text_select_plan(U&'\0061\0308bc');
EXECUTE dist_tbl_text_select_plan('bcde*');
EXECUTE dist_tbl_text_select_plan('/b*c/de');
EXECUTE dist_tbl_text_select_plan('/bcde');
EXECUTE dist_tbl_text_select_plan(U&'\0061\0308bc');
EXECUTE dist_tbl_text_select_plan('bcde*');

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants_local(true) ORDER BY tenant_attribute;

\c - - - :master_port
SET search_path TO citus_stat_tenants;

PREPARE dist_tbl_text_select_plan (text) AS SELECT count(*)>=0 FROM dist_tbl_text WHERE a = $1;

EXECUTE dist_tbl_text_select_plan('/b*c/de');
EXECUTE dist_tbl_text_select_plan('/bcde');
EXECUTE dist_tbl_text_select_plan(U&'\0061\0308bc');
EXECUTE dist_tbl_text_select_plan('bcde*');
EXECUTE dist_tbl_text_select_plan('/b*c/de');
EXECUTE dist_tbl_text_select_plan('/bcde');
EXECUTE dist_tbl_text_select_plan(U&'\0061\0308bc');
EXECUTE dist_tbl_text_select_plan('bcde*');
EXECUTE dist_tbl_text_select_plan('/b*c/de');
EXECUTE dist_tbl_text_select_plan('/bcde');
EXECUTE dist_tbl_text_select_plan(U&'\0061\0308bc');
EXECUTE dist_tbl_text_select_plan('bcde*');

\c - - - :worker_2_port
SET search_path TO citus_stat_tenants;

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants(true) ORDER BY tenant_attribute;

\c - - - :master_port
SET search_path TO citus_stat_tenants;

SELECT citus_stat_tenants_reset();
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'thisisaveryloooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongname';
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;

-- test role permissions
CREATE ROLE stats_non_superuser WITH LOGIN;
SET ROLE stats_non_superuser;

SELECT count(*)>=0 FROM citus_stat_tenants;
SELECT count(*)>=0 FROM citus_stat_tenants_local;
SELECT count(*)>=0 FROM citus_stat_tenants();
SELECT count(*)>=0 FROM citus_stat_tenants_local();

RESET ROLE;
GRANT pg_monitor TO stats_non_superuser;

SET ROLE stats_non_superuser;

SELECT count(*)>=0 FROM citus_stat_tenants;
SELECT count(*)>=0 FROM citus_stat_tenants_local;
SELECT count(*)>=0 FROM citus_stat_tenants();
SELECT count(*)>=0 FROM citus_stat_tenants_local();

RESET ROLE;
DROP ROLE stats_non_superuser;

-- test function push down
CREATE OR REPLACE FUNCTION
  select_from_dist_tbl_text(p_keyword text)
RETURNS boolean LANGUAGE plpgsql AS $fn$
BEGIN
  RETURN(SELECT count(*)>=0 FROM citus_stat_tenants.dist_tbl_text WHERE a = $1);
END;
$fn$;

SELECT create_distributed_function(
  'select_from_dist_tbl_text(text)', 'p_keyword', colocate_with => 'dist_tbl_text'
);

SELECT citus_stat_tenants_reset();

SELECT select_from_dist_tbl_text('/b*c/de');
SELECT select_from_dist_tbl_text('/b*c/de');
SELECT select_from_dist_tbl_text(U&'\0061\0308bc');
SELECT select_from_dist_tbl_text(U&'\0061\0308bc');

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants ORDER BY tenant_attribute;

CREATE OR REPLACE PROCEDURE select_from_dist_tbl_text_proc(
   p_keyword text
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM select_from_dist_tbl_text(p_keyword);
    PERFORM count(*)>=0 FROM citus_stat_tenants.dist_tbl_text WHERE b < 0;
    PERFORM count(*)>=0 FROM citus_stat_tenants.dist_tbl_text;
    PERFORM count(*)>=0 FROM citus_stat_tenants.dist_tbl_text WHERE a = p_keyword;
    COMMIT;
END;$$;

CALL citus_stat_tenants.select_from_dist_tbl_text_proc('/b*c/de');
CALL citus_stat_tenants.select_from_dist_tbl_text_proc('/b*c/de');
CALL citus_stat_tenants.select_from_dist_tbl_text_proc('/b*c/de');
CALL citus_stat_tenants.select_from_dist_tbl_text_proc(U&'\0061\0308bc');
CALL citus_stat_tenants.select_from_dist_tbl_text_proc(U&'\0061\0308bc');
CALL citus_stat_tenants.select_from_dist_tbl_text_proc(U&'\0061\0308bc');
CALL citus_stat_tenants.select_from_dist_tbl_text_proc(NULL);

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants ORDER BY tenant_attribute;

CREATE OR REPLACE VIEW
  select_from_dist_tbl_text_view
AS
  SELECT * FROM citus_stat_tenants.dist_tbl_text;

SELECT count(*)>=0 FROM select_from_dist_tbl_text_view WHERE a = '/b*c/de';
SELECT count(*)>=0 FROM select_from_dist_tbl_text_view WHERE a = '/b*c/de';
SELECT count(*)>=0 FROM select_from_dist_tbl_text_view WHERE a = '/b*c/de';
SELECT count(*)>=0 FROM select_from_dist_tbl_text_view WHERE a = U&'\0061\0308bc';
SELECT count(*)>=0 FROM select_from_dist_tbl_text_view WHERE a = U&'\0061\0308bc';
SELECT count(*)>=0 FROM select_from_dist_tbl_text_view WHERE a = U&'\0061\0308bc';

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants ORDER BY tenant_attribute;

-- single shard distributed table, which is not part of a tenant schema
SELECT citus_stat_tenants_reset();

SET citus.shard_replication_factor TO 1;
CREATE TABLE dist_tbl_text_single_shard(a text, b int);
select create_distributed_table('dist_tbl_text_single_shard', NULL);

INSERT INTO dist_tbl_text_single_shard VALUES ('/b*c/de', 1);
SELECT count(*)>=0 FROM dist_tbl_text_single_shard WHERE a = '/b*c/de';
DELETE FROM dist_tbl_text_single_shard WHERE a = '/b*c/de';
UPDATE dist_tbl_text_single_shard SET b = 1 WHERE a = '/b*c/de';

SELECT tenant_attribute, query_count_in_this_period FROM citus_stat_tenants;

-- schema based tenants
SELECT citus_stat_tenants_reset();

SET citus.enable_schema_based_sharding TO ON;

CREATE SCHEMA citus_stat_tenants_t1;
CREATE TABLE citus_stat_tenants_t1.users(id int);

SELECT id FROM citus_stat_tenants_t1.users WHERE id = 2;
INSERT INTO citus_stat_tenants_t1.users VALUES (1);
UPDATE citus_stat_tenants_t1.users SET id = 2 WHERE id = 1;
DELETE FROM citus_stat_tenants_t1.users WHERE id = 2;

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;

SELECT citus_stat_tenants_reset();

PREPARE schema_tenant_insert_plan (int) AS insert into citus_stat_tenants_t1.users values ($1);
EXECUTE schema_tenant_insert_plan(1);

PREPARE schema_tenant_select_plan (int) AS SELECT count(*) > 1 FROM citus_stat_tenants_t1.users where Id = $1;
EXECUTE schema_tenant_select_plan(1);

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;

SELECT citus_stat_tenants_reset();

-- local execution & prepared statements
\c - - - :worker_2_port
SET search_path TO citus_stat_tenants;

PREPARE schema_tenant_insert_plan (int) AS insert into citus_stat_tenants_t1.users values ($1);
EXECUTE schema_tenant_insert_plan(1);
EXECUTE schema_tenant_insert_plan(1);
EXECUTE schema_tenant_insert_plan(1);
EXECUTE schema_tenant_insert_plan(1);
EXECUTE schema_tenant_insert_plan(1);

PREPARE schema_tenant_select_plan (int) AS SELECT count(*) > 1 FROM citus_stat_tenants_t1.users where Id = $1;
EXECUTE schema_tenant_select_plan(1);
EXECUTE schema_tenant_select_plan(1);
EXECUTE schema_tenant_select_plan(1);
EXECUTE schema_tenant_select_plan(1);
EXECUTE schema_tenant_select_plan(1);

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;

\c - - - :master_port
SET search_path TO citus_stat_tenants;

SET citus.enable_schema_based_sharding TO OFF;

SELECT citus_stat_tenants_reset();

-- test sampling
-- set rate to 0 to disable sampling
SELECT result FROM run_command_on_all_nodes('ALTER SYSTEM set citus.stat_tenants_untracked_sample_rate to 0;');
SELECT result FROM run_command_on_all_nodes('SELECT pg_reload_conf()');

INSERT INTO dist_tbl VALUES (1, 'abcd');
INSERT INTO dist_tbl VALUES (2, 'abcd');
UPDATE dist_tbl SET b = a + 1 WHERE a = 3;
UPDATE dist_tbl SET b = a + 1 WHERE a = 4;
DELETE FROM dist_tbl WHERE a = 5;

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;

-- test sampling
-- set rate to 1 to track all tenants
SELECT result FROM run_command_on_all_nodes('ALTER SYSTEM set citus.stat_tenants_untracked_sample_rate to 1;');
SELECT result FROM run_command_on_all_nodes('SELECT pg_reload_conf()');

SELECT sleep_until_next_period();
SELECT pg_sleep(0.1);

INSERT INTO dist_tbl VALUES (1, 'abcd');
INSERT INTO dist_tbl VALUES (2, 'abcd');
UPDATE dist_tbl SET b = a + 1 WHERE a = 3;
UPDATE dist_tbl SET b = a + 1 WHERE a = 4;
DELETE FROM dist_tbl WHERE a = 5;

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period,
    (cpu_usage_in_this_period>0) AS cpu_is_used_in_this_period, (cpu_usage_in_last_period>0) AS cpu_is_used_in_last_period
FROM citus_stat_tenants(true)
ORDER BY tenant_attribute;

SET client_min_messages TO ERROR;
DROP SCHEMA citus_stat_tenants CASCADE;
DROP SCHEMA citus_stat_tenants_t1 CASCADE;
