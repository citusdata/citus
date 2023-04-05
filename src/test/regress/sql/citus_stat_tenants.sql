CREATE SCHEMA citus_stat_tenants;
SET search_path TO citus_stat_tenants;
SET citus.next_shard_id TO 5797500;
SET citus.shard_replication_factor TO 1;

CREATE OR REPLACE FUNCTION pg_catalog.sleep_until_next_period()
RETURNS VOID
LANGUAGE C
AS 'citus', $$sleep_until_next_period$$;

SELECT citus_stat_tenants_reset();

-- set period to a high number to prevent stats from being reset
SELECT result FROM run_command_on_all_nodes('ALTER SYSTEM SET citus.stat_tenants_period TO 1000000000');
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

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants(true) ORDER BY tenant_attribute;

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
SELECT citus_stat_tenants_reset();

SELECT count(*)>=0 FROM dist_tbl WHERE a = 1;
INSERT INTO dist_tbl VALUES (5, 'abcd');

\c - - - :worker_1_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants_local ORDER BY tenant_attribute;

-- simulate passing the period
SET citus.stat_tenants_period TO 2;
SELECT sleep_until_next_period();

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants_local ORDER BY tenant_attribute;

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
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;
\c - - - :worker_2_port
SET search_path TO citus_stat_tenants;

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;

SELECT citus_stat_tenants_reset();

-- test local queries
-- all of these distribution column values are from second worker

SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/b*c/de';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = '/bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = U&'\0061\0308bc';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde*';

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants_local ORDER BY tenant_attribute;

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

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants_local ORDER BY tenant_attribute;

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

SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stat_tenants ORDER BY tenant_attribute;

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

SET client_min_messages TO ERROR;
DROP SCHEMA citus_stat_tenants CASCADE;
