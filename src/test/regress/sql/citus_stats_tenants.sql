CREATE SCHEMA citus_stats_tenants;
SET search_path TO citus_stats_tenants;
SET citus.next_shard_id TO 5797500;
SET citus.shard_replication_factor TO 1;

CREATE OR REPLACE FUNCTION pg_catalog.clean_citus_stats_tenants()
RETURNS VOID
LANGUAGE C
AS 'citus', $$clean_citus_stats_tenants$$;

SELECT result FROM run_command_on_all_nodes('SELECT clean_citus_stats_tenants()');

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

\c - - - :worker_1_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stats_tenants ORDER BY tenant_attribute;
\c - - - :worker_2_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stats_tenants ORDER BY tenant_attribute;
\c - - - :master_port
SET search_path TO citus_stats_tenants;

SELECT result FROM run_command_on_all_nodes('ALTER SYSTEM SET citus.stats_tenants_period TO 3');
SELECT result FROM run_command_on_all_nodes('SELECT pg_reload_conf()');

SELECT count(*)>=0 FROM dist_tbl WHERE a = 1;
SELECT count(*)>=0 FROM dist_tbl WHERE a = 2;

\c - - - :worker_1_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stats_tenants ORDER BY tenant_attribute;
\c - - - :worker_2_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stats_tenants ORDER BY tenant_attribute;
\c - - - :master_port

SELECT pg_sleep (3);

\c - - - :worker_1_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stats_tenants ORDER BY tenant_attribute;
\c - - - :worker_2_port
SELECT tenant_attribute, read_count_in_this_period, read_count_in_last_period, query_count_in_this_period, query_count_in_last_period FROM citus_stats_tenants ORDER BY tenant_attribute;
\c - - - :master_port
SET search_path TO citus_stats_tenants;

SELECT result FROM run_command_on_all_nodes('ALTER SYSTEM SET citus.stats_tenants_period TO 60');
SELECT result FROM run_command_on_all_nodes('SELECT pg_reload_conf()');

-- queries with multiple tenants should not be counted
SELECT count(*)>=0 FROM dist_tbl WHERE a IN (1, 5);

-- queries with reference tables should not be counted
SELECT count(*)>=0 FROM ref_tbl WHERE a = 1;

\c - - - :worker_1_port
SELECT tenant_attribute, query_count_in_this_period FROM citus_stats_tenants ORDER BY tenant_attribute;
\c - - - :master_port
SET search_path TO citus_stats_tenants;

-- queries with multiple tables but one tenant should be counted
SELECT count(*)>=0 FROM dist_tbl, dist_tbl_2 WHERE dist_tbl.a = 1 AND dist_tbl_2.a = 1;
SELECT count(*)>=0 FROM dist_tbl JOIN dist_tbl_2 ON dist_tbl.a = dist_tbl_2.a WHERE dist_tbl.a = 1;

\c - - - :worker_1_port
SELECT tenant_attribute, query_count_in_this_period FROM citus_stats_tenants WHERE tenant_attribute = '1';
\c - - - :master_port
SET search_path TO citus_stats_tenants;

-- test scoring
-- all of these distribution column values are from second worker
SELECT count(*)>=0 FROM dist_tbl WHERE a = 2;
SELECT count(*)>=0 FROM dist_tbl WHERE a = 3;
SELECT count(*)>=0 FROM dist_tbl WHERE a = 4;
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'abcd';

\c - - - :worker_2_port
SELECT tenant_attribute, query_count_in_this_period, score FROM citus_stats_tenants(true) ORDER BY score DESC;
\c - - - :master_port
SET search_path TO citus_stats_tenants;

SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'abcd';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'abcd';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde';

\c - - - :worker_2_port
SELECT tenant_attribute, query_count_in_this_period, score FROM citus_stats_tenants(true) ORDER BY score DESC;
\c - - - :master_port
SET search_path TO citus_stats_tenants;

SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'bcde';
SELECT count(*)>=0 FROM dist_tbl_text WHERE a = 'cdef';

\c - - - :worker_2_port
SELECT tenant_attribute, query_count_in_this_period, score FROM citus_stats_tenants(true) ORDER BY score DESC;
\c - - - :master_port

SET client_min_messages TO ERROR;
DROP SCHEMA citus_stats_tenants CASCADE;
