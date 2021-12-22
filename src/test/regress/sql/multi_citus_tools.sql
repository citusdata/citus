--
-- MULTI CITUS TOOLS
--
-- tests UDFs created for citus tools
--

CREATE SCHEMA tools;
SET SEARCH_PATH TO 'tools';
SET citus.next_shard_id TO 1240000;

-- test with invalid port, prevent OS dependent warning from being displayed
SET client_min_messages to ERROR;

SELECT * FROM master_run_on_worker(ARRAY['localhost']::text[], ARRAY['666']::int[],
								   ARRAY['select count(*) from pg_dist_shard']::text[],
								   false);

SELECT * FROM master_run_on_worker(ARRAY['localhost']::text[], ARRAY['666']::int[],
								   ARRAY['select count(*) from pg_dist_shard']::text[],
								   true);
RESET client_min_messages;

-- store worker node name and port
SELECT quote_literal(node_name) as node_name, node_port as node_port
	FROM master_get_active_worker_nodes()
	ORDER BY node_port
	LIMIT 1 \gset

-- connect to the first worker and ask for shard count, should return 0
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from pg_dist_shard']::text[],
								   false);

-- connect to the first worker and ask for shards, should fail with
-- expecting a single column error
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select * from pg_dist_shard']::text[],
								   false);

-- query result may only contain a single row
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select a from generate_series(1,2) a']::text[],
								   false);

-- send multiple queries
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['select a from generate_series(1,1) a',
								   		 'select a from generate_series(2,2) a']::text[],
								   false);

-- send multiple queries, one fails
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['select a from generate_series(1,1) a',
								   		 'select a from generate_series(1,2) a']::text[],
								   false);

-- send multiple queries, both fail
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['select a from generate_series(1,2) a',
								   		 'select a from generate_series(1,2) a']::text[],
								   false);
-- can create tables at worker
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['create table first_table(a int, b int)',
								   		 'create table second_table(a int, b int)']::text[],
								   false);

-- can insert into table
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['insert into first_table select a,a from generate_series(1,20) a']::text[],
								   false);
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from first_table']::text[],
								   false);
-- insert into second table twice
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['insert into second_table select * from first_table']::text[],
								   false);
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['insert into second_table select * from first_table']::text[],
								   false);

-- check inserted values at second table
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from second_table']::text[],
								   false);
-- store worker node name and port again
-- previously set variables become unusable after some number of uses
SELECT quote_literal(node_name) as node_name, node_port as node_port
	FROM master_get_active_worker_nodes()
	ORDER BY node_port
	LIMIT 1 \gset

-- create index on tables
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['create index first_table_index on first_table(a)']::text[],
								   false);
-- drop created tables
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['drop table first_table']::text[],
								   false);
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['drop table second_table']::text[],
								   false);

-- verify table is dropped
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from second_table']::text[],
								   false);
--
-- Run the same tests in parallel
--

-- connect to the first worker and ask for shard count, should return 0
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from pg_dist_shard']::text[],
								   true);

-- connect to the first worker and ask for shards, should fail with
-- expecting a single column error
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select * from pg_dist_shard']::text[],
								   true);

-- query result may only contain a single row
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select a from generate_series(1,2) a']::text[],
								   true);

-- send multiple queries
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['select a from generate_series(1,1) a',
								   		 'select a from generate_series(2,2) a']::text[],
								   true);

-- send multiple queries, one fails
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['select a from generate_series(1,1) a',
								   		 'select a from generate_series(1,2) a']::text[],
								   true);

-- send multiple queries, both fail
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['select a from generate_series(1,2) a',
								   		 'select a from generate_series(1,2) a']::text[],
								   true);
-- can create tables at worker
SELECT * FROM master_run_on_worker(ARRAY[:node_name, :node_name]::text[],
								   ARRAY[:node_port, :node_port]::int[],
								   ARRAY['create table first_table(a int, b int)',
								   		 'create table second_table(a int, b int)']::text[],
								   true);

-- store worker node name and port again
-- previously set variables become unusable after some number of uses
SELECT quote_literal(node_name) as node_name, node_port as node_port
	FROM master_get_active_worker_nodes()
	ORDER BY node_port
	LIMIT 1 \gset

-- can insert into table
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['insert into first_table select a,a from generate_series(1,20) a']::text[],
								   true);
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from first_table']::text[],
								   true);
-- insert into second table twice
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['insert into second_table select * from first_table']::text[],
								   true);
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['insert into second_table select * from first_table']::text[],
								   true);

-- check inserted values at second table
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from second_table']::text[],
								   true);

-- create index on tables
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['create index first_table_index on first_table(a)']::text[],
								   true);
-- drop created tables
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['drop table first_table']::text[],
								   true);
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['drop table second_table']::text[],
								   true);
-- verify table is dropped
SELECT * FROM master_run_on_worker(ARRAY[:node_name]::text[], ARRAY[:node_port]::int[],
								   ARRAY['select count(*) from second_table']::text[],
								   true);
-- run_command_on_XXX tests
SELECT * FROM run_command_on_workers('select 1') ORDER BY 2 ASC;
SELECT * FROM run_command_on_workers('select count(*) from pg_dist_partition') ORDER BY 2 ASC;

-- make sure run_on_all_placements respects shardstate
SET citus.shard_count TO 5;
CREATE TABLE check_placements (key int);
SELECT create_distributed_table('check_placements', 'key', 'hash');
SELECT * FROM run_command_on_placements('check_placements', 'select 1');
UPDATE pg_dist_shard_placement SET shardstate = 3
	WHERE shardid % 2 = 0 AND nodeport = :worker_1_port;
SELECT * FROM run_command_on_placements('check_placements', 'select 1');
DROP TABLE check_placements CASCADE;

-- make sure run_on_all_colocated_placements correctly detects colocation
CREATE TABLE check_colocated (key int);
SELECT create_distributed_table('check_colocated', 'key', 'hash');

SET citus.shard_count TO 4;
CREATE TABLE second_table (key int);
SELECT create_distributed_table('second_table', 'key', 'hash');
SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');
-- even when the difference is in replication factor, an error is thrown
DROP TABLE second_table;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 5;
CREATE TABLE second_table (key int);
SELECT create_distributed_table('second_table', 'key', 'hash');
SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');
-- when everything matches, the command is run!
DROP TABLE second_table;
SET citus.shard_replication_factor TO 2;
SET citus.shard_count TO 5;
CREATE TABLE second_table (key int);
SELECT create_distributed_table('second_table', 'key', 'hash');

SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');

DROP TABLE check_colocated CASCADE;
DROP TABLE second_table CASCADE;

-- runs on all shards
SET citus.shard_count TO 5;

CREATE TABLE check_shards (key int);
SELECT create_distributed_table('check_shards', 'key', 'hash');
SELECT * FROM run_command_on_shards('check_shards', 'select 1');
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid % 2 = 0;
SELECT * FROM run_command_on_shards('check_shards', 'select 1');
DROP TABLE check_shards CASCADE;

-- test the connections to worker nodes
SELECT bool_and(success) AS all_nodes_are_successful FROM (
    SELECT citus_check_connection_to_node(nodename, nodeport) AS success
    FROM pg_dist_node
    WHERE isactive = 't' AND noderole='primary'
) subquery;

-- verify that the coordinator can connect to itself
SELECT citus_check_connection_to_node('localhost', :master_port);

-- verify that the connections are not successful for wrong port
-- test with invalid port, prevent OS dependent warning from being displayed
SET client_min_messages TO ERROR;
SELECT citus_check_connection_to_node('localhost', nodeport:=1234);

-- verify that the connections are not successful due to timeouts
SET citus.node_connection_timeout TO 10;
SELECT citus_check_connection_to_node('www.citusdata.com');
RESET citus.node_connection_timeout;

SET client_min_messages TO DEBUG;

-- check the connections in a transaction block
BEGIN;
SELECT citus_check_connection_to_node(nodename, nodeport)
FROM pg_dist_node
WHERE isactive = 't' AND noderole='primary';

CREATE TABLE distributed(id int, data text);
SELECT create_distributed_table('distributed', 'id');
SELECT count(*) FROM distributed;

ROLLBACK;

-- create some roles for testing purposes
SET client_min_messages TO ERROR;

CREATE ROLE role_without_login WITH NOLOGIN;
SELECT 1 FROM run_command_on_workers($$CREATE ROLE role_without_login WITH NOLOGIN$$);

CREATE ROLE role_with_login WITH LOGIN;
SELECT 1 FROM run_command_on_workers($$CREATE ROLE role_with_login WITH LOGIN$$);

SET client_min_messages TO DEBUG;

-- verify that we can create connections only with users with login privileges.
SET ROLE role_without_login;
SELECT citus_check_connection_to_node('localhost', :worker_1_port);

SET ROLE role_with_login;
SELECT citus_check_connection_to_node('localhost', :worker_1_port);

RESET role;

DROP ROLE role_with_login, role_without_login;
SELECT 1 FROM run_command_on_workers($$DROP ROLE role_with_login, role_without_login$$);

-- check connections from a worker node
\c - - - :worker_1_port
SELECT citus_check_connection_to_node('localhost', :master_port);
SELECT citus_check_connection_to_node('localhost', :worker_1_port);
SELECT citus_check_connection_to_node('localhost', :worker_2_port);

\c - - - :master_port

SELECT * FROM citus_check_cluster_node_health() ORDER BY 1,2,3,4;

-- test cluster connectivity when we have broken nodes
SET client_min_messages TO ERROR;
SET citus.node_connection_timeout TO 10;

BEGIN;
INSERT INTO pg_dist_node VALUES
    (123456789, 123456789, 'localhost', 123456789),
    (1234567890, 1234567890, 'www.citusdata.com', 5432);
SELECT * FROM citus_check_cluster_node_health() ORDER BY 5,1,2,3,4;
ROLLBACK;

RESET citus.node_connection_timeout;
RESET client_min_messages;
DROP SCHEMA tools CASCADE;
RESET SEARCH_PATH;
-- set SHOW_CONTEXT back to default
\set SHOW_CONTEXT errors
