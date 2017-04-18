--
-- MULTI CITUS TOOLS
--
-- tests UDFs created for citus tools
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1240000;

-- test with invalid port, prevent OS dependent warning from being displayed
SET client_min_messages to ERROR;
-- PG 9.5 does not show context for plpgsql raise
-- message whereas PG 9.6 shows. disabling it 
-- for this test only to have consistent behavior
-- b/w PG 9.6+ and PG 9.5. 
\set SHOW_CONTEXT never

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
CREATE TABLE check_placements (key int);
SELECT master_create_distributed_table('check_placements', 'key', 'hash');
SELECT master_create_worker_shards('check_placements', 5, 2);
SELECT * FROM run_command_on_placements('check_placements', 'select 1');
UPDATE pg_dist_shard_placement SET shardstate = 3
	WHERE shardid % 2 = 0 AND nodeport = :worker_1_port;
SELECT * FROM run_command_on_placements('check_placements', 'select 1');
DROP TABLE check_placements CASCADE;

-- make sure run_on_all_colocated_placements correctly detects colocation
CREATE TABLE check_colocated (key int);
SELECT master_create_distributed_table('check_colocated', 'key', 'hash');
SELECT master_create_worker_shards('check_colocated', 5, 2);
CREATE TABLE second_table (key int);
SELECT master_create_distributed_table('second_table', 'key', 'hash');
SELECT master_create_worker_shards('second_table', 4, 2);
SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');
-- even when the difference is in replication factor, an error is thrown
SELECT master_drop_all_shards('second_table'::regclass, current_schema(), 'second_table');
SELECT master_create_worker_shards('second_table', 5, 1);
SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');
-- when everything matches, the command is run!
SELECT master_drop_all_shards('second_table'::regclass, current_schema(), 'second_table');
SELECT master_create_worker_shards('second_table', 5, 2);
SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');
-- when a placement is invalid considers the tables to not be colocated
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = (
		SELECT shardid FROM pg_dist_shard
		WHERE nodeport = :worker_1_port AND logicalrelid = 'second_table'::regclass
		ORDER BY 1 ASC LIMIT 1
);
SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');
-- when matching placement is also invalid, considers the tables to be colocated
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = (
		SELECT shardid FROM pg_dist_shard
		WHERE nodeport = :worker_1_port AND logicalrelid = 'check_colocated'::regclass
		ORDER BY 1 ASC LIMIT 1
);
SELECT * FROM run_command_on_colocated_placements('check_colocated', 'second_table',
												  'select 1');
DROP TABLE check_colocated CASCADE;
DROP TABLE second_table CASCADE;

-- runs on all shards
CREATE TABLE check_shards (key int);
SELECT master_create_distributed_table('check_shards', 'key', 'hash');
SELECT master_create_worker_shards('check_shards', 5, 2);
SELECT * FROM run_command_on_shards('check_shards', 'select 1');
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid % 2 = 0;
SELECT * FROM run_command_on_shards('check_shards', 'select 1');
DROP TABLE check_shards CASCADE;

-- set SHOW_CONTEXT back to default
\set SHOW_CONTEXT errors
