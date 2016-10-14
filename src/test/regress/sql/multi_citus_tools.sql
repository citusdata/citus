--
-- MULTI CITUS TOOLS
--
-- tests UDFs created for citus tools
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1230000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1230000;

-- the function is not exposed explicitly, create the entry point
CREATE OR REPLACE FUNCTION master_run_on_worker(worker_name text[], port integer[],
												command text[],
												parallel boolean default false,
												OUT node_name text, OUT node_port integer,
												OUT success boolean, OUT result text)
	RETURNS SETOF record
	LANGUAGE C STABLE STRICT
	AS 'citus.so', $$master_run_on_worker$$;

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

-- drop the function after use
DROP FUNCTION master_run_on_worker(worker_name text[], port integer[], command text[],
						  parallel boolean, OUT node_name text, OUT node_port integer,
						  OUT success boolean, OUT result text);

