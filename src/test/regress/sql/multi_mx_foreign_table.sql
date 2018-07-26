--
-- multi_mx_foreign_table
-- test foreign tables can be created and queried in mx mode
--

-- create fake fdw for use in tests
CREATE FUNCTION fake_fdw_handler()
RETURNS fdw_handler
AS 'citus'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER fake_fdw HANDLER fake_fdw_handler;
CREATE SERVER fake_fdw_server FOREIGN DATA WRAPPER fake_fdw;

-- create fdw on all workers
SELECT FROM run_command_on_workers($$CREATE FUNCTION fake_fdw_handler()
RETURNS fdw_handler
AS 'citus'
LANGUAGE C STRICT; $$);

SELECT FROM run_command_on_workers($$CREATE FOREIGN DATA WRAPPER fake_fdw HANDLER fake_fdw_handler$$);

-- create foreign table
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';
CREATE FOREIGN TABLE fake_table(a int, b int) SERVER fake_fdw_server;
SELECT create_distributed_table('fake_table', 'a');

-- verify it can be queried from mx node
SELECT * FROM fake_table;
SELECT * FROM run_command_on_workers('SELECT count(*) FROM fake_table')
ORDER BY nodeport;

-- create secont fake table
CREATE FOREIGN TABLE second_fake_table(a int, b int) SERVER fake_fdw_server;
SELECT create_distributed_table('second_fake_table', 'a');

-- verify it can be queried from mx nodes
SELECT * FROM second_fake_table;
SELECT * FROM run_command_on_workers('SELECT count(*) FROM second_fake_table')
ORDER BY nodeport;

-- cleanup resources
DROP FUNCTION IF EXISTS fake_fdw_handler() CASCADE;
SELECT * FROM run_command_on_workers('DROP FUNCTION IF EXISTS fake_fdw_handler() CASCADE')
ORDER BY nodeport;
