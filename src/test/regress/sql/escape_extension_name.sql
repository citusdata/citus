CREATE SCHEMA "extension'test";

-- use  a schema name with escape character
SET search_path TO "extension'test";

-- test if citus can escape the extension name

-- this output will help us to understand why we have alternative outputs for this test
-- print true if uuid-ossp is available false otherwise
SELECT CASE WHEN COUNT(*) > 0
  THEN 'CREATE EXTENSION "uuid-ossp"'
  ELSE 'SELECT false AS uuid_ossp_present'
  END AS uuid_present_command
FROM pg_available_extensions()
WHERE name = 'uuid-ossp'
\gset
:uuid_present_command;

-- show that the extension is created on both nodes
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'uuid-ossp'$$);

SET client_min_messages TO WARNING;
DROP EXTENSION "uuid-ossp";
RESET client_min_messages;

-- show that the extension is dropped from both nodes
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'uuid-ossp'$$);

-- show that extension recreation on new nodes works also fine with extension names that require escaping
SELECT 1 from master_remove_node(:'worker_2_host', :worker_2_port);

-- this output will help us to understand why we have alternative outputs for this test
-- print true if uuid-ossp is available false otherwise
SELECT CASE WHEN COUNT(*) > 0
  THEN 'CREATE EXTENSION "uuid-ossp"'
  ELSE 'SELECT false AS uuid_ossp_present'
  END AS uuid_present_command
FROM pg_available_extensions()
WHERE name = 'uuid-ossp'
\gset
:uuid_present_command;

-- and add the other node
SELECT 1 from master_add_node(:'worker_2_host', :worker_2_port);

-- show that the extension exists on both nodes
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'uuid-ossp'$$);

SET client_min_messages TO WARNING;
DROP EXTENSION "uuid-ossp";
RESET client_min_messages;

-- drop the schema and all the objects
SET client_min_messages TO WARNING;
DROP SCHEMA "extension'test" CASCADE;

