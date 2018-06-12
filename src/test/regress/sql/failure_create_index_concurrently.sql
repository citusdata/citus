--
-- failure_create_index_concurrently
-- test create index concurrently command
-- failure.

SELECT citus.mitmproxy('conn.allow()');

SET citus.shard_count = 4; -- two per worker

CREATE SCHEMA index_schema;
SET SEARCH_PATH=index_schema;

CREATE TABLE index_test(id int, value_1 int, value_2 int);
SELECT create_distributed_table('index_test', 'id');

-- kill the connection when create command is issued
SELECT citus.mitmproxy('conn.onQuery(query="CREATE").kill()');

CREATE INDEX CONCURRENTLY idx_index_test ON index_test(id, value_1);

SELECT citus.mitmproxy('conn.allow()');
-- verify index is not created
SELECT * FROM run_command_on_workers($$SELECT count(*) FROM pg_indexes WHERE indexname LIKE 'idx_index_test%' $$)
WHERE nodeport = :worker_2_proxy_port;


DROP TABLE index_test;


CREATE TABLE index_test(id int, value_1 int, value_2 int);
SELECT create_distributed_table('index_test', 'id');

-- kill the connection at the second create command is issued
SELECT citus.mitmproxy('conn.onQuery(query="CREATE").after(1).kill()');

CREATE INDEX CONCURRENTLY idx_index_test ON index_test(id, value_1);

SELECT citus.mitmproxy('conn.allow()');

-- verify only one index is created
SELECT * FROM run_command_on_workers($$SELECT count(*) FROM pg_indexes WHERE indexname LIKE 'idx_index_test%' $$)
WHERE nodeport = :worker_2_proxy_port;

DROP TABLE index_test;


CREATE TABLE index_test(id int, value_1 int, value_2 int);
SELECT create_reference_table('index_test');

-- kill the connection when create command is issued
SELECT citus.mitmproxy('conn.onQuery(query="CREATE").kill()');

CREATE INDEX CONCURRENTLY idx_index_test ON index_test(id, value_1);

SELECT citus.mitmproxy('conn.allow()');

DROP TABLE index_test;


CREATE TABLE index_test(id int, value_1 int, value_2 int);
SELECT create_distributed_table('index_test', 'id');

-- cancel the connection when create command is issued
-- network traffic may differ between execution during cancellation
-- therefore dump_network_traffic() calls are not made
SELECT citus.mitmproxy('conn.onQuery(query="CREATE").cancel(' || pg_backend_pid() || ')');

CREATE INDEX CONCURRENTLY idx_index_test ON index_test(id, value_1);

SELECT citus.mitmproxy('conn.allow()');

DROP TABLE index_test;

CREATE TABLE index_test(id int, value_1 int, value_2 int);
SELECT create_reference_table('index_test');

-- cancel the connection when create command is issued
SELECT citus.mitmproxy('conn.onQuery(query="CREATE").cancel(' || pg_backend_pid() || ')');

CREATE INDEX CONCURRENTLY idx_index_test ON index_test(id, value_1);

SELECT citus.mitmproxy('conn.allow()');

DROP TABLE index_test;


CREATE TABLE index_test(id int, value_1 int, value_2 int);
SELECT create_distributed_table('index_test', 'id');

CREATE INDEX CONCURRENTLY idx_index_test ON index_test(id, value_1);

-- kill the connection when create command is issued
SELECT citus.mitmproxy('conn.onQuery(query="DROP INDEX CONCURRENTLY").kill()');
DROP INDEX CONCURRENTLY IF EXISTS idx_index_test;
SELECT citus.mitmproxy('conn.allow()');

-- verify index is not dropped at worker 2
SELECT * FROM run_command_on_workers($$SELECT count(*) FROM pg_indexes WHERE indexname LIKE 'idx_index_test%' $$)
WHERE nodeport = :worker_2_proxy_port;

RESET SEARCH_PATH;
DROP SCHEMA index_schema CASCADE;

-- verify index is not at worker 2 upon cleanup
SELECT * FROM run_command_on_workers($$SELECT count(*) FROM pg_indexes WHERE indexname LIKE 'idx_index_test%' $$)
WHERE nodeport = :worker_2_proxy_port;
