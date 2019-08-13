--
-- failure_create_table adds failure tests for creating table without data.
--

CREATE SCHEMA failure_create_table;
SET search_path TO 'failure_create_table';

SELECT citus.mitmproxy('conn.allow()');
SET citus.shard_replication_factor TO 1;
SET citus.shard_count to 4;

CREATE TABLE test_table(id int, value_1 int);

-- Kill connection before sending query to the worker 
SELECT citus.mitmproxy('conn.kill()');
SELECT create_distributed_table('test_table','id');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- kill as soon as the coordinator sends CREATE SCHEMA
-- Since schemas are created in separate transaction, schema will
-- be created only on the node which is not behind the proxy.
-- https://github.com/citusdata/citus/pull/1652
SELECT citus.mitmproxy('conn.onQuery(query="^CREATE SCHEMA").kill()');
SELECT create_distributed_table('test_table', 'id');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'failure_create_table'$$);

-- Now, kill the connection while opening transaction on workers.
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
SELECT create_distributed_table('test_table','id');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Now, kill the connection after sending create table command with worker_apply_shard_ddl_command UDF
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_apply_shard_ddl_command").after(1).kill()');
SELECT create_distributed_table('test_table','id');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Kill the connection while creating a distributed table in sequential mode on sending create command
-- with worker_apply_shard_ddl_command UDF.
BEGIN;
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
    SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_apply_shard_ddl_command").after(1).kill()');
    SELECT create_distributed_table('test_table', 'id');
COMMIT;

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Now, cancel the connection while creating transaction
-- workers. Note that, cancel requests will be ignored during
-- shard creation.
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' || pg_backend_pid() || ')');
SELECT create_distributed_table('test_table','id');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

DROP TABLE test_table;
CREATE TABLE test_table(id int, value_1 int);

-- Kill and cancel the connection with colocate_with option while sending the create table command
CREATE TABLE temp_table(id int, value_1 int);
SELECT create_distributed_table('temp_table','id');

SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").kill()');
SELECT create_distributed_table('test_table','id',colocate_with=>'temp_table');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").cancel(' || pg_backend_pid() || ')');
SELECT create_distributed_table('test_table','id',colocate_with=>'temp_table');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Kill and cancel the connection after worker sends "PREPARE TRANSACTION" ack with colocate_with option
SELECT citus.mitmproxy('conn.onCommandComplete(command="PREPARE TRANSACTION").kill()');
SELECT create_distributed_table('test_table','id',colocate_with=>'temp_table');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

SELECT citus.mitmproxy('conn.onCommandComplete(command="PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table','id',colocate_with=>'temp_table');

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- drop tables and schema and recreate to start from a non-distributed schema again
DROP TABLE temp_table;
DROP TABLE test_table;
DROP SCHEMA failure_create_table;
CREATE SCHEMA failure_create_table;
CREATE TABLE test_table(id int, value_1 int);

-- Test inside transaction
-- Kill connection before sending query to the worker 
SELECT citus.mitmproxy('conn.kill()');

BEGIN;
SELECT create_distributed_table('test_table','id');
ROLLBACK;

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Now, kill the connection while creating transaction on workers in transaction.
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');

BEGIN;
SELECT create_distributed_table('test_table','id');
ROLLBACK;

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Now, cancel the connection while creating the transaction on
-- workers. Note that, cancel requests will be ignored during
-- shard creation again in transaction if we're not relying on the
-- executor. So, we'll have two output files
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' || pg_backend_pid() || ')');

BEGIN;
SELECT create_distributed_table('test_table','id');
COMMIT;
SELECT recover_prepared_transactions();
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- drop tables and schema and recreate to start from a non-distributed schema again
DROP TABLE test_table;
DROP SCHEMA failure_create_table;
CREATE SCHEMA failure_create_table;
CREATE TABLE test_table(id int, value_1 int);

-- Test inside transaction and with 1PC
SET citus.multi_shard_commit_protocol TO "1pc";

-- Kill connection before sending query to the worker with 1pc.
SELECT citus.mitmproxy('conn.kill()');

BEGIN;
SELECT create_distributed_table('test_table','id');
ROLLBACK;

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Kill connection while sending create table command with 1pc.
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").kill()');

BEGIN;
SELECT create_distributed_table('test_table','id');
ROLLBACK;

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Now, kill the connection while opening transactions on workers with 1pc. Transaction will be opened due to BEGIN.
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');

BEGIN;
SELECT create_distributed_table('test_table','id');
ROLLBACK;

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Now, cancel the connection while creating transactions on
-- workers with 1pc. Note that, cancel requests will be ignored during
-- shard creation unless the executor is used. So, we'll have two output files
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' || pg_backend_pid() || ')');

BEGIN;
SELECT create_distributed_table('test_table','id');
COMMIT;
SELECT recover_prepared_transactions();
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

DROP TABLE test_table;

-- Test master_create_worker_shards with 2pc
SET citus.multi_shard_commit_protocol TO "2pc";
CREATE TABLE test_table_2(id int, value_1 int);
SELECT master_create_distributed_table('test_table_2', 'id', 'hash');

-- Kill connection before sending query to the worker 
SELECT citus.mitmproxy('conn.kill()');
SELECT master_create_worker_shards('test_table_2', 4, 2);

SELECT count(*) FROM pg_dist_shard;

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Kill the connection after worker sends "PREPARE TRANSACTION" ack
SELECT citus.mitmproxy('conn.onCommandComplete(command="^PREPARE TRANSACTION").kill()');
SELECT master_create_worker_shards('test_table_2', 4, 2);

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

-- Cancel the connection after sending prepare transaction in master_create_worker_shards
SELECT citus.mitmproxy('conn.onCommandComplete(command="PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');
SELECT master_create_worker_shards('test_table_2', 4, 2);

-- Show that there is no pending transaction
SELECT recover_prepared_transactions();

SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'failure_create_table' and table_name LIKE 'test_table%' ORDER BY 1$$);

DROP SCHEMA failure_create_table CASCADE;
SET search_path TO default;
