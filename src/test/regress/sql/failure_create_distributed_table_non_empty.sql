-- 
--  Failure tests for COPY to reference tables 
-- 

-- We have to keep two copies of this failure test
-- because if the shards are created via the executor
-- cancellations are processed, otherwise they are not

CREATE SCHEMA create_distributed_table_non_empty_failure;
SET search_path TO 'create_distributed_table_non_empty_failure';

SET citus.next_shard_id TO 11000000;

SELECT citus.mitmproxy('conn.allow()');

-- we'll start with replication factor 1 and 2pc
SET citus.shard_replication_factor TO 1;
SET citus.shard_count to 4;

CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);

-- in the first test, kill the first connection we sent from the coordinator
SELECT citus.mitmproxy('conn.kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- in the first test, cancel the first connection we sent from the coordinator
SELECT citus.mitmproxy('conn.cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- kill as soon as the coordinator sends CREATE SCHEMA
SELECT citus.mitmproxy('conn.onQuery(query="^CREATE SCHEMA").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'create_distributed_table_non_empty_failure'$$);

-- cancel as soon as the coordinator sends CREATE SCHEMA
-- Note: Schema should be created in workers because Citus
-- does not check for interrupts until GetRemoteCommandResult is called.
-- Since we already sent the command at this stage, the schemas get created in workers
SELECT citus.mitmproxy('conn.onQuery(query="^CREATE SCHEMA").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'create_distributed_table_non_empty_failure'$$);
SELECT run_command_on_workers($$DROP SCHEMA IF EXISTS create_distributed_table_non_empty_failure$$);

-- kill as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'create_distributed_table_non_empty_failure'$$);

-- cancel as soon as the coordinator sends begin
-- if the shards are created via the executor, the table creation will fail
-- otherwise shards will be created because we ignore cancel requests during the shard creation 
-- Interrupts are hold in CreateShardsWithRoundRobinPolicy
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'create_distributed_table_non_empty_failure'$$);
DROP TABLE test_table ;
CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);

-- kill as soon as the coordinator sends CREATE TABLE
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- kill as soon as the coordinator sends COPY
SELECT citus.mitmproxy('conn.onQuery(query="COPY").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- kill when the COPY is completed, it should be rollbacked properly
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- cancel as soon as the coordinator sends COPY, table 
-- should not be created and rollbacked properly
SELECT citus.mitmproxy('conn.onQuery(query="COPY").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- cancel when the COPY is completed, it should be rollbacked properly
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- immediately kill when we see prepare transaction to see if the command
-- successfully rollbacked the created shards
-- we don't want to see the prepared transaction numbers in the warnings
SET client_min_messages TO ERROR;
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- immediately cancel when we see prepare transaction to see if the command
-- successfully rollbacked the created shards
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();

-- kill as soon as the coordinator sends COMMIT
-- shards should be created and kill should not affect
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT PREPARED").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT citus.mitmproxy('conn.allow()');

SELECT recover_prepared_transactions();
DROP TABLE test_table ;
-- since we want to interrupt the schema creation again we need to drop and recreate
-- for citus to redistribute the dependency
DROP SCHEMA create_distributed_table_non_empty_failure;
CREATE SCHEMA create_distributed_table_non_empty_failure;
CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);

-- cancel as soon as the coordinator sends COMMIT
-- shards should be created and kill should not affect
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT PREPARED").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

DROP TABLE test_table ;
CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);

-- kill as soon as the coordinator sends ROLLBACK
-- the command can be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');
BEGIN;
SELECT create_distributed_table('test_table', 'id');
ROLLBACK;
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- cancel as soon as the coordinator sends ROLLBACK
-- should be rollbacked 
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").cancel(' ||  pg_backend_pid() || ')');
BEGIN;
SELECT create_distributed_table('test_table', 'id');
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- We are done with pure create_distributed_table testing and now
-- testing for co-located tables.
CREATE TABLE colocated_table(id int, value_1 int);
SELECT create_distributed_table('colocated_table', 'id');

-- Now, cancel the connection just after transaction is opened on
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' || pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- Now, kill the connection just after transaction is opened on
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'create_distributed_table_non_empty_failure' and table_name LIKE 'test_table%'$$);

-- Now, cancel the connection just after the COPY started to
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- Now, kill the connection just after the COPY started to
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'create_distributed_table_non_empty_failure' and table_name LIKE 'test_table%'$$);

-- Now, cancel the connection when we issue CREATE TABLE on
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT worker_apply_shard_ddl_command").cancel(' || pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- Now, kill the connection when we issue CREATE TABLE on
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^SELECT worker_apply_shard_ddl_command").kill()');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'create_distributed_table_non_empty_failure' and table_name LIKE 'test_table%'$$);

-- Now run the same tests with 1pc
SELECT citus.mitmproxy('conn.allow()');
DROP TABLE colocated_table;
DROP TABLE test_table;
DROP SCHEMA create_distributed_table_non_empty_failure;
CREATE SCHEMA create_distributed_table_non_empty_failure;
CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);
SET citus.multi_shard_commit_protocol TO '1pc';

SELECT citus.mitmproxy('conn.kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'create_distributed_table_non_empty_failure' and table_name LIKE 'test_table%'$$);

-- in the first test, cancel the first connection we sent from the coordinator
SELECT citus.mitmproxy('conn.cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'create_distributed_table_non_empty_failure' and table_name LIKE 'test_table%'$$);

-- kill as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'create_distributed_table_non_empty_failure'$$);

-- cancel as soon as the coordinator sends begin
-- if the shards are created via the executor, the table creation will fail
-- otherwise shards will be created because we ignore cancel requests during the shard creation 
-- Interrupts are hold in CreateShardsWithRoundRobinPolicy
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'create_distributed_table_non_empty_failure'$$);
DROP TABLE test_table ;
CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);

-- kill as soon as the coordinator sends CREATE TABLE
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- kill as soon as the coordinator sends COPY
SELECT citus.mitmproxy('conn.onQuery(query="COPY").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- kill when the COPY is completed, it should be rollbacked properly
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY").kill()');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- cancel as soon as the coordinator sends COPY, table 
-- should not be created and rollbacked properly
SELECT citus.mitmproxy('conn.onQuery(query="COPY").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- cancel when the COPY is completed, it should be rollbacked properly
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY").cancel(' ||  pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- kill as soon as the coordinator sends ROLLBACK
-- the command can be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');
BEGIN;
SELECT create_distributed_table('test_table', 'id');
ROLLBACK;
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- cancel as soon as the coordinator sends ROLLBACK
-- should be rollbacked 
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").cancel(' ||  pg_backend_pid() || ')');
BEGIN;
SELECT create_distributed_table('test_table', 'id');
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- kill as soon as the coordinator sends COMMIT
-- the command can be COMMITed
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
BEGIN;
SELECT create_distributed_table('test_table', 'id');
COMMIT;
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
DROP TABLE test_table;
CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);

-- cancel as soon as the coordinator sends COMMIT
-- should be COMMITed
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").cancel(' ||  pg_backend_pid() || ')');
BEGIN;
SELECT create_distributed_table('test_table', 'id');
COMMIT;
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
DROP TABLE test_table;
CREATE TABLE test_table(id int, value_1 int);
INSERT INTO test_table VALUES (1,1),(2,2),(3,3),(4,4);

CREATE TABLE colocated_table(id int, value_1 int);
SELECT create_distributed_table('colocated_table', 'id');

-- Now, cancel the connection just after transaction is opened on
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' || pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- Now, kill the connection just after transaction is opened on
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- Now, cancel the connection just after the COPY started to
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' || pg_backend_pid() || ')');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;

-- Now, kill the connection just after the COPY started to
-- workers. Note that, when there is a colocated table, interrupts
-- are not held and we can cancel in the middle of the execution
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
SELECT create_distributed_table('test_table', 'id', colocate_with => 'colocated_table');
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='create_distributed_table_non_empty_failure.test_table'::regclass;
SELECT run_command_on_workers($$SELECT count(*) FROM information_schema.tables WHERE table_schema = 'create_distributed_table_non_empty_failure' and table_name LIKE 'test_table%'$$);

SELECT citus.mitmproxy('conn.allow()');
DROP SCHEMA create_distributed_table_non_empty_failure CASCADE;
