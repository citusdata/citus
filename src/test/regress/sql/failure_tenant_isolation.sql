--
-- failure_tenant_isolation
--

-- due to different libpq versions
-- some warning messages differ
-- between local and CI
SET client_min_messages TO ERROR;

CREATE SCHEMA IF NOT EXISTS tenant_isolation;
SET SEARCH_PATH = tenant_isolation;
SET citus.shard_count TO 2;
SET citus.next_shard_id TO 300;
SET citus.shard_replication_factor TO 1;
SELECT pg_backend_pid() as pid \gset
SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE table_1 (id int PRIMARY KEY);
CREATE TABLE table_2 (ref_id int REFERENCES table_1(id) UNIQUE, data int);

SELECT create_distributed_table('table_1', 'id');
SELECT create_distributed_table('table_2', 'ref_id');

CREATE VIEW shard_sizes AS
  SELECT shardid, result AS row_count
  FROM run_command_on_placements('table_1', 'SELECT count(*) FROM %s');

INSERT INTO table_1
SELECT x
FROM generate_series(1, 100) AS f (x);

INSERT INTO table_2
SELECT x, x
FROM generate_series(1, 100) AS f (x);

-- initial shard sizes
SELECT * FROM shard_sizes ORDER BY 1;

-- failure on colocated table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE tenant_isolation.table_2").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on colocated table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE tenant_isolation.table_2").after(1).cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on colocated table population
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO tenant_isolation.table_2").after(2).kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on colocated table population
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO tenant_isolation.table_2").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on colocated table constraints
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE tenant_isolation.table_2 ADD CONSTRAINT").after(1).kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on colocated table constraints
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE tenant_isolation.table_2 ADD CONSTRAINT").after(2).cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');


-- failure on table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE tenant_isolation.table_1").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE tenant_isolation.table_1").after(1).cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on table population
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO tenant_isolation.table_1").after(2).kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on table population
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO tenant_isolation.table_1").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on table constraints
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE tenant_isolation.table_1 ADD CONSTRAINT").after(1).kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on table constraints
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE tenant_isolation.table_1 ADD CONSTRAINT").after(2).cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');


-- failure on dropping old colocated shard
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS tenant_isolation.table_2").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on dropping old colocated shard
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS tenant_isolation.table_2").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on dropping old shard
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS tenant_isolation.table_1").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- cancellation on dropping old shard
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS tenant_isolation.table_1").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');


-- failure on foreign key creation
SELECT citus.mitmproxy('conn.onQuery(query="ADD CONSTRAINT table_2_ref_id_fkey FOREIGN KEY").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on foreign key creation
SELECT citus.mitmproxy('conn.onQuery(query="ADD CONSTRAINT table_2_ref_id_fkey FOREIGN KEY").after(2).cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');


-- failure on shard split transaction
SELECT citus.mitmproxy('conn.onQuery(query="BEGIN").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on shard split transaction
SELECT citus.mitmproxy('conn.onQuery(query="BEGIN").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on shard split transaction commit
SELECT citus.mitmproxy('conn.onQuery(query="COMMIT").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on shard split transaction commit
SELECT citus.mitmproxy('conn.onQuery(query="COMMIT").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction for dropping old tables
SELECT citus.mitmproxy('conn.after(1).onQuery(query="BEGIN").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction for dropping old tables
SELECT citus.mitmproxy('conn.after(1).onQuery(query="BEGIN").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction for foreign key creation
SELECT citus.mitmproxy('conn.after(2).onQuery(query="BEGIN").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction for foreign key creation
SELECT citus.mitmproxy('conn.after(2).onQuery(query="BEGIN").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction commit for foreign key creation
SELECT citus.mitmproxy('conn.after(1).onQuery(query="COMMIT").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction commit for foreign key creation
SELECT citus.mitmproxy('conn.after(1).onQuery(query="COMMIT").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction prepare for dropping old tables
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").kill()');

-- due to libpq version differences, the output might change
-- hence use code block to catch the error
\set VERBOSITY terse
DO LANGUAGE plpgsql
$$
BEGIN
	SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');
	EXCEPTION WHEN OTHERS THEN
	RAISE 'Command failed to execute';
END;
$$;
\set VERBOSITY default

-- failure on transaction prepare for dropping old tables
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction commit for dropping old tables
SELECT citus.mitmproxy('conn.after(2).onQuery(query="COMMIT").kill()');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');

-- failure on transaction commit for dropping old tables
SELECT citus.mitmproxy('conn.after(2).onQuery(query="COMMIT").cancel(' || :pid || ')');
SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE');


-- verify that the tenant is not isolated
SELECT * FROM shard_sizes ORDER BY 1;

-- Verify that tenant can be isolated after unsuccessful attempts
SELECT citus.mitmproxy('conn.allow()');

-- shard sizes after successful tenant isolation
CREATE TABLE old_shards AS SELECT shardid FROM pg_dist_shard;
WITH new_shard AS (
	SELECT isolate_tenant_to_new_shard('table_1', 5, 'CASCADE') AS shardid
)
SELECT row_count
FROM shard_sizes
JOIN new_shard ON shard_sizes.shardid = new_shard.shardid;

SELECT row_count
FROM shard_sizes
WHERE shard_sizes.shardid NOT IN (SELECT * FROM old_shards)
ORDER BY 1;

\set VERBOSITY terse
DROP SCHEMA tenant_isolation CASCADE;
\set VERBOSITY default
