### citus v9.0.1 (October 25, 2019) ###

* Fixes a memory leak in the executor

* Revokes usage from the citus schema from public

### citus v9.0.0 (October 7, 2019) ###

* Adds support for PostgreSQL 12

* Adds UDFs to help with PostgreSQL upgrades

* Distributes types to worker nodes

* Introduces `create_distributed_function` UDF

* Introduces local query execution for Citus MX

* Implements infrastructure for routing `CALL` to MX workers

* Implements infrastructure for routing `SELECT function()` to MX workers

* Adds support for foreign key constraints between reference tables

* Adds a feature flag to turn off `CREATE TYPE` propagation

* Adds option `citus.single_shard_commit_protocol`

* Adds support for `EXPLAIN SUMMARY`

* Adds support for `GENERATE ALWAYS AS STORED`

* Adds support for `serial` and `smallserial` in MX mode

* Adds support for anon composite types on the target list in router queries

* Avoids race condition between `create_reference_table` & `master_add_node`

* Fixes a bug in schemas of distributed sequence definitions

* Fixes a bug that caused `run_command_on_colocated_placements` to fail

* Fixes a bug that leads to various issues when a connection is lost

* Fixes a schema leak on `CREATE INDEX` statement

* Fixes assert failure in bare `SELECT FROM reference table FOR UPDATE` in MX

* Makes `master_update_node` MX compatible

* Prevents `pg_dist_colocation` from multiple records for reference tables

* Prevents segfault in `worker_partition_protocol` edgecase

* Propagates `ALTER FUNCTION` statements for distributed functions

* Propagates `CREATE OR REPLACE FUNCTION` for distributed functions

* Propagates `REINDEX` on tables & indexes

* Provides a GUC to turn of the new dependency propagation functionality

* Uses 2PC in adaptive executor when dealing with replication factors above 1

### citus v8.3.2 (August 09, 2019) ###

* Fixes performance issues by skipping unnecessary relation access recordings

### citus v8.3.1 (July 29, 2019) ###

* Improves Adaptive Executor performance

### citus v8.3.0 (July 10, 2019) ###

* Adds a new distributed executor: Adaptive Executor

* citus.enable_statistics_collection defaults to off (opt-in)

* Adds support for CTEs in router planner for modification queries

* Adds support for propagating SET LOCAL at xact start

* Adds option to force master_update_node during failover

* Deprecates master_modify_multiple_shards

* Improves round robin logic on router queries

* Creates all distributed schemas as superuser on a separate connection

* Makes COPY adapt to connection use behaviour of previous commands

* Replaces SESSION_LIFESPAN with configurable number of connections at xact end

* Propagates ALTER FOREIGN TABLE commands to workers

* Don't schedule tasks on inactive nodes

* Makes DROP/VALIDATE CONSTRAINT tolerant of ambiguous shard extension

* Fixes an issue with subquery map merge jobs as non-root

* Fixes null pointers caused by partial initialization of ConnParamsHashEntry

* Fixes errors caused by joins with shadowed aliases

* Fixes a regression in outer joining subqueries introduced in 8.2.0

* Fixes a crash that can occur under high memory load

* Fixes a bug that selects wrong worker when using round-robin assignment

* Fixes savepoint rollback after multi-shard modify/copy failure

* Fixes bad foreign constraint name search

* Fixes a bug that prevents stack size to be adjusted

### citus v8.2.2 (June 11, 2019) ###

* Fixes a bug in outer joins wrapped in subqueries

### citus v8.2.1 (April 03, 2019) ###

* Fixes a bug that prevents stack size to be adjusted

### citus v8.1.2 (April 03, 2019) ###

* Don't do redundant ALTER TABLE consistency checks at coordinator

* Fixes a bug that prevents stack size to be adjusted

* Fix an issue with some DECLARE .. CURSOR WITH HOLD commands

### citus v8.2.0 (March 28, 2019) ###

* Removes support and code for PostgreSQL 9.6

* Enable more outer joins with reference tables

* Execute CREATE INDEX CONCURRENTLY in parallel

* Treat functions as transaction blocks

* Add support for column aliases on join clauses

* Skip standard_planner() for trivial queries

* Added support for function calls in joins

* Round-robin task assignment policy relies on local transaction id

* Relax subquery union pushdown restrictions for reference tables

* Speed-up run_command_on_shards()

* Address some memory issues in connection config

* Restrict visibility of get_*_active_transactions functions to pg_monitor

* Don't do redundant ALTER TABLE consistency checks at coordinator

* Queries with only intermediate results do not rely on task assignment policy

* Finish connection establishment in parallel for multiple connections

* Fixes a bug related to pruning shards using a coerced value

* Fix an issue with some DECLARE .. CURSOR WITH HOLD commands

* Fixes a bug that could lead to infinite recursion during recursive planning

* Fixes a bug that could prevent planning full outer joins with using clause

* Fixes a bug that could lead to memory leak on `citus_relation_size`

* Fixes a problem that could cause segmentation fault with recursive planning

* Switch CI solution to CircleCI

### citus v8.0.3 (January 9, 2019) ###

* Fixes maintenance daemon panic due to unreleased spinlock

* Fixes an issue with having clause when used with complex joins

### citus v8.1.1 (January 7, 2019) ###

* Fixes maintenance daemon panic due to unreleased spinlock

* Fixes an issue with having clause when used with complex joins

### citus v8.1.0 (December 17, 2018) ###

* Turns on ssl by default for new installations of citus

* Restricts SSL Ciphers to TLS1.2 and above

* Adds support for INSERT INTO SELECT..ON CONFLICT/RETURNING via coordinator

* Adds support for round-robin task assignment for queries to reference tables

* Adds support for SQL tasks using worker_execute_sql_task UDF with task-tracker

* Adds support for VALIDATE CONSTRAINT queries

* Adds support for disabling hash aggregate with HLL

* Adds user ID suffix to intermediate files generated by task-tracker

* Only allow transmit from pgsql_job_cache directory

* Disallows GROUPING SET clauses in subqueries

* Removes restriction on user-defined group ID in node addition functions

* Relaxes multi-shard modify locks when enable_deadlock_prevention is disabled

* Improves security in task-tracker protocol

* Improves permission checks in internal DROP TABLE functions

* Improves permission checks in cluster management functions

* Cleans up UDFs and fixes permission checks

* Fixes crashes caused by stack size increase under high memory load

* Fixes a bug that could cause maintenance daemon panic

### citus v8.0.2 (December 13, 2018) ###

* Fixes a bug that could cause maintenance daemon panic

* Fixes crashes caused by stack size increase under high memory load

### citus v7.5.4 (December 11, 2018) ###

* Fixes a bug that could cause maintenance daemon panic

### citus v8.0.1 (November 27, 2018) ###

* Execute SQL tasks using worker_execute_sql_task UDF when using task-tracker

### citus v7.5.3 (November 27, 2018) ###

* Execute SQL tasks using worker_execute_sql_task UDF when using task-tracker

### citus v7.5.2 (November 14, 2018) ###

* Fixes inconsistent metadata error when shard metadata caching get interrupted

* Fixes a bug that could cause memory leak

* Fixes a bug that prevents recovering wrong transactions in MX

* Fixes a bug to prevent wrong memory accesses on Citus MX under very high load

* Fixes crashes caused by stack size increase under high memory load

### citus v8.0.0 (October 31, 2018) ###

* Adds support for PostgreSQL 11

* Adds support for applying DML operations on reference tables from MX nodes

* Adds distributed locking to truncated MX tables

* Adds support for running TRUNCATE command from MX worker nodes

* Adds views to provide insight about the distributed transactions

* Adds support for TABLESAMPLE in router queries

* Adds support for INCLUDE option in index creation

* Adds option to allow simple DML commands from hot standby

* Adds support for partitioned tables with replication factor > 1

* Prevents a deadlock on concurrent DROP TABLE and SELECT on Citus MX

* Fixes a bug that prevents recovering wrong transactions in MX

* Fixes a bug to prevent wrong memory accesses on Citus MX under very high load

* Fixes a bug in MX mode, calling DROP SCHEMA with existing partitioned table

* Fixes a bug that could cause modifying CTEs to select wrong execution mode

* Fixes a bug preventing rollback in CREATE PROCEDURE

* Fixes a bug on not being able to drop index on a partitioned table

* Fixes a bug on TRUNCATE when there is a foreign key to a reference table

* Fixes a performance issue in prepared INSERT..SELECT

* Fixes a bug which causes errors on DROP DATABASE IF EXISTS

* Fixes a bug to remove intermediate result directory in pull-push execution

* Improves query pushdown planning performance

* Evaluate functions anywhere in query

### citus v7.5.1 (August 28, 2018) ###

* Improves query pushdown planning performance

* Fixes a bug that could cause modifying CTEs to select wrong execution mode

### citus v7.4.2 (July 27, 2018) ###

* Fixes a segfault in real-time executor during online shard move

### citus v7.5.0 (July 25, 2018) ###

* Adds foreign key support from hash distributed to reference tables

* Adds SELECT ... FOR UPDATE support for router plannable queries

* Adds support for non-partition columns in count distinct

* Fixes a segfault in real-time executor during online shard move

* Fixes ALTER TABLE ADD COLUMN constraint check

* Fixes a bug where INSERT ... SELECT was allowed to update distribution column

* Allows DDL commands to be sequentialized via `citus.multi_shard_modify_mode`

* Adds support for topn_union_agg and topn_add_agg across shards

* Adds support for hll_union_agg and hll_add_agg across shards

* Fixes a bug that might cause shards to have a wrong owner

* GUC select_opens_transaction_block defers opening transaction block on workers

* Utils to implement DDLs for policies in future, warn about being unsupported

* Intermediate results use separate connections to avoid interfering with tasks

* Adds a node_conninfo GUC to set outgoing connection settings

### citus v6.2.6 (July 06, 2018) ###

* Adds support for respecting enable_hashagg in the master planner

### citus v7.4.1 (June 20, 2018) ###

* Fixes a bug that could cause transactions to incorrectly proceed after failure

* Fixes a bug on INSERT ... SELECT queries in prepared statements

### citus v7.4.0 (May 15, 2018) ###

* Adds support for non-pushdownable subqueries and CTEs in UPDATE/DELETE queries

* Adds support for pushdownable subqueries and joins in UPDATE/DELETE queries

* Adds faster shard pruning for subqueries

* Adds partitioning support to MX table

* Adds support for (VACUUM | ANALYZE) VERBOSE

* Adds support for multiple ANDs in `HAVING` for pushdown planner

* Adds support for quotation needy schema names

* Improves operator check time in physical planner for custom data types

* Removes broadcast join logic

* Deprecates `large_table_shard_count` and `master_expire_table_cache()`

* Modifies master_update_node to lock write on shards hosted by node over update

* `DROP TABLE` now drops shards as the currrent user instead of the superuser

* Adds specialised error codes for connection failures

* Improves error messages on connection failure

* Fixes issue which prevented multiple `citus_table_size` calls per query

* Tests are updated to use `create_distributed_table`

### citus v7.2.2 (May 4, 2018) ###

* Fixes a bug that could cause SELECTs to crash during a rebalance

### citus v7.3.0 (March 15, 2018) ###

* Adds support for non-colocated joins between subqueries

* Adds support for window functions that can be pushed down to worker

* Adds support for modifying CTEs

* Adds recursive plan for subqueries in WHERE clause with recurring FROM clause

* Adds support for bool_ and bit_ aggregates

* Adds support for Postgres `jsonb` and `json` aggregation functions

* Adds support for respecting enable_hashagg in the master plan

* Adds support for renaming a distributed table

* Adds support for ALTER INDEX (SET|RESET|RENAME TO) commands

* Adds support for setting storage parameters on distributed tables

* Performance improvements to reduce distributed planning time

* Fixes a bug on planner when aggregate is used in ORDER BY

* Fixes a bug on planner when DISTINCT (ON) clause is used with GROUP BY

* Fixes a bug of creating coordinator planner with distinct and aggregate clause

* Fixes a bug that could open a new connection on every table size function call

* Fixes a bug canceling backends that are not involved in distributed deadlocks

* Fixes count distinct bug on column expressions when used with subqueries

* Improves error handling on worker node failures

* Improves error messages for INSERT queries that have subqueries

### citus v7.2.1 (February 6, 2018) ###

* Fixes count distinct bug on column expressions when used with subqueries

* Adds support for respecting enable_hashagg in the master plan

* Fixes a bug canceling backends that are not involved in distributed deadlocks

### citus v7.2.0 (January 16, 2018) ###

* Adds support for CTEs

* Adds support for subqueries that require merge step

* Adds support for set operations (UNION, INTERSECT, ...)

* Adds support for 2PC auto-recovery

* Adds support for querying local tables in CTEs and subqueries

* Adds support for more SQL coverage in subqueries for reference tables

* Adds support for count(distinct) in queries with a subquery

* Adds support for non-equijoins when there is already an equijoin for queries

* Adds support for non-equijoins when there is already an equijoin for subquery

* Adds support for real-time executor to run in transaction blocks

* Adds infrastructure for storing intermediate distributed query results

* Adds a new GUC named `enable_repartition_joins` for auto executor switch

* Adds support for limiting the intermediate result size

* Improves support for queries with unions containing filters

* Improves support for queries with unions containing joins

* Improves support for subqueries in the `WHERE` clause

* Increases `COPY` throughput

* Enables pushing down queries containing only recurring tuples and `GROUP BY`

* Load-balance queries that read from 0 shards

* Improves support for using functions in subqueries

* Fixes a bug that could cause real-time executor to crash during cancellation

* Fixes a bug that could cause real-time executor to get stuck on cancellation

* Fixes a bug that could block modification queries unnecessarily

* Fixes a bug that could cause assigning wrong IDs to transactions

* Fixes a bug that could cause an assert failure with `ANALYZE` statements

* Fixes a bug that could allow pushing down wrong set operations in subqueries

* Fixes a bug that could cause a deadlock in create_distributed_table

* Fixes a bug that could confuse user about `ANALYZE` usage

* Fixes a bug that could lead to false positive distributed deadlock detections

* Relaxes the locking for DDL commands on partitioned tables

* Relaxes the locking on `COPY` with replication

* Logs more remote commands when citus.log_remote_commands is set

### citus v6.2.5 (January 11, 2018) ###

* Fixes a bug that could crash the coordinator while reporting a remote error

### citus v7.1.2 (January 4, 2018) ###

* Fixes a bug that could cause assigning wrong IDs to transactions

* Increases `COPY` throughput

### citus v7.1.1 (December 1, 2017) ###

* Fixes a bug that could prevent pushing down subqueries with reference tables

* Fixes a bug that could create false positive distributed deadlocks

* Fixes a bug that could prevent running concurrent COPY and multi-shard DDL

* Fixes a bug that could mislead users about `ANALYZE` queries

### citus v7.1.0 (November 14, 2017) ###

* Adds support for native queries with multi shard `UPDATE`/`DELETE` queries

* Expands reference table support in subquery pushdown

* Adds window function support for subqueries and `INSERT ... SELECT` queries

* Adds support for `COUNT(DISTINCT) [ON]` queries on non-partition columns

* Adds support for `DISTINCT [ON]` queries on non-partition columns

* Introduces basic usage statistic collector

* Adds support for setting replica identity while creating distributed tables

* Adds support for `ALTER TABLE ... REPLICA IDENTITY` queries

* Adds pushdown support for `LIMIT` and `HAVING` grouped by partition key

* Adds support for `INSERT ... SELECT` queries via worker nodes on MX clusters

* Adds support for adding primary key using already defined index

* Adds parameter to shard copy functions to support distinct replication models

* Changes `shard_name` UDF to omit public schema name

* Adds `master_move_node` UDF to make changes on nodename/nodeport more easy

* Fixes a bug that could cause casting error with `INSERT ... SELECT` queries

* Fixes a bug that could prevent upgrading servers from Citus 6.1

* Fixes a bug that could prevent attaching partitions to a table in schema

* Fixes a bug that could prevent adding a node to cluster with reference table

* Fixes a bug that could cause a crash with `INSERT ... SELECT` queries

* Fixes a bug that could prevent creating a partitoned table on Cloud

* Implements various performance improvements

* Adds internal infrastructures and tests to improve development process

* Addresses various race conditions and deadlocks

* Improves and standardizes error messages

### citus v7.0.3 (October 16, 2017) ###

* Fixes several bugs that could cause crash

* Fixes a bug that could cause deadlock while creating reference tables

* Fixes a bug that could cause false-positives in deadlock detection

* Fixes a bug that could cause  2PC recovery not to work from MX workers

* Fixes a bug that could cause cache incohorency

* Fixes a bug that could cause maintenance daemon to skip cache invalidations

* Improves performance of transaction recovery by using correct index

### citus v7.0.2 (September 28, 2017) ###

* Updates task-tracker to limit file access

### citus v6.2.4 (September 28, 2017) ###

* Updates task-tracker to limit file access

### citus v6.1.3 (September 28, 2017) ###

* Updates task-tracker to limit file access

### citus v7.0.1 (September 12, 2017) ###

* Fixes a bug that could cause memory leaks in `INSERT ... SELECT` queries

* Fixes a bug that could cause incorrect execution of prepared statements

* Fixes a bug that could cause excessive memory usage during COPY

* Incorporates latest changes from core PostgreSQL code

### citus v7.0.0 (August 28, 2017) ###

* Adds support for PostgreSQL 10

* Drops support for PostgreSQL 9.5

* Adds support for multi-row `INSERT`

* Adds support for router `UPDATE` and `DELETE` queries with subqueries

* Adds infrastructure for distributed deadlock detection

* Deprecates `enable_deadlock_prevention` flag

* Adds support for partitioned tables

* Adds support for creating `UNLOGGED` tables

* Adds support for `SAVEPOINT`

* Adds UDF `citus_create_restore_point` for taking distributed snapshots

* Adds support for evaluating non-pushable `INSERT ... SELECT` queries

* Adds support for subquery pushdown on reference tables

* Adds shard pruning support for `IN` and `ANY`

* Adds support for `UPDATE` and `DELETE` commands that prune down to 0 shard

* Enhances transaction support by relaxing some transaction restrictions

* Fixes a bug causing crash if distributed table has no shards

* Fixes a bug causing crash when removing inactive node

* Fixes a bug causing failure during `COPY` on tables with dropped columns

* Fixes a bug causing failure during `DROP EXTENSION`

* Fixes a bug preventing executing `VACUUM` and `INSERT` concurrently

* Fixes a bug in prepared `INSERT` statements containing an implicit cast

* Fixes several issues related to statement cancellations and connections

* Fixes several 2PC related issues

* Removes an unnecessary dependency causing warning messages in pg_dump

* Adds internal infrastructure for follower clusters

* Adds internal infrastructure for progress tracking

* Implements various performance improvements

* Adds internal infrastructures and tests to improve development process

* Addresses various race conditions and deadlocks

* Improves and standardizes error messages

### citus v6.2.3 (July 13, 2017) ###

* Fixes a crash during execution of local CREATE INDEX CONCURRENTLY

* Fixes a bug preventing usage of quoted column names in COPY

* Fixes a bug in prepared INSERTs with implicit cast in partition column

* Relaxes locks in VACUUM to ensure concurrent execution with INSERT

### citus v6.2.2 (May 31, 2017) ###

* Fixes a common cause of deadlocks when repairing tables with foreign keys

### citus v6.2.1 (May 24, 2017) ###

* Relaxes version-check logic to avoid breaking non-distributed commands

### citus v6.2.0 (May 16, 2017) ###

* Increases SQL subquery coverage by pushing down more kinds of queries

* Adds CustomScan API support to allow read-only transactions

* Adds support for `CREATE/DROP INDEX CONCURRENTLY`

* Adds support for `ALTER TABLE ... ADD CONSTRAINT`

* Adds support for `ALTER TABLE ... RENAME COLUMN`

* Adds support for `DISABLE/ENABLE TRIGGER ALL`

* Adds support for expressions in the partition column in INSERTs

* Adds support for query parameters in combination with function evaluation

* Adds support for creating distributed tables from non-empty local tables

* Adds UDFs to get size of distributed tables

* Adds UDFs to add a new node without replicating reference tables

* Adds checks to prevent running Citus binaries with wrong metadata tables

* Improves shard pruning performance for range queries

* Improves planner performance for joins involving co-located tables

* Improves shard copy performance by creating indexes after copy

* Improves task-tracker performance by batching several status checks

* Enables router planner for queries on range partitioned table

* Changes `TRUNCATE` to drop local data only if `enable_ddl_propagation` is off

* Starts to execute DDL on coordinator before workers

* Fixes a bug causing incorrectly reading invalidated cache

* Fixes a bug related to creation of schemas of in workers with incorrect owner

* Fixes a bug related to concurrent run of shard drop functions

* Fixes a bug related to `EXPLAIN ANALYZE` with DML queries

* Fixes a bug related to SQL functions in FROM clause

* Adds a GUC variable to report cross shard queries

* Fixes a bug related to partition columns without native hash function

* Adds internal infrastructures and tests to improve development process

* Addresses various race conditions and deadlocks

* Improves and standardizes error messages

### citus v6.1.2 (May 31, 2017) ###

* Fixes a common cause of deadlocks when repairing tables with foreign keys

### citus v6.1.1 (May 5, 2017) ###

* Fixes a crash caused by router executor use after connection timeouts

* Fixes a crash caused by relation cache invalidation during COPY

* Fixes bug related to DDL use within PL/pgSQL functions

* Fixes a COPY bug related to types lacking binary output functions

* Fixes a bug related to modifications with parameterized partition values

* Fixes improper value interpolation in worker sequence generation

* Guards shard pruning logic against zero-shard tables

* Fixes possible NULL pointer dereference and buffer underflow (via PVS-Studio)

* Fixes a INSERT ... SELECT bug that could push down non-partition column JOINs

### citus v6.1.0 (February 9, 2017) ###

* Implements _reference tables_, transactionally replicated to all nodes

* Adds `upgrade_to_reference_table` UDF to upgrade pre-6.1 reference tables

* Expands prepared statement support to nearly all statements

* Adds support for creating `VIEW`s which reference distributed tables

* Adds targeted `VACUUM`/`ANALYZE` support

* Adds support for the `FILTER` clause in aggregate expressions

* Adds support for function evaluation within `INSERT INTO ... SELECT`

* Adds support for creating foreign key constraints with `ALTER TABLE`

* Adds logic to choose router planner for all queries it supports

* Enhances `create_distributed_table` with parameter for explicit colocation

* Adds generally useful utility UDFs previously available as "Citus Tools"

* Adds user-facing UDFs for locking shard resources and metadata

* Refactors connection and transaction management; giving a consistent experience

* Enhances `COPY` with fully transactional semantics

* Improves support for cancellation for a number of queries and commands

* Adds `column_to_column_name` UDF to help users understand `partkey` values

* Adds `master_disable_node` UDF for temporarily disabling nodes

* Adds proper MX ("masterless") metadata propagation logic

* Adds `start_metadata_sync_to_node` UDF to propagate metadata changes to nodes

* Enhances `SERIAL` compatibility with MX tables

* Adds an `node_connection_timeout` parameter to control node connection timeouts

* Adds `enable_deadlock_prevention` setting to permit multi-node transactions

* Adds a `replication_model` setting to specify replication of new tables

* Changes the `shard_replication_factor` setting's default value to one

* Adds code to automatically set `max_prepared_transactions` if not configured

* Accelerates lookup of colocated shard placements

* Fixes a bug affecting `INSERT INTO ... SELECT` queries using constant values

* Fixes a bug by ensuring `COPY` does not mark placements inactive

* Fixes a bug affecting reads from `pg_dist_shard_placement` table

* Fixes a crash triggered by creating a foreign key without a column

* Fixes a crash related to accessing catalog tables after aborted transactions

* Fixes a bug affecting JOIN queries requiring repartitions

* Fixes a bug affecting node insertions to `pg_dist_node` table

* Fixes a crash triggered by queries with modifying common table expressions

* Fixes a bug affecting workloads with concurrent shard appends and deletions

* Addresses various race conditions and deadlocks

* Improves and standardizes error messages

### citus v6.0.1 (November 29, 2016) ###

* Fixes a bug causing failures during pg_upgrade

* Fixes a bug preventing DML queries during colocated table creation

* Fixes a bug that caused NULL parameters to be incorrectly passed as text

### citus v6.0.0 (November 7, 2016) ###

* Adds compatibility with PostgreSQL 9.6, now the recommended version

* Removes the `pg_worker_list.conf` file in favor of a `pg_dist_node` table

* Adds `master_add_node` and `master_add_node` UDFs to manage membership

* Removes the `\stage` command and corresponding csql binary in favor of `COPY`

* Removes `copy_to_distributed_table` in favor of first-class `COPY` support

* Adds support for multiple DDL statements within a transaction

* Adds support for certain foreign key constraints

* Adds support for parallel `INSERT INTO ... SELECT` against colocated tables

* Adds support for the `TRUNCATE` command

* Adds support for `HAVING` clauses in `SELECT` queries

* Adds support for `EXCLUDE` constraints which include the partition column

* Adds support for system columns in queries (`tableoid`, `ctid`, etc.)

* Adds support for relation name extension within `INDEX` definitions

* Adds support for no-op `UPDATE`s of the partition column

* Adds several general-purpose utility UDFs to aid in Citus maintenance

* Adds `master_expire_table_cache` UDF to forcibly expire cached shards

* Parallelizes the processing of DDL commands which affect distributed tables

* Adds support for repartition jobs using composite or custom types

* Enhances object name extension to handle long names and large shard counts

* Parallelizes the `master_modify_multiple_shards` UDF

* Changes distributed table creation to error if the target table is not empty

* Changes the `pg_dist_shard.logicalrelid` column from an `oid` to `regclass`

* Adds a `placementid` column to `pg_dist_shard_placement`, replacing Oid use

* Removes the `pg_dist_shard.shardalias` distribution metadata column

* Adds `pg_dist_partition.repmodel` to track tables using streaming replication

* Adds internal infrastructure to take snapshots of distribution metadata

* Addresses the need to invalidate prepared statements on metadata changes

* Adds a `mark_tables_colocated` UDF for denoting pre-6.0 manual colocation

* Fixes a bug affecting prepared statement execution within PL/pgSQL

* Fixes a bug affecting `COPY` commands using composite types

* Fixes a bug that could cause crashes during `EXPLAIN EXECUTE`

* Separates worker and master job temporary folders

* Eliminates race condition between distributed modification and repair

* Relaxes the requirement that shard repairs also repair colocated shards

* Implements internal functions to track which tables' shards are colocated

* Adds `pg_dist_partition.colocationid` to track colocation group membership

* Extends shard copy and move operations to respect colocation settings

* Adds `pg_dist_local_group` to prepare for future MX-related changes

* Adds `create_distributed_table` to easily create shards and infer colocation

### citus v5.2.2 (November 7, 2016) ###

* Adds support for `IF NOT EXISTS` clause of `CREATE INDEX` command

* Adds support for `RETURN QUERY` and `FOR ... IN` PL/pgSQL features

* Extends the router planner to handle more queries

* Changes `COUNT` of zero-row sets to return `0` rather than an empty result

* Reduces the minimum permitted `task_tracker_delay` to a single millisecond

* Fixes a bug that caused crashes during joins with a `WHERE false` clause

* Fixes a bug triggered by unique violation errors raised in long transactions

* Fixes a bug resulting in multiple registration of transaction callbacks

* Fixes a bug which could result in stale reads of distribution metadata

* Fixes a bug preventing distributed modifications in some PL/pgSQL functions

* Fixes some code paths that could hypothetically read uninitialized memory

* Lowers log level of _waiting for activity_ messages

### citus v5.2.1 (September 6, 2016) ###

* Fixes subquery pushdown to properly extract outer join qualifiers

* Addresses possible memory leak during multi-shard transactions

### citus v5.2.0 (August 15, 2016) ###

* Drops support for PostgreSQL 9.4; PostgreSQL 9.5 is required

* Adds schema support for tables, other named objects (types, operators, etc.)

* Evaluates non-immutable functions on master in all modification commands

* Adds support for SERIAL types in non-partition columns

* Adds support for RETURNING clause in INSERT, UPDATE, and DELETE commands

* Adds support for multi-statement transactions involving a fixed set of nodes

* Full SQL support for SELECT queries which can be executed on a single worker

* Adds option to perform DDL changes using prepared transactions (2PC)

* Adds an `enable_ddl_propagation` parameter to control DDL propagation

* Accelerates shard pruning during merges

* Adds `master_modify_multiple_shards` UDF to modify many shards at once

* Adds COPY support for arrays of user-defined types

* Now supports parameterized prepared statements for certain use cases

* Extends LIMIT/OFFSET support to all executor types

* Constraint violations now fail fast rather than hitting all placements

* Makes `master_create_empty_shard` aware of shard placement policy

* Reduces unnecessary sleep during queries processed by real-time executor

* Improves task tracker executor's task cleanup logic

* Relaxes restrictions on cancellation of DDL commands

* Removes ONLY keyword from worker SELECT queries

* Error message improvements and standardization

* Moves `master_update_shard_statistics` function to `pg_catalog` schema

* Fixes a bug where hash-partitioned anti-joins could return incorrect results

* Now sets storage type correctly for foreign table-backed shards

* Fixes `master_update_shard_statistics` issue with hash-partitioned tables

* Fixes an issue related to extending table names that require escaping

* Reduces risk of row counter overflows during modifications

* Fixes a crash related to FILTER clause use in COUNT DISTINCT subqueries

* Fixes crashes related to partition columns with high attribute numbers

* Fixes certain subquery and join crashes

* Detects flex for build even if PostgreSQL was built without it

* Fixes assert-enabled crash when `all_modifications_commutative` is true

### citus v5.2.0-rc.1 (August 1, 2016) ###

* Initial 5.2.0 candidate

### citus v5.1.1 (June 17, 2016) ###

* Adds complex count distinct expression support in repartitioned subqueries

* Improves task tracker job cleanup logic, addressing a memory leak

* Fixes bug that generated incorrect results for LEFT JOIN queries

* Improves compatibility with Debian's reproducible builds project

* Fixes build issues on FreeBSD platforms

### citus v5.1.0 (May 17, 2016) ###

* Adds distributed COPY to rapidly populate distributed tables

* Adds support for using EXPLAIN on distributed queries

* Recognizes and fast-paths single-shard SELECT statements automatically

* Increases INSERT throughput via shard pruning optimizations

* Improves planner performance for joins involving tables with many shards

* Adds ability to pass columns as arguments to function calls in UPDATEs

* Introduces transaction manager for use by multi-shard commands

* Adds COUNT(DISTINCT ...) pushdown optimization for hash-partitioned tables

* Adds support for certain UNIQUE indexes on hash- or range-partitioned tables

* Deprecates \stage in favor of using COPY for append-partition tables

* Deprecates `copy_to_distributed_table` in favor of first-class COPY support

* Fixes build problems when using non-packaged PostgreSQL installs

* Fixes bug that sometimes skipped pruning when partitioned by a VARCHAR column

* Fixes bug impeding use of user-defined functions in repartitioned subqueries

* Fixes bug involving queries with equality comparisons of boolean types

* Fixes crash that prevented use alongside `pg_stat_statements`

* Fixes crash arising from SELECT queries that lack a target list

* Improves warning and error messages

### citus v5.1.0-rc.2 (May 10, 2016) ###

* Fixes test failures

* Fixes EXPLAIN output when FORMAT JSON in use

### citus v5.1.0-rc.1 (May 4, 2016) ###

* Initial 5.1.0 candidate

### citus v5.0.1 (April 15, 2016) ###

* Fixes issues on 32-bit systems

### citus v5.0.0 (March 24, 2016) ###

* Public release under AGPLv3

* PostgreSQL extension compatible with PostgreSQL 9.5 and 9.4
