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
