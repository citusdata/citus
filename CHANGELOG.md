### citus v5.2.2 (November 4, 2016) ###

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
