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
