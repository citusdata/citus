-- Setup another Citus cluster before setting up the tests for "regression" cluster
\c postgres - - :worker_1_port
CREATE DATABASE stat_counters_test_db;
\c stat_counters_test_db - - -
CREATE EXTENSION citus;

\c postgres - - :worker_2_port
CREATE DATABASE stat_counters_test_db;
\c stat_counters_test_db - - -
CREATE EXTENSION citus;

\c postgres - - :master_port
CREATE DATABASE stat_counters_test_db;
\c stat_counters_test_db - - -
CREATE EXTENSION citus;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);
SELECT 1 FROM citus_add_node('localhost', :worker_1_port);
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- back to the "regression" database on coordinator that we usually use during tests
\c regression - - -

CREATE SCHEMA stat_counters;
SET search_path TO stat_counters;

SET citus.next_shard_id to 1970000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);
SET client_min_messages TO NOTICE;

-- make sure it's disabled first
SET citus.enable_stat_counters TO false;

-- verify that the UDFs don't do anything when NULL input is provided
SELECT citus_stat_counters(null);
SELECT citus_stat_counters_reset(null);

-- citus_stat_counters lists all the databases that currently exist
SELECT (SELECT COUNT(*) FROM citus_stat_counters) = (SELECT COUNT(*) FROM pg_database);

-- Verify that providing an oid that doesn't correspond to any database
-- returns an empty set. We know that "SELECT MAX(oid)+1 FROM pg_database"
-- is definitely not a valid database oid.
SELECT COUNT(*) = 0 FROM (SELECT citus_stat_counters((MAX(oid)::integer+1)::oid) FROM pg_database) q;

-- This is the first test in multi_1_schedule that calls citus_stat_counters_reset(), so one
-- can could have reset the stats before us. So, here we can test that stats_reset column is
-- NULL for that databases that citus_stat_counters_reset() was certainly not called for.
SELECT stats_reset IS NULL FROM citus_stat_counters WHERE name IN ('template0', 'template1');

-- Even more, calling citus_stat_counters_reset() for a database that no one has connected
-- so far is simply a no-op.
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = 'template0';
SELECT stats_reset IS NULL FROM citus_stat_counters WHERE name = 'template0';

-- but this is not true otherwise
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT stats_reset IS NOT NULL FROM citus_stat_counters WHERE name = current_database();

-- multi_1_schedule has this test in an individual line, so there cannot be any other backends
-- that can update the stat counters other than us except Citus Maintenance Daemon, but
-- Citus Maintenance Daemon is not supposed to update the query related stats, so we can
-- ensure that query related stats are 0.
--
-- So, no one could have incremented query related stats so far.
SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0 FROM citus_stat_counters;

-- Even further, for the databases that don't have Citus extension installed,
-- we should get 0 for other stats too.
SELECT connection_establishment_succeeded = 0,
       connection_establishment_failed = 0,
       connection_reused = 0
FROM (
    SELECT * FROM citus_stat_counters WHERE name NOT IN ('regression', 'stat_counters_test_db')
) q;

CREATE TABLE dist_table (a int, b int);
SELECT create_distributed_table('dist_table', 'a');

-- no single shard queries yet, so it's set to 0
SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- normally this should increment query_execution_single_shard counter, but the GUC is disabled
SELECT * FROM dist_table WHERE a = 1;
SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

SET citus.enable_stat_counters TO true;

-- increment query_execution_single_shard counter
SELECT * FROM dist_table WHERE a = 1;
SELECT query_execution_single_shard = 1 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- reset the stat counters for the current database by providing nothing to citus_stat_counters_reset()
SELECT citus_stat_counters_reset();

SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- increment query_execution_single_shard counter
SELECT * FROM dist_table WHERE a = 1;
SELECT query_execution_single_shard = 1 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- verify that we can reset the stats for a specific database
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT query_execution_single_shard = 0 FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- increment counters a bit
SELECT * FROM dist_table WHERE a = 1;
SELECT * FROM dist_table WHERE a = 1;
SELECT * FROM dist_table WHERE a = 1;
SELECT * FROM dist_table;
SELECT * FROM dist_table;

-- Close the current connection and open a new one to make sure that
-- backends save their stats before exiting.
\c - - - -

-- make sure that the GUC is disabled
SET citus.enable_stat_counters TO false;

-- these will be ineffecitve because the GUC is disabled
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;

-- Verify that we can observe the counters incremented before the GUC was
-- disabled, even when the GUC is disabled.
SELECT query_execution_single_shard = 3, query_execution_multi_shard = 2
FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

SET citus.enable_stat_counters TO true;

-- increment the counters a bit more
SELECT * FROM stat_counters.dist_table WHERE a = 1;
SELECT * FROM stat_counters.dist_table;

SET citus.force_max_query_parallelization TO ON;
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;
RESET citus.force_max_query_parallelization;

-- (*1) For the last two queries, we forced opening as many connections as
-- possible. So, we should expect connection_establishment_succeeded to be
-- incremented by some value closer to 32 shards * 2 queries = 64. However,
-- it might not be that high if the shard queries complete very quickly. So,
-- heuristically, we check that it's at least 50 to avoid making the test
-- flaky.
SELECT query_execution_single_shard = 4, query_execution_multi_shard = 5, connection_establishment_succeeded >= 50
FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- We can even see the counter values for "regression" database from
-- other databases that has Citus installed.
\c stat_counters_test_db - - -

-- make sure that the GUC is disabled
SET citus.enable_stat_counters TO false;

SELECT query_execution_single_shard = 4, query_execution_multi_shard = 5
FROM (SELECT (pg_catalog.citus_stat_counters(oid)).* FROM pg_database WHERE datname = 'regression') q;

-- enable it before exiting to make sure we save (all-zero) stats into the shared hash when exiting
SET citus.enable_stat_counters TO true;

-- repeat some of the tests from a worker node
\c regression - - :worker_1_port

-- make sure that the GUC is disabled
SET citus.enable_stat_counters TO false;

SET client_min_messages TO NOTICE;

-- reset the stat counters for the current database by providing 0 to citus_stat_counters_reset()
SELECT citus_stat_counters_reset(0);

-- No one could have incremented query related stats and connection_reused so far.
SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0, connection_reused = 0 FROM citus_stat_counters WHERE name = current_database();

SET citus.enable_stat_counters TO true;

SELECT * FROM stat_counters.dist_table WHERE a = 1;
SELECT * FROM stat_counters.dist_table WHERE a = 1;

-- first one establishes a connection, the second one reuses it
SELECT connection_reused = 1 FROM citus_stat_counters WHERE name = current_database();

SET citus.force_max_query_parallelization TO ON;
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;
SELECT * FROM stat_counters.dist_table;
RESET citus.force_max_query_parallelization;

-- As in (*1), we don't directly compare connection_establishment_succeeded
-- with 3 * 32 = 96 but with something smaller.
SELECT query_execution_single_shard = 2, query_execution_multi_shard = 3, connection_establishment_succeeded >= 80
FROM citus_stat_counters WHERE name = current_database();

SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();

SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0 FROM citus_stat_counters;

SELECT stats_reset into saved_stats_reset_t1 FROM citus_stat_counters WHERE name = current_database();
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();
SELECT stats_reset into saved_stats_reset_t2 FROM citus_stat_counters WHERE name = current_database();

-- check that that the latter is greater than the former
SELECT t1.stats_reset < t2.stats_reset FROM saved_stats_reset_t1 t1, saved_stats_reset_t2 t2;

DROP TABLE saved_stats_reset_t1, saved_stats_reset_t2;

\c regression postgres - :master_port

CREATE USER stat_counters_test_user;
GRANT ALL PRIVILEGES ON DATABASE regression TO stat_counters_test_user;
GRANT ALL PRIVILEGES ON SCHEMA stat_counters TO stat_counters_test_user;

ALTER USER stat_counters_test_user SET citus.enable_stat_counters TO true;

SET search_path TO stat_counters;
SET citus.next_shard_id to 2010000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

CREATE TABLE dist_table_1 (a int, b int);
SELECT create_distributed_table('dist_table_1', 'a');

CREATE TABLE uncolocated_dist_table (a int, b int);
SELECT create_distributed_table('uncolocated_dist_table', 'a', colocate_with => 'none');

CREATE TABLE single_shard (a int, b int);
SELECT create_distributed_table('single_shard', null);

CREATE TABLE single_shard_1 (a int, b int);
SELECT create_distributed_table('single_shard_1', null);

CREATE TABLE uncolocated_single_shard (a int, b int);
SELECT create_distributed_table('uncolocated_single_shard', null, colocate_with => 'none');

CREATE TABLE ref_table (a int, b int);
SELECT create_reference_table('ref_table');

CREATE TABLE local_table (a int, b int);
INSERT INTO local_table (a, b) VALUES (1, 1), (2, 2), (3, 3);

CREATE TABLE citus_local (a int, b int);
INSERT INTO citus_local (a, b) VALUES (1, 1), (2, 2), (3, 3);
SELECT citus_add_local_table_to_metadata('citus_local');

GRANT ALL ON ALL TABLES IN SCHEMA stat_counters TO stat_counters_test_user;

-- test copy while we're superuser
-- cannot call copy via exec_query_and_check_query_counters

SET citus.enable_stat_counters TO true;

SELECT query_execution_single_shard AS old_query_execution_single_shard,
       query_execution_multi_shard AS old_query_execution_multi_shard
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q \gset

copy dist_table(a) from program 'seq 1'; -- single shard

SELECT query_execution_single_shard - :old_query_execution_single_shard AS query_execution_single_shard_diff,
       query_execution_multi_shard - :old_query_execution_multi_shard AS query_execution_multi_shard_diff
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q;

SELECT query_execution_single_shard AS old_query_execution_single_shard,
       query_execution_multi_shard AS old_query_execution_multi_shard
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q \gset

copy dist_table(a) from program 'seq 2'; -- multi-shard

SELECT query_execution_single_shard - :old_query_execution_single_shard AS query_execution_single_shard_diff,
       query_execution_multi_shard - :old_query_execution_multi_shard AS query_execution_multi_shard_diff
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q;

-- load some data
insert into dist_table (a, b) select i, i from generate_series(1, 2) as i;

SELECT query_execution_single_shard AS old_query_execution_single_shard,
       query_execution_multi_shard AS old_query_execution_multi_shard
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q \gset

copy dist_table to stdout;

SELECT query_execution_single_shard - :old_query_execution_single_shard AS query_execution_single_shard_diff,
       query_execution_multi_shard - :old_query_execution_multi_shard AS query_execution_multi_shard_diff
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q;

SELECT query_execution_single_shard AS old_query_execution_single_shard,
       query_execution_multi_shard AS old_query_execution_multi_shard
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q \gset

copy (select * from dist_table join citus_local on dist_table.a = citus_local.a) to stdout;

SELECT query_execution_single_shard - :old_query_execution_single_shard AS query_execution_single_shard_diff,
       query_execution_multi_shard - :old_query_execution_multi_shard AS query_execution_multi_shard_diff
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q;

SELECT query_execution_single_shard AS old_query_execution_single_shard,
       query_execution_multi_shard AS old_query_execution_multi_shard
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q \gset

copy dist_table to :'temp_dir''stat_counters_dist_table_dump';

SELECT query_execution_single_shard - :old_query_execution_single_shard AS query_execution_single_shard_diff,
       query_execution_multi_shard - :old_query_execution_multi_shard AS query_execution_multi_shard_diff
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q;

SELECT query_execution_single_shard AS old_query_execution_single_shard,
       query_execution_multi_shard AS old_query_execution_multi_shard
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q \gset

copy dist_table from :'temp_dir''stat_counters_dist_table_dump';

SELECT query_execution_single_shard - :old_query_execution_single_shard AS query_execution_single_shard_diff,
       query_execution_multi_shard - :old_query_execution_multi_shard AS query_execution_multi_shard_diff
FROM (
    SELECT (citus_stat_counters(oid)).*
    FROM pg_database WHERE datname = current_database()
) q;

-- empty the table before rest of the tests
truncate dist_table;

\c stat_counters_test_db postgres - :master_port

-- reset from another database as superuser
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = 'regression';

SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0
FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

-- make sure that we can update and read the stats from a non-superuser
\c regression stat_counters_test_user - -

SET search_path TO stat_counters;

CREATE PROCEDURE exec_query_and_check_query_counters(
    input_sql text,
    query_execution_single_shard_diff_expected bigint,
    query_execution_multi_shard_diff_expected bigint
)
LANGUAGE PLPGSQL AS $$
DECLARE
    old_query_execution_single_shard bigint;
    old_query_execution_multi_shard bigint;
    new_query_execution_single_shard bigint;
    new_query_execution_multi_shard bigint;
BEGIN
    SELECT query_execution_single_shard, query_execution_multi_shard
    INTO old_query_execution_single_shard, old_query_execution_multi_shard
    FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

    COMMIT;

	EXECUTE input_sql;

    SELECT query_execution_single_shard, query_execution_multi_shard
    INTO new_query_execution_single_shard, new_query_execution_multi_shard
    FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

    IF (new_query_execution_single_shard - old_query_execution_single_shard != query_execution_single_shard_diff_expected) THEN
        RAISE EXCEPTION 'query_execution_single_shard counter is not incremented as expected, expected % but got %',
            query_execution_single_shard_diff_expected,
            new_query_execution_single_shard - old_query_execution_single_shard;
    END IF;

    IF (new_query_execution_multi_shard - old_query_execution_multi_shard != query_execution_multi_shard_diff_expected) THEN
        RAISE EXCEPTION 'query_execution_multi_shard counter is not incremented as expected, expected % but got %',
            query_execution_multi_shard_diff_expected,
            new_query_execution_multi_shard - old_query_execution_multi_shard;
    END IF;
END;
$$;

CALL exec_query_and_check_query_counters($$
    SELECT * FROM dist_table JOIN dist_table_1 ON dist_table.a = dist_table_1.a WHERE dist_table.a = 1
    $$,
    1, 0
);

CALL exec_query_and_check_query_counters($$
    SELECT * FROM dist_table JOIN dist_table_1 ON dist_table.a = dist_table_1.a
    $$,
    0, 1
);

-- same with explain
--
-- Explain without analyze should never increment the counters.
-- This also applies to all such tests in this file.
CALL exec_query_and_check_query_counters($$
    EXPLAIN
    SELECT * FROM dist_table JOIN dist_table_1 ON dist_table.a = dist_table_1.a
    $$,
    0, 0
);

-- same with explain analyze
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    SELECT * FROM dist_table JOIN dist_table_1 ON dist_table.a = dist_table_1.a
    $$,
    0, 1
);

SET citus.enable_repartition_joins TO true;
-- A repartition join only increments query_execution_multi_shard once, although
-- this doesn't feel so much ideal.
CALL exec_query_and_check_query_counters($$
    SELECT * FROM dist_table JOIN dist_table_1 ON dist_table.a = dist_table_1.b WHERE dist_table.a = 1
    $$,
    0, 1
);
RESET citus.enable_repartition_joins;

-- Subplans and the top level query plans separately increment the counters.
-- We first create an intermediate result for dist_table_1, this increments
-- query_execution_multi_shard by 1. Then we join the intermediate result with
-- ref_table, this increments query_execution_single_shard by 1 because it
-- is a single shard query.
CALL exec_query_and_check_query_counters($$
    SELECT * FROM ref_table LEFT JOIN dist_table ON dist_table.a = ref_table.a
    $$,
    1, 1
);

-- OFFSET 0 forces creating an intermediate result for dist_table, this increments
-- query_execution_multi_shard by 1. Then we query the intermediate result
-- with a single shard query, this increments query_execution_single_shard by 1.
CALL exec_query_and_check_query_counters($$
    SELECT * FROM (SELECT * FROM dist_table OFFSET 0) q
    $$,
    1, 1
);

-- same with explain
CALL exec_query_and_check_query_counters($$
    EXPLAIN
    SELECT * FROM (SELECT * FROM dist_table OFFSET 0) q
    $$,
    0, 0
);

-- same with explain analyze
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    SELECT * FROM (SELECT * FROM dist_table OFFSET 0) q
    $$,
    1, 1
);

CALL exec_query_and_check_query_counters($$
    DELETE FROM dist_table WHERE a = 1
    $$,
    1, 0
);

-- shard pruning is considered too
CALL exec_query_and_check_query_counters($$
    DELETE FROM dist_table WHERE a >= 1 AND a = 1
    $$,
    1, 0
);

CALL exec_query_and_check_query_counters($$
    UPDATE dist_table
    SET b = 1
    FROM dist_table_1
    JOIN ref_table ON dist_table_1.a = ref_table.a
    WHERE dist_table_1.a = 1 AND dist_table.a = dist_table_1.a
    $$,
    1, 0
);

CALL exec_query_and_check_query_counters($$
    DELETE FROM dist_table
    USING dist_table_1
    WHERE dist_table.a = dist_table_1.a
    $$,
    0, 1
);

-- multi-shard insert
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table (a, b) VALUES (-1, -1), (-2, -2), (-3, -3)
    $$,
    0, 1
);

-- single-shard insert
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table (a, b) VALUES (-4, -4)
    $$,
    1, 0
);

PREPARE p1 (bigint) AS SELECT * FROM dist_table WHERE a = $1;

CALL exec_query_and_check_query_counters($$
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    EXECUTE p1(1);
    $$,
    10, 0
);

CALL exec_query_and_check_query_counters($$
    WITH deleted_rows AS (
        -- multi-shard
        DELETE FROM uncolocated_dist_table
        RETURNING *
    ),
    dummy_cte AS (
        SELECT count(*) FROM -- single-shard (cross join between intermediate results)
        (SELECT * FROM dist_table_1 LIMIT 1) q1, -- multi-shard
        (SELECT b, count(*) AS a_count FROM dist_table_1 GROUP BY b) q2 -- multi-shard
    )
    -- multi-shard
    UPDATE dist_table
    SET b = 1
    FROM dist_table_1
    JOIN ref_table ON dist_table_1.a = ref_table.a
    JOIN deleted_rows ON dist_table_1.a = deleted_rows.a
    CROSS JOIN dummy_cte
    WHERE dist_table.a = dist_table_1.a;
    $$,
    1, 4
);

-- Select query is multi-shard and the same is also true for the final insert
-- but only if it doesn't prune to zero shards, which happens when the source
-- table is empty. So here, both query_execution_multi_shard and
-- query_execution_single_shard are incremented by 1.
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table SELECT * FROM uncolocated_dist_table
    $$,
    1, 1
);

insert into uncolocated_dist_table (a, b) values (1, 1), (2, 2), (3, 3);

-- However, the same insert increments query_execution_multi_shard by 2
-- when the source table is not empty.
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table SELECT * FROM uncolocated_dist_table
    $$,
    0, 2
);

CALL exec_query_and_check_query_counters($$
    INSERT INTO single_shard SELECT * FROM single_shard_1
    $$,
    1, 0
);

-- same with explain
CALL exec_query_and_check_query_counters($$
    EXPLAIN
    INSERT INTO single_shard SELECT * FROM single_shard_1
    $$,
    0, 0
);

-- same with explain analyze
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    INSERT INTO single_shard SELECT * FROM single_shard_1
    $$,
    1, 0
);

CALL exec_query_and_check_query_counters($$
    INSERT INTO single_shard SELECT * FROM uncolocated_single_shard
    $$,
    2, 0
);

CALL exec_query_and_check_query_counters($$
    WITH big_cte AS (
        WITH first_cte AS (
            -- multi-shard
            SELECT b, sum(a) AS a_sum
            FROM uncolocated_dist_table
            GROUP BY b
        ),
        dummy_cte AS (
            SELECT count(*) FROM -- single-shard (cross join between intermediate results)
            (SELECT * FROM dist_table_1 ORDER BY a LIMIT 1) q1, -- multi-shard
            (SELECT b, count(*) AS a_count FROM dist_table_1 GROUP BY b) q2 -- multi-shard
        )
        -- multi-shard
        SELECT dist_table.a, dist_table.b
        FROM dist_table
        JOIN dist_table_1 ON dist_table.a = dist_table_1.a
        JOIN first_cte ON dist_table_1.a = first_cte.a_sum
        CROSS JOIN dummy_cte
        WHERE dist_table.a = dist_table_1.a
    ),
    another_cte AS (
        -- single-shard
        SELECT * FROM ref_table ORDER BY a LIMIT 64
    )
    -- final insert: multi-shard
    INSERT INTO dist_table (a, b)
    -- source: multi-shard
    SELECT uncolocated_dist_table.a, uncolocated_dist_table.b FROM uncolocated_dist_table
    LEFT JOIN big_cte ON uncolocated_dist_table.a = big_cte.a
    LEFT JOIN another_cte ON uncolocated_dist_table.a = another_cte.a
    $$,
    2, 6
);

CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table (a, b) SELECT * FROM local_table
    $$,
    0, 1
);

CALL exec_query_and_check_query_counters($$
    INSERT INTO local_table (a, b) SELECT * FROM dist_table
    $$,
    0, 1
);

CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table (a, b) SELECT * FROM citus_local
    $$,
    1, 1
);

CALL exec_query_and_check_query_counters($$
    INSERT INTO citus_local (a, b) SELECT * FROM dist_table
    $$,
    1, 1
);

-- same with explain
CALL exec_query_and_check_query_counters($$
    EXPLAIN
    INSERT INTO citus_local (a, b) SELECT * FROM dist_table
    $$,
    0, 0
);

-- same with explain analyze, not supported today
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    INSERT INTO citus_local (a, b) SELECT * FROM dist_table
    $$,
    1, 1
);

insert into dist_table_1 (a, b) values (1, 1), (2, 2), (3, 3);

-- First, we pull the select (multi-shard) query to the query node and create an
-- intermediate results for it because we cannot pushdown the whole INSERT query.
-- Then, the select query becomes of the form:
--   SELECT .. FROM (SELECT .. FROM read_intermediate_result(..)) intermediate_result
--
-- So, while repartitioning the select query, we perform a single-shard read
-- query because we read from an intermediate result and we then partition it
-- across the nodes. For the read part, we increment query_execution_single_shard
-- because we go through distributed planning if there are read_intermediate_result()
-- calls in a query, so it happens to be a distributed plan and goes through our
-- CustomScan callbacks. For the repartitioning of the intermediate result, just
-- as usual, we don't increment any counters.
--
-- Then, the final insert query happens between the distributed table and the
-- colocated intermediate result, so this increments query_execution_multi_shard
-- by 1.
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table SELECT * FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) q RETURNING *
    $$,
    1, 2
);

-- Same query but without RETURNING - this goes through a different code path, but
-- the counters are still incremented the same way as above.
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table SELECT * FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) q
    $$,
    1, 2
);

-- Same query but inserting a single row makes the final query single-shard too.
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table SELECT * FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 1) q
    $$,
    2, 1
);

-- A similar query but with a cte.
-- Subplan execution for the cte, additionally, first increments query_execution_multi_shard
-- for "SELECT * FROM dist_table" when creating the intermediate result for it and then
-- query_execution_single_shard for;
--   <intermediate-result>
--   EXCEPT
--   SELECT i as a, i as b FROM generate_series(10, 32) AS i
CALL exec_query_and_check_query_counters($$
    WITH cte AS (
        SELECT * FROM dist_table
        EXCEPT
        SELECT i as a, i as b FROM generate_series(10, 32) AS i
    )
    INSERT INTO dist_table
    SELECT q.a, q.b
    FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) q
    JOIN cte ON q.a = cte.a
    RETURNING *
    $$,
    2, 3
);

-- the same query but this time the cte is part of the select, not the insert
CALL exec_query_and_check_query_counters($$
    INSERT INTO dist_table
    WITH cte AS (
        SELECT * FROM dist_table
        EXCEPT
        SELECT i as a, i as b FROM generate_series(10, 32) AS i
    )
    SELECT q.a, q.b
    FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) q
    JOIN cte ON q.a = cte.a
    RETURNING *
    $$,
    2, 3
);

-- same with explain
CALL exec_query_and_check_query_counters($$
    EXPLAIN
    INSERT INTO dist_table
    WITH cte AS (
        SELECT * FROM dist_table
        EXCEPT
        SELECT i as a, i as b FROM generate_series(10, 32) AS i
    )
    SELECT q.a, q.b
    FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) q
    JOIN cte ON q.a = cte.a
    RETURNING *
    $$,
    0, 0
);

-- same with explain analyze, not supported today
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    INSERT INTO dist_table
    WITH cte AS (
        SELECT * FROM dist_table
        EXCEPT
        SELECT i as a, i as b FROM generate_series(10, 32) AS i
    )
    SELECT q.a, q.b
    FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) q
    JOIN cte ON q.a = cte.a
    RETURNING *
    $$,
    2, 3
);

-- A similar one but without the insert, so we would normally expect 2 increments
-- for query_execution_single_shard and 2 for query_execution_multi_shard instead
-- of 3 since the insert is not there anymore.
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    -- single-shard subplan (whole cte)
    WITH cte AS (
        -- multi-shard subplan (lhs of EXCEPT)
        SELECT * FROM dist_table
        EXCEPT
        SELECT i as a, i as b FROM generate_series(10, 32) AS i
    )
    SELECT q.a, q.b
    FROM (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) q -- multi-shard subplan (subquery q)
    JOIN cte ON q.a = cte.a
    $$,
    2, 2
);

-- safe to push-down
CALL exec_query_and_check_query_counters($$
    SELECT * FROM (SELECT * FROM dist_table UNION SELECT * FROM dist_table) as foo
    $$,
    0, 1
);

-- weird but not safe to pushdown because the set operation is NOT wrapped into a subquery.
CALL exec_query_and_check_query_counters($$
    SELECT * FROM dist_table UNION SELECT * FROM dist_table
    $$,
    1, 2
);

SET citus.local_table_join_policy TO "prefer-local";
CALL exec_query_and_check_query_counters($$
    SELECT * FROM dist_table, local_table WHERE dist_table.a = local_table.a
    $$,
    0, 1
);
RESET citus.local_table_join_policy;

CALL exec_query_and_check_query_counters($$
    MERGE INTO dist_table AS t
    USING dist_table_1 AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 1
);

-- First, we pull the merge (multi-shard) query to the query node and create an
-- intermediate results for it because we cannot pushdown the whole INSERT query.
-- Then, the merge query becomes of the form:
--   SELECT .. FROM (SELECT .. FROM read_intermediate_result(..)) citus_insert_select_subquery
--
-- So, while repartitioning the source query, we perform a single-shard read
-- query because we read from an intermediate result and we then partition it
-- across the nodes. For the read part, we increment query_execution_single_shard
-- because we go through distributed planning if there are read_intermediate_result()
-- calls in a query, so it happens to be a distributed plan and goes through our
-- CustomScan callbacks. For the repartitioning of the intermediate result, just
-- as usual, we don't increment any counters.
--
-- Then, the final merge query happens between the distributed table and the
-- colocated intermediate result, so this increments query_execution_multi_shard
-- by 1.
CALL exec_query_and_check_query_counters($$
    MERGE INTO dist_table AS t
    USING (SELECT * FROM dist_table_1 ORDER BY a LIMIT 16) AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    1, 2
);

truncate dist_table;

CALL exec_query_and_check_query_counters($$
    insert into dist_table (a, b) select i, i from generate_series(1, 128) as i
    $$,
    0, 1
);

CALL exec_query_and_check_query_counters($$
    MERGE INTO dist_table AS t
    USING uncolocated_dist_table AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 2
);

-- same with explain
CALL exec_query_and_check_query_counters($$
    EXPLAIN
    MERGE INTO dist_table AS t
    USING uncolocated_dist_table AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 0
);

-- same with explain analyze, not supported today
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    MERGE INTO dist_table AS t
    USING uncolocated_dist_table AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 2
);

truncate dist_table, ref_table, uncolocated_dist_table;

insert into dist_table (a, b) select i, i from generate_series(1, 128) as i;
insert into uncolocated_dist_table (a, b) select i, i from generate_series(1, 95) as i;
insert into ref_table (a, b) select i, i from generate_series(33, 128) as i;

CALL exec_query_and_check_query_counters($$
    WITH cte AS (
        SELECT uncolocated_dist_table.a, uncolocated_dist_table.b
        FROM uncolocated_dist_table JOIN ref_table ON uncolocated_dist_table.a = ref_table.a
    )
    MERGE INTO dist_table AS t
    USING cte AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 2
);

truncate dist_table, dist_table_1;

insert into dist_table (a, b) select i, i from generate_series(1, 128) as i;
insert into dist_table_1 (a, b) select i, i from generate_series(1, 95) as i;

-- Not ideal but since this contains both distributed and reference tables,
-- we directly decide partitioning for the source instead of pulling it to
-- the query node and repartitioning from there.
CALL exec_query_and_check_query_counters($$
    WITH cte AS (
        SELECT dist_table_1.a, dist_table_1.b
        FROM dist_table_1 JOIN ref_table ON dist_table_1.a = ref_table.a
    )
    MERGE INTO dist_table AS t
    USING cte AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 2
);

-- pushable
CALL exec_query_and_check_query_counters($$
    WITH cte AS (
        SELECT dist_table_1.a, dist_table_1.b * 2 as b FROM dist_table_1
    )
    MERGE INTO dist_table AS t
    USING cte AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 1
);

-- same with explain
CALL exec_query_and_check_query_counters($$
    EXPLAIN
    WITH cte AS (
        SELECT dist_table_1.a, dist_table_1.b * 2 as b FROM dist_table_1
    )
    MERGE INTO dist_table AS t
    USING cte AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 0
);

-- same with explain analyze
CALL exec_query_and_check_query_counters($$
    EXPLAIN (ANALYZE)
    WITH cte AS (
        SELECT dist_table_1.a, dist_table_1.b * 2 as b FROM dist_table_1
    )
    MERGE INTO dist_table AS t
    USING cte AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 1
);

-- pushable
CALL exec_query_and_check_query_counters($$
    MERGE INTO dist_table AS t
    USING (SELECT dist_table_1.a, dist_table_1.b * 2 as b FROM dist_table_1) AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 1
);

-- pushable
CALL exec_query_and_check_query_counters($$
    MERGE INTO dist_table AS t
    USING dist_table_1 AS s ON t.a = s.a
    WHEN MATCHED THEN
        UPDATE SET b = s.b
    WHEN NOT MATCHED THEN
        INSERT (a, b) VALUES (s.a, s.b)
    $$,
    0, 1
);

-- citus_stat_counters lists all the databases that currently exist
SELECT (SELECT COUNT(*) FROM citus_stat_counters) = (SELECT COUNT(*) FROM pg_database);

-- verify that we cannot execute citus_stat_counters_reset() from a non-superuser
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();

\c - postgres - -

ALTER USER stat_counters_test_user SUPERUSER;

\c - stat_counters_test_user - -

-- verify that another superuser can execute citus_stat_counters_reset()
SELECT citus_stat_counters_reset(oid) FROM pg_database WHERE datname = current_database();

SELECT query_execution_single_shard = 0, query_execution_multi_shard = 0
FROM (SELECT (citus_stat_counters(oid)).* FROM pg_database WHERE datname = current_database()) q;

\c regression postgres - :master_port

-- drop the test cluster
\c regression - - :worker_1_port
DROP DATABASE stat_counters_test_db WITH (FORCE);
\c regression - - :worker_2_port
DROP DATABASE stat_counters_test_db WITH (FORCE);

\c regression - - :master_port

-- save its oid before dropping
SELECT oid AS stat_counters_test_db_oid FROM pg_database WHERE datname = 'stat_counters_test_db' \gset

DROP DATABASE stat_counters_test_db WITH (FORCE);

-- even if the database is dropped, citus_stat_counters() still returns a row for it
SELECT COUNT(*) = 1 FROM citus_stat_counters() WHERE database_id = :'stat_counters_test_db_oid';
SELECT COUNT(*) = 1 FROM (SELECT citus_stat_counters(:'stat_counters_test_db_oid')) q;

-- However, citus_stat_counters just ignores dropped databases
SELECT COUNT(*) = 0 FROM citus_stat_counters WHERE name = 'stat_counters_test_db';

-- clean up for the current database
REVOKE ALL ON DATABASE regression FROM stat_counters_test_user;
REVOKE ALL ON SCHEMA stat_counters FROM stat_counters_test_user;
REVOKE ALL ON ALL TABLES IN SCHEMA stat_counters FROM stat_counters_test_user;

SET client_min_messages TO WARNING;
DROP SCHEMA stat_counters CASCADE;
DROP USER stat_counters_test_user;
