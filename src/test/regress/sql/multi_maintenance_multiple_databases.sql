-- This test verfies a behavioir of maintenance daemon in multi-database environment
-- It checks that distributed deadlock detection and 2PC transaction recovery should respect the citus.shared_pool_size_maintenance_quota.
-- To do that, it created 100 databases and syntactically generates distributed transactions in various states there.

SELECT $definition$
       ALTER SYSTEM SET citus.recover_2pc_interval TO '-1';
       ALTER SYSTEM SET citus.distributed_deadlock_detection_factor = '-1';
       SELECT pg_reload_conf();
       $definition$ AS turn_off_maintenance
\gset

SELECT $deinition$
ALTER SYSTEM SET citus.recover_2pc_interval TO '5s';
ALTER SYSTEM SET citus.max_maintenance_shared_pool_size = 10;
ALTER SYSTEM RESET citus.distributed_deadlock_detection_factor;
SELECT pg_reload_conf();
$deinition$ AS turn_on_maintenance
\gset

SELECT $definition$
       DO
       $do$
       DECLARE
           index int;
           db_name text;
           current_port int;
           db_create_statement text;
       BEGIN
       SELECT setting::int FROM pg_settings WHERE name = 'port'
       INTO current_port;
       FOR index IN 1..100
           LOOP
           SELECT format('db%s', index)
           INTO db_name;

           SELECT format('CREATE DATABASE %I', db_name)
           INTO db_create_statement;

           PERFORM dblink(format('dbname=regression host=localhost port=%s user=postgres', current_port),
               db_create_statement);
           PERFORM dblink(format('dbname=%s host=localhost port=%s user=postgres', db_name, current_port),
               'CREATE EXTENSION citus;');
           IF (SELECT groupid = 0 FROM pg_dist_node WHERE nodeport = current_port) THEN
               PERFORM dblink_exec(format('dbname=%s host=localhost port=%s user=postgres', db_name, current_port),
                   format($add_workers$SELECT citus_add_node('localhost', %s);COMMIT;$add_workers$, nodeport))
               FROM pg_dist_node
               WHERE groupid != 0 AND isactive AND noderole = 'primary';
               END IF;
           END LOOP;
       END;
       $do$;
       $definition$ AS create_databases
\gset

-- Code reiles heavily on dblink for cross-db and cross-node queries
CREATE EXTENSION IF NOT EXISTS dblink;

-- Disable maintenance operations to prepare the environment
:turn_off_maintenance

\c - - - :worker_1_port

:turn_off_maintenance

\c - - - :worker_2_port

:turn_off_maintenance

-- Create databases

\c - - - :worker_1_port

:create_databases

SELECT count(*)
FROM pg_database
WHERE datname LIKE 'db%';

\c - - - :worker_2_port

:create_databases

SELECT count(*)
FROM pg_database
WHERE datname LIKE 'db%';

\c - - - :master_port

:create_databases

SELECT count(*)
FROM pg_database
WHERE datname LIKE 'db%';

-- Generate distributed transactions

\c - - - :master_port

DO
$do$
    DECLARE
        index                       int;
        db_name                     text;
        transaction_to_abort_name   text;
        transaction_to_commit_name  text;
        transaction_to_be_forgotten text;
        coordinator_port            int;
    BEGIN
        FOR index IN 1..100
            LOOP
                SELECT format('db%s', index)
                INTO db_name;

                SELECT format('citus_0_1234_3_0_%s', oid)
                FROM pg_database
                WHERE datname = db_name
                INTO transaction_to_abort_name;

                SELECT format('citus_0_1234_4_0_%s', oid)
                FROM pg_database
                WHERE datname = db_name
                INTO transaction_to_commit_name;

                SELECT format('citus_0_should_be_forgotten_%s', oid)
                FROM pg_database
                WHERE datname = db_name
                INTO transaction_to_be_forgotten;

                SELECT setting::int
                FROM pg_settings
                WHERE name = 'port'
                INTO coordinator_port;

                -- Prepare transactions on workers
                PERFORM dblink_exec(format('dbname=%s host=localhost port=%s user=postgres', db_name, nodeport),
                                    format($worker_cmd$
                                           BEGIN;
                                           CREATE TABLE should_abort
                                               (value int);
                                           PREPARE TRANSACTION '%s';

                                           BEGIN;
                                           CREATE TABLE should_commit
                                               (value int);
                                           PREPARE TRANSACTION '%s';
                                           $worker_cmd$, transaction_to_abort_name, transaction_to_commit_name))
                FROM pg_dist_node
                WHERE groupid != 0
                  AND isactive
                  AND noderole = 'primary';

                -- Fill the pg_dist_transaction
                PERFORM dblink_exec(format('dbname=%s host=localhost port=%s user=postgres', db_name, coordinator_port),
                                    format($coordinator_cmd$
                                           INSERT INTO pg_dist_transaction
                                           SELECT groupid, '%s' FROM pg_dist_node
                                           UNION ALL
                                           SELECT groupid, '%s' FROM pg_dist_node;
                                           $coordinator_cmd$, transaction_to_commit_name, transaction_to_be_forgotten));
            END LOOP;
    END;
$do$;

-- Verify state before enabling maintenance
\c - - - :master_port

SELECT count(*) = 400 AS pg_dist_transaction_before_recovery_coordinator_test
FROM pg_database,
     dblink(format('dbname=%s host=localhost port=%s user=postgres', datname,
                   (SELECT setting::int FROM pg_settings WHERE name = 'port')),
            $statement$
            SELECT groupid, gid
            FROM pg_dist_transaction
            WHERE gid LIKE 'citus_0_1234_4_0_%'
                OR gid LIKE 'citus_0_should_be_forgotten_%'
            $statement$) AS t(groupid integer, gid text)
WHERE datname LIKE 'db%';

SELECT count(*) = 0 AS cached_connections_before_recovery_coordinator_test
FROM pg_stat_activity
WHERE state = 'idle'
  AND now() - backend_start > '5 seconds'::interval;

\c - - - :worker_1_port

SELECT count(*) = 100 AS pg_prepared_xacts_before_recover_worker_1_test
FROM pg_prepared_xacts
WHERE gid LIKE 'citus_0_1234_4_0_%'
   OR gid LIKE 'citus_0_should_be_forgotten_%';

SELECT count(*) = 0 AS cached_connections_before_recovery_worker_1_test
FROM pg_stat_activity
WHERE state = 'idle'
  AND now() - backend_start > '5 seconds'::interval;

\c - - - :worker_2_port

SELECT count(*) = 100 AS pg_prepared_xacts_before_recover_worker_2_test
FROM pg_prepared_xacts
WHERE gid LIKE 'citus_0_1234_4_0_%'
   OR gid LIKE 'citus_0_should_be_forgotten_%';

SELECT count(*) = 0 AS cached_connections_before_recovery_worker_2_test
FROM pg_stat_activity
WHERE state = 'idle'
  AND now() - backend_start > '5 seconds'::interval;

-- Turn on the maintenance

\c - - - :master_port

:turn_on_maintenance

\c - - - :worker_1_port

:turn_on_maintenance

\c - - - :worker_2_port

:turn_on_maintenance

\c - - - :master_port

-- Verify maintenance result

SELECT count(*) = 0 AS too_many_clients_test
FROM regexp_split_to_table(pg_read_file('../log/postmaster.log'), E'\n') AS t(log_line)
WHERE log_line LIKE '%sorry, too many clients already%';

DO
$$
    DECLARE
        pg_dist_transaction_after_recovery_coordinator_test boolean;
        cached_connections_after_recovery_coordinator_test  boolean;
    BEGIN
        FOR i IN 0 .. 300
            LOOP
                IF i = 300 THEN RAISE 'Waited too long'; END IF;
                SELECT count(*) = 0
                FROM pg_database,
                     dblink(format('dbname=%s host=localhost port=%s user=postgres', datname,
                                   (SELECT setting::int FROM pg_settings WHERE name = 'port')),
                            $statement$
                            SELECT groupid, gid
                            FROM pg_dist_transaction
                            WHERE gid LIKE 'citus_0_1234_4_0_%'
                                OR gid LIKE 'citus_0_should_be_forgotten_%'
                            $statement$) AS t(groupid integer, gid text)
                WHERE datname LIKE 'db%'
                INTO pg_dist_transaction_after_recovery_coordinator_test;

                SELECT count(*) = 0 AS cached_connections_after_recovery_coordinator_test
                FROM pg_stat_activity
                WHERE state = 'idle'
                  AND now() - backend_start > '5 seconds'::interval
                INTO cached_connections_after_recovery_coordinator_test;

                IF (pg_dist_transaction_after_recovery_coordinator_test
                    AND cached_connections_after_recovery_coordinator_test) THEN
                    EXIT;
                END IF;

                PERFORM pg_sleep_for('1 SECOND'::interval);
            END LOOP;
    END
$$;

\c - - - :worker_1_port

SELECT count(*) = 0 AS too_many_clients_test
FROM regexp_split_to_table(pg_read_file('../log/postmaster.log'), E'\n') AS t(log_line)
WHERE log_line LIKE '%sorry, too many clients already%';

SELECT count(*) = 0 AS pg_prepared_xacts_after_recover_worker_1_test
FROM pg_prepared_xacts
WHERE gid LIKE 'citus_0_1234_4_0_%'
   OR gid LIKE 'citus_0_should_be_forgotten_%';

DO
$$
    BEGIN
        FOR i IN 0 .. 300
            LOOP
                IF i = 300 THEN RAISE 'Waited too long'; END IF;
                IF (SELECT count(*) = 0 AS cached_connections_after_recovery_worker_1_test
                    FROM pg_stat_activity
                    WHERE state = 'idle'
                      AND now() - backend_start > '5 seconds'::interval) THEN
                    EXIT;
                END IF;
                PERFORM pg_sleep_for('1 SECOND'::interval);
            END LOOP;
    END
$$;

\c - - - :worker_2_port

SELECT count(*) = 0 AS too_many_clients_test
FROM regexp_split_to_table(pg_read_file('../log/postmaster.log'), E'\n') AS t(log_line)
WHERE log_line LIKE '%sorry, too many clients already%';

SELECT count(*) = 0 AS pg_prepared_xacts_after_recover_worker_2_test
FROM pg_prepared_xacts
WHERE gid LIKE 'citus_0_1234_4_0_%'
   OR gid LIKE 'citus_0_should_be_forgotten_%';

DO
$$
    BEGIN
        FOR i IN 0 .. 300
            LOOP
                IF i = 300 THEN RAISE 'Waited too long'; END IF;
                IF (SELECT count(*) = 0 AS cached_connections_after_recovery_worker_2_test
                    FROM pg_stat_activity
                    WHERE state = 'idle'
                      AND now() - backend_start > '5 seconds'::interval) THEN
                    EXIT;
                END IF;
                PERFORM pg_sleep_for('1 SECOND'::interval);
            END LOOP;
    END
$$;

-- Cleanup

\c - - - :master_port

SELECT $definition$
       DO
       $do$
       DECLARE
           index int;
           db_name text;
           current_port int;
       BEGIN
       SELECT setting::int FROM pg_settings WHERE name = 'port'
       INTO current_port;
       FOR index IN 1..100
           LOOP
           SELECT format('db%s', index)
           INTO db_name;

           PERFORM dblink(format('dbname=%s host=localhost port=%s user=postgres', db_name, current_port),
               'DROP EXTENSION citus;');
           END LOOP;
       END;
       $do$;

       -- Dropping tables explicitly because ProcSignalBarrier prevents from using dblink
       DROP DATABASE db1 WITH (FORCE);
       DROP DATABASE db2 WITH (FORCE);
       DROP DATABASE db3 WITH (FORCE);
       DROP DATABASE db4 WITH (FORCE);
       DROP DATABASE db5 WITH (FORCE);
       DROP DATABASE db6 WITH (FORCE);
       DROP DATABASE db7 WITH (FORCE);
       DROP DATABASE db8 WITH (FORCE);
       DROP DATABASE db9 WITH (FORCE);
       DROP DATABASE db10 WITH (FORCE);
       DROP DATABASE db11 WITH (FORCE);
       DROP DATABASE db12 WITH (FORCE);
       DROP DATABASE db13 WITH (FORCE);
       DROP DATABASE db14 WITH (FORCE);
       DROP DATABASE db15 WITH (FORCE);
       DROP DATABASE db16 WITH (FORCE);
       DROP DATABASE db17 WITH (FORCE);
       DROP DATABASE db18 WITH (FORCE);
       DROP DATABASE db19 WITH (FORCE);
       DROP DATABASE db20 WITH (FORCE);
       DROP DATABASE db21 WITH (FORCE);
       DROP DATABASE db22 WITH (FORCE);
       DROP DATABASE db23 WITH (FORCE);
       DROP DATABASE db24 WITH (FORCE);
       DROP DATABASE db25 WITH (FORCE);
       DROP DATABASE db26 WITH (FORCE);
       DROP DATABASE db27 WITH (FORCE);
       DROP DATABASE db28 WITH (FORCE);
       DROP DATABASE db29 WITH (FORCE);
       DROP DATABASE db30 WITH (FORCE);
       DROP DATABASE db31 WITH (FORCE);
       DROP DATABASE db32 WITH (FORCE);
       DROP DATABASE db33 WITH (FORCE);
       DROP DATABASE db34 WITH (FORCE);
       DROP DATABASE db35 WITH (FORCE);
       DROP DATABASE db36 WITH (FORCE);
       DROP DATABASE db37 WITH (FORCE);
       DROP DATABASE db38 WITH (FORCE);
       DROP DATABASE db39 WITH (FORCE);
       DROP DATABASE db40 WITH (FORCE);
       DROP DATABASE db41 WITH (FORCE);
       DROP DATABASE db42 WITH (FORCE);
       DROP DATABASE db43 WITH (FORCE);
       DROP DATABASE db44 WITH (FORCE);
       DROP DATABASE db45 WITH (FORCE);
       DROP DATABASE db46 WITH (FORCE);
       DROP DATABASE db47 WITH (FORCE);
       DROP DATABASE db48 WITH (FORCE);
       DROP DATABASE db49 WITH (FORCE);
       DROP DATABASE db50 WITH (FORCE);
       DROP DATABASE db51 WITH (FORCE);
       DROP DATABASE db52 WITH (FORCE);
       DROP DATABASE db53 WITH (FORCE);
       DROP DATABASE db54 WITH (FORCE);
       DROP DATABASE db55 WITH (FORCE);
       DROP DATABASE db56 WITH (FORCE);
       DROP DATABASE db57 WITH (FORCE);
       DROP DATABASE db58 WITH (FORCE);
       DROP DATABASE db59 WITH (FORCE);
       DROP DATABASE db60 WITH (FORCE);
       DROP DATABASE db61 WITH (FORCE);
       DROP DATABASE db62 WITH (FORCE);
       DROP DATABASE db63 WITH (FORCE);
       DROP DATABASE db64 WITH (FORCE);
       DROP DATABASE db65 WITH (FORCE);
       DROP DATABASE db66 WITH (FORCE);
       DROP DATABASE db67 WITH (FORCE);
       DROP DATABASE db68 WITH (FORCE);
       DROP DATABASE db69 WITH (FORCE);
       DROP DATABASE db70 WITH (FORCE);
       DROP DATABASE db71 WITH (FORCE);
       DROP DATABASE db72 WITH (FORCE);
       DROP DATABASE db73 WITH (FORCE);
       DROP DATABASE db74 WITH (FORCE);
       DROP DATABASE db75 WITH (FORCE);
       DROP DATABASE db76 WITH (FORCE);
       DROP DATABASE db77 WITH (FORCE);
       DROP DATABASE db78 WITH (FORCE);
       DROP DATABASE db79 WITH (FORCE);
       DROP DATABASE db80 WITH (FORCE);
       DROP DATABASE db81 WITH (FORCE);
       DROP DATABASE db82 WITH (FORCE);
       DROP DATABASE db83 WITH (FORCE);
       DROP DATABASE db84 WITH (FORCE);
       DROP DATABASE db85 WITH (FORCE);
       DROP DATABASE db86 WITH (FORCE);
       DROP DATABASE db87 WITH (FORCE);
       DROP DATABASE db88 WITH (FORCE);
       DROP DATABASE db89 WITH (FORCE);
       DROP DATABASE db90 WITH (FORCE);
       DROP DATABASE db91 WITH (FORCE);
       DROP DATABASE db92 WITH (FORCE);
       DROP DATABASE db93 WITH (FORCE);
       DROP DATABASE db94 WITH (FORCE);
       DROP DATABASE db95 WITH (FORCE);
       DROP DATABASE db96 WITH (FORCE);
       DROP DATABASE db97 WITH (FORCE);
       DROP DATABASE db98 WITH (FORCE);
       DROP DATABASE db99 WITH (FORCE);
       DROP DATABASE db100 WITH (FORCE);

       SELECT count(*) = 0 as all_databases_dropped
       FROM pg_database
       WHERE datname LIKE 'db%';

       ALTER SYSTEM RESET citus.recover_2pc_interval;
       ALTER SYSTEM RESET citus.distributed_deadlock_detection_factor;
       ALTER SYSTEM RESET citus.max_maintenance_shared_pool_size;
       SELECT pg_reload_conf();

       $definition$ AS cleanup
\gset

:cleanup

\c - - - :worker_1_port

:cleanup

\c - - - :worker_2_port

:cleanup

\c - - - :master_port

DROP EXTENSION IF EXISTS dblink;
