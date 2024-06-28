import asyncio

import pytest
from psycopg.errors import DeadlockDetected

# For every database there is expected to be 2 queries,
# so ~80 connections will be held by deadlocks. Another 5 is expected to be used by maintenance daemon,
# leaving ~15 available
DATABASES_NUMBER = 40


async def test_multiple_databases_distributed_deadlock_detection(cluster):

    # Disable maintenance on all nodes
    for node in cluster.nodes:
        node.configure(
            "citus.recover_2pc_interval = '-1'",
            "citus.distributed_deadlock_detection_factor = '-1'",
            "citus.max_maintenance_shared_pool_size = 5",
        )
        node.restart()

    # Prepare database names for test
    db_names = [f"db{db_index}" for db_index in range(1, DATABASES_NUMBER + 1)]

    # Create and configure databases
    for db_name in db_names:
        nodes = cluster.workers + [cluster.coordinator]
        for node in nodes:
            node.create_database(f"{db_name}")
            with node.cur(dbname=db_name) as node_cursor:
                node_cursor.execute("CREATE EXTENSION citus;")
                if node == cluster.coordinator:
                    for worker in cluster.workers:
                        node_cursor.execute(
                            "SELECT pg_catalog.citus_add_node(%s, %s)",
                            (worker.host, worker.port),
                        )
                    node_cursor.execute(
                        """
                          CREATE TABLE public.deadlock_detection_test (user_id int UNIQUE, some_val int);
                          SELECT create_distributed_table('public.deadlock_detection_test', 'user_id');
                          INSERT INTO public.deadlock_detection_test SELECT i, i FROM generate_series(1,2) i;
                        """
                    )

    async def create_deadlock(db_name, run_on_coordinator):
        """Function to prepare a deadlock query in a given database"""
        # Init connections and store for later commits
        if run_on_coordinator:
            first_connection = await cluster.coordinator.aconn(
                dbname=db_name, autocommit=False
            )
            first_cursor = first_connection.cursor()
            second_connection = await cluster.coordinator.aconn(
                dbname=db_name, autocommit=False
            )
            second_cursor = second_connection.cursor()
        else:
            first_connection = await cluster.workers[0].aconn(
                dbname=db_name, autocommit=False
            )
            first_cursor = first_connection.cursor()
            second_connection = await cluster.workers[1].aconn(
                dbname=db_name, autocommit=False
            )
            second_cursor = second_connection.cursor()

        # initiate deadlock
        await first_cursor.execute(
            "UPDATE public.deadlock_detection_test SET some_val = 1 WHERE user_id = 1;"
        )
        await second_cursor.execute(
            "UPDATE public.deadlock_detection_test SET some_val = 2 WHERE user_id = 2;"
        )

        # Test that deadlock is resolved by a maintenance daemon
        with pytest.raises(DeadlockDetected):

            async def run_deadlocked_queries():
                await asyncio.gather(
                    second_cursor.execute(
                        "UPDATE public.deadlock_detection_test SET some_val = 2 WHERE user_id = 1;"
                    ),
                    first_cursor.execute(
                        "UPDATE public.deadlock_detection_test SET some_val = 1 WHERE user_id = 2;"
                    ),
                )

            await asyncio.wait_for(run_deadlocked_queries(), 300)

        await first_connection.rollback()
        await second_connection.rollback()

    async def enable_maintenance_when_deadlocks_ready():
        """Function to enable maintenance daemons, when all the expected deadlock queries are ready"""
        # Let deadlocks commence
        await asyncio.sleep(2)
        # Check that queries are deadlocked
        databases_with_deadlock = set()
        while len(databases_with_deadlock) < DATABASES_NUMBER:
            for db_name in (db for db in db_names if db not in databases_with_deadlock):
                for node in cluster.nodes:
                    async with node.acur(dbname=db_name) as cursor:
                        expected_lock_count = 4 if node == cluster.coordinator else 2
                        await cursor.execute(
                            """
                                SELECT count(*) = %s AS deadlock_created
                                FROM pg_locks
                                         INNER JOIN pg_class pc ON relation = oid
                                WHERE relname LIKE 'deadlock_detection_test%%'""",
                            (expected_lock_count,),
                        )
                        queries_deadlocked = await cursor.fetchone()
                        if queries_deadlocked[0]:
                            databases_with_deadlock.add(db_name)
        # Enable maintenance back
        for node in cluster.nodes:
            node.reset_configuration(
                "citus.recover_2pc_interval",
                "citus.distributed_deadlock_detection_factor",
            )
            node.reload()

    # Distribute deadlocked queries among all nodes in the cluster
    tasks = list()
    for idx, db_name in enumerate(db_names):
        run_on_coordinator = True if idx % 3 == 0 else False
        tasks.append(
            create_deadlock(db_name=db_name, run_on_coordinator=run_on_coordinator)
        )

    tasks.append(enable_maintenance_when_deadlocks_ready())

    # await for the results
    await asyncio.gather(*tasks)

    # Check for "too many clients" on all nodes
    for node in cluster.nodes:
        with node.cur() as cursor:
            cursor.execute(
                """
                    SELECT count(*) AS too_many_clients_errors_count
                    FROM regexp_split_to_table(pg_read_file(%s), E'\n') AS t(log_line)
                    WHERE log_line LIKE '%%sorry, too many clients already%%';""",
                (node.log_path.as_posix(),),
            )
            too_many_clients_errors_count = cursor.fetchone()[0]
            assert too_many_clients_errors_count == 0
