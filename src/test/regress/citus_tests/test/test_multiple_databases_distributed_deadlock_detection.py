import asyncio

import psycopg
import pytest
from psycopg.errors import DeadlockDetected

DATABASES_NUMBER = 40


async def test_multiple_databases_distributed_deadlock_detection(cluster):
    # Disable maintenance on all nodes
    for node in cluster.nodes:
        node.sql("ALTER SYSTEM SET citus.recover_2pc_interval TO '-1';")
        node.sql("ALTER SYSTEM SET citus.distributed_deadlock_detection_factor = '-1';")
        node.sql("ALTER SYSTEM SET citus.max_maintenance_shared_pool_size = 10;")
        node.sql("SELECT pg_reload_conf();")

    # Prepare database names for test
    db_names = [f'db{db_index}' for db_index in range(1, DATABASES_NUMBER + 1)]

    # Create and configure databases
    for db_name in db_names:
        nodes = cluster.workers + [cluster.coordinator]
        for node in nodes:
            node.sql(f'CREATE DATABASE {db_name}')
            with node.cur(dbname=db_name) as node_cursor:
                node_cursor.execute("CREATE EXTENSION citus;")
                if node == cluster.coordinator:
                    for worker in cluster.workers:
                        node_cursor.execute(f"SELECT citus_add_node('localhost', {worker.port});")
                    node_cursor.execute("""
                          CREATE TABLE public.deadlock_detection_test (user_id int UNIQUE, some_val int);
                          SELECT create_distributed_table('public.deadlock_detection_test', 'user_id');
                          INSERT INTO public.deadlock_detection_test SELECT i, i FROM generate_series(1,2) i;
                        """)

    print("Setup is done")

    async def test_deadlock(db_name, run_on_coordinator):
        """Function to prepare a deadlock query in a given database"""
        # Init connections and store for later commits
        if run_on_coordinator:
            first_connection = await cluster.coordinator.aconn(dbname=db_name, autocommit=False)
            first_cursor = first_connection.cursor()
            second_connection = await cluster.coordinator.aconn(dbname=db_name, autocommit=False)
            second_cursor = second_connection.cursor()
        else:
            first_connection = await cluster.workers[0].aconn(dbname=db_name, autocommit=False)
            first_cursor = first_connection.cursor()
            second_connection = await cluster.workers[1].aconn(dbname=db_name, autocommit=False)
            second_cursor = second_connection.cursor()

        # initiate deadlock
        await first_cursor.execute("UPDATE public.deadlock_detection_test SET some_val = 1 WHERE user_id = 1;")
        await second_cursor.execute("UPDATE public.deadlock_detection_test SET some_val = 2 WHERE user_id = 2;")

        # Test that deadlock is resolved by a maintenance daemon
        with pytest.raises(DeadlockDetected):
            async def run_deadlocked_queries():
                await asyncio.gather(
                    second_cursor.execute("UPDATE public.deadlock_detection_test SET some_val = 2 WHERE user_id = 1;"),
                    first_cursor.execute("UPDATE public.deadlock_detection_test SET some_val = 1 WHERE user_id = 2;")
                )

            await asyncio.wait_for(run_deadlocked_queries(), 300)

    async def enable_maintenance_when_deadlocks_ready():
        """Function to enable maintenance daemons, when all the expected deadlock queries are ready"""
        # Let deadlocks commence
        await asyncio.sleep(2)
        # cluster.debug()
        # Check that queries are deadlocked
        databases_with_deadlock = set()
        while len(databases_with_deadlock) < DATABASES_NUMBER:
            for db_name in (db for db in db_names if
                            db not in databases_with_deadlock):
                for node in cluster.nodes:
                    async with node.acur(dbname=db_name) as cursor:
                        expected_lock_count = 4 if node == cluster.coordinator else 2
                        await cursor.execute(f"""
                                SELECT count(*) = {expected_lock_count} AS deadlock_created
                                FROM pg_locks
                                         INNER JOIN pg_class pc ON relation = oid
                                WHERE relname LIKE 'deadlock_detection_test%'""")
                        queries_deadlocked = await cursor.fetchone()
                        if queries_deadlocked[0]:
                            print(f"Queries are deadlocked on {db_name}")
                            databases_with_deadlock.add(db_name)

        print("Queries on all databases are deadlocked, enabling maintenance")

        # Enable maintenance back
        for node in cluster.nodes:
            node.sql("ALTER SYSTEM RESET citus.recover_2pc_interval;")
            node.sql("ALTER SYSTEM RESET citus.distributed_deadlock_detection_factor;")
            node.sql("SELECT pg_reload_conf();")

    tasks = list()
    for idx, db_name in enumerate(db_names):
        run_on_coordinator = True if idx % 3 == 0 else False
        tasks.append(test_deadlock(db_name=db_name, run_on_coordinator=run_on_coordinator))

    tasks.append(enable_maintenance_when_deadlocks_ready())

    await asyncio.gather(*tasks)
