def test_main_commited_outer_not_yet(cluster):
    c = cluster.coordinator
    w0 = cluster.workers[0]

    # create a non-main database
    c.sql("CREATE DATABASE db1")

    # we will use cur1 to simulate non-main database user and
    # cur2 to manually do the steps we would do in the main database
    with c.cur(dbname="db1") as cur1, c.cur() as cur2:
        # let's start a transaction and find its transaction id
        cur1.execute("BEGIN")
        cur1.execute("SELECT txid_current()")
        txid = cur1.fetchall()

        # using the transaction id of the cur1 simulate the main database commands manually
        cur2.execute("BEGIN")
        cur2.execute(
            "SELECT citus_internal.start_management_transaction(%s)", (str(txid[0][0]),)
        )
        cur2.execute(
            "SELECT citus_internal.execute_command_on_remote_nodes_as_user('CREATE USER u1;', 'postgres')"
        )
        cur2.execute(
            "SELECT citus_internal.mark_object_distributed(1260, 'u1', 123123, 'postgres')"
        )
        cur2.execute("COMMIT")

        # run the transaction recovery
        c.sql("SELECT recover_prepared_transactions()")

        # user should not be created on the worker because outer transaction is not committed yet
        role_before_commit = w0.sql_value(
            "SELECT count(*) FROM pg_roles WHERE rolname = 'u1'"
        )

        assert (
            int(role_before_commit) == 0
        ), "role is in pg_dist_object despite not committing"

        # user should not be in pg_dist_object on the coordinator because outer transaction is not committed yet
        pdo_coordinator_before_commit = c.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid = 123123"
        )

        assert (
            int(pdo_coordinator_before_commit) == 0
        ), "role is in pg_dist_object on coordinator despite not committing"

        # user should not be in pg_dist_object on the worker because outer transaction is not committed yet
        pdo_worker_before_commit = w0.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid::regrole::text = 'u1'"
        )

        assert (
            int(pdo_worker_before_commit) == 0
        ), "role is in pg_dist_object on worker despite not committing"

        # commit in cur1 so the transaction recovery thinks this is a successful transaction
        cur1.execute("COMMIT")

        # run the transaction recovery again after committing
        c.sql("SELECT recover_prepared_transactions()")

        # check that the user is created by the transaction recovery on the worker
        role_after_commit = w0.sql_value(
            "SELECT count(*) FROM pg_roles WHERE rolname = 'u1'"
        )

        assert (
            int(role_after_commit) == 1
        ), "role is not created during recovery despite committing"

        # check that the user is in pg_dist_object on the coordinator after transaction recovery
        pdo_coordinator_after_commit = c.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid = 123123"
        )

        assert (
            int(pdo_coordinator_after_commit) == 1
        ), "role is not in pg_dist_object on coordinator after recovery despite committing"

        # check that the user is in pg_dist_object on the worker after transaction recovery
        pdo_worker_after_commit = w0.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid::regrole::text = 'u1'"
        )

        assert (
            int(pdo_worker_after_commit) == 1
        ), "role is not in pg_dist_object on worker after recovery despite committing"

    c.sql("DROP DATABASE db1")
    c.sql(
        "SELECT citus_internal.execute_command_on_remote_nodes_as_user('DROP USER u1', 'postgres')"
    )
    c.sql(
        """
        SELECT run_command_on_workers($$
            DELETE FROM pg_dist_object
            WHERE objid::regrole::text = 'u1'
        $$)
        """
    )
    c.sql(
        """
        DELETE FROM pg_dist_object
        WHERE objid = 123123
        """
    )


def test_main_commited_outer_aborted(cluster):
    c = cluster.coordinator
    w0 = cluster.workers[0]

    # create a non-main database
    c.sql("CREATE DATABASE db2")

    # we will use cur1 to simulate non-main database user and
    # cur2 to manually do the steps we would do in the main database
    with c.cur(dbname="db2") as cur1, c.cur() as cur2:
        # let's start a transaction and find its transaction id
        cur1.execute("BEGIN")
        cur1.execute("SELECT txid_current()")
        txid = cur1.fetchall()

        # using the transaction id of the cur1 simulate the main database commands manually
        cur2.execute("BEGIN")
        cur2.execute(
            "SELECT citus_internal.start_management_transaction(%s)", (str(txid[0][0]),)
        )
        cur2.execute(
            "SELECT citus_internal.execute_command_on_remote_nodes_as_user('CREATE USER u2;', 'postgres')"
        )
        cur2.execute(
            "SELECT citus_internal.mark_object_distributed(1260, 'u2', 321321, 'postgres')"
        )
        cur2.execute("COMMIT")

        # abort cur1 so the transaction recovery thinks this is an aborted transaction
        cur1.execute("ABORT")

        # check that the user is not yet created on the worker
        role_before_recovery = w0.sql_value(
            "SELECT count(*) FROM pg_roles WHERE rolname = 'u2'"
        )

        assert int(role_before_recovery) == 0, "role is already created before recovery"

        # check that the user is not in pg_dist_object on the coordinator
        pdo_coordinator_before_recovery = c.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid = 321321"
        )

        assert (
            int(pdo_coordinator_before_recovery) == 0
        ), "role is already in pg_dist_object on coordinator before recovery"

        # check that the user is not in pg_dist_object on the worker
        pdo_worker_before_recovery = w0.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid::regrole::text = 'u2'"
        )

        assert (
            int(pdo_worker_before_recovery) == 0
        ), "role is already in pg_dist_object on worker before recovery"

        # run the transaction recovery
        c.sql("SELECT recover_prepared_transactions()")

        # check that the user is not created by the transaction recovery on the worker
        role_after_recovery = w0.sql_value(
            "SELECT count(*) FROM pg_roles WHERE rolname = 'u2'"
        )

        assert (
            int(role_after_recovery) == 0
        ), "role is created during recovery despite aborting"

        # check that the user is not in pg_dist_object on the coordinator after transaction recovery
        pdo_coordinator_after_recovery = c.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid = 321321"
        )

        assert (
            int(pdo_coordinator_after_recovery) == 0
        ), "role is in pg_dist_object on coordinator after recovery despite aborting"

        # check that the user is not in pg_dist_object on the worker after transaction recovery
        pdo_worker_after_recovery = w0.sql_value(
            "SELECT count(*) FROM pg_dist_object WHERE objid::regrole::text = 'u2'"
        )

        assert (
            int(pdo_worker_after_recovery) == 0
        ), "role is in pg_dist_object on worker after recovery despite aborting"

    c.sql("DROP DATABASE db2")
