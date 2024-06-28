# This test checks that once citus.main_db is set and the
# server is restarted. A Citus Maintenance Daemon for the main_db
# is launched. This should happen even if there is no query run
# in main_db yet.
import time


def wait_until_maintenance_deamons_start(deamoncount, cluster):
    i = 0
    n = 0

    while i < 10:
        i += 1
        n = cluster.coordinator.sql_value(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';"
        )

        if n == deamoncount:
            break

        time.sleep(0.1)

    assert n == deamoncount


def test_set_maindb(cluster_factory):
    cluster = cluster_factory(0)

    # Test that once citus.main_db is set to a database name
    # there are two maintenance deamons running upon restart.
    # One maintenance deamon for the database of the current connection
    # and one for the citus.main_db.
    cluster.coordinator.create_database("mymaindb")
    cluster.coordinator.configure("citus.main_db='mymaindb'")
    cluster.coordinator.restart()

    assert cluster.coordinator.sql_value("SHOW citus.main_db;") == "mymaindb"

    wait_until_maintenance_deamons_start(2, cluster)

    assert (
        cluster.coordinator.sql_value(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon' AND datname='mymaindb';"
        )
        == 1
    )

    # Test that once citus.main_db is set to empty string
    # there is only one maintenance deamon for the database
    # of the current connection.
    cluster.coordinator.configure("citus.main_db=''")
    cluster.coordinator.restart()
    assert cluster.coordinator.sql_value("SHOW citus.main_db;") == ""

    wait_until_maintenance_deamons_start(1, cluster)

    # Test that after citus.main_db is dropped. The maintenance
    # deamon for this database is terminated.
    cluster.coordinator.configure("citus.main_db='mymaindb'")
    cluster.coordinator.restart()
    assert cluster.coordinator.sql_value("SHOW citus.main_db;") == "mymaindb"

    wait_until_maintenance_deamons_start(2, cluster)

    cluster.coordinator.drop_database("mymaindb")

    wait_until_maintenance_deamons_start(1, cluster)

    assert (
        cluster.coordinator.sql_value(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon' AND datname='mymaindb';"
        )
        == 0
    )
