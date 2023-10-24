# This test checks that once citus.main_db is set and the
# server is restarted. A Citus Maintenance Daemon for the main_db
# is launched. This should happen even if there is no query run
# in main_db yet.
def test_set_maindb(cluster_factory):
    cluster = cluster_factory(0)

    cluster.coordinator.create_database("mymaindb")
    cluster.coordinator.configure("citus.main_db='mymaindb'")
    cluster.coordinator.restart()

    assert cluster.coordinator.sql_value("SHOW citus.main_db;") == "mymaindb"

    assert (
        cluster.coordinator.sql_value(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';"
        )
        == 2
    )

    assert (
        cluster.coordinator.sql_value(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon' AND datname='mymaindb';"
        )
        == 1
    )

    cluster.coordinator.configure("citus.main_db=''")
    cluster.coordinator.restart()
    assert cluster.coordinator.sql_value("SHOW citus.main_db;") == ""
    assert (
        cluster.coordinator.sql_value(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';"
        )
        == 1
    )

    
    cluster.coordinator.cleanup_databases()
