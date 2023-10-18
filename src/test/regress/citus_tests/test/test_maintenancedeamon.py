# This test checks that once citus.main_db is set and the
# server is restarted. A Citus Maintenance Daemon for the main_db
# is launched. This should happen even if there is no query run
# in main_db yet.
def test_set_maindb(coord):
    with coord.cur() as cur1:
        cur1.execute("CREATE DATABASE mymaindb;")
        coord.configure("citus.main_db='mymaindb'")
        coord.restart()

        assert coord.sql_value("SHOW citus.main_db;") == "mymaindb"

        assert (
            coord.sql_value(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';"
            )
            == 2
        )

        assert (
            coord.sql_value(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon' AND datname='mymaindb';"
            )
            == 1
        )
