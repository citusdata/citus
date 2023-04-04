import psycopg
import pytest


def test_freezing(coord):
    coord.configure("vacuum_freeze_min_age = 50000", "vacuum_freeze_table_age = 50000")
    coord.restart()

    # create columnar table and insert simple data to verify the data survives
    # a crash
    coord.sql("CREATE TABLE test_row(i int)")
    coord.sql("INSERT INTO test_row VALUES (1) ")
    coord.sql(
        "CREATE TABLE test_columnar_freeze(i int) USING columnar WITH(autovacuum_enabled=false)"
    )
    coord.sql("INSERT INTO test_columnar_freeze VALUES (1)")

    for _ in range(0, 7):
        with coord.cur() as cur:
            for _ in range(0, 10_000):
                cur.execute("UPDATE test_row SET i = i + 1")

    frozen_age = coord.sql_value(
        """
        select age(relfrozenxid)
        from pg_class where relname='test_columnar_freeze';
    """
    )

    assert frozen_age > 70_000, "columnar table was frozen"
    coord.sql("VACUUM FREEZE test_columnar_freeze")

    frozen_age = coord.sql_value(
        """
        select age(relfrozenxid)
        from pg_class where relname='test_columnar_freeze';
    """
    )
    assert frozen_age < 70_000, "columnar table was not frozen"


def test_recovery(coord):
    # create columnar table and insert simple data to verify the data survives a crash
    coord.sql("CREATE TABLE t1 (a int, b text) USING columnar")
    coord.sql(
        "INSERT INTO t1 SELECT a, 'hello world ' || a FROM generate_series(1,1002) AS a"
    )

    # simulate crash
    coord.stop("immediate")
    coord.start()

    row_count = coord.sql_value("SELECT count(*) FROM t1")
    assert row_count == 1002, "columnar didn't recover data before crash correctly"

    # truncate the table to verify the truncation survives a crash
    coord.sql("TRUNCATE t1")
    # simulate crash
    coord.stop("immediate")
    coord.start()

    row_count = coord.sql_value("SELECT count(*) FROM t1")
    assert row_count == 0, "columnar didn't recover the truncate correctly"

    # test crashing while having an open transaction
    with pytest.raises(
        psycopg.OperationalError,
        match="server closed the connection unexpectedly|consuming input failed: EOF detected",
    ):
        with coord.transaction() as cur:
            cur.execute(
                "INSERT INTO t1 SELECT a, 'hello world ' || a FROM generate_series(1,1003) AS a"
            )
            # simulate crash
            coord.stop("immediate")

    coord.start()

    row_count = coord.sql_value("SELECT count(*) FROM t1")
    assert row_count == 0, "columnar didn't recover uncommited transaction"

    # test crashing while having a prepared transaction
    with pytest.raises(
        psycopg.OperationalError,
        match="server closed the connection unexpectedly|consuming input failed: EOF detected",
    ):
        with coord.transaction() as cur:
            cur.execute(
                "INSERT INTO t1 SELECT a, 'hello world ' || a FROM generate_series(1,1004) AS a"
            )
            cur.execute("PREPARE TRANSACTION 'prepared_xact_crash'")
            # simulate crash
            coord.stop("immediate")

    coord.start()

    row_count = coord.sql_value("SELECT count(*) FROM t1")
    assert row_count == 0, "columnar didn't recover uncommitted prepared transaction"

    coord.sql("COMMIT PREPARED 'prepared_xact_crash'")

    row_count = coord.sql_value("SELECT count(*) FROM t1")
    assert row_count == 1004, "columnar didn't recover committed transaction"

    # test crash recovery with copied data
    with coord.cur() as cur:
        with cur.copy("COPY t1 FROM STDIN") as copy:
            copy.write_row((1, "a"))
            copy.write_row((2, "b"))
            copy.write_row((3, "c"))

    # simulate crash
    coord.stop("immediate")
    coord.start()

    row_count = coord.sql_value("SELECT count(*) FROM t1")
    assert row_count == 1007, "columnar didn't recover after copy"
