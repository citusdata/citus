import psycopg
import pytest


def test_create_drop_citus(coord):
    with coord.cur() as cur1:
        with coord.cur() as cur2:
            # Conn1 drops the extension
            # and Conn2 cannot use it.
            cur1.execute("DROP EXTENSION citus")

            with pytest.raises(psycopg.errors.UndefinedFunction):
                # Conn1 dropped the extension. citus_version udf
                # cannot be found.sycopg.errors.UndefinedFunction
                # is expected here.
                cur2.execute("SELECT citus_version();")

            # Conn2 creates the extension,
            # Conn1 is able to use it immediadtely.
            cur2.execute("CREATE EXTENSION citus")
            cur1.execute("SELECT citus_version();")
            cur1.execute("DROP EXTENSION citus;")

    with coord.cur() as cur1:
        with coord.cur() as cur2:
            # A connection is able to create and use the extension
            # within a transaction block.
            cur1.execute("BEGIN;")
            cur1.execute("CREATE TABLE t1(id int);")
            cur1.execute("CREATE EXTENSION citus;")
            cur1.execute("SELECT create_reference_table('t1')")
            cur1.execute("ABORT;")

            # Conn1 aborted so Conn2 is be able to create and
            # use the extension within a transaction block.
            cur2.execute("BEGIN;")
            cur2.execute("CREATE TABLE t1(id int);")
            cur2.execute("CREATE EXTENSION citus;")
            cur2.execute("SELECT create_reference_table('t1')")
            cur2.execute("COMMIT;")

            # Conn2 commited so Conn1 is be able to use the
            # extension immediately.
            cur1.execute("SELECT citus_version();")
