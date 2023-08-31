import psycopg
import pytest


def test_create_drop_citus(coord):

    with coord.cur() as cur1:
        with coord.cur() as cur2:

            # test that connection 1 DROPs the extension
            # and connection 2 gets notified.
            cur1.execute("DROP EXTENSION citus")

            try:
                cur2.execute("SELECT citus_version();")
                # connection 1 dropped citus extension
                # so psycopg.errors.UndefinedFunction is expected here.
            except psycopg.errors.UndefinedFunction:
                cur2.execute("SELECT 1;")

            # test that connection 2 CREATEs the extension
            # and connection 1 gets notified.
            cur2.execute("CREATE EXTENSION citus")
            cur1.execute("SELECT citus_version();")

            cur1.execute("DROP EXTENSION citus;")

    with coord.cur() as cur1:
        cur1.execute("CREATE TABLE t1(id int);");
        cur1.execute("CREATE EXTENSION citus;");

    with coord.cur() as cur2:
         cur2.execute("SELECT create_reference_table('t1')");
         cur2.execute("SELECT 1 FROM t1;");
