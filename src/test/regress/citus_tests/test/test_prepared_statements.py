import time

def test_call_param(cluster):
    # create a distributed table and an associated distributed procedure
    # to ensure parameterized CALL succeed, even when the param is the
    # distribution key.
    coord = cluster.coordinator
    coord.sql("CREATE TABLE test(i int)")
    coord.sql(
        """
        CREATE PROCEDURE p(_i INT) LANGUAGE plpgsql AS $$
        BEGIN
        INSERT INTO test(i) VALUES (_i);
        END; $$
        """
    )
    sql = "CALL p(%s)"

    # prepare/exec before distributing
    coord.sql_prepared(sql, (1,))

    coord.sql("SELECT create_distributed_table('test', 'i')")
    coord.sql(
        "SELECT create_distributed_function('p(int)', distribution_arg_name := '_i', colocate_with := 'test')"
    )

    # prepare/exec after distribution
    coord.sql_prepared(sql, (2,))

    sum_i = coord.sql_value("select sum(i) from test;")

    assert sum_i == 3


def test_call_param2(cluster):
    # Get the coordinator node from the Citus cluster
    coord = cluster.coordinator

    coord.sql("CREATE TABLE t (p int, i int)")
    coord.sql(
        """
        CREATE PROCEDURE f(_p INT, _i INT) LANGUAGE plpgsql AS $$
        BEGIN
        -- Example logic that uses the parameters (you can add your own logic)
        INSERT INTO t (p, i) VALUES (_p, _i);
        END; $$
        """
    )
    coord.sql("SELECT create_distributed_table('t', 'p')")
    coord.sql(
        "SELECT create_distributed_function('f(int, int)', distribution_arg_name := '_p', colocate_with := 't')"
    )

    sql_insert_and_call = "CALL f(1, %s);"

    # cluster.coordinator.psql_debug()
    # cluster.debug()

    # After distributing the table, insert more data and call the procedure again
    coord.sql_prepared(sql_insert_and_call, (2,))

    # Step 6: Check the result
    sum_i = coord.sql_value("SELECT count(*) FROM t;")

    assert sum_i == 1