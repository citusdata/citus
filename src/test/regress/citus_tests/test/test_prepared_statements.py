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
    # create a distributed table with two columns and an associated distributed procedure
    # to ensure parameterized CALL succeed, even when the first param is the
    # distribution key but there are additional params.
    coord = cluster.coordinator

    # 1) create table with two columns
    coord.sql("DROP TABLE IF EXISTS test CASCADE")
    coord.sql("CREATE TABLE test(i int, j int)")

    # 2) create a procedure taking both columns as inputs
    coord.sql(
        """
        CREATE PROCEDURE p(_i INT, _j INT)
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO test(i, j) VALUES (_i, _j);
        END;
        $$;
        """
    )

    sql = "CALL p(2, %s)"

    # 4) distribute table on column i and function on _i
    coord.sql("SELECT create_distributed_table('test', 'i')")
    coord.sql(
        """
        SELECT create_distributed_function(
            'p(int, int)',
            distribution_arg_name := '_i',
            colocate_with := 'test'
        )
        """
    )

    # 5) prepare/exec after distribution
    coord.sql_prepared(sql, (20,))

    # 6) verify both inserts happened
    sum_i = coord.sql_value("SELECT sum(i) FROM test;")
    sum_j = coord.sql_value("SELECT sum(j) FROM test;")

    assert sum_i == 2
    assert sum_j == 20
