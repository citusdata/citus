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
