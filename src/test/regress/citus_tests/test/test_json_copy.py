import json

import pytest
from psycopg import pq


def copy_out(pgconn, command):
    pgconn.send_query(command.encode())
    copy_result = pgconn.get_result()
    assert copy_result.status == pq.ExecStatus.COPY_OUT

    chunks = []
    while True:
        size, data = pgconn.get_copy_data(0)
        if size > 0:
            chunks.append(bytes(data))
        elif size == -1:
            break
        else:
            raise AssertionError("COPY OUT failed")

    command_result = pgconn.get_result()
    assert command_result.status == pq.ExecStatus.COMMAND_OK
    assert pgconn.get_result() is None
    return copy_result, command_result, b"".join(chunks)


def test_json_copy_protocol_and_routing(cluster):
    coord = cluster.coordinator
    if coord.sql_value("SELECT current_setting('server_version_num')::int") < 190000:
        pytest.skip("JSON COPY is only available on PostgreSQL 19+")

    coord.sql("CREATE TABLE json_copy (id int, payload text)")
    coord.sql("""
        SELECT create_distributed_table(
            'json_copy', 'id', shard_count => 4, colocate_with => 'none')
        """)
    coord.sql("""
        INSERT INTO json_copy
        SELECT id, 'value-' || id
        FROM (
            SELECT DISTINCT ON (
                get_shard_id_for_distribution_column('json_copy', id))
                id
            FROM generate_series(1, 100) id
            ORDER BY get_shard_id_for_distribution_column('json_copy', id), id
            LIMIT 2
        ) distinct_shards
        """)
    assert coord.sql_value("""
            SELECT count(DISTINCT
                get_shard_id_for_distribution_column('json_copy', id))
            FROM json_copy
            """) == 2

    with coord.conn() as conn:
        direct_copy, direct_command, direct_data = copy_out(
            conn.pgconn,
            "COPY json_copy TO STDOUT (FORMAT json, FORCE_ARRAY true)",
        )
        query_copy, query_command, query_data = copy_out(
            conn.pgconn,
            "COPY (SELECT * FROM json_copy) TO STDOUT "
            "(FORMAT json, FORCE_ARRAY true)",
        )

        assert direct_copy.nfields == query_copy.nfields == 1
        assert direct_command.command_tuples == query_command.command_tuples == 2
        assert sorted(json.loads(direct_data), key=lambda row: row["id"]) == sorted(
            json.loads(query_data), key=lambda row: row["id"]
        )

        coord.sql("TRUNCATE json_copy")
        empty_copy, empty_command, empty_data = copy_out(
            conn.pgconn,
            "COPY json_copy TO STDOUT (FORMAT json, FORCE_ARRAY true)",
        )

        assert empty_copy.nfields == 1
        assert empty_command.command_tuples == 0
        assert json.loads(empty_data) == []
