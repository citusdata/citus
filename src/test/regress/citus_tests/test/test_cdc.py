import asyncio

from psycopg import sql
from psycopg.conninfo import make_conninfo

from common import CitusCluster, Postgres, QueryRunner


def prepare_workers_for_cdc_publication(cluster: CitusCluster):
    for node in cluster.workers:
        node.create_publication("cdc_publication", sql.SQL("FOR TABLE sensors"))
        node.create_logical_replication_slot(
            "cdc_replication_slot", "citus", twophase=True
        )


def connect_cdc_client_to_citus_cluster_publications(
    cluster: CitusCluster, cdc_client: Postgres
):
    for i, node in enumerate(cluster.workers):
        connection_string = make_conninfo(**node.default_connection_options())
        copy_data = i == 0
        cdc_client.create_subscription(
            f"cdc_subscription_{i}",
            sql.SQL(
                """
                CONNECTION {}
                PUBLICATION cdc_publication
                WITH (
                    create_slot=false,
                    enabled=true,
                    slot_name=cdc_replication_slot,
                    copy_data={}
                )
            """
            ).format(
                sql.Literal(connection_string),
                copy_data,
            ),
        )


def wait_for_cdc_client_to_catch_up_with_workers(cluster: CitusCluster):
    for i, node in enumerate(cluster.workers):
        node.wait_for_catchup(f"cdc_subscription_{i}")


def compare_cdc_table_contents(node1: QueryRunner, node2: QueryRunner):
    query = "SELECT * FROM sensors ORDER BY measureid, eventdatetime, measure_data"

    with node1.cur() as cur1:
        with node2.cur() as cur2:
            cur1.execute(query)
            cur2.execute(query)
            result1 = cur1.fetchall()
            result2 = cur2.fetchall()
            assert result1 == result2


def create_cdc_schema(cluster, cdc_client):
    initial_schema = """
        CREATE TABLE sensors(
            measureid               integer,
            eventdatetime           timestamptz,
            measure_data            jsonb,
            measure_quantity        decimal(15, 2),
            measure_status          char(1),
            measure_comment         varchar(44),
            PRIMARY KEY (measureid, eventdatetime, measure_data));

        CREATE INDEX index_on_sensors ON sensors(lower(measureid::text));
        ALTER INDEX index_on_sensors ALTER COLUMN 1 SET STATISTICS 1000;
        CREATE INDEX hash_index_on_sensors ON sensors USING HASH((measure_data->'IsFailed'));
        CREATE INDEX index_with_include_on_sensors ON sensors ((measure_data->'IsFailed')) INCLUDE (measure_data, eventdatetime, measure_status);
        CREATE STATISTICS stats_on_sensors (dependencies) ON measureid, eventdatetime FROM sensors;
    """

    cluster.coordinator.sql(initial_schema)
    cdc_client.sql(initial_schema)


def run_cdc_tests(cluster, cdc_client):
    prepare_workers_for_cdc_publication(cluster)
    connect_cdc_client_to_citus_cluster_publications(cluster, cdc_client)
    wait_for_cdc_client_to_catch_up_with_workers(cluster)

    cluster.coordinator.sql(
        """
        INSERT INTO sensors
        SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
        FROM generate_series(0,10)i;
        """
    )
    wait_for_cdc_client_to_catch_up_with_workers(cluster)
    compare_cdc_table_contents(cluster, cdc_client)

    cluster.coordinator.sql(
        """
        UPDATE sensors
            SET
            eventdatetime=NOW(),
            measure_data = jsonb_set(measure_data, '{val}', measureid::text::jsonb , TRUE),
            measure_status = CASE
                WHEN measureid % 2 = 0
                    THEN 'y'
                    ELSE 'n'
                END,
            measure_comment= 'Comment:' || measureid::text;
        """
    )
    wait_for_cdc_client_to_catch_up_with_workers(cluster)
    compare_cdc_table_contents(cluster, cdc_client)

    cluster.coordinator.sql("DELETE FROM sensors WHERE (measureid % 2) = 0")
    wait_for_cdc_client_to_catch_up_with_workers(cluster)
    compare_cdc_table_contents(cluster, cdc_client)


def test_cdc_create_distributed_table(cluster, coord):
    cdc_client = coord

    create_cdc_schema(cluster, cdc_client)

    cluster.coordinator.sql("SELECT create_distributed_table('sensors', 'measureid')")
    run_cdc_tests(cluster, cdc_client)


def test_cdc_create_distributed_table_concurrently(cluster, coord):
    cdc_client = coord

    create_cdc_schema(cluster, cdc_client)

    cluster.coordinator.sql(
        "SELECT create_distributed_table_concurrently('sensors', 'measureid')"
    )
    run_cdc_tests(cluster, cdc_client)


async def test_cdc_parallel_insert(cluster, coord):
    cdc_client = coord

    create_cdc_schema(cluster, cdc_client)
    cluster.coordinator.sql(
        """
        INSERT INTO sensors
        SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
        FROM generate_series(0,100) i
        """
    )
    tasks = []
    tasks.append(
        cluster.coordinator.asql(
            "SELECT create_distributed_table_concurrently('sensors', 'measureid')"
        )
    )
    tasks.append(
        cluster.coordinator.asql(
            """
            INSERT INTO sensors
            SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
            FROM generate_series(-10,-1) i
            """
        )
    )
    await asyncio.gather(*tasks)

    prepare_workers_for_cdc_publication(cluster)
    connect_cdc_client_to_citus_cluster_publications(cluster, cdc_client)
    wait_for_cdc_client_to_catch_up_with_workers(cluster)
    compare_cdc_table_contents(cluster, cdc_client)


def test_cdc_schema_change_and_move(cluster, coord):
    cdc_client = coord

    create_cdc_schema(cluster, cdc_client)

    # insert data into the sensors table in the coordinator node before distributing the table.
    cluster.coordinator.sql(
        """
        INSERT INTO sensors
        SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
        FROM generate_series(0,100) i
        """
    )
    cluster.coordinator.sql(
        "SELECT create_distributed_table_concurrently('sensors', 'measureid', shard_count := 2)"
    )

    prepare_workers_for_cdc_publication(cluster)
    connect_cdc_client_to_citus_cluster_publications(cluster, cdc_client)
    wait_for_cdc_client_to_catch_up_with_workers(cluster)
    compare_cdc_table_contents(cluster, cdc_client)

    shardid = cluster.coordinator.sql_value(
        "SELECT shardid FROM citus_shards WHERE nodename=%s and nodeport=%s",
        (cluster.workers[0].host, cluster.workers[0].port),
    )
    cluster.coordinator.sql("ALTER TABLE sensors DROP COLUMN measure_quantity")

    cluster.coordinator.sql(
        "SELECT citus_move_shard_placement(%s, %s, %s, %s, %s, 'force_logical')",
        (
            shardid,
            cluster.workers[0].host,
            cluster.workers[0].port,
            cluster.workers[1].host,
            cluster.workers[1].port,
        ),
    )

    cluster.coordinator.sql(
        """
        INSERT INTO sensors
        SELECT i, '2020-01-05', '{}', 'A', 'I <3 Citus'
        FROM generate_series(-10,-1) i
        """
    )
    cdc_client.sql("DROP subscription cdc_subscription_0")
    cdc_client.sql("DROP subscription cdc_subscription_1")
    cdc_client.sql("DELETE FROM sensors")
    cdc_client.sql("ALTER TABLE sensors DROP COLUMN measure_quantity")
    for node in cluster.workers:
        node.create_logical_replication_slot(
            "cdc_replication_slot", "citus", twophase=True
        )

    connect_cdc_client_to_citus_cluster_publications(cluster, cdc_client)
    wait_for_cdc_client_to_catch_up_with_workers(cluster)
    compare_cdc_table_contents(cluster, cdc_client)
