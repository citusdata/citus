import psycopg
import pytest
from pprint import pprint

def test_isolate(cluster):

    cluster.coordinator.configure("citus.split_shard_group_size_threshold to 100")
    cluster.coordinator.reload()
    cluster.coordinator.sql("CREATE TABLE test_row(i integer)")
    cluster.coordinator.sql("SELECT create_distributed_table('test_row','i',shard_count:=1)")
    cluster.coordinator.sql("ANALYZE test_row")
    cluster.coordinator.sql("INSERT INTO test_row SELECT 1 from generate_series(1,20000) ")
    print(cluster.coordinator.sql_row("select count(*), avg(i) from test_row"))

    row_count1 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    query = "SET client_min_messages = DEBUG4; SELECT citus_auto_shard_split_start('block_writes')"
    jobId = cluster.coordinator.sql_value(query)
    print(cluster.coordinator.sql_value("select command from pg_dist_background_task where status = 'runnable'"))

    cluster.coordinator.sql("SELECT citus_job_wait({})".format(jobId))

    row_count2 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    assert row_count2==row_count1+1, "Tenant didn't get isolate"


def test_split(cluster):

    cluster.coordinator.configure("citus.split_shard_group_size_threshold to 100")
    cluster.coordinator.reload()
    cluster.coordinator.sql("CREATE TABLE test_row(i integer)")
    cluster.coordinator.sql("SELECT create_distributed_table('test_row','i',shard_count:=1)")
    cluster.coordinator.sql("INSERT INTO test_row SELECT generate_series(1,20000) ")

    row_count1 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    query = "SELECT citus_auto_shard_split_start('block_writes')"
    jobId = cluster.coordinator.sql_value(query)
    print(cluster.coordinator.sql_value("select command from pg_dist_background_task where status = 'runnable'"))

    cluster.coordinator.sql("SELECT citus_job_wait({})".format(jobId))

    row_count2 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    assert row_count2==row_count1+1, "Shard didn't get split"
