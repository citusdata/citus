import psycopg
import pytest

def test_isolate(cluster):

    cluster.coordinator.restart()
    cluster.coordinator.sql("SET citus.split_shard_group_size_threshold to 100")
    cluster.coordinator.sql("CREATE TABLE test_row(i integer)")
    cluster.coordinator.sql("SELECT create_distributed_table('test_row','i',shard_count:=1)")
    cluster.coordinator.sql("INSERT INTO test_row SELECT 1 from generate_series(1,20000) ")

    row_count1 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    query = "SELECT citus_auto_shard_split_start('block_writes')"
    jobId = cluster.coordinator.sql_value(query)

    cluster.coordinator.sql("SELECT citus_job_wait({})".format(jobId))

    row_count2 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    assert row_count2==row_count1+1, "Tenant didn't get isolate"


def test_split(cluster):

    cluster.coordinator.restart()
    cluster.coordinator.sql("SET citus.split_shard_group_size_threshold to 100")
    cluster.coordinator.sql("CREATE TABLE test_row(i integer)")
    cluster.coordinator.sql("SELECT create_distributed_table('test_row','i',shard_count:=1)")
    cluster.coordinator.sql("INSERT INTO test_row SELECT generate_series(1,20000) ")

    row_count1 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    query = "SELECT citus_auto_shard_split_start('block_writes')"
    jobId = cluster.coordinator.sql_value(query)

    cluster.coordinator.sql("SELECT citus_job_wait({})".format(jobId))

    row_count2 = cluster.coordinator.sql_value("SELECT count(*) FROM pg_dist_shard")

    assert row_count2==row_count1+1, "Shard didn't get split"