import re
from contextlib import ExitStack

import pytest


@pytest.fixture
def shard_table(cluster):
    for worker in cluster.workers:
        worker.configure("log_statement = 'all'")
        worker.reload()

    with cluster.coordinator.cur() as cur:
        cur.execute("SET citus.shard_count = 2")
        cur.execute("SET citus.next_shard_id = 8775000")
        cur.execute("SET citus.explain_all_tasks = on")
        cur.execute("CREATE TABLE t(a int, b int)")
        cur.execute("SELECT create_distributed_table('t', 'a')")
        yield cur


def subplan(cur, query, analyze=True):
    cur.execute(f"EXPLAIN (FORMAT JSON, ANALYZE {analyze}, COSTS OFF) {query}")
    plan = cur.fetchone()[0][0]["Plan"]
    return plan["Distributed Query"]["Subplans"][0]["PlannedStmt"][0]["Plan"]


def check_shards(cur, plan):
    assert plan["Actual Rows"] == 3
    cur.execute(
        "SELECT shardid, nodeport FROM citus_shards WHERE table_name = 't'::regclass"
    )
    ports = dict(cur.fetchall())
    rows = {}
    tasks = plan["Distributed Query"]["Job"]["Tasks"]
    assert len(tasks) == 2
    for task in tasks:
        remote = task["Remote Plan"][0][0]["Plan"]
        shard = int(remote["Relation Name"].removeprefix("t_"))
        assert f"port={ports[shard]} " in task["Node"]
        assert int(task["Tuple data received from node"].split()[0]) > 0
        assert remote["Actual Loops"] == 1
        rows[shard] = remote["Actual Rows"]
    assert rows == {8775000: 2, 8775001: 1}


LIVE_INSERT = (
    r"(?i)(?:statement|execute [^:]*): EXPLAIN \(ANALYZE TRUE[^\n]*\) INSERT INTO"
)
SAVED_FETCH = r"(?i)(?:statement|execute [^:]*): SELECT [^\n]* FROM worker_last_saved_explain_analyze\(\)"


def test_subplan_explain(cluster, shard_table):
    cur = shard_table
    insert = "WITH c AS (INSERT INTO t VALUES (6,66),(7,77),(8,88) RETURNING *) SELECT * FROM c"
    plain = subplan(cur, insert, analyze=False)
    assert all("Remote Plan" in t for t in plain["Distributed Query"]["Job"]["Tasks"])
    with ExitStack() as logs:
        for worker in cluster.workers:
            logs.enter_context(worker.log_contains(SAVED_FETCH, times=1))
            logs.enter_context(worker.log_contains(LIVE_INSERT, times=0))
        plan = subplan(cur, insert)
    check_shards(cur, plan)
    check_shards(
        cur, subplan(cur, "WITH c AS MATERIALIZED (SELECT * FROM t) SELECT * FROM c")
    )


def test_subplan_explain_missing_report(cluster, shard_table):
    cur = shard_table
    # Deferred routing uses 6 for execution, then 7 (a different shard) when
    # ExecutorStart rebuilds the task for display. No report describes that task.
    cur.execute("CREATE SEQUENCE route START 6")
    with ExitStack() as logs:
        for worker in cluster.workers:
            logs.enter_context(worker.log_contains(LIVE_INSERT, times=0))
        plan = subplan(
            cur,
            "WITH c AS (INSERT INTO t VALUES (nextval('route'), 1) RETURNING *) SELECT * FROM c",
        )
    assert plan["Actual Rows"] == 1
    [task] = plan["Distributed Query"]["Job"]["Tasks"]
    assert task["Worker measurements"] == "unavailable"
    assert "Tuple data received from node" not in task
    assert "Remote Plan" not in task


def test_subplan_explain_nested_logger(cluster, shard_table):
    cur = shard_table
    cur.execute("CREATE SEQUENCE data_value START 61")
    cur.execute("LOAD 'auto_explain'")
    cur.execute("SET auto_explain.log_analyze = on")
    cur.execute("SET auto_explain.log_nested_statements = on")
    cur.execute("SET auto_explain.log_format = json")
    cur.execute("SET auto_explain.log_min_duration = 0")

    # The logger prints the original portal as JSON, then EXPLAIN prints its
    # saved TEXT report. The rebuilt INSERT has the same shard but a new value.
    with ExitStack() as logs:
        logs.enter_context(cluster.coordinator.log_contains(r'"Saved Remote Plan": "'))
        for worker in cluster.workers:
            logs.enter_context(worker.log_contains(LIVE_INSERT, times=0))
        cur.execute("""
            EXPLAIN (ANALYZE, VERBOSE, COSTS OFF)
            WITH c AS (INSERT INTO t VALUES (6, nextval('data_value')) RETURNING *)
            SELECT * FROM c
        """)
        output = "\n".join(row[0] for row in cur.fetchall())
    assert re.search(r"Query: INSERT INTO [^\n]* VALUES \(6,\s*61\)", output)
