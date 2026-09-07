"""
Failure / interruption coverage for force_logical_auto_identity shard moves.

The regression test shard_move_without_replica_identity proves the happy path and
uses synthetic pg_dist_cleanup records to exercise the cleanup ordering. This test
covers the missing failure window: the move is paused right before the initial copy
(after it has already switched the SOURCE shard to REPLICA IDENTITY FULL and created
the publication / slot / subscription), its backend is terminated to simulate a
crash / cancellation, and we assert that the resource-cleanup framework fully rolls
the state back:

  * the source shard's REPLICA IDENTITY is restored to 'd';
  * the move publication and replication slot are gone on the source;
  * the move subscription is gone on the target;
  * the data and the placement are unchanged (the move did not complete).

The pause uses the same advisory lock (44000, 55152) that the isolation tester uses;
it is only honored when citus.running_under_citus_test_suite is on, so the move
session sets it.
"""

import threading


def _worker_by_port(cluster, port):
    for worker in cluster.workers:
        if worker.port == port:
            return worker
    raise AssertionError(f"no worker on port {port}")


def test_auto_identity_move_failure_restores_source(cluster):
    coord = cluster.coordinator

    coord.sql("DROP TABLE IF EXISTS t_fail")
    coord.sql("SET citus.shard_count TO 1")
    coord.sql("SET citus.shard_replication_factor TO 1")
    coord.sql("CREATE TABLE t_fail (a int, b text)")
    coord.sql("SELECT create_distributed_table('t_fail', 'a', colocate_with => 'none')")
    coord.sql("INSERT INTO t_fail SELECT g, 'v' || g FROM generate_series(1, 100) g")

    shardid = coord.sql_value(
        "SELECT min(shardid) FROM pg_dist_shard WHERE logicalrelid = 't_fail'::regclass"
    )
    src_name, src_port = coord.sql_row(
        "SELECT nodename, nodeport FROM pg_dist_shard_placement WHERE shardid = %s",
        (shardid,),
    )
    tgt_name, tgt_port = coord.sql_row(
        "SELECT nodename, nodeport FROM pg_dist_node "
        "WHERE groupid <> 0 AND noderole = 'primary' AND isactive "
        "AND (nodename, nodeport) <> (%s, %s) "
        "ORDER BY nodeport LIMIT 1",
        (src_name, src_port),
    )
    src = _worker_by_port(cluster, src_port)
    tgt = _worker_by_port(cluster, tgt_port)

    # Conn A holds the advisory lock, so the move blocks right before the initial
    # copy -- after the source is already REPLICA IDENTITY FULL and the
    # publication / slot / subscription exist.
    lock_conn = coord.conn()
    with lock_conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(44000, 55152)")

    move_error = {}

    def run_move():
        try:
            move_conn = coord.conn()
            with move_conn.cursor() as cur:
                cur.execute("SET citus.running_under_citus_test_suite = on")
                cur.execute(
                    "SELECT citus_move_shard_placement(%s, %s, %s, "
                    "%s, %s, shard_transfer_mode => "
                    "'force_logical_auto_identity')",
                    (shardid, src_name, src_port, tgt_name, tgt_port),
                )
        except Exception as exc:  # noqa: BLE001 - terminate is expected to raise
            move_error["exc"] = exc

    move_thread = threading.Thread(target=run_move, daemon=True)
    move_thread.start()

    try:
        # Wait until the source shard has actually been switched to REPLICA
        # IDENTITY FULL, i.e. the move has reached the mutation phase and is now
        # parked on the advisory lock. run_command_on_placements is used (rather
        # than a direct pg_class read) because Citus hides shard tables from
        # pg_class by default.
        coord.poll_query_until(
            "SELECT bool_or(result = 'f') FROM run_command_on_placements("
            "'t_fail', 'SELECT relreplident FROM pg_class "
            "WHERE oid = ''%s''::regclass')"
        )
        # the publication already exists on the source at this point
        assert (
            src.sql_value(
                "SELECT count(*) FROM pg_publication "
                "WHERE pubname LIKE 'citus_shard_move_publication_%'"
            )
            >= 1
        )

        # Terminate the parked move backend to simulate a crash / cancellation.
        killed = coord.sql_value(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
            "WHERE query LIKE '%citus_move_shard_placement%' "
            "AND query LIKE '%force_logical_auto_identity%' "
            "AND pid <> pg_backend_pid()"
        )
        assert killed >= 1, "did not find the parked move backend to terminate"
    finally:
        # release the advisory lock either way so nothing hangs
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(44000, 55152)")
        lock_conn.close()

    move_thread.join(timeout=60)
    assert not move_thread.is_alive(), "move thread did not finish after terminate"
    assert "exc" in move_error, "terminating the move backend should have raised"

    # Drive the cleanup framework (the maintenance daemon also does this).
    coord.sql("CALL citus_cleanup_orphaned_resources()")

    # The source shard's replica identity is restored to the default 'd'.
    coord.poll_query_until(
        "SELECT bool_and(result = 'd') FROM run_command_on_placements("
        "'t_fail', 'SELECT relreplident FROM pg_class "
        "WHERE oid = ''%s''::regclass')"
    )

    # No move publication / slot on the source, no move subscription on the target.
    src.poll_query_until(
        "SELECT count(*) = 0 FROM pg_publication "
        "WHERE pubname LIKE 'citus_shard_move_publication_%'"
    )
    src.poll_query_until(
        "SELECT count(*) = 0 FROM pg_replication_slots "
        "WHERE slot_name LIKE 'citus_shard_move_slot_%'"
    )
    tgt.poll_query_until(
        "SELECT count(*) = 0 FROM pg_subscription "
        "WHERE subname LIKE 'citus_shard_move_subscription_%'"
    )

    # The data survived and the placement never moved (the move did not complete).
    assert coord.sql_value("SELECT count(*) FROM t_fail") == 100
    assert (
        coord.sql_value(
            "SELECT nodeport FROM pg_dist_shard_placement WHERE shardid = %s "
            "AND shardstate = 1",
            (shardid,),
        )
        == src_port
    )

    coord.sql("DROP TABLE t_fail")
