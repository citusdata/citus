"""
Regression coverage for the force_logical_auto_identity catch-up helper index:
a large value inserted DURING the move must not stall the move.

Background. When force_logical_auto_identity moves a table that has no replica
identity, Citus builds a throwaway single-column btree "helper" index on the
DESTINATION shard to speed up the logical-replication catch-up. The helper index
exists only on the destination. If it were built on a variable-width column, a
future large value would be accepted on the source (which has no such index) but
rejected by the destination helper index once it exceeds the btree entry size
limit. The subscriber would then retry that change forever and the move would
never finish. ChooseHelperIndexColumn was fixed to only pick fixed-width columns
so no future value can stall replication; a table whose only candidate columns
are variable-width simply gets no helper index (the subscriber sequential-scans
during catch-up).

This test proves that fix end to end with a large value that is streamed (not
initial-copied) while the move is in flight.

Determinism. We park the move on the before-copy advisory lock (55152, 44000),
which fires in LogicallyReplicateShards AFTER the replication slot's snapshot is
exported but BEFORE the initial copy and BEFORE the helper index is built. A row
inserted while parked is therefore not visible to the slot snapshot: it is not in
the initial copy, it is streamed during catch-up, and it is applied on the
destination after the helper index would have been built. That is exactly the
window the fix addresses. The lock is only honored when
citus.running_under_citus_test_suite is on, which the move session sets.

Before the fix this move never completes: the destination helper index rejects a
streamed oversized value, the subscriber retries it forever, and catch-up never
finishes. With the fix the all-variable-width table gets no helper index and the
move completes.
"""

import threading


def _worker_by_port(cluster, port):
    for worker in cluster.workers:
        if worker.port == port:
            return worker
    raise AssertionError(f"no worker on port {port}")


def test_auto_identity_move_large_streamed_value_does_not_stall(cluster):
    coord = cluster.coordinator

    coord.sql("DROP TABLE IF EXISTS t_big_stream")
    coord.sql("SET citus.shard_count TO 1")
    coord.sql("SET citus.shard_replication_factor TO 1")
    # Every candidate column is variable-width (text) and the table has no replica
    # identity, so before the fix the throwaway helper index would be built on one
    # of these text columns. With the fix no helper index is built at all.
    coord.sql("CREATE TABLE t_big_stream (a text, b text)")
    coord.sql(
        "SELECT create_distributed_table('t_big_stream', 'a', colocate_with => 'none')"
    )
    # Short values initially, so the initial copy (and, before the fix, the helper
    # index build over the copied rows) succeed.
    coord.sql(
        "INSERT INTO t_big_stream SELECT 'k' || g, 'v' || g "
        "FROM generate_series(1, 100) g"
    )

    shardid = coord.sql_value(
        "SELECT min(shardid) FROM pg_dist_shard "
        "WHERE logicalrelid = 't_big_stream'::regclass"
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

    # Hold the before-copy advisory lock so the move parks right before the initial
    # copy -- after the replication slot snapshot has been exported but before the
    # copy and the helper index build. A row inserted while parked is not in the slot
    # snapshot, so it is streamed during catch-up and applied on the destination
    # after the helper index has been built.
    lock_conn = coord.conn()
    with lock_conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(55152, 44000)")

    move_error = {}

    def run_move():
        try:
            move_conn = coord.conn()
            with move_conn.cursor() as cur:
                cur.execute("SET citus.running_under_citus_test_suite = on")
                cur.execute(
                    "SELECT citus_move_shard_placement(%s, %s, %s, %s, %s, "
                    "shard_transfer_mode => 'force_logical_auto_identity')",
                    (shardid, src_name, src_port, tgt_name, tgt_port),
                )
        except Exception as exc:  # noqa: BLE001 - a stalled move is a test failure
            move_error["exc"] = exc

    move_thread = threading.Thread(target=run_move, daemon=True)
    move_thread.start()

    try:
        # Wait until the move backend is parked on the before-copy advisory lock.
        coord.poll_query_until(
            "SELECT count(*) >= 1 FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND wait_event = 'advisory' "
            "AND query LIKE '%citus_move_shard_placement%' "
            "AND query LIKE '%force_logical_auto_identity%' "
            "AND pid <> pg_backend_pid()"
        )

        # Also require the move's replication slot to already exist on the source.
        # The slot's snapshot is what the initial copy uses, so once it exists any
        # rows we insert are guaranteed to be after the snapshot: they are streamed
        # during catch-up rather than included in the copy.
        src.poll_query_until(
            "SELECT count(*) >= 1 FROM pg_replication_slots "
            "WHERE slot_name LIKE 'citus_shard_move_slot_%'"
        )

        # Insert many large, poorly-compressible values while parked. Because the
        # slot snapshot is already exported, these rows are not in the initial copy;
        # they are streamed during catch-up and applied on the destination after the
        # helper index has been built. Each value is far larger than the btree entry
        # size limit (~2704 bytes) and is concatenated distinct md5 hashes, so it
        # cannot be compressed under the limit; the values go into both text columns,
        # so a helper index on either one rejects them. We insert many rows (not one)
        # so that catch-up reliably applies at least one of them while the throwaway
        # helper index still exists -- a single streamed row can slip through the
        # narrow window before the helper is dropped on a fast move.
        coord.sql(
            "INSERT INTO t_big_stream(a, b) SELECT payload, payload FROM ("
            "SELECT s, string_agg(md5((g * 1000 + s)::text || random()::text), '') "
            "AS payload FROM generate_series(1, 200) s, generate_series(1, 400) g "
            "GROUP BY s) q"
        )
    finally:
        # Release the lock so the move can proceed regardless of what happened above.
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(55152, 44000)")
        lock_conn.close()

    # With the fix the move finishes; before the fix the streamed large values stall
    # the subscriber apply on the helper index and the move never finishes.
    move_thread.join(timeout=90)
    assert (
        not move_thread.is_alive()
    ), "move did not finish: a large streamed value stalled the catch-up helper index"
    assert "exc" not in move_error, f"move raised unexpectedly: {move_error.get('exc')}"

    # The move completed: the placement is on the target and the data is intact,
    # including the large streamed rows.
    assert (
        coord.sql_value(
            "SELECT nodeport FROM pg_dist_shard_placement "
            "WHERE shardid = %s AND shardstate = 1",
            (shardid,),
        )
        == tgt_port
    )
    assert coord.sql_value("SELECT count(*) FROM t_big_stream") == 300
    assert coord.sql_value("SELECT max(length(a)) FROM t_big_stream") >= 4000

    # No leftover move subscription on the target.
    tgt.poll_query_until(
        "SELECT count(*) = 0 FROM pg_subscription "
        "WHERE subname LIKE 'citus_shard_move_subscription_%'"
    )

    coord.sql("DROP TABLE t_big_stream")
