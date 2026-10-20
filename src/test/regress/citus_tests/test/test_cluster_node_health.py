import contextlib
import re
import socket
import threading
import time

import psycopg
import pytest

PROBE_QUERY_PREFIX = "SELECT citus_check_connection_to_node("
ACTIVE_PROBES_QUERY = """
    SELECT count(*) FROM pg_stat_activity
    WHERE state = 'active' AND pid <> pg_backend_pid() AND starts_with(query, %s)
"""


class BlackholeServer:
    """A TCP server that accepts connections but never replies

    Every Postgres connection attempt to it hangs until the client gives up,
    which lets us make each connectivity check towards it take exactly
    citus.node_connection_timeout on the node that runs the check.
    """

    def __init__(self):
        self.host = "127.0.0.1"
        self.listener = socket.create_server((self.host, 0), backlog=128)
        self.port = self.listener.getsockname()[1]
        self.lock = threading.Lock()
        self.connections = set()
        self.peak_connection_count = 0
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while True:
            try:
                connection, _ = self.listener.accept()
            except OSError:
                return

            with self.lock:
                self.connections.add(connection)
                self.peak_connection_count = max(
                    self.peak_connection_count, len(self.connections)
                )
            threading.Thread(target=self._hold, args=(connection,), daemon=True).start()

    def _hold(self, connection):
        # read until the client closes the connection, but never answer
        try:
            while connection.recv(1024):
                pass
        except OSError:
            # A client disconnect or server shutdown can interrupt recv().
            pass

        with self.lock:
            self.connections.discard(connection)
        connection.close()

    @property
    def connection_count(self):
        with self.lock:
            return len(self.connections)

    def close(self):
        self.listener.shutdown(socket.SHUT_RDWR)
        self.listener.close()
        with self.lock:
            for connection in self.connections:
                connection.shutdown(socket.SHUT_RDWR)


def set_node_connection_timeout(cluster, timeout_ms):
    """Sets the timeout of the connectivity checks on every source node

    Each check runs in a backend on its source node, so the timeout towards
    the target is determined by the setting on the source, not on the node
    that runs citus_check_cluster_node_health().
    """
    for node in cluster.nodes:
        node.configure(f"citus.node_connection_timeout = {timeout_ms}")
        node.reload()

    for node in cluster.nodes:
        node.poll_query_until(
            "SELECT setting::int = %s FROM pg_settings WHERE name = 'citus.node_connection_timeout'",
            (timeout_ms,),
        )


def add_fake_nodes(cur, blackholes):
    for i, blackhole in enumerate(blackholes):
        cur.execute(
            "INSERT INTO pg_dist_node (nodeid, groupid, nodename, nodeport) VALUES (%s, %s, %s, %s)",
            (1000000 + i, 1000000 + i, blackhole.host, blackhole.port),
        )


def test_cluster_node_health_checks_sources_concurrently(cluster):
    timeout_ms = 1000
    timeout = timeout_ms / 1000
    set_node_connection_timeout(cluster, timeout_ms)

    blackholes = [BlackholeServer() for _ in range(3)]
    try:
        with cluster.coordinator.cur(autocommit=False) as cur:
            add_fake_nodes(cur, blackholes)

            start = time.monotonic()
            cur.execute(
                "SELECT from_nodename, from_nodeport, to_nodename, to_nodeport, result "
                "FROM citus_check_cluster_node_health()"
            )
            rows = cur.fetchall()
            elapsed = time.monotonic() - start

            cur.connection.rollback()
    finally:
        for blackhole in blackholes:
            blackhole.close()

    real_nodes = [(node.host, node.port) for node in cluster.nodes]
    fake_nodes = [(blackhole.host, blackhole.port) for blackhole in blackholes]

    # rows are emitted in source-major order over the sorted node list
    all_nodes = sorted(real_nodes + fake_nodes)
    expected_rows = []
    for source in all_nodes:
        for target in all_nodes:
            if source in fake_nodes:
                result = None
            elif target in fake_nodes:
                result = False
            else:
                result = True
            expected_rows.append((*source, *target, result))

    assert rows == expected_rows

    # Executing the checks one by one waits for one timeout for every
    # connection to a fake source node, and for one timeout for every check
    # from a real source node to a fake target node. Executing them
    # concurrently waits for one timeout while connecting to all source nodes,
    # and then for one timeout per fake target node.
    serial_lower_bound = (len(fake_nodes) + len(real_nodes) * len(fake_nodes)) * timeout
    concurrent_estimate = (1 + len(fake_nodes)) * timeout

    assert elapsed >= concurrent_estimate
    assert elapsed < (concurrent_estimate + serial_lower_bound) / 2

    # all real source nodes checked each fake target node at the same time
    for blackhole in blackholes:
        assert blackhole.peak_connection_count == len(real_nodes)


def test_cluster_node_health_cancellation_stops_remote_checks(cluster):
    # make the checks towards the fake node outlive the test unless cancelled
    probe_timeout_ms = 120000
    set_node_connection_timeout(cluster, probe_timeout_ms)

    blackhole = BlackholeServer()
    probe_command = f"{PROBE_QUERY_PREFIX}'{blackhole.host}', {blackhole.port})"

    def checks_are_in_flight():
        return all(
            node.sql_value(ACTIVE_PROBES_QUERY, (probe_command,)) == 1
            for node in cluster.nodes
        )

    # each source node cancels its in-flight check instead of waiting for the timeout
    cancelled_check_log = (
        r"ERROR:  canceling statement due to user request\n[^\n]*STATEMENT:  "
        + re.escape(probe_command)
    )

    try:
        with contextlib.ExitStack() as stack:
            for node in cluster.nodes:
                stack.enter_context(node.log_contains(cancelled_check_log, times=1))

            with cluster.coordinator.conn(autocommit=False) as conn:
                cur = conn.cursor()

                # only let the coordinator give up quickly on the fake source node
                cur.execute("SET LOCAL citus.node_connection_timeout TO 1000")
                add_fake_nodes(cur, [blackhole])

                canceller_result = []

                def cancel_when_checks_are_in_flight():
                    try:
                        deadline = time.monotonic() + 30
                        while not checks_are_in_flight():
                            assert time.monotonic() < deadline
                            time.sleep(0.1)

                        cluster.coordinator.sql(
                            "SELECT pg_catalog.pg_cancel_backend(%s::integer)",
                            (conn.info.backend_pid,),
                        )
                        canceller_result.append(None)
                    except Exception as e:
                        canceller_result.append(e)

                canceller = threading.Thread(target=cancel_when_checks_are_in_flight)
                canceller.start()

                try:
                    with pytest.raises(psycopg.errors.QueryCanceled):
                        cur.execute("SELECT * FROM citus_check_cluster_node_health()")
                    cancelled_at = time.monotonic()
                finally:
                    canceller.join()
                    if canceller_result and canceller_result[0] is not None:
                        raise canceller_result[0]

                assert canceller_result == [None]

                # every real source node had a check towards the fake node in flight
                assert blackhole.peak_connection_count == len(cluster.nodes)

                conn.rollback()

            # The checks on the source nodes would keep waiting for the fake
            # node until their timeout if they were not cancelled, as they do
            # not notice that the connection from the coordinator went away.
            for node in cluster.nodes:
                node.poll_query_until(
                    ACTIVE_PROBES_QUERY.replace("count(*)", "count(*) = 0"),
                    (probe_command,),
                )

            deadline = time.monotonic() + 30
            while blackhole.connection_count > 0:
                assert time.monotonic() < deadline
                time.sleep(0.1)

            assert time.monotonic() - cancelled_at < probe_timeout_ms / 1000 / 4
    finally:
        blackhole.close()
