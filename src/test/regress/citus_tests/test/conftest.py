import pytest
from common import CitusCluster, Postgres, cleanup_test_leftovers, parallel_run


@pytest.fixture(scope="session")
def cluster_factory_session(tmp_path_factory):
    """The session level pytest fixture that creates and caches citus clusters

    IMPORTANT: This should not be used directly, but only indirectly through
    the cluster_factory fixture.
    """
    clusters = {}

    def _make_or_get_cluster(worker_count: int):
        if worker_count not in clusters:
            clusters[worker_count] = CitusCluster(
                tmp_path_factory.mktemp(f"cluster{worker_count}-"), worker_count
            )
        return clusters[worker_count]

    yield _make_or_get_cluster

    parallel_run(CitusCluster.cleanup, clusters.values())


@pytest.fixture
def cluster_factory(cluster_factory_session, request):
    """The pytest fixture that creates and caches citus clusters

    When the function provided by the factory is called, it returns a cluster
    with the given worker count. This cluster is cached across tests, so that
    future invocations with the same worker count don't need to create a
    cluster again, but can reuse the previously created one.

    To try and make sure that tests don't depend on eachother this tries very
    hard to clean up anything that is created during the test.

    It also prints the Postgres logs that were produced during the test to
    stdout. Normally these will be hidden, but when a test fails pytest will
    show all stdout output produced during the test. Thus showing the Postgres
    logs in that case makes it easier to debug.
    """

    log_handles = []
    clusters = []
    nodes = []

    def _make_or_get_cluster(worker_count: int):
        nonlocal log_handles
        nonlocal nodes
        cluster = cluster_factory_session(worker_count)
        if cluster.failed_reset:
            cluster.reset()
            cluster.failed_reset = False
        clusters.append(cluster)
        log_handles += [(node, node.log_handle()) for node in cluster.nodes]
        nodes += cluster.nodes

        # Create a dedicated schema for the test and use it by default
        cluster.coordinator.create_schema(request.node.originalname)
        cluster.schema = request.node.originalname

        return cluster

    yield _make_or_get_cluster

    try:
        # We clean up test leftovers on all nodes together, instead of per
        # cluster. The reason for this is that some subscriptions/publication
        # pairs might be between different clusters. And by cleaning them up
        # all together, the ordering of the DROPs is easy to make correct.
        cleanup_test_leftovers(nodes)
        parallel_run(Postgres.prepare_reset, nodes)
        parallel_run(Postgres.restart, nodes)
    except Exception:
        for cluster in clusters:
            cluster.failed_reset = True
        raise
    finally:
        for node, log in log_handles:
            print(f"\n\nPG_LOG: {node.pgdata}\n")
            print(log.read())
            log.close()


@pytest.fixture(name="coord")
def coordinator(cluster_factory):
    """Sets up a clean single-node Citus cluster for this test"""
    yield cluster_factory(0).coordinator


@pytest.fixture
def cluster(cluster_factory):
    """Sets up a clean 2-worker Citus cluste for this test"""
    yield cluster_factory(2)
