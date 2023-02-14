import pytest
import os
import filelock
from .utils import Postgres


@pytest.fixture(scope="session")
def coordinator_session(tmp_path_factory):
    """Starts a new Postgres db that is shared for tests in this process"""
    pg = Postgres(tmp_path_factory.getbasetemp() / "coordinator" / "pgdata")
    pg.initdb()
    os.truncate(pg.hba_path, 0)

    pg.ssl_access("all", "trust")
    pg.nossl_access("all", "trust")
    pg.commit_hba()

    pg.start()
    pg.sql("CREATE EXTENSION citus")

    yield pg

    pg.cleanup()


@pytest.fixture(name="coord")
def coordinator(coordinator_session):
    """Sets up a clean Postgres for this test, using the session Postgres

    It als prints the Postgres logs that were created during the test. This can
    be useful for debugging a failure.
    """
    pg = coordinator_session
    # Resets any changes to Postgres settings from previous tests
    pg.reset_hba()
    os.truncate(pg.pgdata / "postgresql.auto.conf", 0)
    pg.reload()

    with pg.log_path.open() as f:
        f.seek(0, os.SEEK_END)
        yield pg
        print("\n\nPG_LOG\n")
        print(f.read())
