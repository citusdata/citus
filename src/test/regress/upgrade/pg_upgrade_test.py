#!/usr/bin/env python3

"""upgrade_test
Usage:
    upgrade_test --old-bindir=<old-bindir> --new-bindir=<new-bindir> --pgxsdir=<pgxsdir>

Options:
    --old-bindir=<old-bindir>              The old PostgreSQL executable directory(ex: '~/.pgenv/pgsql-10.4/bin')
    --new-bindir=<new-bindir>              The new PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
"""

from config import (
    PGUpgradeConfig, USER, NODE_PORTS,
    NODE_NAMES, DBNAME, COORDINATOR_NAME,
    WORKER_PORTS, AFTER_PG_UPGRADE_SCHEDULE, BEFORE_PG_UPGRADE_SCHEDULE
)
from docopt import docopt
import utils
import atexit
import subprocess
import sys
import shutil
import os

import upgrade_common as common

def citus_prepare_pg_upgrade(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "SELECT citus_prepare_pg_upgrade();")


def perform_postgres_upgrade(old_bindir, new_bindir, old_datadir, new_datadir):
    for node_name in NODE_NAMES:
        base_new_data_path = os.path.abspath(new_datadir)
        base_old_data_path = os.path.abspath(old_datadir)
        with utils.cd(base_new_data_path):
            abs_new_data_path = os.path.join(base_new_data_path, node_name)
            abs_old_data_path = os.path.join(base_old_data_path, node_name)
            command = [
                os.path.join(new_bindir, 'pg_upgrade'),
                '--username', USER,
                '--old-bindir', old_bindir,
                '--new-bindir', new_bindir,
                '--old-datadir', abs_old_data_path,
                '--new-datadir', abs_new_data_path
            ]
            subprocess.call(command)


def citus_finish_pg_upgrade(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "SELECT citus_finish_pg_upgrade();")


def stop_all_databases(old_bindir, new_bindir, old_datadir, new_datadir):
    common.stop_databases(old_bindir, old_datadir)
    common.stop_databases(new_bindir, new_datadir)


def main(config):
    common.initialize_temp_dir(config.temp_dir)
    common.initialize_citus_cluster(config.old_bindir, config.old_datadir, config.settings)
    common.run_pg_regress(config.old_bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], BEFORE_PG_UPGRADE_SCHEDULE)
    common.run_pg_regress(config.old_bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], AFTER_PG_UPGRADE_SCHEDULE)

    citus_prepare_pg_upgrade(config.old_bindir)
    common.stop_databases(config.old_bindir, config.old_datadir)

    common.initialize_db_for_cluster(
        config.new_bindir, config.new_datadir, config.settings)
    perform_postgres_upgrade(
        config.old_bindir, config.new_bindir, config.old_datadir, config.new_datadir)
    common.start_databases(config.new_bindir, config.new_datadir)
    citus_finish_pg_upgrade(config.new_bindir)

    common.run_pg_regress(config.new_bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], AFTER_PG_UPGRADE_SCHEDULE)


if __name__ == '__main__':
    config = PGUpgradeConfig(docopt(__doc__, version='upgrade_test'))
    atexit.register(stop_all_databases, config.old_bindir,
                    config.new_bindir, config.old_datadir, config.new_datadir)
    main(config)
