#!/usr/bin/env python3

"""upgrade_test
Usage:
    upgrade_test --old-bindir=<old-bindir> --new-bindir=<new-bindir> --pgxsdir=<pgxsdir>

Options:
    --old-bindir=<old-bindir>              The old PostgreSQL executable directory(ex: '~/.pgenv/pgsql-10.4/bin')
    --new-bindir=<new-bindir>              The new PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
"""

import atexit
import os
import subprocess
import sys

from docopt import docopt

# https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ignore E402 because these imports require addition to path
import common  # noqa: E402
import utils  # noqa: E402
from utils import USER  # noqa: E402

from config import (  # noqa: E402
    AFTER_PG_UPGRADE_SCHEDULE,
    BEFORE_PG_UPGRADE_SCHEDULE,
    PGUpgradeConfig,
)


def citus_prepare_pg_upgrade(pg_path, node_ports):
    for port in node_ports:
        utils.psql(pg_path, port, "SELECT citus_prepare_pg_upgrade();")


def perform_postgres_upgrade(
    old_bindir, new_bindir, old_datadir, new_datadir, node_names
):
    for node_name in node_names:
        base_new_data_path = os.path.abspath(new_datadir)
        base_old_data_path = os.path.abspath(old_datadir)
        with utils.cd(base_new_data_path):
            abs_new_data_path = os.path.join(base_new_data_path, node_name)
            abs_old_data_path = os.path.join(base_old_data_path, node_name)
            command = [
                os.path.join(new_bindir, "pg_upgrade"),
                "--username",
                USER,
                "--old-bindir",
                old_bindir,
                "--new-bindir",
                new_bindir,
                "--old-datadir",
                abs_old_data_path,
                "--new-datadir",
                abs_new_data_path,
            ]
            subprocess.run(command, check=True)


def citus_finish_pg_upgrade(pg_path, node_ports):
    for port in node_ports:
        utils.psql(pg_path, port, "SELECT citus_finish_pg_upgrade();")


def stop_all_databases(old_bindir, new_bindir, old_datadir, new_datadir, config):
    common.stop_databases(
        old_bindir, old_datadir, config.node_name_to_ports, config.name
    )
    common.stop_databases(
        new_bindir, new_datadir, config.node_name_to_ports, config.name
    )


def main(config):
    common.initialize_temp_dir(config.temp_dir)
    common.initialize_citus_cluster(
        config.old_bindir, config.old_datadir, config.settings, config
    )
    common.run_pg_regress(
        config.old_bindir,
        config.pg_srcdir,
        config.coordinator_port(),
        BEFORE_PG_UPGRADE_SCHEDULE,
    )
    common.run_pg_regress(
        config.old_bindir,
        config.pg_srcdir,
        config.coordinator_port(),
        AFTER_PG_UPGRADE_SCHEDULE,
    )

    citus_prepare_pg_upgrade(config.old_bindir, config.node_name_to_ports.values())
    # prepare should be idempotent, calling it a second time should never fail.
    citus_prepare_pg_upgrade(config.old_bindir, config.node_name_to_ports.values())
    common.stop_databases(
        config.old_bindir, config.old_datadir, config.node_name_to_ports, config.name
    )

    common.initialize_db_for_cluster(
        config.new_bindir,
        config.new_datadir,
        config.settings,
        config.node_name_to_ports.keys(),
    )
    perform_postgres_upgrade(
        config.old_bindir,
        config.new_bindir,
        config.old_datadir,
        config.new_datadir,
        config.node_name_to_ports.keys(),
    )
    common.start_databases(
        config.new_bindir,
        config.new_datadir,
        config.node_name_to_ports,
        config.name,
        {},
    )
    citus_finish_pg_upgrade(config.new_bindir, config.node_name_to_ports.values())

    common.run_pg_regress(
        config.new_bindir,
        config.pg_srcdir,
        config.coordinator_port(),
        AFTER_PG_UPGRADE_SCHEDULE,
    )


if __name__ == "__main__":
    config = PGUpgradeConfig(docopt(__doc__, version="upgrade_test"))
    atexit.register(
        stop_all_databases,
        config.old_bindir,
        config.new_bindir,
        config.old_datadir,
        config.new_datadir,
        config,
    )
    main(config)
