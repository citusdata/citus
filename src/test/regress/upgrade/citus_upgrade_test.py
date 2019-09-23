#!/usr/bin/env python3

"""citus_upgrade_test
Usage:
    citus_upgrade_test --bindir=<bindir> --citus-version=<citus-version> --pgxsdir=<pgxsdir> --pg-version=<pg-version> 

Options:
    --bindir=<bindir>                      The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-10.4/bin')
    --citus-version=<citus-version>        Citus version, this should be a branch or tag(ex: v8.0.0)
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --pg-version<pg-version>               Major Postgres version to use(ex: 11)
"""

import subprocess
import atexit
import os

import utils

from docopt import docopt

from config import (
    CitusUpgradeConfig, NODE_PORTS, COORDINATOR_NAME, BEFORE_CITUS_UPGRADE_SCHEDULE,
    NODE_NAMES, USER, AFTER_CITUS_UPGRADE_SCHEDULE, WORKER1PORT,
    AFTER_CITUS_UPGRADE_COORD_SCHEDULE, BEFORE_CITUS_UPGRADE_COORD_SCHEDULE
)
from upgrade_common import initialize_temp_dir, initialize_citus_cluster, run_pg_regress, stop_databases


def verify_initial_version(config):
    for port in NODE_PORTS.values():
        run_pg_regress(config.bindir, config.pg_srcdir,
                       port, BEFORE_CITUS_UPGRADE_SCHEDULE.format(config.citus_version))


def run_test_on_coordinator(config, schedule):
    run_pg_regress(config.bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], schedule)


def verify_upgrade_mixed_mode(config):
    for port in NODE_PORTS.values():
        schedule = AFTER_CITUS_UPGRADE_SCHEDULE
        if port == WORKER1PORT:
            schedule = BEFORE_CITUS_UPGRADE_SCHEDULE.format(
                config.citus_version)
        run_pg_regress(config.bindir, config.pg_srcdir,
                       port, schedule)


def verify_upgrade(config):
    for port in NODE_PORTS.values():
        run_pg_regress(config.bindir, config.pg_srcdir,
                       port, AFTER_CITUS_UPGRADE_SCHEDULE)


def install_citus(citus_version, pg_version):
    with utils.cd('/'):
        subprocess.call(
            ['tar', 'xvf', '/install-pg{}-citus{}.tar'.format(pg_version, citus_version)])


def install_citus_master(pg_version):
    with utils.cd('~/project'):
        abs_tar_path = os.path.abspath('./install-{}.tar'.format(pg_version))
        with utils.cd('/'):
            subprocess.call(['tar', 'xvf', abs_tar_path])


def run_alter_citus_mixed_mode(pg_path):
    for port in NODE_PORTS.values():
        if port == WORKER1PORT:
            continue
        utils.psql(pg_path, port, "ALTER EXTENSION citus UPDATE;")


def run_alter_citus(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "ALTER EXTENSION citus UPDATE;")


def restart_database(pg_path, abs_data_path, node_name):
    command = [
        os.path.join(pg_path, 'pg_ctl'), 'restart',
        '--pgdata', abs_data_path,
        '-U', USER,
        '-o', '-p {}'.format(NODE_PORTS[node_name]),
        '--log', os.path.join(abs_data_path, 'logfile_' + node_name)
    ]
    subprocess.call(command)


def restart_databases_mixed_mode(pg_path, rel_data_path):
    for node_name in NODE_NAMES:
        if NODE_PORTS[node_name] == WORKER1PORT:
            continue
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        restart_database(
            pg_path=pg_path, abs_data_path=abs_data_path, node_name=node_name)


def restart_databases(pg_path, rel_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        restart_database(
            pg_path=pg_path, abs_data_path=abs_data_path, node_name=node_name)


def run_citus_upgrade_test(config, mixed_mode):
    install_citus(config.citus_version, config.pg_version)
    initialize_temp_dir(config.temp_dir)
    initialize_citus_cluster(
        config.bindir, config.datadir, config.settings)

    verify_initial_version(config)
    run_test_on_coordinator(config, BEFORE_CITUS_UPGRADE_COORD_SCHEDULE)
    install_citus_master(config.pg_version)

    if mixed_mode:
        restart_databases_mixed_mode(config.bindir, config.datadir)
        run_alter_citus_mixed_mode(config.bindir)
        verify_upgrade_mixed_mode(config)
    else:
        restart_databases(config.bindir, config.datadir)
        run_alter_citus(config.bindir)
        verify_upgrade(config)

    run_test_on_coordinator(config, AFTER_CITUS_UPGRADE_COORD_SCHEDULE)


def main(config):
    run_citus_upgrade_test(config=config, mixed_mode=False)
    stop_databases(config.bindir, config.datadir)
    run_citus_upgrade_test(config=config, mixed_mode=True)


if __name__ == '__main__':
    config = CitusUpgradeConfig(docopt(__doc__, version='citus_upgrade_test'))
    atexit.register(stop_databases, config.bindir, config.datadir)
    main(config)
