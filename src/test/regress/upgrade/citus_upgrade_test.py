#!/usr/bin/env python3

"""citus_upgrade_test
Usage:
    citus_upgrade_test --bindir=<bindir> --citus-version=<citus-version> --pgxsdir=<pgxsdir> --pg-version=<pg-version> 

Options:
    --bindir=<bindir>                      The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --citus-version=<citus-version>        Citus version, this should be a branch or tag(ex: v8.0.0)
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --pg-version<pg-version>               Major Postgres version to use(ex: 11)
"""

import subprocess
import atexit
import os
import re
import sys

import utils

from docopt import docopt

from config import (
    CitusUpgradeConfig, NODE_PORTS, COORDINATOR_NAME, CITUS_VERSION_SQL, MASTER_VERSION,
    NODE_NAMES, USER, WORKER1PORT, MASTER, HOME,
    AFTER_CITUS_UPGRADE_COORD_SCHEDULE, BEFORE_CITUS_UPGRADE_COORD_SCHEDULE
)
from upgrade_common import initialize_temp_dir, initialize_citus_cluster, run_pg_regress, stop_databases

def main(config):
    run_citus_upgrade_test(config=config, mixed_mode=False)
    stop_databases(config.bindir, config.datadir)
    run_citus_upgrade_test(config=config, mixed_mode=True)

def run_citus_upgrade_test(config, mixed_mode):
    install_citus(config.citus_version, config.pg_version)
    initialize_temp_dir(config.temp_dir)
    initialize_citus_cluster(
        config.bindir, config.datadir, config.settings)

    verify_initial_version(config)
    run_test_on_coordinator(config, BEFORE_CITUS_UPGRADE_COORD_SCHEDULE)
    remove_citus(config.citus_version, config.pg_version)
    install_citus(MASTER, config.pg_version)

    restart_databases(config.bindir, config.datadir, mixed_mode)
    run_alter_citus(config.bindir, mixed_mode)
    verify_upgrade(config, mixed_mode)

    run_test_on_coordinator(config, AFTER_CITUS_UPGRADE_COORD_SCHEDULE)
    remove_citus(MASTER, config.pg_version)

def install_citus(citus_version, pg_version):
    with utils.cd('/'):
        tar_path = get_tar_path(citus_version, pg_version)
        subprocess.call(['tar', 'xvf', tar_path])



def verify_initial_version(config):
    for port in NODE_PORTS.values():
        actual_citus_version = get_actual_citus_version(config.bindir, port)
        expected_citus_version = get_version_number(config.citus_version)
        if expected_citus_version != actual_citus_version:
            sys.exit(1)

def get_version_number(version):
    return re.findall('\d+.\d+', version)[0]

def get_actual_citus_version(pg_path, port):
    citus_version = utils.psql(pg_path, port, CITUS_VERSION_SQL)
    citus_version = citus_version.decode('utf-8')
    return get_version_number(citus_version)

def run_test_on_coordinator(config, schedule):
    run_pg_regress(config.bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], schedule)

def remove_citus(citus_version, pg_version):
    with utils.cd('/'):
        tar_path = get_tar_path(citus_version, pg_version)
        remove_tar_files(tar_path)

def get_tar_path(citus_version, pg_version):
    if citus_version == MASTER:
        return os.path.join(HOME, 'project/install-{}.tar'.format(pg_version))
    return './install-pg{}-citus{}.tar'.format(pg_version, citus_version) 

def remove_tar_files(tar_path):
    ps = subprocess.Popen(('tar', 'tf', tar_path), stdout=subprocess.PIPE)
    output = subprocess.check_output(('xargs', '-d', '\n', 'rm', '-v'), stdin=ps.stdout)
    ps.wait()

def restart_databases(pg_path, rel_data_path, mixed_mode):
    for node_name in NODE_NAMES:
        if mixed_mode and NODE_PORTS[node_name] == WORKER1PORT:
            continue
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        restart_database(
            pg_path=pg_path, abs_data_path=abs_data_path, node_name=node_name)    

def restart_database(pg_path, abs_data_path, node_name):
    command = [
        os.path.join(pg_path, 'pg_ctl'), 'restart',
        '--pgdata', abs_data_path,
        '-U', USER,
        '-o', '-p {}'.format(NODE_PORTS[node_name]),
        '--log', os.path.join(abs_data_path, 'logfile_' + node_name)
    ]
    subprocess.call(command)

def run_alter_citus(pg_path, mixed_mode):
    for port in NODE_PORTS.values():
        if mixed_mode and port == WORKER1PORT:
            continue
        utils.psql(pg_path, port, "ALTER EXTENSION citus UPDATE;")

def verify_upgrade(config, mixed_mode):
    for port in NODE_PORTS.values():
        actual_citus_version = get_actual_citus_version(config.bindir, port)
        expected_citus_version = MASTER_VERSION
        if mixed_mode and port == WORKER1PORT:
            expected_citus_version = get_version_number(config.citus_version)
        if expected_citus_version != actual_citus_version:
            sys.exit(1)

if __name__ == '__main__':
    config = CitusUpgradeConfig(docopt(__doc__, version='citus_upgrade_test'))
    atexit.register(stop_databases, config.bindir, config.datadir)
    main(config)