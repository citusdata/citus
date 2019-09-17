#!/usr/bin/env python3

"""citus_upgrade_test
Usage:
    citus_upgrade_test --bindir=<bindir> --citus-version=<citus-version> --pgxsdir=<pgxsdir> 

Options:
    --bindir=<bindir>                      The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-10.4/bin')
    --citus-version=<citus-version>        Citus version(ex: v8.0.0)
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
"""

import subprocess
import atexit
import os 

import utils

from docopt import docopt

from config import CitusUpgradeConfig, NODE_PORTS, COORDINATOR_NAME, BEFORE_CITUS_UPGRADE_SCHEDULE, NODE_NAMES, USER
from upgrade_test import initialize_temp_dir, initialize_citus_cluster, run_pg_regress, stop_databases


def run_test_in_all_nodes(config):
    for port in NODE_PORTS.values():
        run_pg_regress(config.bindir, config.pg_srcdir,
                port, BEFORE_CITUS_UPGRADE_SCHEDULE.format(config.citus_version))  

def install_citus(citus_version):
    with utils.cd('/'):
        subprocess.call(['tar', 'xvf', '/install-pg11-citus{}.tar'.format(citus_version)])

def run_alter_citus(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "ALTER EXTENSION citus UPDATE;")

def restart_databases(pg_path, rel_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, 'pg_ctl'), 'restart',
            '--pgdata', abs_data_path,
            '-U', USER,
            '-o', '-p {}'.format(NODE_PORTS[node_name]),
            '--log', os.path.join(abs_data_path, 'logfile_' + node_name)
        ]
        subprocess.call(command)    

def main(config):
    install_citus(config.citus_version)
    initialize_temp_dir(config.temp_dir)
    initialize_citus_cluster(
        config.bindir, config.datadir, config.settings)  
    run_test_in_all_nodes(config)    
    install_citus("citusUpgrade")
    restart_databases(config.bindir, config.datadir)
    run_alter_citus(config.bindir)
    # verify upgrade
    
    
if __name__ == '__main__':
    config = CitusUpgradeConfig(docopt(__doc__, version='citus_upgrade_test'))
    atexit.register(stop_databases, config.bindir, config.datadir)
    main(config)
    













