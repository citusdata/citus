#!/usr/bin/env python3

"""upgrade_test
Usage:
    upgrade_test --old-bindir=<old-bindir> --new-bindir=<new-bindir> --pgxsdir=<pgxsdir>

Options:
    --old-bindir=<old-bindir>              The old PostgreSQL executable directory;(ex: '~/.pgenv/pgsql/bin')
    --new-bindir=<new-bindir>              New postgres binary absolute path(ex: '~/.pgenv/pgsql-11.3/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
"""

import utils
import atexit
import subprocess
import sys
import shutil
import os

from docopt import docopt

from config import (
    Config, USER, NODE_PORTS,
    NODE_NAMES, DBNAME, COORDINATOR_NAME,
    WORKER_PORTS, AFTER_UPGRADE_SCHEDULE, BEFORE_UPGRADE_SCHEDULE
)


def initialize_temp_dir(temp_dir):
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)
    # Give full access to TEMP_DIR so that postgres user can use it.
    os.chmod(temp_dir, 0o777)


def initialize_db_for_cluster(pg_path, rel_data_path, settings):
    subprocess.call(['mkdir', rel_data_path])
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, 'initdb'),
            '--pgdata', abs_data_path,
            '--username', USER
        ]
        subprocess.call(command)
        add_settings(abs_data_path, settings)


def add_settings(abs_data_path, settings):
    conf_path = os.path.join(abs_data_path, 'postgresql.conf')
    with open(conf_path, 'a') as conf_file:
        for setting_key, setting_val in settings.items():
            setting = "{setting_key} = \'{setting_val}\'\n".format(
                setting_key=setting_key,
                setting_val=setting_val)
            conf_file.write(setting)


def start_databases(pg_path, rel_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, 'pg_ctl'),
            '--pgdata', abs_data_path,
            '-U', USER,
            "-o '-p {}'".format(NODE_PORTS[node_name]),
            '--log', os.path.join(pg_path, 'logfile_' + node_name),
            'start'
        ]
        subprocess.call(command)


def create_citus_extension(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "CREATE EXTENSION citus;")


def add_workers(pg_path):
    for port in WORKER_PORTS:
        command = "SELECT * from master_add_node('localhost', {port});".format(
            port=port)
        utils.psql(pg_path, NODE_PORTS[COORDINATOR_NAME], command)


def run_pg_regress(pg_path, PG_SRCDIR, port, schedule):
    command = [
        os.path.join(PG_SRCDIR, 'src/test/regress/pg_regress'),
        '--port', str(port),
        '--schedule', schedule,
        '--bindir', pg_path,
        '--user', USER,
        '--dbname', DBNAME,
        '--use-existing'
    ]
    exit_code = subprocess.call(command)
    if exit_code != 0:
        sys.exit(exit_code)


def citus_prepare_pg_upgrade(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "SELECT citus_prepare_pg_upgrade();")


def stop_databases(pg_path, rel_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, 'pg_ctl'),
            '--pgdata', abs_data_path,
            '-U', USER,
            "-o '-p {}'".format(NODE_PORTS[node_name]),
            '--log', os.path.join(pg_path, 'logfile_' + node_name),
            'stop'
        ]
        subprocess.call(command)


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


def initialize_citus_cluster(old_bindir, old_datadir, settings):
    initialize_db_for_cluster(old_bindir, old_datadir, settings)
    start_databases(old_bindir, old_datadir)
    create_citus_extension(old_bindir)
    add_workers(old_bindir)


def stop_all_databases(old_bindir, new_bindir, old_datadir, new_datadir):
    stop_databases(old_bindir, old_datadir)
    stop_databases(new_bindir, new_datadir)


def main(config):
    initialize_temp_dir(config.temp_dir)
    initialize_citus_cluster(
        config.old_bindir, config.old_datadir, config.settings)

    run_pg_regress(config.old_bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], BEFORE_UPGRADE_SCHEDULE)

    citus_prepare_pg_upgrade(config.old_bindir)
    stop_databases(config.old_bindir, config.old_datadir)

    initialize_db_for_cluster(
        config.new_bindir, config.new_datadir, config.settings)
    perform_postgres_upgrade(
        config.old_bindir, config.new_bindir, config.old_datadir, config.new_datadir)
    start_databases(config.new_bindir, config.new_datadir)
    citus_finish_pg_upgrade(config.new_bindir)

    run_pg_regress(config.new_bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], AFTER_UPGRADE_SCHEDULE)


if __name__ == '__main__':
    config = Config(docopt(__doc__, version='upgrade_test'))
    #atexit.register(stop_all_databases, config.old_bindir,
    #                config.new_bindir, config.old_datadir, config.new_datadir)
    main(config)
