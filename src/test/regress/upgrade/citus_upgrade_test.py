#!/usr/bin/env python3

"""citus_upgrade_test
Usage:
    citus_upgrade_test [options] --bindir=<bindir> --pgxsdir=<pgxsdir> [--citus-old-version=<citus-old-version>]

Options:
    --bindir=<bindir>                       The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --citus-pre-tar=<citus-pre-tar>         Tarball with the citus artifacts to use as the base version to upgrade from
    --citus-post-tar=<citus-post-tar>       Tarball with the citus artifacts to use as the new version to upgrade to
    --pgxsdir=<pgxsdir>           	        Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --citus-old-version=<citus-old-version> Citus old version for local execution(ex v8.0.0)
    --mixed                                 Run the verification phase with one node not upgraded.
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

import upgrade_common as common


def main(config):
    install_citus(config.pre_tar_path)
    common.initialize_temp_dir(config.temp_dir)
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings)

    report_initial_version(config)
    run_test_on_coordinator(config, BEFORE_CITUS_UPGRADE_COORD_SCHEDULE)
    remove_citus(config.pre_tar_path)
    install_citus(config.post_tar_path)

    restart_databases(config.bindir, config.datadir, config.mixed_mode)
    run_alter_citus(config.bindir, config.mixed_mode)
    verify_upgrade(config, config.mixed_mode)

    run_test_on_coordinator(config, AFTER_CITUS_UPGRADE_COORD_SCHEDULE)
    remove_citus(config.post_tar_path)


def install_citus(tar_path):
    with utils.cd('/'):
        subprocess.call(['tar', 'xvf', tar_path])


def report_initial_version(config):
    for port in NODE_PORTS.values():
        actual_citus_version = get_actual_citus_version(config.bindir, port)
        print("port:{} citus version {}".format(port, actual_citus_version))


def get_version_number(version):
    return re.findall('\d+.\d+', version)[0]


def get_actual_citus_version(pg_path, port):
    citus_version = utils.psql(pg_path, port, CITUS_VERSION_SQL)
    citus_version = citus_version.decode('utf-8')
    return get_version_number(citus_version)


def run_test_on_coordinator(config, schedule):
    common.run_pg_regress(config.bindir, config.pg_srcdir,
                          NODE_PORTS[COORDINATOR_NAME], schedule)


def remove_citus(tar_path):
    with utils.cd('/'):
        remove_tar_files(tar_path)


def remove_tar_files(tar_path):
    ps = subprocess.Popen(('tar', 'tf', tar_path), stdout=subprocess.PIPE)
    output = subprocess.check_output(('xargs', 'rm', '-v'), stdin=ps.stdout)
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
        if expected_citus_version != actual_citus_version and not (mixed_mode and port == WORKER1PORT):
            print("port: {} citus version {} expected {}".format(
                port, actual_citus_version, expected_citus_version))
            sys.exit(1)
        else:
            print("port:{} citus version {}".format(port, actual_citus_version))


def isLocalExecution(arguments):
    return arguments['--citus-old-version']


def generate_citus_tarballs(citus_version):
    tmp_dir = 'tmp_citus_tarballs'
    citus_old_tarpath = os.path.abspath(os.path.join(
        tmp_dir, 'install-citus{}.tar'.format(citus_version)))
    citus_new_tarpath = os.path.abspath(os.path.join(tmp_dir, 'install-citusmaster.tar'))

    common.initialize_temp_dir_if_not_exists(tmp_dir)
    local_script_path = os.path.abspath('upgrade/generate_citus_tarballs.sh')
    with utils.cd(tmp_dir):
        subprocess.check_call([
            local_script_path, citus_version
        ])

    return [citus_old_tarpath, citus_new_tarpath]

if __name__ == '__main__':
    args = docopt(__doc__, version='citus_upgrade_test')
    if isLocalExecution(args):
        citus_tarball_paths = generate_citus_tarballs(
            args['--citus-old-version'])
        args['--citus-pre-tar'] = citus_tarball_paths[0]
        args['--citus-post-tar'] = citus_tarball_paths[1]
    config = CitusUpgradeConfig(args)
    atexit.register(common.stop_databases, config.bindir, config.datadir)
    main(config)
