#!/usr/bin/env python3

"""citus_upgrade_test
Usage:
    citus_upgrade_test [options] --bindir=<bindir> --pgxsdir=<pgxsdir> [--citus-old-version=<citus-old-version>]

Options:
    --bindir=<bindir>                       The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --citus-pre-tar=<citus-pre-tar>         Tarball with the citus artifacts to use as the base version to upgrade from
    --citus-post-tar=<citus-post-tar>       Tarball with the citus artifacts to use as the new version to upgrade to
    --pgxsdir=<pgxsdir>           	        Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --citus-old-version=<citus-old-version> Citus old version for local run(ex v8.0.0)
    --mixed                                 Run the verification phase with one node not upgraded.
"""

import subprocess
import atexit
import os
import re
import sys

# https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utils
from utils import USER

from docopt import docopt

from config import (
    CitusUpgradeConfig,
    CITUS_VERSION_SQL,
    MASTER_VERSION,
    AFTER_CITUS_UPGRADE_COORD_SCHEDULE,
    BEFORE_CITUS_UPGRADE_COORD_SCHEDULE,
    MIXED_AFTER_CITUS_UPGRADE_SCHEDULE,
    MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE,
)

import common


def main(config):
    install_citus(config.pre_tar_path)
    common.initialize_temp_dir(config.temp_dir)
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings, config
    )

    report_initial_version(config)
    before_upgrade_schedule = get_before_upgrade_schedule(config.mixed_mode)
    run_test_on_coordinator(config, before_upgrade_schedule)
    remove_citus(config.pre_tar_path)
    install_citus(config.post_tar_path)

    restart_databases(config.bindir, config.datadir, config.mixed_mode, config)
    run_alter_citus(config.bindir, config.mixed_mode, config)
    verify_upgrade(config, config.mixed_mode, config.node_name_to_ports.values())

    after_upgrade_schedule = get_after_upgrade_schedule(config.mixed_mode)
    run_test_on_coordinator(config, after_upgrade_schedule)
    remove_citus(config.post_tar_path)


def install_citus(tar_path):
    with utils.cd("/"):
        subprocess.run(["tar", "xvf", tar_path], check=True)


def report_initial_version(config):
    for port in config.node_name_to_ports.values():
        actual_citus_version = get_actual_citus_version(config.bindir, port)
        print("port:{} citus version {}".format(port, actual_citus_version))


def get_version_number(version):
    return re.findall(r"\d+.\d+", version)[0]


def get_actual_citus_version(pg_path, port):
    citus_version = utils.psql_capture(pg_path, port, CITUS_VERSION_SQL)
    citus_version = citus_version.decode("utf-8")
    return get_version_number(citus_version)


def run_test_on_coordinator(config, schedule):
    common.run_pg_regress(
        config.bindir, config.pg_srcdir, config.coordinator_port(), schedule
    )


def remove_citus(tar_path):
    with utils.cd("/"):
        remove_tar_files(tar_path)


def remove_tar_files(tar_path):
    ps = subprocess.Popen(("tar", "tf", tar_path), stdout=subprocess.PIPE)
    output = subprocess.check_output(("xargs", "rm", "-v"), stdin=ps.stdout)
    ps.wait()


def restart_databases(pg_path, rel_data_path, mixed_mode, config):
    for node_name in config.node_name_to_ports.keys():
        if (
            mixed_mode
            and config.node_name_to_ports[node_name] == config.chosen_random_worker_port
        ):
            continue
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        restart_database(
            pg_path=pg_path,
            abs_data_path=abs_data_path,
            node_name=node_name,
            node_ports=config.node_name_to_ports,
            logfile_prefix=config.name,
        )


def restart_database(pg_path, abs_data_path, node_name, node_ports, logfile_prefix):
    command = [
        os.path.join(pg_path, "pg_ctl"),
        "restart",
        "--pgdata",
        abs_data_path,
        "-U",
        USER,
        "-o",
        "-p {}".format(node_ports[node_name]),
        "--log",
        os.path.join(abs_data_path, common.logfile_name(logfile_prefix, node_name)),
    ]
    subprocess.run(command, check=True)


def run_alter_citus(pg_path, mixed_mode, config):
    for port in config.node_name_to_ports.values():
        if mixed_mode and port == config.chosen_random_worker_port:
            continue
        utils.psql(pg_path, port, "ALTER EXTENSION citus UPDATE;")


def verify_upgrade(config, mixed_mode, node_ports):
    for port in node_ports:
        actual_citus_version = get_actual_citus_version(config.bindir, port)
        expected_citus_version = MASTER_VERSION
        if expected_citus_version != actual_citus_version and not (
            mixed_mode and port == config.chosen_random_worker_port
        ):
            print(
                "port: {} citus version {} expected {}".format(
                    port, actual_citus_version, expected_citus_version
                )
            )
            sys.exit(1)
        else:
            print("port:{} citus version {}".format(port, actual_citus_version))


def get_before_upgrade_schedule(mixed_mode):
    if mixed_mode:
        return MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE
    else:
        return BEFORE_CITUS_UPGRADE_COORD_SCHEDULE


def get_after_upgrade_schedule(mixed_mode):
    if mixed_mode:
        return MIXED_AFTER_CITUS_UPGRADE_SCHEDULE
    else:
        return AFTER_CITUS_UPGRADE_COORD_SCHEDULE


# IsRunningOnLocalMachine returns true if the upgrade test is run on
# local machine, in which case the old citus version will be installed
# and it will be upgraded to the current code.
def IsRunningOnLocalMachine(arguments):
    return arguments["--citus-old-version"]


def generate_citus_tarballs(citus_version):
    tmp_dir = "tmp_citus_tarballs"
    citus_old_tarpath = os.path.abspath(
        os.path.join(tmp_dir, "install-citus{}.tar".format(citus_version))
    )
    citus_new_tarpath = os.path.abspath(
        os.path.join(tmp_dir, "install-citusmaster.tar")
    )

    common.initialize_temp_dir_if_not_exists(tmp_dir)
    dirpath = os.path.dirname(os.path.realpath(__file__))
    local_script_path = os.path.join(dirpath, "generate_citus_tarballs.sh")
    with utils.cd(tmp_dir):
        subprocess.check_call([local_script_path, citus_version])

    return [citus_old_tarpath, citus_new_tarpath]


if __name__ == "__main__":
    args = docopt(__doc__, version="citus_upgrade_test")
    if IsRunningOnLocalMachine(args):
        citus_tarball_paths = generate_citus_tarballs(args["--citus-old-version"])
        args["--citus-pre-tar"] = citus_tarball_paths[0]
        args["--citus-post-tar"] = citus_tarball_paths[1]
    config = CitusUpgradeConfig(args)
    atexit.register(
        common.stop_databases,
        config.bindir,
        config.datadir,
        config.node_name_to_ports,
        config.name,
    )
    main(config)
