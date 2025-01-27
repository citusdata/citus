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

import multiprocessing
import os
import re
import subprocess
import sys

from docopt import docopt

# https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ignore E402 because these imports require addition to path
import common  # noqa: E402
import utils  # noqa: E402
from common import CI, PG_MAJOR_VERSION, REPO_ROOT, run  # noqa: E402
from utils import USER  # noqa: E402

from config import (  # noqa: E402
    AFTER_CITUS_UPGRADE_COORD_SCHEDULE,
    BEFORE_CITUS_UPGRADE_COORD_SCHEDULE,
    CITUS_VERSION_SQL,
    MASTER_VERSION,
    MIXED_AFTER_CITUS_UPGRADE_SCHEDULE,
    MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE,
    CitusUpgradeConfig,
)


def main(config):
    before_upgrade_schedule = get_before_upgrade_schedule(config.mixed_mode)
    after_upgrade_schedule = get_after_upgrade_schedule(config.mixed_mode)
    run_citus_upgrade_tests(config, before_upgrade_schedule, after_upgrade_schedule)


def run_citus_upgrade_tests(config, before_upgrade_schedule, after_upgrade_schedule):
    install_citus(config.pre_tar_path)
    common.initialize_temp_dir(config.temp_dir)
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings, config
    )

    report_initial_version(config)
    run_test_on_coordinator(config, before_upgrade_schedule)
    remove_citus(config.pre_tar_path)
    if after_upgrade_schedule is None:
        return

    install_citus(config.post_tar_path)

    restart_databases(config.bindir, config.datadir, config.mixed_mode, config)
    run_alter_citus(config.bindir, config.mixed_mode, config)
    verify_upgrade(config, config.mixed_mode, config.node_name_to_ports.values())

    run_test_on_coordinator(config, after_upgrade_schedule)
    remove_citus(config.post_tar_path)


def install_citus(tar_path):
    if tar_path:
        with utils.cd("/"):
            run(["tar", "xvf", tar_path], shell=False)
    else:
        with utils.cd(REPO_ROOT):
            run(f"make -j{multiprocessing.cpu_count()} -s install")


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
    if tar_path:
        with utils.cd("/"):
            remove_tar_files(tar_path)


def remove_tar_files(tar_path):
    ps = subprocess.Popen(("tar", "tf", tar_path), stdout=subprocess.PIPE)
    subprocess.check_output(("xargs", "rm", "-v"), stdin=ps.stdout)
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


import subprocess
import utils

def run_alter_citus(pg_path, mixed_mode, config):
    for port in config.node_name_to_ports.values():
        if mixed_mode and port == config.chosen_random_worker_port:
            continue

        alter_extension_sql = """
            SELECT '=== BEFORE ALTER ===' AS info;

            SELECT pid, query, state, wait_event_type, wait_event
            FROM pg_stat_activity
            ORDER BY pid;

            SELECT l.locktype,
                l.relation::regclass AS relname,
                l.mode,
                l.granted,
                a.pid,
                a.query AS current_query
            FROM pg_locks l
            JOIN pg_stat_activity a ON (l.pid = a.pid)
            ORDER BY l.relation, l.pid;

            ALTER EXTENSION citus UPDATE;
        """

        debug_sql = """
            SELECT '=== AFTER ALTER ===' AS info;

            SELECT pid, query, state, wait_event_type, wait_event
            FROM pg_stat_activity
            ORDER BY pid;

            SELECT l.locktype,
                l.relation::regclass AS relname,
                l.mode,
                l.granted,
                a.pid,
                a.query AS current_query
            FROM pg_locks l
            JOIN pg_stat_activity a ON (l.pid = a.pid)
            ORDER BY l.relation, l.pid;
        """

        try:
            utils.psql(pg_path, port, alter_extension_sql)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Failed to run 'ALTER EXTENSION citus UPDATE;' on port {port}")
            print(f"        psql returned: {e.returncode}, {e.output}")

            try:
                utils.psql(pg_path, port, debug_sql)
                
            except subprocess.CalledProcessError as inner_e:
                print(f"[WARNING] Could not retrieve diagnostic info on port {port}: {inner_e}")

            raise



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


def generate_citus_tarball(citus_version):
    tmp_dir = "tmp_citus_tarballs"
    citus_old_tarpath = os.path.abspath(
        os.path.join(tmp_dir, f"install-pg{PG_MAJOR_VERSION}-citus{citus_version}.tar")
    )

    common.initialize_temp_dir_if_not_exists(tmp_dir)
    dirpath = os.path.dirname(os.path.realpath(__file__))
    local_script_path = os.path.join(dirpath, "generate_citus_tarballs.sh")
    with utils.cd(tmp_dir):
        subprocess.check_call([local_script_path, str(PG_MAJOR_VERSION), citus_version])

    return citus_old_tarpath


if __name__ == "__main__":
    args = docopt(__doc__, version="citus_upgrade_test")
    if not CI:
        citus_tarball_path = generate_citus_tarball(args["--citus-old-version"])
        config = CitusUpgradeConfig(args, citus_tarball_path, None)
    else:
        config = CitusUpgradeConfig(
            args, args["--citus-pre-tar"], args["--citus-post-tar"]
        )

    main(config)
