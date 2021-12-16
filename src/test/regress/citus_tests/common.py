import os
import shutil
import sys
import subprocess
import atexit

import utils
from utils import USER, cd


def initialize_temp_dir(temp_dir):
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)
    # Give full access to TEMP_DIR so that postgres user can use it.
    os.chmod(temp_dir, 0o777)


def initialize_temp_dir_if_not_exists(temp_dir):
    if os.path.exists(temp_dir):
        return
    os.mkdir(temp_dir)
    # Give full access to TEMP_DIR so that postgres user can use it.
    os.chmod(temp_dir, 0o777)


def initialize_db_for_cluster(pg_path, rel_data_path, settings, node_names):
    subprocess.run(["mkdir", rel_data_path], check=True)
    for node_name in node_names:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, "initdb"),
            "--pgdata",
            abs_data_path,
            "--username",
            USER,
        ]
        subprocess.run(command, check=True)
        add_settings(abs_data_path, settings)


def add_settings(abs_data_path, settings):
    conf_path = os.path.join(abs_data_path, "postgresql.conf")
    with open(conf_path, "a") as conf_file:
        for setting_key, setting_val in settings.items():
            setting = "{setting_key} = '{setting_val}'\n".format(
                setting_key=setting_key, setting_val=setting_val
            )
            conf_file.write(setting)


def create_role(pg_path, port, node_ports, user_name):
    for port in node_ports:
        command = "SELECT worker_create_or_alter_role('{}', 'CREATE ROLE {} WITH LOGIN CREATEROLE CREATEDB;', NULL)".format(
            user_name, user_name
        )
        utils.psql(pg_path, port, command)
        command = "GRANT CREATE ON DATABASE postgres to {}".format(user_name)
        utils.psql(pg_path, port, command)


def coordinator_should_haveshards(pg_path, port):
    command = "SELECT citus_set_node_property('localhost', {}, 'shouldhaveshards', true)".format(
        port
    )
    utils.psql(pg_path, port, command)


def start_databases(pg_path, rel_data_path, node_name_to_ports, logfile_prefix):
    for node_name in node_name_to_ports.keys():
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        node_port = node_name_to_ports[node_name]
        command = [
            os.path.join(pg_path, "pg_ctl"),
            "start",
            "--pgdata",
            abs_data_path,
            "-U",
            USER,
            "-o",
            "-p {}".format(node_port),
            "--log",
            os.path.join(abs_data_path, logfile_name(logfile_prefix, node_name)),
        ]
        subprocess.run(command, check=True)
    atexit.register(
        stop_databases,
        pg_path,
        rel_data_path,
        node_name_to_ports,
        logfile_prefix,
        no_output=True,
    )


def create_citus_extension(pg_path, node_ports):
    for port in node_ports:
        utils.psql(pg_path, port, "CREATE EXTENSION citus;")


def run_pg_regress(pg_path, pg_srcdir, port, schedule):
    should_exit = True
    _run_pg_regress(pg_path, pg_srcdir, port, schedule, should_exit)
    subprocess.run("bin/copy_modified", check=True)


def run_pg_regress_without_exit(
    pg_path,
    pg_srcdir,
    port,
    schedule,
    output_dir=".",
    input_dir=".",
    user="postgres",
    extra_tests="",
):
    should_exit = False
    exit_code = _run_pg_regress(
        pg_path,
        pg_srcdir,
        port,
        schedule,
        should_exit,
        output_dir,
        input_dir,
        user,
        extra_tests,
    )
    copy_binary_path = os.path.join(input_dir, "copy_modified_wrapper")
    exit_code |= subprocess.call(copy_binary_path)
    return exit_code


def _run_pg_regress(
    pg_path,
    pg_srcdir,
    port,
    schedule,
    should_exit,
    output_dir=".",
    input_dir=".",
    user="postgres",
    extra_tests="",
):
    command = [
        os.path.join(pg_srcdir, "src/test/regress/pg_regress"),
        "--port",
        str(port),
        "--schedule",
        schedule,
        "--bindir",
        pg_path,
        "--user",
        user,
        "--dbname",
        "postgres",
        "--inputdir",
        input_dir,
        "--outputdir",
        output_dir,
        "--use-existing",
    ]
    if extra_tests != "":
        command.append(extra_tests)

    exit_code = subprocess.call(command)
    if should_exit and exit_code != 0:
        sys.exit(exit_code)
    return exit_code


def save_regression_diff(name, output_dir):
    path = os.path.join(output_dir, "regression.diffs")
    if not os.path.exists(path):
        return
    new_file_path = os.path.join(output_dir, "./regression_{}.diffs".format(name))
    print("new file path:", new_file_path)
    shutil.move(path, new_file_path)


def sync_metadata_to_workers(pg_path, worker_ports, coordinator_port):
    for port in worker_ports:
        command = (
            "SELECT * from start_metadata_sync_to_node('localhost', {port});".format(
                port=port
            )
        )
        utils.psql(pg_path, coordinator_port, command)


def add_coordinator_to_metadata(pg_path, coordinator_port):
    command = "SELECT citus_add_node('localhost', {}, groupId := 0)".format(
        coordinator_port
    )
    utils.psql(pg_path, coordinator_port, command)


def add_workers(pg_path, worker_ports, coordinator_port):
    for port in worker_ports:
        command = "SELECT * from master_add_node('localhost', {port});".format(
            port=port
        )
        utils.psql(pg_path, coordinator_port, command)


def logfile_name(logfile_prefix, node_name):
    return "logfile_" + logfile_prefix + "_" + node_name


def stop_databases(
    pg_path, rel_data_path, node_name_to_ports, logfile_prefix, no_output=False
):
    for node_name in node_name_to_ports.keys():
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        node_port = node_name_to_ports[node_name]
        command = [
            os.path.join(pg_path, "pg_ctl"),
            "stop",
            "--pgdata",
            abs_data_path,
            "-U",
            USER,
            "-o",
            "-p {}".format(node_port),
            "--log",
            os.path.join(abs_data_path, logfile_name(logfile_prefix, node_name)),
        ]
        if no_output:
            subprocess.call(
                command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        else:
            subprocess.call(command)


def initialize_citus_cluster(bindir, datadir, settings, config):
    # In case there was a leftover from previous runs, stop the databases
    stop_databases(
        bindir, datadir, config.node_name_to_ports, config.name, no_output=True
    )
    initialize_db_for_cluster(
        bindir, datadir, settings, config.node_name_to_ports.keys()
    )
    start_databases(bindir, datadir, config.node_name_to_ports, config.name)
    create_citus_extension(bindir, config.node_name_to_ports.values())
    add_workers(bindir, config.worker_ports, config.coordinator_port())
    if config.is_mx:
        sync_metadata_to_workers(bindir, config.worker_ports, config.coordinator_port())
    if config.add_coordinator_to_metadata:
        add_coordinator_to_metadata(bindir, config.coordinator_port())
    config.setup_steps()
