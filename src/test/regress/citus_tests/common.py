import os
import shutil
import sys
import subprocess
import atexit
import concurrent.futures

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


def parallel_run(function, items, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(function, item, *args, **kwargs)
            for item in items
        ]
        for future in futures:
            future.result()

def initialize_db_for_cluster(pg_path, rel_data_path, settings, node_names):
    subprocess.run(["mkdir", rel_data_path], check=True)

    def initialize(node_name):
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, "initdb"),
            "--pgdata",
            abs_data_path,
            "--username",
            USER,
            "--no-sync",
            # --allow-group-access is used to ensure we set permissions on
            # private keys correctly
            "--allow-group-access",
            "--encoding",
            "UTF8"
        ]
        subprocess.run(command, check=True)
        add_settings(abs_data_path, settings)

    parallel_run(initialize, node_names)


def add_settings(abs_data_path, settings):
    conf_path = os.path.join(abs_data_path, "postgresql.conf")
    with open(conf_path, "a") as conf_file:
        for setting_key, setting_val in settings.items():
            setting = "{setting_key} = '{setting_val}'\n".format(
                setting_key=setting_key, setting_val=setting_val
            )
            conf_file.write(setting)


def create_role(pg_path, node_ports, user_name):
    def create(port):
        command = "SET citus.enable_ddl_propagation TO OFF; SELECT worker_create_or_alter_role('{}', 'CREATE ROLE {} WITH LOGIN CREATEROLE CREATEDB;', NULL)".format(
            user_name, user_name
        )
        utils.psql(pg_path, port, command)
        command = "SET citus.enable_ddl_propagation TO OFF; GRANT CREATE ON DATABASE postgres to {}".format(user_name)
        utils.psql(pg_path, port, command)

    parallel_run(create, node_ports)


def coordinator_should_haveshards(pg_path, port):
    command = "SELECT citus_set_node_property('localhost', {}, 'shouldhaveshards', true)".format(
        port
    )
    utils.psql(pg_path, port, command)


def start_databases(pg_path, rel_data_path, node_name_to_ports, logfile_prefix, env_variables):
    def start(node_name):
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

        # set the application name if requires
        if env_variables != {}:
            os.environ.update(env_variables)

        subprocess.run(command, check=True)

    parallel_run(start, node_name_to_ports.keys())

    # We don't want parallel shutdown here because that will fail when it's
    # tried in this atexit call with an error like:
    # cannot schedule new futures after interpreter shutdown
    atexit.register(
        stop_databases,
        pg_path,
        rel_data_path,
        node_name_to_ports,
        logfile_prefix,
        no_output=True,
        parallel=False,
    )


def create_citus_extension(pg_path, node_ports):
    def create(port):
        utils.psql(pg_path, port, "CREATE EXTENSION citus;")

    parallel_run(create, node_ports)


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
    pg_path, rel_data_path, node_name_to_ports, logfile_prefix, no_output=False, parallel=True
):
    def stop(node_name):
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

    if parallel:
        parallel_run(stop, node_name_to_ports.keys())
    else:
        for node_name in node_name_to_ports.keys():
            stop(node_name)


def initialize_citus_cluster(bindir, datadir, settings, config):
    # In case there was a leftover from previous runs, stop the databases
    stop_databases(
        bindir, datadir, config.node_name_to_ports, config.name, no_output=True
    )
    initialize_db_for_cluster(
        bindir, datadir, settings, config.node_name_to_ports.keys()
    )
    start_databases(bindir, datadir, config.node_name_to_ports, config.name, config.env_variables)
    create_citus_extension(bindir, config.node_name_to_ports.values())
    add_workers(bindir, config.worker_ports, config.coordinator_port())
    if config.is_mx:
        sync_metadata_to_workers(bindir, config.worker_ports, config.coordinator_port())
    if config.add_coordinator_to_metadata:
        add_coordinator_to_metadata(bindir, config.coordinator_port())
    config.setup_steps()
