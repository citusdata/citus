
import os
import shutil
import sys
import subprocess

import utils

from config import NODE_NAMES, NODE_PORTS, COORDINATOR_NAME, USER, WORKER_PORTS, DBNAME, CitusBaseClusterConfig


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
    subprocess.run(['mkdir', rel_data_path], check=True)
    for node_name in node_names:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, 'initdb'),
            '--pgdata', abs_data_path,
            '--username', USER
        ]
        subprocess.run(command, check=True)
        add_settings(abs_data_path, settings)


def add_settings(abs_data_path, settings):
    conf_path = os.path.join(abs_data_path, 'postgresql.conf')
    with open(conf_path, 'a') as conf_file:
        for setting_key, setting_val in settings.items():
            setting = "{setting_key} = \'{setting_val}\'\n".format(
                setting_key=setting_key,
                setting_val=setting_val)
            conf_file.write(setting)


def start_databases(pg_path, rel_data_path, node_name_to_ports):
    for node_name in node_name_to_ports.keys():
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        node_port = node_name_to_ports[node_name]
        command = [
            os.path.join(pg_path, 'pg_ctl'), 'start',
            '--pgdata', abs_data_path,
            '-U', USER,
            '-o', '-p {}'.format(node_port),
            '--log', os.path.join(abs_data_path, 'logfile_' + node_name)
        ]
        subprocess.run(command, check=True)

def create_citus_extension(pg_path, node_ports):
    for port in node_ports:
        utils.psql(pg_path, port, "CREATE EXTENSION citus;")

def run_pg_regress(pg_path, pg_srcdir, port, schedule):
    should_exit = True
    _run_pg_regress(pg_path, pg_srcdir, port, schedule, should_exit)

def run_pg_regress_without_exit(pg_path, pg_srcdir, port, schedule):
    should_exit = False
    return _run_pg_regress(pg_path, pg_srcdir, port, schedule, should_exit)  

def _run_pg_regress(pg_path, pg_srcdir, port, schedule, should_exit):
    command = [
        os.path.join(pg_srcdir, 'src/test/regress/pg_regress'),
        '--port', str(port),
        '--schedule', schedule,
        '--bindir', pg_path,
        '--user', USER,
        '--dbname', DBNAME,
        '--use-existing'
    ]
    exit_code = subprocess.call(command)
    # subprocess.run('bin/copy_modified', check=True)
    if should_exit and exit_code != 0:
        sys.exit(exit_code)
    return exit_code    

def save_regression_diff(name):
    if not os.path.exists('regressions.diff'):
        return
    file_name = "./results/regression.{}".format(name)
    shutil.move("regressions.diffs", file_name)

def sync_metadata_to_workers(pg_path):
    for port in WORKER_PORTS:
        command = "SELECT * from start_metadata_sync_to_node('localhost', {port});".format(
            port=port)
        utils.psql(pg_path, NODE_PORTS[COORDINATOR_NAME], command)
    

def add_workers(pg_path, worker_ports):
    for port in worker_ports:
        command = "SELECT * from master_add_node('localhost', {port});".format(
            port=port)
        utils.psql(pg_path, NODE_PORTS[COORDINATOR_NAME], command)

def stop_databases(pg_path, rel_data_path, node_name_to_ports, no_output = False):
    for node_name in node_name_to_ports.keys():
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        node_port = node_name_to_ports[node_name]
        command = [
            os.path.join(pg_path, 'pg_ctl'), 'stop',
            '--pgdata', abs_data_path,
            '-U', USER,
            '-o', '-p {}'.format(node_port),
            '--log', os.path.join(abs_data_path, 'logfile_' + node_name)
        ]
        if no_output:
            subprocess.call(command, stdout=subprocess.DEVNULL)
        else:
            subprocess.call(command)


def initialize_citus_cluster(old_bindir, old_datadir, settings, config):
    # In case there was a leftover from previous runs, stop the databases
    stop_databases(config.bindir, config.datadir, config.node_name_to_ports, no_output=True)
    initialize_db_for_cluster(old_bindir, old_datadir, settings, config.node_name_to_ports.keys())
    start_databases(old_bindir, old_datadir, config.node_name_to_ports)
    create_citus_extension(old_bindir, config.node_name_to_ports.values())
    add_workers(old_bindir, config.worker_ports)
    if isinstance(config, CitusBaseClusterConfig):
        config.setup_steps()    
