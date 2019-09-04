import utils
from config import *
import os
import shutil


def initialize_temp_dir():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.mkdir(TEMP_DIR)
    # Give full access to TEMP_DIR so that postgres user can use it.
    os.chmod(TEMP_DIR, 0o777)


def initialize_db_for_cluster(pg_path, base_data_path):
    utils.run('mkdir ' + base_data_path)
    for node_name in NODE_NAMES:
        abs_data_path = base_data_path + '/' + node_name
        pg_command = pg_path + '/initdb'
        utils.run(pg_command + ' --pgdata=' +
                  abs_data_path + ' --username=' + USER)
        add_citus_to_shared_preload_libraries(abs_data_path)


def get_add_citus_to_shared_preload_library_cmd(abs_data_path):
    return 'echo "shared_preload_libraries = \'citus\'" >> {}/postgresql.conf'.format(abs_data_path)


def add_citus_to_shared_preload_libraries(abs_data_path):
    utils.run(get_add_citus_to_shared_preload_library_cmd(abs_data_path))


def start_databases(pg_path, base_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = base_data_path + '/' + node_name
        command = '{}/pg_ctl --pgdata={} -U={} -o "-p {}" --log={}/logfile_{} start'.format(pg_path,
                                                                                                    abs_data_path, USER, NODE_PORTS[node_name], pg_path, node_name)
        utils.run(command)


def create_citus_extension(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "CREATE EXTENSION citus;")


def add_workers(pg_path):
    for port in WORKER_PORTS:
        command = "SELECT * from master_add_node('localhost', {});".format(
            port)
        utils.psql(pg_path, NODE_PORTS[COORDINATOR_NAME], command)


def run_pg_regress(pg_path, pg_regress_path, port, schedule):
    command = "{}/pg_regress --port={} --schedule={} --bindir={} --user={} --use-existing --dbname={}".format(
        pg_regress_path, port, schedule, pg_path, USER, DBNAME)
    utils.run(command)


def citus_prepare_pg_upgrade(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "SELECT citus_prepare_pg_upgrade();")


def stop_databases(pg_path, base_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = base_data_path + '/' + node_name
        command = '{}/pg_ctl --pgdata={} -U={} -o "-p {}" --log={}/logfile_{} stop'.format(pg_path,
                                                                                                   abs_data_path, USER, NODE_PORTS[node_name], pg_path, node_name)
        utils.run(command)


def perform_postgres_upgrade():
    with utils.cd("~"):
        for node_name in NODE_NAMES:
            abs_new_data_path = NEW_PG_DATA_PATH + "/" + node_name
            abs_old_data_path = CURRENT_PG_DATA_PATH + "/" + node_name
            command = "{}/pg_upgrade --username={} --old-bindir={} --new-bindir={} --old-datadir={} \
                                        --new-datadir={}".format(NEW_PG_PATH, USER, CURRENT_PG_PATH, NEW_PG_PATH,
                                                                 abs_old_data_path, abs_new_data_path)
            utils.run(command)


def citus_finish_pg_upgrade(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "SELECT citus_finish_pg_upgrade();")


def initialize_citus_cluster():
    initialize_db_for_cluster(CURRENT_PG_PATH, CURRENT_PG_DATA_PATH)
    start_databases(CURRENT_PG_PATH, CURRENT_PG_DATA_PATH)
    create_citus_extension(CURRENT_PG_PATH)
    add_workers(CURRENT_PG_PATH)


def main():
    initialize_temp_dir()
    initialize_citus_cluster()

    run_pg_regress(CURRENT_PG_PATH, PG_REGRESS_PATH,
                   NODE_PORTS[COORDINATOR_NAME], SCHEDULE_PATH + '/before_upgrade_schedule')
    citus_prepare_pg_upgrade(CURRENT_PG_PATH)
    stop_databases(CURRENT_PG_PATH, CURRENT_PG_DATA_PATH)

    initialize_db_for_cluster(NEW_PG_PATH, NEW_PG_DATA_PATH)
    perform_postgres_upgrade()
    start_databases(NEW_PG_PATH, NEW_PG_DATA_PATH)
    citus_finish_pg_upgrade(NEW_PG_PATH)

    run_pg_regress(NEW_PG_PATH, PG_REGRESS_PATH,
                   NODE_PORTS[COORDINATOR_NAME], SCHEDULE_PATH + '/after_upgrade_schedule')


if __name__ == '__main__':
    main()
