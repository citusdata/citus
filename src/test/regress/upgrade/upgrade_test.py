"""upgrade_test
Usage:
    upgrade_test --old-bindir=<old-bindir> --new-bindir=<new-bindir> --postgres-srcdir=<postgres-srcdir> [--citus-path=<citus-path>]

Options:
    --old-bindir=<old-bindir>              The old PostgreSQL executable directory;(ex: '~/.pgenv/pgsql/bin')
    --new-bindir=<new-bindir>              New postgres binary absolute path(ex: '~/.pgenv/pgsql-11.3/bin')
    --postgres-srcdir=<postgres-srcdir>    Path to postgres build directory(ex: ~/.pgenv/src/postgresql-11.3/src/test/regress)
    --citus-path=<citus-path>              Absolute path for citus directory (default: ~/citus)
"""


import utils
from config import *
from docopt import docopt
import os
import shutil
import sys


def initialize_temp_dir():
    if os.path.exists(config[TEMP_DIR]):
        shutil.rmtree(config[TEMP_DIR])
    os.mkdir(config[TEMP_DIR])
    # Give full access to TEMP_DIR so that postgres user can use it.
    os.chmod(config[TEMP_DIR], 0o777)


def initialize_db_for_cluster(pg_path, rel_data_path):
    utils.run('mkdir ' + rel_data_path)
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        pg_command = pg_path + '/initdb'
        utils.run(pg_command + ' --pgdata=' +
                  abs_data_path + ' --username=' + USER)
        add_citus_to_shared_preload_libraries(abs_data_path)


def get_add_citus_to_shared_preload_library_cmd(abs_data_path):
    return 'echo "shared_preload_libraries = \'citus\'" >> {abs_data_path}/postgresql.conf'.format(
        abs_data_path=abs_data_path)


def add_citus_to_shared_preload_libraries(abs_data_path):
    utils.run(get_add_citus_to_shared_preload_library_cmd(abs_data_path))


def start_databases(pg_path, rel_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = '{pg_path}/pg_ctl --pgdata={pgdata} -U={user}" \
        " -o "-p {port}" --log={pg_path}/logfile_{node_name} start'.format(
            pg_path=pg_path, pgdata=abs_data_path, user=USER,
            port=NODE_PORTS[node_name], node_name=node_name)
        utils.run(command)


def create_citus_extension(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "CREATE EXTENSION citus;")


def add_workers(pg_path):
    for port in WORKER_PORTS:
        command = "SELECT * from master_add_node('localhost', {port});".format(
            port=port)
        utils.psql(pg_path, NODE_PORTS[COORDINATOR_NAME], command)


def run_pg_regress(pg_path, pg_src_path, port, schedule):
    command = "{pg_src_path}/src/test/regress/pg_regress --port={port}" \
        " --schedule={schedule_path} --bindir={pg_path} --user={user} --use-existing --dbname={dbname}".format(
            pg_src_path=pg_src_path, port=port,
            schedule_path=schedule, pg_path=pg_path,
            user=USER, dbname=DBNAME)
    exit_code = utils.run(command)
    if exit_code != 0:
        sys.exit(exit_code)


def citus_prepare_pg_upgrade(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "SELECT citus_prepare_pg_upgrade();")


def stop_databases(pg_path, rel_data_path):
    for node_name in NODE_NAMES:
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = '{pg_path}/pg_ctl --pgdata={pgdata} -U={user}" \
        " -o "-p {port}" --log={pg_path}/logfile_{node_name} stop'.format(
            pg_path=pg_path, pgdata=abs_data_path, user=USER,
            port=NODE_PORTS[node_name], node_name=node_name)
        utils.run(command)


def perform_postgres_upgrade():
    for node_name in NODE_NAMES:
        base_new_data_path = os.path.abspath(config[NEW_PG_DATA_PATH])
        base_old_data_path = os.path.abspath(config[CURRENT_PG_DATA_PATH])
        with utils.cd(base_new_data_path):
            abs_new_data_path = os.path.join(base_new_data_path, node_name)
            abs_old_data_path = os.path.join(base_old_data_path, node_name)
            command = "{pg_new_bindir}/pg_upgrade --username={user}" \
                " --old-bindir={pg_old_bindir} --new-bindir={pg_new_bindir} --old-datadir={old_datadir} \
                                        --new-datadir={new_datadir}".format(
                    pg_new_bindir=config[NEW_BINDIR], user=USER,
                    pg_old_bindir=config[OLD_BINDIR],
                    old_datadir=abs_old_data_path, new_datadir=abs_new_data_path)
            utils.run(command)


def citus_finish_pg_upgrade(pg_path):
    for port in NODE_PORTS.values():
        utils.psql(pg_path, port, "SELECT citus_finish_pg_upgrade();")


def initialize_citus_cluster():
    initialize_db_for_cluster(config[OLD_BINDIR], config[CURRENT_PG_DATA_PATH])
    start_databases(config[OLD_BINDIR], config[CURRENT_PG_DATA_PATH])
    create_citus_extension(config[OLD_BINDIR])
    add_workers(config[OLD_BINDIR])


def main():
    initialize_temp_dir()
    initialize_citus_cluster()

    run_pg_regress(config[OLD_BINDIR], config[PG_SRC_PATH],
                   NODE_PORTS[COORDINATOR_NAME], BEFORE_UPGRADE_SCHEDULE)
    citus_prepare_pg_upgrade(config[OLD_BINDIR])
    stop_databases(config[OLD_BINDIR], config[CURRENT_PG_DATA_PATH])

    initialize_db_for_cluster(config[NEW_BINDIR], config[NEW_PG_DATA_PATH])
    perform_postgres_upgrade()
    start_databases(config[NEW_BINDIR], config[NEW_PG_DATA_PATH])
    citus_finish_pg_upgrade(config[NEW_BINDIR])

    run_pg_regress(config[NEW_BINDIR], config[PG_SRC_PATH],
                   NODE_PORTS[COORDINATOR_NAME], AFTER_UPGRADE_SCHEDULE)


if __name__ == '__main__':
    init_config(docopt(__doc__, version='upgrade_test'))
    main()
