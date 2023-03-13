#!/usr/bin/env python3

"""join_gen_test
Usage:
    join_gen_test --bindir=<bindir> --pgxsdir=<pgxsdir>

Options:
    --bindir=<bindir>                      PostgreSQL executable directory(ex: '~/.pgenv/pgsql-10.4/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
"""

import os
import subprocess
import sys

from docopt import docopt

# https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import common

import config as cfg


def run_test(config):
    # start cluster
    common.initialize_temp_dir(cfg.CITUS_ARBITRARY_TEST_DIR)
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings, config
    )

    # run test
    scriptDirPath = os.path.dirname(os.path.abspath(__file__))
    testRunCommand = "bash {}/citus_compare_dist_local_joins.sh {}".format(
        scriptDirPath, config.coordinator_port()
    )
    process = subprocess.Popen(
        testRunCommand.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()

    # stop cluster
    common.stop_databases(
        config.bindir, config.datadir, config.node_name_to_ports, config.name
    )

    print(stdout)
    print(stderr)
    print(process.returncode)
    sys.exit(process.returncode)


if __name__ == "__main__":
    citusBaseClusterConfig = cfg.CitusBaseClusterConfig(
        docopt(__doc__, version="join_gen_test")
    )

    run_test(citusBaseClusterConfig)
