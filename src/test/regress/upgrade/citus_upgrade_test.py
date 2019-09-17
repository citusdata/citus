#!/usr/bin/env python3

"""citus_upgrade_test
Usage:
    citus_upgrade_test --bindir=<bindir> --citus-version=<citus-version> --pgxsdir=<pgxsdir>

Options:
    --bindir=<bindir>                      The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-10.4/bin')
    --citus-version=<citus-version>        Citus version(ex: v8.0.0)
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
"""

from docopt import docopt

from config import CitusUpgradeConfig, NODE_PORTS, COORDINATOR_NAME, BEFORE_CITUS_UPGRADE_SCHEDULE
from upgrade_test import initialize_temp_dir, initialize_citus_cluster, run_pg_regress


def main(config):
    initialize_temp_dir(config.temp_dir)
    initialize_citus_cluster(
        config.bindir, config.datadir, config.settings)
    run_pg_regress(config.bindir, config.pg_srcdir,
                   NODE_PORTS[COORDINATOR_NAME], BEFORE_CITUS_UPGRADE_SCHEDULE.format(config.citus_version))    
    
    
if __name__ == '__main__':
    config = CitusUpgradeConfig(docopt(__doc__, version='citus_upgrade_test'))
    main(config)
    













