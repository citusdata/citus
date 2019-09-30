# Upgrade Tests

## Postgres Upgrade Test

Postgres upgrade test is used for testing postgres version upgrade with citus installed.
Before running the script, make sure that:

- You have downloaded citus.
- You have two different postgres versions.
- Citus is installed to both of the postgres versions. For each postgres version:
  - In citus source directory run:

  ```bash
    make clean
    ./configure PG_CONFIG=<your path to postgres pg config>
    PG_CONFIG=<your path to postgres pg config> make
    sudo PG_CONFIG=<your path to postgres pg config> make install
  ```

  Make sure you do this for both postgres versions, pg_config should be different for each postgres version.

- Install `pipenv` and run in `citus/src/test/regress`:

```bash
    pipenv install
    pipenv shell
```

- Finally run upgrade test in `citus/src/test/regress`:

```bash
    pipenv run make check-pg-upgrade old-bindir=<old-bindir> new-bindir=<new-bindir>
```

To see full command list:

```bash
    pipenv run upgrade/pg_upgrade_test.py -help
```

How the postgres upgrade test works:

- Temporary folder `tmp_upgrade` is created in `src/test/regress/`, if one exists it is removed first.
- Database is initialized and citus cluster is created(1 coordinator + 2 workers) with old postgres.
- `before_pg_upgrade_schedule` is run with `pg_regress`. This schedule does not drop any tables or data so that we can verify upgrade.
- `citus_prepare_pg_upgrade` is run in coordinators and workers.
- Old database is stopped.
- A new database is initialized with new postgres under `tmp_upgrade`.
- Postgres upgrade is performed.
- New database is started in both coordinators and workers.
- `citus_finish_pg_upgrade` is run in coordinators and workers to finalize the upgrade step.
- `after_pg_upgrade_schedule` is run with `pg_regress` to verify that the previously created tables, and data still exist. Router and realtime queries are used to verify this.

## Citus Upgrade Test

Citus upgrade test is used for testing citus version upgrades from specific version to master. The purpose of this test is to ensure that a newly made change does not result in unexpected upgrade errors.

The citus upgrade test is designed to be run on a docker image for CircleCI, so we won't explain here how to run it in your local.

Currently the citus upgrade test assumes that:

- In the root directory("/") there are citus installation tar files for postgres versions. The tar files are used to install specific citus version with a postgres version. The tar files format is `install-pg{pg-major}-citus{citus-version}.tar` such as install-pg11-citusv8.0.0.tar.
- `install-{pg_major}.tar` tars exist in `$HOME/project` folder. These tars should contain the checked out citus installation files. Currently build job creates these tars.

How the citus upgrade test work:

- The script takes postgres version and the initial citus version.
- It installs the given citus version from `install-pg{pg-major}-citus{citus-version}.tar`.
- It creates a citus cluster(1 coordinator 2 workers).
- It runs `before_citus{citus_version}_upgrade` schedule to check that the version is installed correctly.
- It installs the checked out citus version from `install-{pg_major}.tar`.
- It restarts the database and runs `ALTER EXTENSION citus UPGRADE`.
- It runs `after_citus_upgrade` schedule to verify that the upgrade is successful.
- It stops the cluster.

Note that when the version of citus changes, we should update `after_citus_upgrade.out` with the new version of citus otherwise that will be outdated and it will fail.

There is a target for citus upgrade test:

```bash
make check-citus-upgrade pg-version=<pg-version> bindir=<bindir> citus-version=<citus-version>
```

To see full command list:

```bash
    pipenv run upgrade/citus_upgrade_test.py -help
```
