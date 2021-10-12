# Upgrade Tests

## Postgres Upgrade Test

Postgres upgrade test is used for testing postgres version upgrade with citus installed.
Before running the script, make sure that:

-   You have downloaded citus.
-   You have two different postgres versions.
-   Citus is installed to both of the postgres versions. For each postgres version:

    -   In citus source directory run:

    ```bash
      make clean
      ./configure PG_CONFIG=<your path to postgres pg config>
      PG_CONFIG=<your path to postgres pg config> make
      sudo PG_CONFIG=<your path to postgres pg config> make install
    ```

    Make sure you do this for both postgres versions, pg_config should be different for each postgres version.

-   Install `pipenv` and run in `citus/src/test/regress`:

```bash
    pipenv install
    pipenv shell
```

-   Finally run upgrade test in `citus/src/test/regress`:

```bash
    pipenv run make check-pg-upgrade old-bindir=<old-bindir> new-bindir=<new-bindir>
```

To see full command list:

```bash
    pipenv run citus_tests/upgrade/pg_upgrade_test.py -help
```

How the postgres upgrade test works:

-   Temporary folder `tmp_upgrade` is created in `src/test/regress/`, if one exists it is removed first.
-   Database is initialized and citus cluster is created(1 coordinator + 2 workers) with old postgres.
-   `before_pg_upgrade_schedule` is run with `pg_regress`. This schedule sets up any
    objects and data that will be tested for preservation after the upgrade. It
-   `after_pg_upgrade_schedule` is run with `pg_regress` to verify that the output
    of those tests is the same before the upgrade as after.
-   `citus_prepare_pg_upgrade` is run in coordinators and workers.
-   Old database is stopped.
-   A new database is initialized with new postgres under `tmp_upgrade`.
-   Postgres upgrade is performed.
-   New database is started in both coordinators and workers.
-   `citus_finish_pg_upgrade` is run in coordinators and workers to finalize the upgrade step.
-   `after_pg_upgrade_schedule` is run with `pg_regress` to verify that the previously created tables, and data still exist. Router and realtime queries are used to verify this.

### Writing new PG upgrade tests

The main important thing is that we have `upgrade_{name}_before` and
`upgrade_{name}_after` tests. The `before` files are used to setup any objects
and data before the upgrade. The `after` tests shouldn't have any side effects
since they are run twice (once before and once after the upgrade).

Furthermore, anything that is basic Citus functionality should go in the
`upgrade_basic_before`/`upgrade_basic_after` tests. This test file is used
during PG upgrades and Citus upgrades. Any features that don't work in old Citus
versions should thus be added to their own file, because that file will then
only be run during PG versions.

## Citus Upgrade Test

Citus upgrade test is used for testing citus version upgrades from specific version to master. The purpose of this test is to ensure that a newly made change does not result in unexpected upgrade errors.

Currently the citus upgrade test assumes that:

-   You have citus artifact tarballs, both for old version and master.

How the citus upgrade test work:

-   The script takes `citus-pre-tar` and `citus-post-tar` which should contain citus artifacts.
-   It installs the given citus version from `citus-pre-tar`.
-   It creates a citus cluster(1 coordinator 2 workers).
-   It reports the initial versions.
-   It installs the checked out citus version from `citus-post-tar`.
-   It restarts the database and runs `ALTER EXTENSION citus UPGRADE`.
-   It runs `after_citus_upgrade` schedule to verify that the upgrade is successful.
-   It stops the cluster.

Note that when the version of citus changes, we should update `MASTER_VERSION` with the new version of citus otherwise that will be outdated and it will fail.

There is a target for citus upgrade test. We run citus upgrade tests both in normal mode and in mixed mode. In mixed mode, we don't upgrade one of the workers. `'citus.enable_version_checks' : 'false'` is used to prevent citus from giving an error for mixed mode.

To see full command list:

```bash
    pipenv run upgrade/citus_upgrade_test.py -help
```

In order to run citus upgrade tests locally you can use:

```bash
    pipenv run make check-citus-upgrade-local citus-old-version=v8.0.0
```

For mixed mode:

```bash
    pipenv run make check-citus-upgrade-mixed-local citus-old-version=v8.0.0
```
