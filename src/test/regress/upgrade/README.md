Upgrade test is used for testing postgres version upgrade with citus installed.
Before running the script, make sure that:
- You have downloaded citus.
- You have two different postgres versions.
- Citus is installed to both of the postgres versions. For each postgres version:
    - In citus source directory run:
    ```
        make clean
        ./configure PG_CONFIG=<your path to postgres pg config>
        PG_CONFIG=<your path to postgres pg config> make
        sudo PG_CONFIG=<your path to postgres pg config> make install
    ```
    Make sure you do this for both postgres versions, pg_config should be different for each postgres version.
- Install `pipenv` and run in `citus/src/test/regress`:
```
    pipenv install
    pipenv shell
```

- Finally run upgrade test in `citus/src/test/regress`:
```
    pipenv run make check-upgrade old-bindir=<old-bindir> new-bindir=<new-bindir>
```

To see full command list:

```
    pipenv run upgrade/pg_upgrade_test.py -help
```


How the upgrade test works:
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


