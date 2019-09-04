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
- Finally run the script in `citus/src/test/regress`:
```
    pipenv run python upgrade/upgrade_test.py --old-bindir=<old-bindir> --new-bindir=<new-bindir> --postgres-srcdir=<postgres-srcdir>
```

To see full command list:

```
    pipenv run python upgrade/upgrade_test.py -help
```


