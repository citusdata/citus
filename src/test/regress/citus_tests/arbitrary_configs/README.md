# Arbitrary Configs

## Usage

To run tests in parallel use:

```bash
# will run 4 configs in parallel
make check-arbitrary-configs parallel=4
```

To run tests sequentially use:

```bash
make check-arbitrary-configs parallel=1
```

To run only some configs:

```bash
# Config names should be comma separated
make check-arbitrary-configs CONFIGS=CitusSingleNodeClusterConfig,CitusSmallSharedPoolSizeConfig
```

To run only some test files with some config:

```bash
make check-arbitrary-base CONFIGS=CitusSingleNodeClusterConfig EXTRA_TESTS=dropped_columns_1
```

To get a deterministic run, you can give the random's seed:

```bash
make check-arbitrary-configs parallel=4 seed=12312
```

The `seed` will be in the output of the run.

## General Info

In our regular regression tests, we can see all the details about either planning or execution but this means
we need to run the same query under different configs/cluster setups again and again, which is not really maintanable.

When we don't care about the internals of how planning/execution is done but the correctness, especially with different configs
this infrastructure can be used.

With `check-arbitrary-configs` target, the following happens:

-   a bunch of configs are loaded, which are defined in `config.py`. These configs have different settings such as different shard count, different citus settings, postgres settings, worker amount, or different metadata.
-   For each config, a separate data directory is created for tests in `tmp_citus_test` with the config's name.
-   For each config, `create_schedule` is run on the coordinator to setup the necessary tables.
-   For each config, `sql_schedule` is run. `sql_schedule` is run on the coordinator if it is a non-mx cluster. And if it is mx, it is either run on the coordinator or a random worker.
-   Tests results are checked if they match with the expected.

When tests results don't match, you can see the regression diffs in a config's datadir, such as `tmp_citus_tests/dataCitusSingleNodeClusterConfig`.

We also have a PostgresConfig which runs all the test suite with Postgres.
By default configs use regular user, but we have a config to run as a superuser as well.

So the infrastructure tests:

-   Postgres vs Citus
-   Mx vs Non-Mx
-   Superuser vs regular user
-   Arbitrary Citus configs

## Adding a new test

When you want to add a new test, you can add the create statements to `create_schedule` and add the sql queries to `sql_schedule`.
If you are adding Citus UDFs that should be a NO-OP for Postgres, make sure to override the UDFs in `postgres.sql`.

If the test needs to be skipped in some configs, you can do that by adding the test names in the `skip_tests` array for
each config. The test files associated with the skipped test will be set to empty so the test will pass without the actual test
being run.

## Adding a new config

You can add your new config to `config.py`. Make sure to extend either `CitusDefaultClusterConfig` or `CitusMXBaseClusterConfig`.

## Debugging failures

On the CI, upon a failure, all logfiles will be uploaded as artifacts, so you can check the artifacts tab.
All the regressions will be shown as part of the job on CI.

In your local, you can check the regression diffs in config's datadirs as in `tmp_citus_tests/dataCitusSingleNodeClusterConfig`.
