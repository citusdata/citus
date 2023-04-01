# How our testing works

We use the test tooling of postgres to run our tests. This tooling is very
simple but effective. The basics it runs a series of `.sql` scripts, gets
their output and stores that in `results/$sqlfilename.out`. It then compares the
actual output to the expected output with a simple `diff` command:

```bash
diff results/$sqlfilename.out expected/$sqlfilename.out
```

## Schedules

Which sql scripts to run is defined in a schedule file, e.g. `multi_schedule`,
`multi_mx_schedule`.

## Makefile

In our `Makefile` we have rules to run the different types of test schedules.
You can run them from the root of the repository like so:

```bash
# e.g. the multi_schedule
make install -j9 && make -C src/test/regress/ check-multi
```

Take a look at the makefile for a list of all the testing targets.

### Running a specific test

Often you want to run a specific test and don't want to run everything. You can
simply use `run_test.py [test_name]` script like below in that case. It detects the test schedule
and make target to run the given test.

```bash
src/test/regress/citus_tests/run_test.py multi_utility_warnings
```
You can pass `--repeat` or `r` parameter to run the given test for multiple times.

```bash
src/test/regress/citus_tests/run_test.py multi_utility_warnings -r 1000
```

To force the script to use base schedules rather than minimal ones, you can
pass `-b` or `--use-base-schedule`.

```bash
src/test/regress/citus_tests/run_test.py coordinator_shouldhaveshards -r 1000 --use-base-schedule
```

If you would like to run a specific test on a certain target you can use one
of the following commands to do so:

```bash
# If your tests needs almost no setup you can use check-minimal
make install -j9 && make -C src/test/regress/ check-minimal EXTRA_TESTS='multi_utility_warnings'
# Often tests need some testing data, if you get missing table errors using
# check-minimal you should try check-base
make install -j9 && make -C src/test/regress/ check-base EXTRA_TESTS='with_prepare'
# Sometimes this is still not enough and some other test needs to be run before
# the test you want to run. You can do so by adding it to EXTRA_TESTS too.
make install -j9 && make -C src/test/regress/ check-base EXTRA_TESTS='add_coordinator coordinator_shouldhaveshards'
```


## Normalization

The output of tests is sadly not completely predictable. Still we want to
compare the output of different runs and error when the important things are
different. We do this by not using the regular system `diff` to compare files.
Instead we use `src/test/regress/bin/diff` which does the following things:

1. Change the `$sqlfilename.out` file by running it through `sed` using the
   `src/test/regress/bin/normalize.sed` file. This does stuff like replacing
   numbers that keep changing across runs with an `XXX` string, e.g. portnumbers
   or transaction numbers.
2. Backup the original output to `$sqlfilename.out.unmodified` in case it's
   needed for debugging
3. Compare the changed `results` and `expected` files with the system `diff`
   command.

## Updating the expected test output

Sometimes you add a test to an existing file, or test output changes in a way
that's not bad (possibly even good if support for queries is added). In those
cases you want to update the expected test output.
The way to do this is very simple, you run the test and copy the new .out file
in the `results` directory to the `expected` directory, e.g.:

```bash
make install -j9 && make -C src/test/regress/ check-minimal EXTRA_TESTS='multi_utility_warnings'
cp src/test/regress/{results,expected}/multi_utility_warnings.out
```

## Adding a new test file

Adding a new test file is quite simple:

1. Write the SQL file in the `sql` directory
2. Add it to a schedule file, to make sure it's run in CI
3. Run the test
4. Check that the output is as expected
5. Copy the `.out` file from `results` to `expected`

## Isolation testing

See [`src/test/regress/spec/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/spec/README.md)

## Pytest testing

See [`src/test/regress/citus_tests/test/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/citus_tests/test/README.md)

## Upgrade testing

See [`src/test/regress/citus_tests/upgrade/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/citus_tests/upgrade/README.md)

## Arbitrary configs testing

See [`src/test/regress/citus_tests/arbitrary_configs/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/citus_tests/arbitrary_configsupgrade/README.md)

## Failure testing

See [`src/test/regress/mitmscripts/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/mitmscripts/README.md)

## Perl test setup script

To automatically setup a citus cluster in tests we use our
`src/test/regress/pg_regress_multi.pl` script. This sets up a citus cluster and
then starts the standard postgres test tooling. You almost never have to change
this file.

## Handling different test outputs

Sometimes the test output changes because we run tests in different configurations.
The most common example is an output that changes in different Postgres versions.
We highly encourage to find a way to avoid these test outputs.
You can try the following, if applicable to the changing output:
- Change the test such that you still test what you want, but you avoid the different test outputs.
- Reduce the test verbosity via: `\set VERBOSITY terse`, `SET client_min_messages TO error`, etc
- Drop the specific test lines altogether, if the test is not critical.
- Use utility functions that modify the output to your preference,
like [coordinator_plan](https://github.com/citusdata/citus/blob/main/src/test/regress/sql/multi_test_helpers.sql#L23),
which modifies EXPLAIN output
- Add [a normalization rule](https://github.com/citusdata/citus/blob/main/ci/README.md#normalize_expectedsh)

Alternative test output files are highly discouraged, so only add one when strictly necessary.
In order to maintain a clean test suite, make sure to explain why it has an alternative
output in the test header, and when we can drop the alternative output file in the future.

For example:

```sql
--
-- MULTI_INSERT_SELECT
--
-- This test file has an alternative output because of the change in the
-- display of SQL-standard function's arguments in INSERT/SELECT in PG15.
-- The alternative output can be deleted when we drop support for PG14
--
```
Including important keywords, like "PG14", "PG15", "alternative output" will
help cleaning up in the future.

## Randomly failing tests

In CI sometimes a test fails randomly, we call these tests "flaky". To fix these
flaky tests see [`src/test/regress/flaky_tests.md`](https://github.com/citusdata/citus/blob/main/src/test/regress/flaky_tests.md)
