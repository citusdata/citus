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
use one of the following commands to do so:
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

## Upgrade testing
See [`src/test/regress/upgrade/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/upgrade/README.md)

## Failure testing

See [`src/test/regress/mitmscripts/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/mitmscripts/README.md)

## Perl test setup script

To automatically setup a citus cluster in tests we use our
`src/test/regress/pg_regress_multi.pl` script. This sets up a citus cluster and
then starts the standard postgres test tooling. You almost never have to change
this file.
