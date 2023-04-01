# Pytest based tests

## Usage

Run all tests in parallel:

```bash
pytest -n auto
```

Run all tests sequentially:
```bash
pytest
```

Run a specific test:
```bash
pytest test/test_columnar.py::test_recovery
```

Run a specific test file in parallel:
```bash
pytest -n auto test/test_columnar.py
```

Run any test that contains a certain string in the name:
```bash
pytest -k recovery
```

Run tests without it capturing stdout/stderr. This can be useful to see the
logs of a passing test:
```bash
pytest -s test/test_columnar.py::test_recovery
```

## General info

Our other tests work by comparing output of a sequence of SQL commands that's
executed by `psql` to an expected output. If there's a difference between the
expected and actual output, then the tests fails. This works fine for many
cases, but certain types of tests are hard to write and a lot of care usually
has to be taken to make sure output is completely identical in every run.

The tests in this directory use a different approach and use
[`pytest`][pytest-docs] to run tests that are written in the Python programming
language. This idea is similar to TAP tests that are part of Postgres, with the
important difference that those are written in Perl.

In the sections below you can find most stuff you'll need to know about
`pytest` to run and write such tests, but if you want more detailed info some
useful references are:
- [A blog with pytest tips and tricks][pytest-tips]
- [The official pytest docs][pytest-docs]

[pytest-docs]: https://docs.pytest.org/en/stable/
[pytest-tips]: https://pythontest.com/pytest-tips-tricks/

## Adding a new test

Tests are automatically discovered by `pytest` using a simple but effective
heuristic. In this directory (`src/test/regress/citus_tests/test`) it finds
all of the files that are named `test_{some name}.py`. Those files
are then searched for function names starting with the `test_` prefix. All those
functions are considered tests by `pytest`.


### Fixtures aka Dependency Injection aka Teardown/Cleanup

An important part of tests is that they have some dependencies. The most
important dependency for us is usually a running Citus cluster. These
dependencies are provided by what `pytest` calls [fixtures]. Fixtures are
functions that `yield` a value. Anything before the `yield` is done during setup
and anything after the yield is done during teardown of the test (or whole
session). All our fixtures are defined in `conftest.py`.


Using a fixture in a test is very easy, but looks like a lot of magic. All you
have to do is make sure your test function has an argument with the same name as
the name of the fixture. For example:

```python
def test_some_query(cluster):
    cluster.coordinator.sql("SELECT 1")
```

If you need a cluster of a specific size you can use the `cluster_factory`
fixture:
```python
def test_with_100_workers(cluster_factory):
    cluster = cluster_factory(100)
```

If you want more details on how fixtures work a few useful pages of the pytest
docs are:
- [About fixtures][fixtures]
- [How to use fixtures][fixtures-how-to]
- [Fixtures reference][fixtures-reference]

[fixtures]: https://docs.pytest.org/en/stable/explanation/fixtures.html
[fixtures-how-to]: https://docs.pytest.org/en/stable/how-to/fixtures.html
[fixtures-reference]: https://docs.pytest.org/en/stable/reference/fixtures.html
## Connecting to a test postgres

Sometimes your test is failing in an unexpected way and the easiest way to find
out why is to connect to Postgres at a certain point interactively.

### Using `psql_debug`
The easiest way is to use the `psql_debug()` method of your `Cluster` or
`Postgres` instance.
```python
def test_something(cluster):
    # working stuff

    cluster.coordinator.psql_debug()

    # unexpectedly failing test
```

Then run this test with stdout/stderr capturing disabled (`-s`) and it will show
you an interactive `psql` prompt right at that point in the test:
```bash
$ pytest -s test/test_your_thing.py::test_something

...

psql (15.2)
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off)
Type "help" for help.

127.0.0.1 postgres@postgres:10201-20016=
> select 1;

```


### Debug manually

Sometimes you need to connect to more than one node though. For that you can use
a `Cluster` its `debug` method instead.

```python
def test_something(cluster):
    # working stuff

    cluster.debug()

    # unexpectedly failing test
```


Then run this test with stdout/stderr capturing disabled (`-s`) and it will show
you the connection string for each of the nodes in the cluster:
```bash
$ PG_FORCE_PORTS=true pytest -s test/test_your_thing.py::test_something
...

The nodes in this cluster and their connection strings are:
/tmp/pytest-of-jelte/pytest-752/cluster2-0/coordinator:
    "host=127.0.0.1 port=10202 dbname=postgres user=postgres options='-c search_path=test_recovery' connect_timeout=3 client_encoding=UTF8"
/tmp/pytest-of-jelte/pytest-752/cluster2-0/worker0:
    "host=127.0.0.1 port=10203 dbname=postgres user=postgres options='-c search_path=test_recovery' connect_timeout=3 client_encoding=UTF8"
/tmp/pytest-of-jelte/pytest-752/cluster2-0/worker1:
    "host=127.0.0.1 port=10204 dbname=postgres user=postgres options='-c search_path=test_recovery' connect_timeout=3 client_encoding=UTF8"
Press Enter to continue running the test...
```

Then in another terminal you can manually connect to as many of them as you want.
Using `PG_FORCE_PORTS` is recommended here, to make sure that the ports will
stay the same across runs of the tests. That way you can reuse the connection
strings that you got from a previous run, if you need to debug again.
