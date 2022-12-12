# How to fix flaky tests

Flaky tests happen when for some reason our tests return non-deterministic
results.

There are three different causes of flaky tests:
1. Tests that don't make sure output is consistent, i.e. a bug in our tests
2. Bugs in our testing infrastructure
3. Bugs in Citus itself

All of these impact the happiness and productivity of our developers, because we
have to rerun tests to make them pass. But apart from developer happiness and
productivity, 3 also impacts our users, and by ignoring flaky tests we can miss
problems that our users could run into. This reduces the effectiveness of our
tests.


## Reproducing a flaky test

### 1. Reproduce a flaky test in CI
Before trying to fix the flakyness, it's important that you can reproduce the
flaky test. Often it only reproduces in CI, so we have a CI job that can help
you reproduce flakyness consistently by running the same test a lot of times.
You can configure CI to run this job by setting the `flaky_test`

```diff
   flaky_test:
     type: string
-    default: ''
+    default: 'isolation_shard_rebalancer_progress'
```

Once you get this job to consistently fail in CI, you can continue with the next
steps to make it instead consistently pass. If the failure doesn't reproduce
with this CI job, it's almost certainly caused by running it concurrently with
other tests. See the "Don't run test in parallel with others" section below on
how to fix that.

### 2. Reproduce a flaky test in local environment
To reproduce the flaky tests on your local environment, you can use `run-test.py [test_name]`
script like below.

```bash
src/test/regress/citus_tests/run_test.py isolation_shard_rebalancer_progress -r 1000 --use-base-schedule --use-whole-schedule-line
```

The script above will try to run the whole line in the schedule file containing the test name by using the related base_schedule (rather than a minimal_schedule), 1000 times.

## Easy fixes

The following types of issues all fall within the category 1: bugs in our tests.

### Expected records but different order

**Issue**: A query returns the right result, but they are in a different order
than expected by the output.

**Fix**: Add an extra column to the ORDER BY clause of the query to make the
output consistent

**Example**
```diff
  8970008 | colocated_dist_table                   | -2147483648   | 2147483647    | localhost |    57637
  8970009 | colocated_partitioned_table            | -2147483648   | 2147483647    | localhost |    57637
  8970010 | colocated_partitioned_table_2020_01_01 | -2147483648   | 2147483647    | localhost |    57637
- 8970011 | reference_table                        |               |               | localhost |    57637
  8970011 | reference_table                        |               |               | localhost |    57638
+ 8970011 | reference_table                        |               |               | localhost |    57637
 (13 rows)
```

**Example fix**:

```diff
-ORDER BY logicalrelid, shardminvalue::BIGINT;
+ORDER BY logicalrelid, shardminvalue::BIGINT, nodeport;
```

### Expected logs but different order

**Issue**: The logs in the regress output are displayed in a different order
than what the output file shows

**Fix**: It's simple: don't log these things during the test. There are two common
ways of achieving this:
1. If you don't care about the logs for this query at all, then you can change
   the log `VERBOSITY` or lower `client_min_messages`.
2. If these are logs of uninteresting commands created by
   `citus.log_remote_commands`, but you care about some of the other remote
   commands being as expected, then you can use `citus.grep_remote_commands` to
   only display the commands that you care about.

**Example of issue 1**:
```diff
select alter_table_set_access_method('ref','heap');
 NOTICE:  creating a new table for alter_table_set_access_method.ref
 NOTICE:  moving the data of alter_table_set_access_method.ref
 NOTICE:  dropping the old alter_table_set_access_method.ref
 NOTICE:  drop cascades to 2 other objects
-DETAIL:  drop cascades to materialized view m_ref
-drop cascades to view v_ref
+DETAIL:  drop cascades to view v_ref
+drop cascades to materialized view m_ref
 CONTEXT:  SQL statement "DROP TABLE alter_table_set_access_method.ref CASCADE"
 NOTICE:  renaming the new table to alter_table_set_access_method.ref
  alter_table_set_access_method
 -------------------------------

 (1 row)
```

**Example fix of issue 1**
```diff
+\set VERBOSITY terse
```

**Example of issue 2**
```diff
SET citus.log_remote_commands TO ON;
 -- should propagate to all workers because no table is specified
 ANALYZE;
 NOTICE:  issuing BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;SELECT assign_distributed_transaction_id(0, 3461, '2022-08-19 01:56:06.35816-07');
 DETAIL:  on server postgres@localhost:57637 connectionId: 1
 NOTICE:  issuing BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;SELECT assign_distributed_transaction_id(0, 3461, '2022-08-19 01:56:06.35816-07');
 DETAIL:  on server postgres@localhost:57638 connectionId: 2
 NOTICE:  issuing SET citus.enable_ddl_propagation TO 'off'
 DETAIL:  on server postgres@localhost:57637 connectionId: 1
-NOTICE:  issuing SET citus.enable_ddl_propagation TO 'off'
-DETAIL:  on server postgres@localhost:xxxxx connectionId: xxxxxxx
 NOTICE:  issuing ANALYZE
 DETAIL:  on server postgres@localhost:57637 connectionId: 1
+NOTICE:  issuing SET citus.enable_ddl_propagation TO 'off'
+DETAIL:  on server postgres@localhost:57638 connectionId: 2
 NOTICE:  issuing ANALYZE
 DETAIL:  on server postgres@localhost:57638 connectionId: 2
```

**Example fix of issue 2**
```diff
 SET citus.log_remote_commands TO ON;
+SET citus.grep_remote_commands = '%ANALYZE%';
```

### Isolation test completes in different order

**Issue**: There's no defined order in which the steps in two different sessions
complete, because they don't block each other. This can happen when two sessions
were both blocked by a third session, but when the third session releases the
lock the first two can both continue.

**Fix**: Use the isolation test ["marker" feature][marker-feature] to make sure
one step can only complete after another has completed.

[marker-feature]: https://github.com/postgres/postgres/blob/c68a1839902daeb42cf1ebc89edfdd91c00e5091/src/test/isolation/README#L163-L188


**Example**
```diff
-step s1-shard-move-c1-block-writes: <... completed>
+step s4-shard-move-sep-block-writes: <... completed>
 citus_move_shard_placement
 --------------------------

 (1 row)

-step s4-shard-move-sep-block-writes: <... completed>
+step s1-shard-move-c1-block-writes: <... completed>
 citus_move_shard_placement
 --------------------------
```

**Example fix**
```diff
+permutation ... "s1-shard-move-c1-block-writes" "s4-shard-move-sep-block-writes" ...
+permutation ... "s1-shard-move-c1-block-writes" "s4-shard-move-sep-block-writes"("s1-shard-move-c1-block-writes") ...
```

### Disk size numbers are not exactly like expected

**Issue**: In some tests we show the disk size of a table, but due to various
postgres background processes such as vacuuming these sizes can change slightly.

**Fix**: Expect a certain range of disk sizes instead of a specific one.

**Example**
```diff
 VACUUM (INDEX_CLEANUP ON, PARALLEL 1) local_vacuum_table;
 SELECT pg_size_pretty( pg_total_relation_size('local_vacuum_table') );
  pg_size_pretty
 ----------------
- 21 MB
+ 22 MB
 (1 row)
```

**Example fix**
```diff
-SELECT pg_size_pretty( pg_total_relation_size('local_vacuum_table') );
- pg_size_pretty
+SELECT CASE WHEN s BETWEEN 20000000 AND 25000000 THEN 22500000 ELSE s END
+FROM pg_total_relation_size('local_vacuum_table') s ;
+    s
 ---------------------------------------------------------------------
- 21 MB
+ 22500000
```


## Isolation test flakyness

If the flaky test is an isolation test, first read the Postgres docs on dealing
with [race conditions in isolation tests][pg-isolation-docs]. A common example
was already listed above, but the Postgres docs list some other types too and
explain how to make their output consistent.

[pg-isolation-docs]: https://github.com/postgres/postgres/blob/c68a1839902daeb42cf1ebc89edfdd91c00e5091/src/test/isolation/README#L152


## Ruling out common sources of randomness as the cause

If it's none of the above, then probably the reason why the test is flaky is not
immediately obvious. There are a few things that can introduce randomness into
our test suite. To keep your sanity while investigating, it's good to rule these
out as the cause (or even better determine that they are the cause).

### Don't run test in parallel with others

Check in the schedule if the test is run in parallel with others. If it is,
remove it from there and check if it's still flaky.

**Example**
```diff
 test: multi_partitioning_utils replicated_partitioned_table
-test: multi_partitioning partitioning_issue_3970
+test: multi_partitioning
+test: partitioning_issue_3970
 test: drop_partitioned_table
```

### Use a fixed number of connections

The adaptive executor of Citus sometimes opens extra connections to do stuff in
parallel to speed up multi-shard queries. This happens especially in CI, because
CI machines are sometimes slow. There are two ways to get a consistent number of
connections:

1. Use `citus.max_adaptive_executor_pool_size` to limit the connections
2. Use `citus.force_max_query_parallelization` to always open the maximum number
   of connections.

**Example**
```diff
 ALTER TABLE dist_partitioned_table ADD CONSTRAINT constraint1 UNIQUE (dist_col, partition_col);
+ERROR:  canceling the transaction since it was involved in a distributed deadlock
```

**Example of fix 1**
```diff
+SET citus.max_adaptive_executor_pool_size TO 1;
 ALTER TABLE dist_partitioned_table ADD CONSTRAINT constraint1 UNIQUE (dist_col, partition_col);
+RESET citus.max_adaptive_executor_pool_size;
```

**Example of fix 2**
```diff
+SET citus.force_max_query_parallelization TO 1;
 ALTER TABLE dist_partitioned_table ADD CONSTRAINT constraint1 UNIQUE (dist_col, partition_col);
+RESET citus.force_max_query_parallelization;
```

IMPORTANT: If this helps, this could very well indicate a bug. Check with
senior/principal engineers if it's expected that it helps in this case.


## What to do if this all doesn't work?

If none of the advice above worked, the first thing to try is read the failing
test in detail and try to understand how it works. Often, with a bit of thinking
you can figure out why it's failing in the way that it's failing. If you cannot
figure it out yourself, it's good to ask senior/principal engineers, maybe they
can think of the reason. Or maybe they're certain that it's an actual bug.

### What to do when you cannot fix or find the bug?

If it turns out to be an actual bug in Citus, but fixing the bug (or finding its
cause) is hard, making the test output consistent is already an improvement over
the status quo. Be sure to create an issue though for the bug. Even if you're
not entirely sure what's causing it you can still create an issue describing how
to reproduce the flakiness.


## What to do if output can never be consistent?

There are still a few ways to make our test suite less flaky, even if you
figured out that the output that Postgres gives can never be made consistent.

### Normalizing random output

If for some reason you cannot make consistent output then our
[`normalize.sed`][normalize] might come to the rescue. This allows us to
normalize certain lines to one specific output.

**Example**
```diff
-CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s.
+CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.02 s.
```

**Fix by changing inconsistent parts of line**
```sed
# ignore timing statistics for VACUUM VERBOSE
s/CPU: user: .*s, system: .*s, elapsed: .*s/CPU: user: X.XX s, system: X.XX s, elapsed: X.XX s/
```

**Fix by completely removing line**
```sed
# ignore timing statistics for VACUUM VERBOSE
/CPU: user: .*s, system: .*s, elapsed: .*s/d
```

[normalize]:
https://github.com/citusdata/citus/blob/main/src/test/regress/bin/normalize.sed

### Removing the flaky test

Sometimes removing the test is the only way to make our test suite less flaky.
Of course this is a last resort, but sometimes it's what we want. If running the
test does more bad than good, removing will be a net positive.


## PR descriptions of flaky tests

Even if a fix for a flaky test is very simple without a clear description it can
be hard for a reviewer (or a future git spelunker) to understand its purpose.
A good PR description of a flaky test includes the following things:
1. Name of the test that was flaky
2. The part of the regression.diffs file that caused the test to fail randomly
3. A link to a CI run that failed because of this flaky test
4. Explanation of why this output was non-deterministic (was it a bug in Citus?)
5. Explanation of how this change makes the test deterministic

An example of such a PR description is this one from [#6272][6272]:

[6272]: https://github.com/citusdata/citus/pull/6272

> Sometimes in CI our multi_utilities test fails like this:
> ```diff
>  VACUUM (INDEX_CLEANUP ON, PARALLEL 1) local_vacuum_table;
>  SELECT CASE WHEN s BETWEEN 20000000 AND 25000000 THEN 22500000 ELSE s END size
>  FROM pg_total_relation_size('local_vacuum_table') s ;
>     size
>  ----------
> - 22500000
> + 39518208
>  (1 row)
> ```
> Source: https://app.circleci.com/pipelines/github/citusdata/citus/26641/workflows/5caea99c-9f58-4baa-839a-805aea714628/jobs/762870
>
> Apparently VACUUM is not as reliable in cleaning up as we thought. This
> PR increases the range of allowed values to make the test reliable. Important
> to note is that the range is still completely outside of the allowed range of
> the initial size. So we know for sure that some data was cleaned up.
