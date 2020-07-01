# CI scripts

We have a few scripts that we run in CI to confirm that code confirms to our
standards. Be sure you have followed the setup in the [Following our coding
conventions](https://github.com/citusdata/citus/blob/master/CONTRIBUTING.md#following-our-coding-conventions)
section of `CONTRIBUTING.md`. Once you've done that, most of them should be
fixed automatically, when running:
```
make reindent
```

See the sections below for details on what a specific failing script means.

## `citus_indent`

We format all our code using the coding conventions in the
[citus_indent](https://github.com/citusdata/tools/tree/develop/uncrustify)
tool. This tool uses `uncrustify` under the hood. See [Following our coding
conventions](https://github.com/citusdata/citus/blob/master/CONTRIBUTING.md#following-our-coding-conventions) on how to install this.

## `editorconfig.sh`

You should install the Editorconfig plugin for your editor/IDE
https://editorconfig.org/

## `banned.h.sh`

You're using a C library function that is banned by Microsoft, mostly because of
risk for buffer overflows. This page lists the Microsoft suggested replacements:
https://liquid.microsoft.com/Web/Object/Read/ms.security/Requirements/Microsoft.Security.SystemsADM.10082#guide
These replacements are only available on Windows normally. Since we build for
Linux we make most of them available with this header file:
```c
#include "distributed/citus_safe_lib.h"
```
This uses https://github.com/intel/safestringlib to provide them.

However, still not all of them are available. For those cases we provide
some extra functions in `citus_safe_lib.h`, with similar functionality.

If none of those replacements match your requirements you have to do one of the
following:
1. Add a replacement to `citus_safe_lib.{c,h}` that handles the same error cases
   that the `{func_name}_s` function that Microsoft suggests.
2. Add a `/* IGNORE-BANNED */` comment to the line that complains. Doing this
   requires also adding a comment before explaining why this specific use of the
   function is safe.

## `check_enterprise_merge.sh`

When you open a PR on community, if it creates a conflict with
enterprise-master, the check-merge-to-enterprise will fail. Say your branch name
is `$PR_BRANCH`, we will refer to `$PR_BRANCH` on community as
`community/$PR_BRANCH` and on enterprise as `enterprise/$PR_BRANCH`. If the
job already passes, you are done, nothing further required! Otherwise follow the
below steps. First make sure these two things are the case:

1. Get approval from your reviewer for `community/$PR_BRANCH`. Only follow the
   next steps after you are about to merge the branch to community master.
2. Make sure your commits are in a nice state, since you should not do
   "squash and merge" on Github later. Otherwise you will certainly get
   duplicate commits and possibly get merge conflicts with enterprise again.

Once that's done, you need to create a merged version of your PR branch on the
enterprise repo. For example if `community` is added as a remote in
your enterprise repo, you can do the following:

```bash
export PR_BRANCH=<YOUR BRANCHNAME OF THE PR HERE>
git checkout enterprise-master
git pull # Make sure your local enterprise-master is up to date
git fetch community # Fetch your up to date branch name
git checkout -b "$PR_BRANCH" enterprise-master
```
Now you have X in your enterprise repo, which we refer to as
`enterprise/$PR_BRANCH` (even though in git commands you would reference it as
`origin/$PR_BRANCH`). This branch is currently the same as `enterprise-master`.
First to make review easier, you should merge community master into it. This
should apply without any merge conflicts:

```bash
git merge community/master
```
Now you need to merge `community/$PR_BRANCH` to `enterprise/$PR_BRANCH`. Solve
any conflicts and make sure to remove any parts that should not be in enterprise
even though it doesn't have a conflict, on enterprise repository:

```bash
git merge "community/$PR_BRANCH"
```

1. You should push this branch to the enterprise repo. This is so that the job
   on community will see this branch.
2. Wait until tests on `enterprise/$PR_BRANCH` pass.
3. Create a PR on the enterprise repo for your `enterprise/$PR_BRANCH` branch.
4. You should get approval for the merge conflict changes on
   `enterprise/$PR_BRANCH`, preferably from the same reviewer as they are
   familiar with the change.
5. You should rerun the `check-merge-to-enterprise` check on
   `community/$PR_BRANCH`. You can use re-run from failed option in circle CI.
6. You can now merge the PR on community. Be sure to NOT use "squash and merge",
   but instead use the regular "merge commit" mode.
7. You can now merge the PR on enterprise. Be sure to NOT use "squash and merge",
   but instead use the regular "merge commit" mode.

The subsequent PRs on community will be able to pass the
`check-merge-to-enterprise` check as long as they don't have a conflict with
`enterprise-master`.

## `check_sql_snapshots.sh`

To allow for better diffs during review we have snapshots of SQL UDFs. This
means that `latest.sql` is not up to date with the SQL file of the highest
version number in the directory. The output of the script shows you what is
different.

## `check_all_tests_are_run.sh`

A test should always be included in a schedule file, otherwise it will not be
run in CI. This is most commonly forgotten for newly added tests. In that case
the dev ran it locally without running a full schedule with something like:
```bash
make -C src/test/regress/ check-minimal EXTRA_TESTS='multi_create_table_new_features'
```

## `check_all_ci_scripts_are_run.sh`

This is the meta CI script. This checks that all existing CI scripts are
actually run in CI. This is most commonly forgotten for newly added CI tests
that the developer only ran locally. It also checks that all CI scripts have a
section in this `README.md` file and that they include `ci/ci_helpers.sh`.

## `disallow_c_comments_in_migrations.sh`

We do not use C-style comments in migration files as the stripped
zero-length migration files cause warning during packaging.
Instead use SQL type comments, i.e:
```
-- this is a comment
```
See [#3115](https://github.com/citusdata/citus/pull/3115) for more info.


## `disallow_long_changelog_entries.sh`

Having changelog items with entries that are longer than 80 characters are
forbidden. It's allowed to split up the entry over multiple lines, as long as
each line of the entry is 80 characters or less.

## `normalize_expected.sh`

All files in `src/test/expected` should be committed in normalized form.
This error mostly happens if someone added a new normalization rule and you have
not rerun tests that you have added.

We normalize the test output files using a `sed` script called
[`normalize.sed`](https://github.com/citusdata/citus/blob/master/src/test/regress/bin/normalize.sed).
The reason for this is that some output changes randomly in ways we don't care
about. An example of this is when an error happens on a different port number,
or a different worker shard, or a different placement, etc. Either randomly or
because we are running the tests in a slightly different configuration.

## `remove_useless_declarations.sh`

This script tries to make sure that we don't add useless declarations to our
code. What it effectively does is replace this:
```c
int a = 0;
int b = 2;
Assert(b == 2);
a = b + b;
```
With this equivalent, but shorter version:
```c
int b = 2;
Assert(b == 2);
int a = b + b;
```

It relies on the fact that `citus_indent` formats our code in certain ways. So
before running this script, make sure that you've done that.
This replacement is all done using a [regex replace](xkcd.com/1171), so it's
definitely possible there's a bug in there. So far no bad ones have been found.

A known issue is that it does not replace code in a block after an `#ifdef` like
this.
```c
int foo = 0;
#ifdef SOMETHING
foo = 1
#else
foo = 2
#endif
```
This was deemed to be error prone and not worth the effort.
