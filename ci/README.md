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

## `build-citus.sh`

This is the script used during the build phase of the extension. Historically this script
was embedded in the docker images. This made maintenance a hassle. Now it lives in tree
with the rest of the source code.

When this script fails you most likely have a build error on the postgres version it was
building at the time of the failure. Fix the compile error and push a new version of your
code to fix.

## `check_enterprise_merge.sh`

This check exists to make sure that we can always merge the `master` branch of
`community` into the `enterprise-master` branch of the `enterprise` repo.
There are two conditions in which this check passes:

1. There are no merge conflicts between your PR branch and `enterprise-master` and after this merge the code compiles.
2. There are merge conflicts, but there is a branch with the same name in the
   enterprise repo that:
   1. Contains the last commit of the community branch with the same name.
   2. Merges cleanly into `enterprise-master`
3. After merging, the code can be compiled.

If the job already passes, you are done, nothing further required! Otherwise
follow the below steps.

### Prerequisites

Before continuing with the real steps make sure you have done the following
(this only needs to be done once):
1. You have enabled `git rerere` in globally or in your enterprise repo
   ([docs](https://git-scm.com/docs/git-rerere), [very useful blog](https://medium.com/@porteneuve/fix-conflicts-only-once-with-git-rerere-7d116b2cec67#.3vui844dt)):
   ```bash
   # Enables it globally for all repos
   git config --global rerere.enabled true
   # Enables it only for the enterprise repo
   cd <enterprise-repo>
   git config rerere.enabled true
   ```
2. You have set up the `community` remote on your enterprise as
   [described in CONTRIBUTING.md](https://github.com/citusdata/citus-enterprise/blob/enterprise-master/CONTRIBUTING.md#merging-community-changes-onto-enterprise).


#### Important notes on `git rerere`

This is very useful as it will make sure git will automatically redo merges that
you have done before. However, this has a downside too. It will also redo merges
that you did, but that were incorrect. Two work around this you can use these
commands.
1. Make `git rerere` forget a merge:
   ```bash
   git rerere forget <badly_merged_file>
   ```
2. During conflict resolution where `git rerere` already applied the bad merge,
   simply forgetting it is not enough. Since it is already applied. In that case
   you also have to undo the apply using:
   ```bash
   git checkout --conflict=merge <badly_merged_file>
   ```

### Actual steps

After the prerequisites are met we continue on to the real steps. Say your
branch name is `$PR_BRANCH`, we will refer to `$PR_BRANCH` on community as
`community/$PR_BRANCH` and on enterprise as `enterprise/$PR_BRANCH`. First make
sure these two things are the case:

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

### What to do when your branch got outdated?

So there's one issue that can occur. Your branch will become outdated with
master and you have to make it up to date. There are two ways to do this using
`git merge` or `git rebase`. As usual, `git merge` is a bit easier than `git
rebase`, but clutters git history. This section will explain both. If you don't
know which one makes the most sense, start with `git rebase`. It's possible that
for whatever reason this doesn't work or becomes very complex, for instance when
new merge conflicts appear. Feel free to fall back to `git merge` in that case,
by using `git rebase --abort`.

#### Updating both branches with `git rebase`

In the community repo, first update the outdated branch using `rebase`:

```bash
git checkout $PR_BRANCH
# Keep a backup in case you want to fallback to the merge approach
git checkout -b ${PR_BRANCH}-backup
git checkout $PR_BRANCH
# Actually update the branch
git fetch origin
git rebase origin/master
git push origin $PR_BRANCH --force-with-lease
```

In the enterprise repo, rebase onto the new community branch with
`--preserve-merges`:

```bash
git checkout $PR_BRANCH
git fetch community
git rebase community/$PR_BRANCH --preserve-merges
```

Automatic merge might have failed with the above command. However, because of
`git rerere` it should have re-applied your original merge resolution. If this
is indeed the case it should show something like this in the output of the
previous command (note the `Resolved ...` line):
```
CONFLICT (content): Merge conflict in <file_path>
Resolved '<file_path>' using previous resolution.
Automatic merge failed; fix conflicts and then commit the result.
Error redoing merge <merge_sha>
```

Confirm that the merge conflict is indeed resolved correctly. In that case you
can do the following:
```bash
# Add files that were conflicting
git add "$(git diff --name-only --diff-filter=U)"
git rebase --continue
```

Before pushing you should do a final check that the commit hash of your final
non merge commit matches the commit hash that's on the community repo. If that's
not the case, you should fallback to the `git merge` approach.
```bash
git reset origin/$PR_BRANCH --hard
```

If the commit hashes were as expected, push the branch:
```bash
git push origin $PR_BRANCH --force-with-lease
```

#### Updating both branches with `git merge`

If you are falling back to the `git merge` approach after trying the
`git rebase` approach, you should first restore the original branch on the
community repo.
```bash
git checkout $PR_BRANCH
git reset ${PR_BRANCH}-backup --hard
git push origin $PR_BRANCH --force-with-lease
```

In the community repo, first update the outdated branch using `merge`:

```bash
git checkout $PR_BRANCH
git fetch origin
git merge origin/master
git push origin $PR_BRANCH
```

In the enterprise repo, merge with the updated `community/$PR_BRANCH`:

```bash
git checkout $PR_BRANCH
git fetch community
git merge community/$PR_BRANCH
git push origin $PR_BRANCH
```

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

## `check_migration_files.sh`

A branch that touches a set of upgrade scripts is also expected to touch
corresponding downgrade scripts as well. If this script fails, read the output
and make sure you update the downgrade scripts in the printed list. If you
really don't need a downgrade to run any SQL. You can write a comment in the
file explaining why a downgrade step is not necessary.

## `disallow_c_comments_in_migrations.sh`

We do not use C-style comments in migration files as the stripped
zero-length migration files cause warning during packaging.
Instead use SQL type comments, i.e:
```
-- this is a comment
```
See [#3115](https://github.com/citusdata/citus/pull/3115) for more info.

## `disallow_hash_comments_in_spec_files.sh`

We do not use comments starting with # in spec files because it creates errors
from C preprocessor that expects directives after this character.
Instead use C type comments, i.e:
```
// this is a single line comment

/*
 * this is a multi line comment
 */
```

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

## `fix_gitignore.sh`

This script checks and fixes issues with `.gitignore` rules:


1. Makes sure we do not commit any generated files that should be ignored. If there is an
   ignored file in the git tree, the user is expected to review the files that are removed
   from the git tree and commit them.

## `check_gucs_are_alphabetically_sorted.sh`

This script checks the order of the GUCs defined in `shared_library_init.c`.
To solve this failure, please check `shared_library_init.c` and make sure that the GUC
definitions are in alphabetical order.

## `print_stack_trace.sh`

This script prints stack traces for failed tests, if they left core files.

## `sort_and_group_includes.sh`

This script checks and fixes issues with include grouping and sorting in C files.

Includes are grouped in the following groups:
 - System includes (eg. `#include <math>`)
 - Postgres.h include (eg. `#include "postgres.h"`)
 - Toplevel postgres includes (includes not in a directory eg. `#include "miscadmin.h`)
 - Postgres includes in a directory (eg. `#include "catalog/pg_type.h"`)
 - Toplevel citus includes (includes not in a directory eg. `#include "pg_version_constants.h"`)
 - Columnar includes (eg. `#include "columnar/columnar.h"`)
 - Distributed includes (eg. `#include "distributed/maintenanced.h"`)

Within every group the include lines are sorted alphabetically.
