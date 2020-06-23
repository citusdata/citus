# check-merge-to-enterprise Job

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
