
# check-merge-to-enterprise Job

When you open a PR on community, if it creates a conflict with enterprise-master, the check-merge-to-enterprise will fail. Say your branch name is `X`, we will refer to `X` on community as `community/X` and on enterprise as `enterprise/X`. In order to make the job pass:

- You first need to get approval from your reviewer for `community/X`. Only follow the next steps after you are about to merge the branch to community master. Otherwise, the `check-merge-to-enterprise` check might falsely pass. (For example when you added new commits to `community/X` but forgot to update `enterprise/X`).
- You need to synchronize the same branch, `X` locally on enterprise (ideally don't push yet), and merge `enterprise-master` to `enterprise/X`. Solve any conflicts( and make sure to remove any parts that should not be in enterprise even though it doesn't have a conflict), on enterprise repository:

```bash
git checkout enterprise-master
git pull # make sure enterprise-master in your local is up-to-date
git checkout X
git merge enterprise-master
```

- You should push this to enterprise and open a PR so that the job on community will see this branch.
- You should get approval for the merge conflict changes on `enterprise/X`, preferably from the same reviewer as they are familiar with the change.
- You should rerun the `check-merge-to-enterprise` check on `community/X`. You can use re-run from failed option in circle CI.
- You can now merge `community/X` to `community/master`.
- You can merge `enterprise/X` to `enterprise-master`.
- Manually merge `community master` to `enterprise-master`.
- You can use `git checkout --ours <conflicting file names>` for all conflicting files. This will use local/our version to resolve the conflicts.
- Now you can push to `enterprise-master`.

The subsequent PRs on community will be able to pass the `check-merge-to-enterprise` check as long as they don't have a conflict with `enterprise-master`.
