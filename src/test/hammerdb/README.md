
# How to trigger hammerdb benchmark jobs

You can trigger two types of hammerdb benchmark jobs:
-ch_benchmark (analytical and transactional queries)
-tpcc_benchmark (only transactional queries)

Your branch will be run against `master` branch.

In order to trigger the jobs prepend `ch_benchmark/` or `tpcc_benchmark/` to your branch and push it.

For example if you were running on a feature/improvement branch with name `improve/adaptive_executor`. In order to trigger a tpcc benchmark, you can do the following:

```bash
git checkout improve/adaptive_executor
git checkout -b tpcc_benchmark/improve/adaptive_executor
git push origin tpcc_benchmark/improve/adaptive_executor # the tpcc benchmark job will be triggered.
```

You will see the results in a branch in [https://github.com/citusdata/release-test-results](https://github.com/citusdata/release-test-results).

The branch name will be something like: `citus_github_push/citusbot_tpcc_benchmark_rg/<date>/<date>`.

On success, which is the vast majority of the cases, the resource groups on Azure which are automatically created for this purpose are deleted automatically. On failure, you need to manually drop the resource groups named: `citusbot_ch_benchmark_rg` and `citusbot_tpcc_benchmark_rg`. To check whether the job failed, go to Azure portal and search for these resource group names. If the resource group doesn't have any virtual machines or the virtual machines don't have any CPU/Memory load at all, it is highly possible that the job failed. Delete the resource groups and re-try with another branch.

These jobs use the hammerdb tool under the hood, please see [https://github.com/citusdata/test-automation#running-automated-hammerdb-benchmark](https://github.com/citusdata/test-automation#running-automated-hammerdb-benchmark) for more details.
