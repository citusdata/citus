
# How to trigger hammerdb benchmark jobs

You can trigger two types of hammerdb benchmark jobs:
-ch_benchmark (analytical and transactional queries)
-tpcc_bnehcmark (only transactional queries)

If need to prepend `ch_benchmark/` or `tpcc_benchmark/` prefix to your branch to trigger these jobs
relatively.

You will see the results in [https://github.com/citusdata/release-test-results](https://github.com/citusdata/release-test-results).

**You will need to delete the resource groups manually from portal, the resource groups are `citusbot_ch_benchmark_rg` and `citusbot_tpcc_benchmark_rg`.**

These jobs use the hammerdb tool under the hood, please see [https://github.com/citusdata/test-automation#running-automated-hammerdb-benchmark](https://github.com/citusdata/test-automation#running-automated-hammerdb-benchmark) for more details.
