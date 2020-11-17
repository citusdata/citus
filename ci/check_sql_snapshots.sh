#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh

for udf_dir in src/backend/distributed/sql/udfs/* src/backend/columnar/sql/udfs/*; do
    # We want to find the last snapshotted sql file, to make sure it's the same
    # as "latest.sql". This is done by:
    # 1. Getting the filenames in the UDF directory (using find instead of ls, to keep shellcheck happy)
    # 2. Filter out latest.sql
    # 3. Sort using "version sort"
    # 4. Get the last one using tail
    latest_snapshot=$(\
        find "$udf_dir" -iname "*.sql" -exec basename {} \; \
        | { grep --invert-match latest.sql || true; } \
        | sort --version-sort \
        | tail --lines 1);
    diff --unified --color=auto "$udf_dir/latest.sql" "$udf_dir/$latest_snapshot"; \
done
