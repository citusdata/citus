#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh
cd src/test/regress

for test_dir in sql spec; do
    test_names=$(find $test_dir -iname '*.sql' | sed "s#^$test_dir/\\(.*\\)\\.\\(sql\\|spec\\)\$#\\1#g")
    for name in $test_names; do
        if ! grep "\\b$name\\b" ./*_schedule > /dev/null; then
            echo "ERROR: Test with name \"$name\" is not used in any of the schedule files"
            exit 1
        fi
    done
done
