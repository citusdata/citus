#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh


cd src/test/regress

# 1. Find all *.sql and *.spec files in the sql, and spec directories
# 2. Strip the extension and the directory
# 3. Ignore names that end with .include, those files are meant to be in an C
#    preprocessor #include statement. They should not be in schedules.
test_names=$(
    find sql spec -iname "*.sql" -o -iname "*.spec" |
    sed -E 's#^\w+/([^/]+)\.[^.]+$#\1#g' |
    grep -v '.include$'
)
for name in $test_names; do
    if ! grep "\\b$name\\b" ./*_schedule > /dev/null; then
        echo "ERROR: Test with name \"$name\" is not used in any of the schedule files"
        exit 1
    fi
done
