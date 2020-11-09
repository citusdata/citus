#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh


cd src/test/regress

# 1. Find all *.sql *.spec and *.source files in the sql, spec and input
#    directories
# 2. Strip the extension and the directory
# 3. Ignore names that end with .include, those files are meant to be in an C
#    preprocessor #include statement. They should not be in schedules.
test_names=$(
    find sql spec input -iname "*.sql" -o -iname "*.spec" -o -iname "*.source" |
    sed -E 's#^\w+/([^/]+)\.[^.]+$#\1#g' |
    grep -v '.include$'
)
code=0
for name in $test_names; do
    if ! grep "\\b$name\\b" ./*_schedule > /dev/null; then
        echo "ERROR: Test with name \"$name\" is not used in any of the schedule files"
        code=1
    fi
done
exit $code
