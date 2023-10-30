#!/bin/bash
set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh


# 1. Find all *.sh files in the ci directory
# 2. Strip the directory
# 3. Exclude some scripts that we should not run in CI directly
ci_scripts=$(
    find ci/ -iname "*.sh" |
    sed -E 's#^ci/##g' |
    grep -v -E '^(ci_helpers.sh|fix_style.sh)$'
)
for script in $ci_scripts; do
    if ! grep "\\bci/$script\\b" -r .github > /dev/null; then
        echo "ERROR: CI script with name \"$script\" is not actually used in .github folder"
        exit 1
    fi
    if ! grep "^## \`$script\`\$" ci/README.md > /dev/null; then
        echo "ERROR: CI script with name \"$script\" does not have a section in ci/README.md"
        exit 1
    fi
    if ! grep "source ci/ci_helpers.sh" "ci/$script" > /dev/null; then
        echo "ERROR: CI script with name \"$script\" does not include ci/ci_helpers.sh"
        exit 1
    fi
done
