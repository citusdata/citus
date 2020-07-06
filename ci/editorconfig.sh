#!/bin/bash

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

for f in $(git ls-tree -r HEAD --name-only); do
    if [ "$f" = "${f%.out}" ]  &&
        [ "$f" = "${f%.data}" ] &&
        [ "$f" = "${f%.png}" ] &&
        [ -f "$f" ] &&
        [ "$(echo "$f" | cut -d / -f1)" != "vendor" ] &&
        [ "$(dirname "$f")" != "src/test/regress/output" ]
    then
        # Trim trailing whitespace
        sed -e 's/[[:space:]]*$//' -i "./$f"
        # Add final newline if not there
        if [ -n "$(tail -c1 "$f")" ]; then
            echo >> "$f"
        fi
    fi
done
