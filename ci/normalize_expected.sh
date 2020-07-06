#!/bin/bash

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

for f in $(git ls-tree -r HEAD --name-only src/test/regress/expected/*.out); do
	sed -Ef src/test/regress/bin/normalize.sed < "$f" > "$f.modified"
	mv "$f.modified" "$f"
done
