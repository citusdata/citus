#!/bin/bash

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# extract citus gucs in the form of <tab><tab>"citus.X"
grep -P "^[\t][\t]\"citus\.[a-zA-Z_0-9]+\"" src/backend/distributed/shared_library_init.c > gucs.out
sort -c gucs.out
rm gucs.out
