#!/bin/bash

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# extract citus gucs in the form of "citus.X"
grep -o -E "(\.*\"citus\.\w+\")," src/backend/distributed/shared_library_init.c > gucs.out
sort -c gucs.out
rm gucs.out
