#!/bin/bash

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# Find the line that exactly matches "RegisterCitusConfigVariables(void)" in
# shared_library_init.c. grep command returns something like
# "934:RegisterCitusConfigVariables(void)" and we extract the line number
# with cut.
RegisterCitusConfigVariables_begin_linenumber=$(grep -n "^RegisterCitusConfigVariables(void)$" src/backend/distributed/shared_library_init.c | cut -d: -f1)

# Consider the lines starting from $RegisterCitusConfigVariables_begin_linenumber,
# grep the first line that starts with "}" and extract the line number with cut
# as in the previous step.
RegisterCitusConfigVariables_length=$(tail -n +$RegisterCitusConfigVariables_begin_linenumber src/backend/distributed/shared_library_init.c | grep -n -m 1 "^}$" | cut -d: -f1)

# extract the function definition of RegisterCitusConfigVariables into a temp file
tail -n +$RegisterCitusConfigVariables_begin_linenumber src/backend/distributed/shared_library_init.c | head -n $(($RegisterCitusConfigVariables_length)) > RegisterCitusConfigVariables_func_def.out

# extract citus gucs in the form of <tab><tab>"citus.X"
grep -P "^[\t][\t]\"citus\.[a-zA-Z_0-9]+\"" RegisterCitusConfigVariables_func_def.out > gucs.out
LC_COLLATE=C sort -c gucs.out
rm gucs.out
rm RegisterCitusConfigVariables_func_def.out
