#! /bin/bash

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# This file checks for the existence of downgrade scripts for every upgrade script that is changed in the branch.

# create list of migration files for upgrades
upgrade_files=$(git diff --name-only origin/main | { grep "src/backend/distributed/sql/citus--.*sql" || exit 0 ; })
downgrade_files=$(git diff --name-only origin/main | { grep "src/backend/distributed/sql/downgrades/citus--.*sql" || exit 0 ; })
ret_value=0

for file in $upgrade_files
do
    # There should always be 2 matches, and no need to avoid splitting here
    # shellcheck disable=SC2207
    versions=($(grep --only-matching --extended-regexp "[0-9]+\.[0-9]+[-.][0-9]+" <<< "$file"))

    from_version=${versions[0]};
    to_version=${versions[1]};

    downgrade_migration_file="src/backend/distributed/sql/downgrades/citus--$to_version--$from_version.sql"

    # check for the existence of migration scripts
    if [[ $(grep --line-regexp --count "$downgrade_migration_file" <<< "$downgrade_files") == 0 ]]
    then
        echo "$file is updated, but $downgrade_migration_file is not updated in branch"
        ret_value=1
    fi
done

exit $ret_value;
