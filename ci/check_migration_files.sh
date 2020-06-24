#! /bin/bash

set -euo pipefail
# This file checks for the existence of downgrade scripts for every upgrade script that is changed in the branch.

# list of migration files, and early exit if no migration scripts are touched
migration_files=$(git diff --name-only origin/master | grep "src/backend/distributed/sql/citus--.*sql") || exit 0
ret_value=0

for file in $migration_files
do
    # There should always be 2 matches, and no need to avoid splitting here
    # shellcheck disable=SC2207
    versions=($(grep -Eo "\d\.\d-\d" <<< "$file"))

    from_version=${versions[0]};
    to_version=${versions[1]};

    reverted_migration_file="src/backend/distributed/sql/citus--$to_version--$from_version.sql"

    # check for the existence of reverted migration scripts
    if [[ $(grep -xc reverted_migration_file <<< "$migration_files") == 0 ]]
    then
        echo "$file is updated, but $reverted_migration_file is missing in branch"
        ret_value=1
    fi
done

exit $ret_value;
