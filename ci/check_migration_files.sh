#! /bin/bash

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# This file checks that upgrade and downgrade migration scripts are kept in
# sync in the branch:
#   * if an upgrade script is removed, the corresponding downgrade must be
#     removed too;
#   * if an upgrade script is added or modified, the corresponding downgrade
#     must be added or modified too (and vice versa).
#
# We pass --no-renames so that a rename shows up as a delete + add pair,
# making the matching by file name straightforward.

# Returns "deleted" if the given file appears as deleted in the diff, "changed"
# if it appears with any other status, and "absent" if it does not appear.
file_change_status() {
    local file=$1
    local status
    status=$(git diff --name-status --no-renames origin/main -- "$file" | cut -f1)
    if [[ -z "$status" ]]; then
        echo "absent"
    elif [[ "$status" == "D" ]]; then
        echo "deleted"
    else
        echo "changed"
    fi
}

upgrade_files=$(git diff --name-only --no-renames origin/main | { grep "src/backend/distributed/sql/citus--.*sql" || exit 0 ; })
downgrade_files=$(git diff --name-only --no-renames origin/main | { grep "src/backend/distributed/sql/downgrades/citus--.*sql" || exit 0 ; })
ret_value=0

check_pair() {
    local upgrade_file=$1
    local downgrade_file=$2
    local upgrade_status downgrade_status
    upgrade_status=$(file_change_status "$upgrade_file")
    downgrade_status=$(file_change_status "$downgrade_file")

    if [[ "$upgrade_status" != "$downgrade_status" ]]; then
        echo "$upgrade_file is $upgrade_status, but $downgrade_file is $downgrade_status in branch"
        ret_value=1
    fi
}

for file in $upgrade_files
do
    # There should always be 2 matches, and no need to avoid splitting here
    # shellcheck disable=SC2207
    versions=($(grep --only-matching --extended-regexp "[0-9]+\.[0-9]+[-.][0-9]+" <<< "$file"))

    from_version=${versions[0]};
    to_version=${versions[1]};

    downgrade_migration_file="src/backend/distributed/sql/downgrades/citus--$to_version--$from_version.sql"
    check_pair "$file" "$downgrade_migration_file"
done

# Also check downgrade-only changes that have no matching upgrade change.
for file in $downgrade_files
do
    # There should always be 2 matches, and no need to avoid splitting here
    # shellcheck disable=SC2207
    versions=($(grep --only-matching --extended-regexp "[0-9]+\.[0-9]+[-.][0-9]+" <<< "$file"))

    # downgrade file is named citus--<to>--<from>.sql; flip back to upgrade.
    to_version=${versions[0]};
    from_version=${versions[1]};

    upgrade_migration_file="src/backend/distributed/sql/citus--$from_version--$to_version.sql"

    # The upgrade file already has a change, so we should've already checked the
    # pair in the previous loop; skip if so.
    if grep --line-regexp --quiet "$upgrade_migration_file" <<< "$upgrade_files"; then
        continue
    fi

    check_pair "$upgrade_migration_file" "$file"
done

exit $ret_value;
