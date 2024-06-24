#!/bin/sh

# fail if trying to reference a variable that is not set.
set -u / set -o nounset
# exit immediately if a command fails
set -e

cidir="${0%/*}"
cd ${cidir}/..

citus_indent . --quiet
black . --quiet
isort . --quiet
ci/editorconfig.sh
ci/remove_useless_declarations.sh
ci/disallow_c_comments_in_migrations.sh
ci/disallow_hash_comments_in_spec_files.sh
ci/disallow_long_changelog_entries.sh
ci/normalize_expected.sh
ci/fix_gitignore.sh
ci/print_stack_trace.sh
ci/sort_and_group_includes.sh
