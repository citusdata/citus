#! /bin/bash
set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# Remove all the ignored files from git tree, and error out
# find all ignored files in git tree, and use quotation marks to prevent word splitting on filenames with spaces in them
# NOTE: Option --cached is needed to avoid a bug in git ls-files command.
ignored_lines_in_git_tree=$(git ls-files --ignored --cached --exclude-standard | sed 's/.*/"&"/')

if [[ -n $ignored_lines_in_git_tree ]]
then
    echo "Ignored files should not be in git tree!"
    echo "${ignored_lines_in_git_tree}"

    echo "Removing these files from git tree, please review and commit"
    echo "$ignored_lines_in_git_tree" | xargs git rm -r --cached
    exit 1
fi
