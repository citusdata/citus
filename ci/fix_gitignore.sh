#! /bin/bash
# shellcheck disable=SC2012

set -euo pipefail
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# We list all the .source files in alphabetical order, and do a substitution
# before writing the resulting file names that are created by those templates in
# relevant .gitignore files
#
# 1. Capture the file name without the .source extension
# 2. Add the desired extension at the end
# 3. Add a / character at the beginning of each line to conform to .gitignore file format
#
# e.g. tablespace.source -> /tablespace.sql
ls -1 src/test/regress/input | sed -E "s#(.*)\.source#/\1.sql#" > src/test/regress/sql/.gitignore

# e.g. tablespace.source -> /tablespace.out
ls -1 src/test/regress/output | sed -E "s#(.*)\.source#/\1.out#" > src/test/regress/expected/.gitignore

# Remove all the ignored files from git tree, and error out
# find all ignored files in git tree, and use quotation marks to prevent word splitting on filenames with spaces in them
ignored_lines_in_git_tree=$(git ls-files --ignored --exclude-standard | sed 's/.*/"&"/')

if [[ -n $ignored_lines_in_git_tree ]]
then
    echo "Ignored files should not be in git tree!"
    echo "${ignored_lines_in_git_tree}"

    echo "Removing these files from git tree, please review and commit"
    echo "$ignored_lines_in_git_tree" | xargs git rm -r --cached
    exit 1
fi
