#! /bin/bash

set -euo pipefail

# make ** match all directories and subdirectories
shopt -s globstar

# shellcheck disable=SC1091
source ci/ci_helpers.sh

# We do not use c-style comments in migration files as the stripped
# zero-length migration files cause warning during packaging
# See #3115 for more info

# In this file, we aim to keep the indentation intact by capturing whitespaces,
# and reusing them if needed. GNU sed unfortunately does not support lookaround assertions.

# /* -> --
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#/\*#--#g'

# */ -> `` (empty string)
# remove all whitespaces immediately before the match
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#\s*\*/\s*##g'

# * -> --
# keep the indentation
# allow only whitespaces before the match
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#^\(\s*\) \*#\1--#g'

# // -> --
# do not touch http:// or similar by allowing only whitespaces before //
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#^\(\s*\)//#\1--#g'
