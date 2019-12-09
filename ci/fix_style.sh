#!/bin/sh

# fail if trying to reference a variable that is not set.
set -u / set -o nounset
# exit immediately if a command fails
set -e

cidir="${0%/*}"
cd ${cidir}/..

ci/editorconfig.sh
ci/remove_useless_declarations.sh

citus_indent . --quiet
