#! /bin/bash

set -eu
# shellcheck disable=SC1091
source ci/ci_helpers.sh

# Having changelog items with entries that are longer than 80 characters are forbidden.
# Find all lines with disallowed length, and for all such lines store
#  - line number
#  - length of the line
#  - the line content
too_long_lines=$(awk 'length() > 80 {print NR,"(",length(),"characters ) :",$0}' CHANGELOG.md)

if [[ -n $too_long_lines ]]
then
    echo "We allow at most 80 characters in CHANGELOG.md."
    echo "${too_long_lines}"
    exit 1
fi
