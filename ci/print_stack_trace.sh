#!/bin/bash

set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh

# find all core files
core_files=( $(find . -type f -regex .*core.*\d*.*postgres) )
if [ ${#core_files[@]} -gt 0 ]; then
    # print stack traces for the core files
    for core_file in "${core_files[@]}"
    do
        # set print frame-arguments all: show all scalars + structures in the frame
        # set print pretty on:           show structures in indented mode
        # set print addr off:            do not show pointer address
        # thread apply all bt full:      show stack traces for all threads
        gdb --batch \
            -ex "set print frame-arguments all" \
            -ex "set print pretty on" \
            -ex "set print addr off" \
            -ex "thread apply all bt full" \
            postgres "${core_file}"
    done
fi
