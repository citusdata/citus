#!/bin/bash

# For echo commands "set -x" would show the message effectively twice. Once as
# part of the echo command shown by "set -x" and once because of the output of
# the echo command. We do not want "set -x" to show the echo command. We only
# want to see the actual message in the output of echo itself. This function is
# a trick to do so. Read the StackOverflow post below to understand why this
# works and what this works around.
# Source: https://superuser.com/a/1141026/242593
shopt -s expand_aliases
alias echo='{ save_flags="$-"; set +x;} 2> /dev/null && echo_and_restore'
echo_and_restore() {
        builtin echo "$*"
        #shellcheck disable=SC2154
        case "$save_flags" in
         (*x*)  set -x
        esac
}

# Make sure that on a failing exit we show a useful message
hint_on_fail() {
    exit_code=$?
    # Get filename of the currently running script
    # Source: https://stackoverflow.com/a/192337/2570866
    filename=$(basename "$0")
    if [ $exit_code != 0 ]; then
        echo "HINT: To solve this failure look here: https://github.com/citusdata/citus/blob/master/ci/README.md#$filename"
    fi
    exit $exit_code
}
trap hint_on_fail EXIT

