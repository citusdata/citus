#!/bin/bash

# Testing this script locally requires you to set the following environment
# variables:
# CIRCLE_BRANCH, GIT_USERNAME and GIT_TOKEN

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e

PR_BRANCH="${CIRCLE_BRANCH}"
ENTERPRISE_REMOTE="https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/citusdata/citus-enterprise"

# For echo commands "set -x" would show the message effectively twice. Once as
# part of the echo command shown by "set -x" and once because of the output of
# the echo command. We do not want "set -x" to show the echo command. We only
# want to see the actual message in the output of echo itself. This function is
# a trick to do so. Read the StackOverflow post below to understand why this
# works and what this works around.
# Source: https://superuser.com/a/1141026/242593
alias echo='{ save_flags="$-"; set +x;} 2> /dev/null; echo_and_restore'
echo_and_restore() {
        builtin echo "$*"
        #shellcheck disable=SC2154
        case "$save_flags" in
         (*x*)  set -x
        esac
}

# List executed commands. This is done so debugging this script is easier when
# it fails. It's explicitely done after git remote add so username and password
# are not shown in CI output (even though it's also filtered out by CircleCI)
set -x

# Clone current git repo to a temporary working directory and go there
GIT_DIR_ROOT="$(git rev-parse --show-toplevel)"
TMP_GIT_DIR="$(mktemp -d -t citus-merge-check.XXXXXXXXX)"
git clone "$GIT_DIR_ROOT" "$TMP_GIT_DIR"
cd "$TMP_GIT_DIR"

# Disable "set -x" again, because $ENTERPRISE_REMOTE contains passwords
{ set +x ; } 2> /dev/null
git remote add enterprise "$ENTERPRISE_REMOTE"
set -x

git remote set-url --push enterprise no-pushing

# Fetch enterprise-master
git fetch enterprise enterprise-master


# Try to fetch the enterprise version of the branch. If it fails we assume it
# does not exist. This seems reasonble, because even in case of other failures
# (e.g. network) we still continue as if the enterprise version of the branch
# does not exist.
if ! git fetch enterprise "$PR_BRANCH" > /dev/null 2>&1 ; then
    echo "INFO: enterprise/$PR_BRANCH was not found"
    # If the current branch does not exist on the enterprise repo, then all we
    # have to check is if it can be merged into enterprise master without
    # problems.
    # this will exit since -e option is set and it will return non-zero code on conflicts.
    git checkout enterprise/enterprise-master
    # Check if we can merge the PR branch into enterprise-master
    git merge --no-ff --no-commit "origin/$PR_BRANCH"
    exit 0
fi

# Show the top commit of the enterprise branch to make debugging easier
git log -n 1 "enterprise/$PR_BRANCH"

# Check that this branch contains the top commit of the enterprise-master
# branch. If it does not it means it might not be able to be merged into it
# automatically. So we fail in that case.
if ! git merge-base --is-ancestor enterprise/enterprise-master "enterprise/$PR_BRANCH" ; then
    echo "ERROR: enterprise/$PR_BRANCH is not up to date with enterprise-master"
    exit 1
fi

# Check that this branch contains the top commit of the current community PR
# branch. If it does not it means it's not up to date with the current PR, so
# the enterprise branch should be updated.
if ! git merge-base --is-ancestor "origin/$PR_BRANCH" "enterprise/$PR_BRANCH" ; then
    echo "ERROR: enterprise/$PR_BRANCH is not up to date with community PR branch"
    exit 1
fi

# Because of the two checks above we now know that it contains both the last
# enterprise-master and the last community PR commit. The only way that's
# possible is if they were merged. So we are happy now.
