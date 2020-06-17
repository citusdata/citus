#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e

PR_BRANCH="${CIRCLE_BRANCH}"

git remote add enterprise "https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/citusdata/citus-enterprise"

# List executed commands. This is done so debugging this script is easier when
# it fails. It's explicitely done after git remote add so username and password
# are not shown in CI output (even though it's also filtered out by CircleCI)
set -x

# Prevent any pushes
git remote set-url --push origin no-pushing
git remote set-url --push enterprise no-pushing

git config user.email "citus-bot@microsoft.com"
git config user.name "citus bot"

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
    git checkout enterprise-master
    # Check if we can merge the PR branch into enterprise-master
    git merge --no-ff --no-commit "origin/$PR_BRANCH"
    exit 0
fi

# Show the top commit of the enterprise branch to make debugging easier
git log -n 1 "enterprise/$PR_BRANCH"

# Check that this branch contains both the top commits of community PR branch
# and the top commit of enterprise-master. This means this branch has them
# merged.

# The "2> /dev/null" bit is to make sure that the "set -x" output is ignored
# for the subshell
git merge-base --is-ancestor enterprise/enterprise-master "enterprise/$PR_BRANCH" \
    || { \
        echo "ERROR: enterprise/$PR_BRANCH is not up to date with enterprise-master" \
        && exit 1 ;
    } 2> /dev/null;
git merge-base --is-ancestor "origin/$PR_BRANCH" "enterprise/$PR_BRANCH" \
    || { \
        echo "ERROR: enterprise/$PR_BRANCH is not up to date with community PR branch" \
        && exit 1 ;
    } 2> /dev/null;
