#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e

# try_merge sees if we can merge "from" branch to "to" branch
# it will exit with nonzero code if the merge fails because of conflicts.
try_merge() {
    to=$1
    from=$2
    git checkout "${to}"
    # this will exit since -e option is set and it will return non-zero code on conflicts.
    git merge --no-ff --no-commit "${from}"

}

git config --global user.email "citus-bot@microsoft.com" 
git config --global user.name "citus bot" 

cd ~
git clone https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/citusdata/citus-enterprise
cd citus-enterprise
git fetch --all

# echo commands
set -x
git branch -r --list

branch_name="${CIRCLE_BRANCH}"

# check if the branch on community exists on enterprise
# the output will not be empty if it does
if [ `git branch -r --list origin/$branch_name` ]
then 
    try_merge enterprise-master origin/$branch_name
else
    # add community as a remote 
    git remote add --no-tags community git@github.com:citusdata/citus.git
    # prevent pushes to community
    git remote set-url --push community no-pushing
    git fetch --all
    try_merge enterprise-master community/$branch_name
fi


