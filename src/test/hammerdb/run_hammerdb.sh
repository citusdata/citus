#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e

rg=$1

export RESOURCE_GROUP_NAME="${rg}"
export AZURE_REGION=westus2
# the branch name is stored in CIRCLE_BRANCH env variable in CI jobs.
export BRANCH="${CIRCLE_BRANCH}"
git clone https://github.com/citusdata/test-automation.git
cd test-automation

test_automation_dir=$(pwd)
# add the ssh keys
eval `ssh-agent -s`
ssh-add

ssh-keygen -y -f ~/.ssh/id_rsa > ~/.ssh/id_rsa.pub

now=$(date +"%m_%d_%Y_%s")
new_branch_name=delete_me/"${rg}"/"${now}"
git checkout -b "${new_branch_name}"

cd ./fabfile/hammerdb_confs
branch_config="${BRANCH}".ini
# create a config for this branch
cp master.ini "${branch_config}"
# put the branch name to the config file.
sed -i "s/master/${BRANCH}/g" "${branch_config}"

# TODO:: change this, for testing purposes.
sed -i "s/pg_duration 200/pg_duration 10/g" run.tcl

git add -A
git commit -m "test hammerdb: ${rg} vs master"
git push origin "${new_branch_name}"

cd "${test_automation_dir}"
cd ./hammerdb

# create cluster and run the hammerd benchmark
./create-run.sh


