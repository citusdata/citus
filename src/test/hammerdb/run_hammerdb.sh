#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x

rg=$1

export RESOURCE_GROUP_NAME="${rg}"
export AZURE_REGION=westus2
# the branch name is stored in CIRCLE_BRANCH env variable in CI jobs.
export BRANCH="${CIRCLE_BRANCH}"

# add github to known hosts
echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" >> ~/.ssh/known_hosts

set +x
git config --global credential.helper store
git clone https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/citusdata/test-automation
set -x

cd test-automation

test_automation_dir=$(pwd)
# add the ssh keys
eval `ssh-agent -s`
ssh-add

ssh-keygen -y -f ~/.ssh/id_rsa > ~/.ssh/id_rsa.pub

now=$(date +"%m_%d_%Y_%s")
new_branch_name=citus_github_push/"${rg}"/"${now}"
git checkout -b "${new_branch_name}"

cd ./fabfile/hammerdb_confs
branch_config=current_branch.ini
# create a config for this branch
cp master.ini "${branch_config}"

# put the branch name to the config file.
sed -i "s@master@${BRANCH}@g" "${branch_config}"

cd "${test_automation_dir}"
cd ./hammerdb

git config --global user.email "citus-bot@microsoft.com"
git config --global user.name "citus bot"
git add -A
git commit -m "test hammerdb: ${rg} vs master"
git push origin "${new_branch_name}"

cd "${test_automation_dir}"
cd ./hammerdb

if [ "$rg" = "citusbot_ch_benchmark_rg" ]; then
    export IS_CH=true
    export IS_TPCC=true
fi

if [ "$rg" = "citusbot_tpcc_benchmark_rg" ]; then
    export IS_CH=false
    export IS_TPCC=true
fi

# create cluster and run the hammerd benchmark
./create-run.sh


