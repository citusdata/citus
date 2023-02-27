set -x
package_type=${1}

# Since $HOME is set in GH_Actions as /github/home, pyenv fails to create virtualenvs.
# For this script, we set $HOME to /root and then set it back to /github/home.
GITHUB_HOME="${HOME}"
export HOME="/root"

eval "$(pyenv init -)"
pyenv versions
pyenv virtualenv ${PACKAGING_PYTHON_VERSION} packaging_env
pyenv activate packaging_env

git clone -b v0.8.24 --depth=1  https://github.com/citusdata/tools.git tools
python3 -m pip install -r tools/packaging_automation/requirements.txt
python3 -m tools.packaging_automation.validate_build_output --output_file output.log \
                                                            --ignore_file .github/packaging/packaging_ignore.yml \
                                                            --package_type ${package_type}
pyenv deactivate
# Set $HOME back to /github/home
export HOME=${GITHUB_HOME}
