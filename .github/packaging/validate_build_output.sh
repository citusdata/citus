package_type=${1}
git clone -b requirement_latest --depth=1  https://github.com/citusdata/tools.git tools
python3 -m pip install -r tools/packaging_automation/requirements.txt
python3 -m tools.packaging_automation.validate_build_output --output_file output.log \
                                                            --ignore_file .github/packaging/packaging_ignore.yml \
                                                            --package_type ${package_type}
