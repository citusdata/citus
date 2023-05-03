#!/bin/bash

# make bash behave
set -euo pipefail

psql_user=$1
psql_db=$2
psql_port=$3
seed=${4:-""}

runDDLs()
{
    # run ddls
    psql -U "${psql_user}" -d "${psql_db}" -p "${psql_port}" -f "${out_folder}"/ddls.sql > /dev/null
}

runUndistributeTables()
{
    undistribute_all_tables_command='SELECT undistribute_table(logicalrelid) FROM pg_dist_partition;'
    # run undistribute all tables
    psql -U "${psql_user}" -d "${psql_db}" -p "${psql_port}" -c "${undistribute_all_tables_command}" > /dev/null
}

runQueries()
{
    out_filename=$1

    # run dmls
    # echo queries and comments for query tracing
    psql -U "${psql_user}" -d "${psql_db}" -p "${psql_port}" \
        --echo-all \
        -f "${out_folder}"/queries.sql > "${out_filename}" 2>&1
}

showDiffs()
{
    pushd . && cd "${script_folder}" && python3 diff-checker.py && popd
}

# run query generator and let it create output ddls and queries
script_folder=$(dirname "$0")
out_folder="${script_folder}"/../out
pushd . && cd "${script_folder}"/.. && python3 generate_queries.py --seed="${seed}" && popd

# remove result files if exists
rm -rf "${out_folder}"/dist_queries.out "${out_folder}"/local_queries.out

# run ddls
runDDLs

# runs dmls for distributed tables
runQueries "${out_folder}"/dist_queries.out

# undistribute all dist tables
runUndistributeTables

# runs the same dmls for pg local tables
runQueries "${out_folder}"/local_queries.out

# see diffs in results
showDiffs
