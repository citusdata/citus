#!/bin/bash

# make bash behave
set -euo pipefail

psql_port=$1

runDDLs()
{
    # run ddls
    psql -U postgres -d postgres -p "${psql_port}" -f "${out_folder}"/ddls.sql > /dev/null
}

runUndistributeTables()
{
    undistribute_all_tables_command='SELECT undistribute_table(logicalrelid) FROM pg_dist_partition;'
    # run undistribute all tables
    psql -U postgres -d postgres -p "${psql_port}" -c "${undistribute_all_tables_command}" > /dev/null
}

runQueries()
{
    out_filename=$1

    # run dmls
    # echo queries for query tracing
    psql -U postgres -d postgres -p "${psql_port}" \
        --echo-queries \
        -f "${out_folder}"/queries.sql > "${out_filename}" 2>&1
}

showDiffs()
{
    # - show results in unified format
    # - do not consider queries as diff since we change table names for local and
    #   dist queries even if both queries are same
    diff -u "${out_folder}"/local_queries.out \
            "${out_folder}"/dist_queries.out \
            > "${out_folder}"/local_dist.diffs
}

# run query generator and let it create output ddls and queries
script_folder=$(dirname "$0")
query_gen_folder="${script_folder}"/..
out_folder="${query_gen_folder}"/out
cd "${query_gen_folder}" && python3 main.py

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
