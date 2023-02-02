#!/bin/bash

# make bash behave
set -euo pipefail

psql_port=$1

prepareLocalDdlFiles()
{
    # remove create_distributed_table and create_reference_table lines from ddls.sql
    sed -i '/create_distributed_table/d' "${out_folder}"/ddls.sql
    sed -i '/create_reference_table/d' "${out_folder}"/ddls.sql

    # remove INSERT INTO lines from ddls.sql
    sed -i '/INSERT INTO/d' "${out_folder}"/ddls.sql

    # replace CREATE TABLE tbl(...) with CREATE TABLE tbl AS
    sed -i 's/CREATE TABLE \(.\+\)(.\+);/CREATE TABLE \1 AS SELECT * FROM \1;/g' "${out_folder}"/ddls.sql

    # convert dist and ref table names in DROP IF EXISTS with loc in ddls.sql
    sed -i 's/DROP TABLE IF EXISTS dist\([0-9]\+\)/DROP TABLE IF EXISTS locd\1/g' "${out_folder}"/ddls.sql
    sed -i 's/DROP TABLE IF EXISTS ref\([0-9]\+\)/DROP TABLE IF EXISTS locr\1/g' "${out_folder}"/ddls.sql

    # convert dist and ref table names in CREATE part(not select part) with loc in ddls.sql
    sed -i 's/CREATE TABLE dist/CREATE TABLE locd/g' "${out_folder}"/ddls.sql
    sed -i 's/CREATE TABLE ref/CREATE TABLE locr/g' "${out_folder}"/ddls.sql
}

prepareLocalQueryFiles()
{
    # convert dist and ref table names with loc in queries.sql
    sed -i 's/dist\([0-9]\+\)/locd\1/g' "${out_folder}"/queries.sql
    sed -i 's/ref\([0-9]\+\)/locr\1/g' "${out_folder}"/queries.sql

    # rename replaced outputs for local table psql run
    mv "${out_folder}"/ddls.sql "${out_folder}"/local_ddls.sql
    mv "${out_folder}"/queries.sql "${out_folder}"/local_queries.sql
}

runQueries()
{
    # remove out files if exists
    rm -rf "${out_folder}"/dist_queries.out "${out_folder}"/local_queries.out

    # run ddls for local and distributed tables sequentially
    psql -U postgres -d postgres -p "${psql_port}" -f "${out_folder}"/dist_ddls.sql > /dev/null
    psql -U postgres -d postgres -p "${psql_port}" -f "${out_folder}"/local_ddls.sql > /dev/null

    # run dmls for local and distributed tables sequentially
    psql -U postgres -d postgres -p "${psql_port}" -f "${out_folder}"/dist_queries.sql > "${out_folder}"/dist_queries.out 2>&1
    psql -U postgres -d postgres -p "${psql_port}" -f "${out_folder}"/local_queries.sql > "${out_folder}"/local_queries.out 2>&1
}

# run query generator and let it create output ddls and queries
script_folder=$(dirname "$0")
query_gen_folder="${script_folder}"/..
out_folder="${query_gen_folder}"/out
cd "${query_gen_folder}" && python3 main.py

# copy outputs for distributed table psql run
cp "${out_folder}"/ddls.sql "${out_folder}"/dist_ddls.sql
cp "${out_folder}"/queries.sql "${out_folder}"/dist_queries.sql

# creates ddl and query files for local tables
prepareLocalDdlFiles
prepareLocalQueryFiles

# runs ddls sequentially and then queries from parallel sessions
runQueries

# see diffs in results (returns with exit code 1 if there is any diff)
diff "${out_folder}"/local_queries.out "${out_folder}"/dist_queries.out > "${out_folder}"/local_dist.diffs
