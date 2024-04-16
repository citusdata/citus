/*-------------------------------------------------------------------------
 *
 * worker_partition_protocol.c
 *
 * Deprecated functions related to generating re-partitioning files.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_cleanup_job_schema_cache);
PG_FUNCTION_INFO_V1(worker_create_schema);
PG_FUNCTION_INFO_V1(worker_fetch_foreign_file);
PG_FUNCTION_INFO_V1(worker_fetch_partition_file);
PG_FUNCTION_INFO_V1(worker_hash_partition_table);
PG_FUNCTION_INFO_V1(worker_merge_files_into_table);
PG_FUNCTION_INFO_V1(worker_merge_files_and_run_query);
PG_FUNCTION_INFO_V1(worker_range_partition_table);
PG_FUNCTION_INFO_V1(worker_repartition_cleanup);


/*
 * worker_range_partition_table is a deprecated function that we keep around for
 * testing downgrade scripts.
 */
Datum
worker_range_partition_table(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}


/*
 * worker_hash_partition_table is a deprecated function that we keep around for
 * testing downgrade scripts.
 */
Datum
worker_hash_partition_table(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}


/*
 * worker_create_schema is deprecated and only kept for testing downgrade scripts.
 */
Datum
worker_create_schema(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}


/*
 * worker_repartition_cleanup removes the job directory and schema with the given job id .
 */
Datum
worker_repartition_cleanup(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}


/*
 * worker_merge_files_into_table is deprecated and only kept for testing downgrade
 * scripts.
 */
Datum
worker_merge_files_into_table(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}


/*
 * worker_merge_files_and_run_query is deprecated and only kept for testing downgrade
 * scripts.
 */
Datum
worker_merge_files_and_run_query(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}


/*
 * worker_cleanup_job_schema_cache is deprecated and only kept for testing downgrade
 * scripts.
 */
Datum
worker_cleanup_job_schema_cache(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}


/*
 * worker_fetch_foreign_file UDF is a stub UDF to install Citus flawlessly.
 * Otherwise we need to delete them from our sql files, which is confusing
 */
Datum
worker_fetch_foreign_file(PG_FUNCTION_ARGS)
{
	ereport(DEBUG2, (errmsg("this function is deprecated and no longer is used")));
	PG_RETURN_VOID();
}


/*
 * worker_fetch_partition_file is a deprecated function that we keep around for
 * testing downgrade scripts.
 */
Datum
worker_fetch_partition_file(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("this function is deprecated and only kept for testing "
						   "downgrade scripts")));
}
