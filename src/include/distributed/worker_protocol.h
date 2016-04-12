/*-------------------------------------------------------------------------
 *
 * worker_protocol.h
 *	  Header for shared declarations that are used for caching remote resources
 *	  on worker nodes, and also for applying distributed execution primitives.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_PROTOCOL_H
#define WORKER_PROTOCOL_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/fd.h"
#include "utils/array.h"


/* Number of rows to prefetch when reading data with a cursor */
#define ROW_PREFETCH_COUNT 50

/* Directory, file, table name, and UDF related defines for distributed tasks */
#define PG_JOB_CACHE_DIR "pgsql_job_cache"
#define JOB_DIRECTORY_PREFIX "job_"
#define JOB_SCHEMA_PREFIX "pg_merge_job_"
#define TASK_FILE_PREFIX "task_"
#define TASK_TABLE_PREFIX "task_"
#define TABLE_FILE_PREFIX "table_"
#define PARTITION_FILE_PREFIX "p_"
#define ATTEMPT_FILE_SUFFIX ".attempt"
#define MERGE_TABLE_SUFFIX "_merge"
#define MIN_JOB_DIRNAME_WIDTH 4
#define MIN_TASK_FILENAME_WIDTH 6
#define MIN_PARTITION_FILENAME_WIDTH 5
#define FOREIGN_FILENAME_OPTION "filename"
#define CSTORE_TABLE_SIZE_FUNCTION_NAME "cstore_table_size"

/* Defines used for fetching files and tables */
/* the tablename in the overloaded COPY statement is the to-be-transferred file */
#define TRANSMIT_REGULAR_COMMAND "COPY \"%s\" TO STDOUT WITH (format 'transmit')"
#define COPY_OUT_COMMAND "COPY %s TO STDOUT"
#define COPY_IN_COMMAND "COPY %s FROM '%s'"

/* Defines that relate to fetching foreign tables */
#define FOREIGN_CACHED_FILE_PATH "pg_foreign_file/cached/%s"
#define GET_TABLE_DDL_EVENTS "SELECT master_get_table_ddl_events('%s')"
#define SET_FOREIGN_TABLE_FILENAME "ALTER FOREIGN TABLE %s OPTIONS (SET filename '%s')"
#define FOREIGN_FILE_PATH_COMMAND "SELECT worker_foreign_file_path('%s')"
#define SET_SEARCH_PATH_COMMAND "SET search_path TO %s"
#define CREATE_TABLE_COMMAND "CREATE TABLE %s (%s)"
#define CREATE_TABLE_AS_COMMAND "CREATE TABLE %s (%s) AS (%s)"


/*
 * RangePartitionContext keeps range re-partitioning related data. The Btree
 * comparison function is set according to the partitioned column's data type.
 */
typedef struct RangePartitionContext
{
	FmgrInfo *comparisonFunction;
	Datum *splitPointArray;
	int32 splitPointCount;
} RangePartitionContext;


/*
 * HashPartitionContext keeps hash re-partitioning related data. The hashing
 * function is set according to the partitioned column's data type.
 */
typedef struct HashPartitionContext
{
	FmgrInfo *hashFunction;
	uint32 partitionCount;
} HashPartitionContext;


/*
 * FileOutputStream helps buffer write operations to a file; these writes are
 * then regularly flushed to the underlying file. This structure differs from
 * standard file output streams in that it keeps a larger buffer, and only
 * supports appending data to virtual file descriptors.
 */
typedef struct FileOutputStream
{
	File fileDescriptor;
	StringInfo fileBuffer;
	StringInfo filePath;
} FileOutputStream;


/* Config variables managed via guc.c */
extern int PartitionBufferSize;
extern bool ExpireCachedShards;
extern bool BinaryWorkerCopyFormat;


/* Function declarations local to the worker module */
extern StringInfo JobSchemaName(uint64 jobId);
extern StringInfo TaskTableName(uint32 taskId);
extern bool JobSchemaExists(StringInfo schemaName);
extern StringInfo JobDirectoryName(uint64 jobId);
extern StringInfo TaskDirectoryName(uint64 jobId, uint32 taskId);
extern StringInfo PartitionFilename(StringInfo directoryName, uint32 partitionId);
extern bool JobDirectoryElement(const char *filename);
extern bool DirectoryExists(StringInfo directoryName);
extern void CreateDirectory(StringInfo directoryName);
extern void RemoveDirectory(StringInfo filename);
extern StringInfo InitTaskDirectory(uint64 jobId, uint32 taskId);
extern void RemoveJobSchema(StringInfo schemaName);
extern Datum * DeconstructArrayObject(ArrayType *arrayObject);
extern int32 ArrayObjectCount(ArrayType *arrayObject);
extern FmgrInfo * GetFunctionInfo(Oid typeId, Oid accessMethodId, int16 procedureId);

/* Function declarations shared with the master planner */
extern StringInfo TaskFilename(StringInfo directoryName, uint32 taskId);
extern List * ExecuteRemoteQuery(const char *nodeName, uint32 nodePort,
								 StringInfo queryString);
extern List * ColumnDefinitionList(List *columnNameList, List *columnTypeList);
extern CreateStmt * CreateStatement(RangeVar *relation, List *columnDefinitionList);
extern CopyStmt * CopyStatement(RangeVar *relation, char *sourceFilename);
extern Datum CompareCall2(FmgrInfo *funcInfo, Datum leftArgument, Datum rightArgument);

/* Function declaration for parsing tree node */
extern Node * ParseTreeNode(const char *ddlCommand);

/* Function declarations for applying distributed execution primitives */
extern Datum worker_fetch_partition_file(PG_FUNCTION_ARGS);
extern Datum worker_fetch_query_results_file(PG_FUNCTION_ARGS);
extern Datum worker_apply_shard_ddl_command(PG_FUNCTION_ARGS);
extern Datum worker_range_partition_table(PG_FUNCTION_ARGS);
extern Datum worker_hash_partition_table(PG_FUNCTION_ARGS);
extern Datum worker_merge_files_into_table(PG_FUNCTION_ARGS);
extern Datum worker_merge_files_and_run_query(PG_FUNCTION_ARGS);
extern Datum worker_cleanup_job_schema_cache(PG_FUNCTION_ARGS);

/* Function declarations for fetching regular and foreign tables */
extern Datum worker_fetch_foreign_file(PG_FUNCTION_ARGS);
extern Datum worker_fetch_regular_table(PG_FUNCTION_ARGS);
extern Datum worker_append_table_to_shard(PG_FUNCTION_ARGS);
extern Datum worker_foreign_file_path(PG_FUNCTION_ARGS);
extern Datum worker_find_block_local_path(PG_FUNCTION_ARGS);


#endif   /* WORKER_PROTOCOL_H */
