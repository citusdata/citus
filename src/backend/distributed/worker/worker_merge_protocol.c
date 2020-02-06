/*-------------------------------------------------------------------------
 *
 * worker_merge_protocol.c
 *
 * Routines for merging partitioned files into a single file or table. Merging
 * files is one of the threee distributed execution primitives that we apply on
 * worker nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#if PG_VERSION_NUM >= 120000
#include "access/genam.h"
#include "access/table.h"
#endif
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_namespace.h"
#include "commands/copy.h"
#include "commands/tablecmds.h"
#include "common/string.h"
#include "distributed/metadata_cache.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "distributed/task_tracker_protocol.h"
#include "distributed/task_tracker.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "commands/schemacmds.h"
#include "distributed/resource_lock.h"


/* Local functions forward declarations */
static List * ArrayObjectToCStringList(ArrayType *arrayObject);
static void CreateTaskTable(StringInfo schemaName, StringInfo relationName,
							List *columnNameList, List *columnTypeList);
static void CopyTaskFilesFromDirectory(StringInfo schemaName, StringInfo relationName,
									   StringInfo sourceDirectoryName, Oid userId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_merge_files_into_table);
PG_FUNCTION_INFO_V1(worker_merge_files_and_run_query);
PG_FUNCTION_INFO_V1(worker_cleanup_job_schema_cache);
PG_FUNCTION_INFO_V1(worker_create_schema);
PG_FUNCTION_INFO_V1(worker_repartition_cleanup);


/*
 * worker_create_schema creates a schema with the given job id in local.
 */
Datum
worker_create_schema(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	text *ownerText = PG_GETARG_TEXT_P(1);
	char *ownerString = TextDatumGetCString(ownerText);


	StringInfo jobSchemaName = JobSchemaName(jobId);
	CheckCitusVersion(ERROR);

	bool schemaExists = JobSchemaExists(jobSchemaName);
	if (!schemaExists)
	{
		CreateJobSchema(jobSchemaName, ownerString);
	}

	PG_RETURN_VOID();
}


/*
 * worker_repartition_cleanup removes the job directory and schema with the given job id .
 */
Datum
worker_repartition_cleanup(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	StringInfo jobDirectoryName = JobDirectoryName(jobId);
	StringInfo jobSchemaName = JobSchemaName(jobId);

	CheckCitusVersion(ERROR);

	Oid schemaId = get_namespace_oid(jobSchemaName->data, false);

	EnsureSchemaOwner(schemaId);
	CitusRemoveDirectory(jobDirectoryName->data);
	RemoveJobSchema(jobSchemaName);
	PG_RETURN_VOID();
}


/*
 * worker_merge_files_into_table creates a task table within the job's schema,
 * which should have already been created by the task tracker protocol, and
 * copies files in its task directory into this table. If the schema doesn't
 * exist, the function defaults to the 'public' schema. Note that, unlike
 * partitioning functions, this function is not always idempotent. On success,
 * the function creates the table and loads data, and subsequent calls to the
 * function error out because the table already exist. On failure, the task
 * table creation commands are rolled back, and the function can be called
 * again.
 */
Datum
worker_merge_files_into_table(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 taskId = PG_GETARG_UINT32(1);
	ArrayType *columnNameObject = PG_GETARG_ARRAYTYPE_P(2);
	ArrayType *columnTypeObject = PG_GETARG_ARRAYTYPE_P(3);

	StringInfo jobSchemaName = JobSchemaName(jobId);
	StringInfo taskTableName = TaskTableName(taskId);
	StringInfo taskDirectoryName = TaskDirectoryName(jobId, taskId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Oid userId = GetUserId();

	/* we should have the same number of column names and types */
	int32 columnNameCount = ArrayObjectCount(columnNameObject);
	int32 columnTypeCount = ArrayObjectCount(columnTypeObject);

	CheckCitusVersion(ERROR);

	if (columnNameCount != columnTypeCount)
	{
		ereport(ERROR, (errmsg("column name array size: %d and type array size: %d"
							   " do not match", columnNameCount, columnTypeCount)));
	}

	/*
	 * If the schema for the job isn't already created by the task tracker
	 * protocol, we fall to using the default 'public' schema.
	 */
	bool schemaExists = JobSchemaExists(jobSchemaName);
	if (!schemaExists)
	{
		/*
		 * For testing purposes, we allow merging into a table in the public schema,
		 * but only when running as superuser.
		 */

		if (!superuser())
		{
			ereport(ERROR, (errmsg("job schema does not exist"),
							errdetail("must be superuser to use public schema")));
		}

		resetStringInfo(jobSchemaName);
		appendStringInfoString(jobSchemaName, "public");
	}
	else
	{
		Oid schemaId = get_namespace_oid(jobSchemaName->data, false);

		EnsureSchemaOwner(schemaId);
	}

	/* create the task table and copy files into the table */
	List *columnNameList = ArrayObjectToCStringList(columnNameObject);
	List *columnTypeList = ArrayObjectToCStringList(columnTypeObject);

	CreateTaskTable(jobSchemaName, taskTableName, columnNameList, columnTypeList);

	/* need superuser to copy from files */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	CopyTaskFilesFromDirectory(jobSchemaName, taskTableName, taskDirectoryName,
							   userId);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	PG_RETURN_VOID();
}


/*
 * worker_merge_files_and_run_query creates a merge task table within the job's
 * schema, which should have already been created by the task tracker protocol.
 * It copies files in its task directory into this table. Then it runs final
 * query to create result table of the job.
 *
 * Note that here we followed a different approach to create a task table for merge
 * files than worker_merge_files_into_table(). In future we should unify these
 * two approaches. For this purpose creating a directory_fdw extension and using
 * it would make sense. Then we can merge files with a query or without query
 * through directory_fdw.
 */
Datum
worker_merge_files_and_run_query(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 taskId = PG_GETARG_UINT32(1);
	text *createMergeTableQueryText = PG_GETARG_TEXT_P(2);
	text *createIntermediateTableQueryText = PG_GETARG_TEXT_P(3);

	const char *createMergeTableQuery = text_to_cstring(createMergeTableQueryText);
	const char *createIntermediateTableQuery =
		text_to_cstring(createIntermediateTableQueryText);

	StringInfo taskDirectoryName = TaskDirectoryName(jobId, taskId);
	StringInfo jobSchemaName = JobSchemaName(jobId);
	StringInfo intermediateTableName = TaskTableName(taskId);
	StringInfo mergeTableName = makeStringInfo();
	StringInfo setSearchPathString = makeStringInfo();
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Oid userId = GetUserId();

	CheckCitusVersion(ERROR);

	/*
	 * If the schema for the job isn't already created by the task tracker
	 * protocol, we fall to using the default 'public' schema.
	 */
	bool schemaExists = JobSchemaExists(jobSchemaName);
	if (!schemaExists)
	{
		resetStringInfo(jobSchemaName);
		appendStringInfoString(jobSchemaName, "public");
	}
	else
	{
		Oid schemaId = get_namespace_oid(jobSchemaName->data, false);

		EnsureSchemaOwner(schemaId);
	}

	appendStringInfo(setSearchPathString, SET_SEARCH_PATH_COMMAND, jobSchemaName->data);

	/* Add "public" to search path to access UDFs in public schema */
	appendStringInfo(setSearchPathString, ",public");

	int connected = SPI_connect();
	if (connected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	int setSearchPathResult = SPI_exec(setSearchPathString->data, 0);
	if (setSearchPathResult < 0)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   setSearchPathString->data)));
	}

	int createMergeTableResult = SPI_exec(createMergeTableQuery, 0);
	if (createMergeTableResult < 0)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   createMergeTableQuery)));
	}

	/* need superuser to copy from files */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	appendStringInfo(mergeTableName, "%s%s", intermediateTableName->data,
					 MERGE_TABLE_SUFFIX);
	CopyTaskFilesFromDirectory(jobSchemaName, mergeTableName, taskDirectoryName,
							   userId);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	int createIntermediateTableResult = SPI_exec(createIntermediateTableQuery, 0);
	if (createIntermediateTableResult < 0)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   createIntermediateTableQuery)));
	}

	int finished = SPI_finish();
	if (finished != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not disconnect from SPI manager")));
	}

	PG_RETURN_VOID();
}


/*
 * worker_cleanup_job_schema_cache walks over all schemas in the database, and
 * removes schemas whose names start with the job schema prefix. Note that this
 * function does not perform any locking; we expect it to be called at process
 * start-up time before any merge tasks are run. Further note that this function
 * runs within the scope of a particular database (template1, postgres) and can
 * only delete schemas within that database.
 */
Datum
worker_cleanup_job_schema_cache(PG_FUNCTION_ARGS)
{
	Relation pgNamespace = NULL;
#if PG_VERSION_NUM >= 120000
	TableScanDesc scanDescriptor = NULL;
#else
	HeapScanDesc scanDescriptor = NULL;
#endif
	ScanKey scanKey = NULL;
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;

	CheckCitusVersion(ERROR);

	pgNamespace = heap_open(NamespaceRelationId, AccessExclusiveLock);
#if PG_VERSION_NUM >= 120000
	scanDescriptor = table_beginscan_catalog(pgNamespace, scanKeyCount, scanKey);
#else
	scanDescriptor = heap_beginscan_catalog(pgNamespace, scanKeyCount, scanKey);
#endif

	heapTuple = heap_getnext(scanDescriptor, ForwardScanDirection);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_namespace schemaForm = (Form_pg_namespace) GETSTRUCT(heapTuple);
		char *schemaName = NameStr(schemaForm->nspname);

		char *jobSchemaFound = strstr(schemaName, JOB_SCHEMA_PREFIX);
		if (jobSchemaFound != NULL)
		{
			StringInfo jobSchemaName = makeStringInfo();
			appendStringInfoString(jobSchemaName, schemaName);

			RemoveJobSchema(jobSchemaName);
		}

		heapTuple = heap_getnext(scanDescriptor, ForwardScanDirection);
	}

	heap_endscan(scanDescriptor);
	heap_close(pgNamespace, AccessExclusiveLock);

	PG_RETURN_VOID();
}


/* Constructs a standardized job schema name for the given job id. */
StringInfo
JobSchemaName(uint64 jobId)
{
	StringInfo jobSchemaName = makeStringInfo();
	appendStringInfo(jobSchemaName, "%s%0*" INT64_MODIFIER "u", JOB_SCHEMA_PREFIX,
					 MIN_JOB_DIRNAME_WIDTH, jobId);

	return jobSchemaName;
}


/* Constructs a standardized task table name for the given task id. */
StringInfo
TaskTableName(uint32 taskId)
{
	StringInfo taskTableName = makeStringInfo();
	appendStringInfo(taskTableName, "%s%0*u",
					 TASK_TABLE_PREFIX, MIN_TASK_FILENAME_WIDTH, taskId);

	return taskTableName;
}


/* Creates a list of cstrings from a single dimensional array object. */
static List *
ArrayObjectToCStringList(ArrayType *arrayObject)
{
	List *cstringList = NIL;
	Datum *datumArray = DeconstructArrayObject(arrayObject);
	int32 arraySize = ArrayObjectCount(arrayObject);

	for (int32 arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
	{
		Datum datum = datumArray[arrayIndex];
		char *cstring = TextDatumGetCString(datum);

		cstringList = lappend(cstringList, cstring);
	}

	Assert(cstringList != NIL);
	return cstringList;
}


/* Checks if a schema with the given schema name exists. */
bool
JobSchemaExists(StringInfo schemaName)
{
	Datum schemaNameDatum = CStringGetDatum(schemaName->data);
	bool schemaExists = SearchSysCacheExists(NAMESPACENAME, schemaNameDatum, 0, 0, 0);

	return schemaExists;
}


/* Removes the schema and all tables within the schema, if the schema exists. */
void
RemoveJobSchema(StringInfo schemaName)
{
	Datum schemaNameDatum = CStringGetDatum(schemaName->data);

	Oid schemaId = GetSysCacheOid1Compat(NAMESPACENAME, Anum_pg_namespace_oid,
										 schemaNameDatum);
	if (OidIsValid(schemaId))
	{
		ObjectAddress schemaObject = { 0, 0, 0 };

		bool permissionsOK = pg_namespace_ownercheck(schemaId, GetUserId());
		if (!permissionsOK)
		{
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA, schemaName->data);
		}

		schemaObject.classId = NamespaceRelationId;
		schemaObject.objectId = schemaId;
		schemaObject.objectSubId = 0;

		/*
		 * We first delete all tables in this schema. Rather than relying on the
		 * schema command, we call the dependency mechanism directly so that we
		 * can suppress notice messages that are typically displayed during
		 * cascading deletes.
		 */
		performDeletion(&schemaObject, DROP_CASCADE,
						PERFORM_DELETION_INTERNAL |
						PERFORM_DELETION_QUIETLY |
						PERFORM_DELETION_SKIP_ORIGINAL |
						PERFORM_DELETION_SKIP_EXTENSIONS);

		CommandCounterIncrement();

		/* drop the empty schema */
		performDeletion(&schemaObject, DROP_RESTRICT, 0);
		CommandCounterIncrement();
	}
	else
	{
		ereport(DEBUG2, (errmsg("schema \"%s\" does not exist, skipping",
								schemaName->data)));
	}
}


/* Creates a simple table that only defines columns, in the given schema. */
static void
CreateTaskTable(StringInfo schemaName, StringInfo relationName,
				List *columnNameList, List *columnTypeList)
{
	Oid relationId PG_USED_FOR_ASSERTS_ONLY = InvalidOid;

	Assert(schemaName != NULL);
	Assert(relationName != NULL);

	/*
	 * This new relation doesn't log to WAL, as the table creation and data copy
	 * statements occur in the same transaction. Still, we want to make the
	 * relation unlogged once we upgrade to PostgreSQL 9.1.
	 */
	RangeVar *relation = makeRangeVar(schemaName->data, relationName->data, -1);
	List *columnDefinitionList = ColumnDefinitionList(columnNameList, columnTypeList);

	CreateStmt *createStatement = CreateStatement(relation, columnDefinitionList);

	ObjectAddress relationObject = DefineRelation(createStatement, RELKIND_RELATION,
												  InvalidOid, NULL,
												  NULL);
	relationId = relationObject.objectId;

	Assert(relationId != InvalidOid);
	CommandCounterIncrement();
}


/*
 * ColumnDefinitionList creates and returns a list of column definition objects
 * from two lists of column names and types. As an example, this function takes
 * in two single elements lists: "l_quantity" and "decimal(15, 2)". The function
 * then returns a list with one column definition, where the column's name is
 * l_quantity, its type is numeric, and the type modifier represents (15, 2).
 */
List *
ColumnDefinitionList(List *columnNameList, List *columnTypeList)
{
	List *columnDefinitionList = NIL;
	ListCell *columnNameCell = NULL;
	ListCell *columnTypeCell = NULL;

	forboth(columnNameCell, columnNameList, columnTypeCell, columnTypeList)
	{
		const char *columnName = (const char *) lfirst(columnNameCell);
		const char *columnType = (const char *) lfirst(columnTypeCell);

		/*
		 * We should have a SQL compatible column type declaration; we first
		 * convert this type to PostgreSQL's type identifiers and modifiers.
		 */
		Oid columnTypeId = InvalidOid;
		int32 columnTypeMod = -1;
		bool missingOK = false;

		parseTypeString(columnType, &columnTypeId, &columnTypeMod, missingOK);
		TypeName *typeName = makeTypeNameFromOid(columnTypeId, columnTypeMod);

		/* we then create the column definition */
		ColumnDef *columnDefinition = makeNode(ColumnDef);
		columnDefinition->colname = (char *) columnName;
		columnDefinition->typeName = typeName;
		columnDefinition->is_local = true;
		columnDefinition->is_not_null = false;
		columnDefinition->raw_default = NULL;
		columnDefinition->cooked_default = NULL;
		columnDefinition->constraints = NIL;

		columnDefinitionList = lappend(columnDefinitionList, columnDefinition);
	}

	return columnDefinitionList;
}


/*
 * CreateStatement creates and initializes a simple table create statement that
 * only has column definitions.
 */
CreateStmt *
CreateStatement(RangeVar *relation, List *columnDefinitionList)
{
	CreateStmt *createStatement = makeNode(CreateStmt);
	createStatement->relation = relation;
	createStatement->tableElts = columnDefinitionList;
	createStatement->inhRelations = NIL;
	createStatement->constraints = NIL;
	createStatement->options = NIL;
	createStatement->oncommit = ONCOMMIT_NOOP;
	createStatement->tablespacename = NULL;
	createStatement->if_not_exists = false;

	return createStatement;
}


/*
 * CopyTaskFilesFromDirectory finds all files in the given directory, except for
 * those having an attempt suffix. The function then copies these files into the
 * database table identified by the given schema and table name.
 *
 * The function makes sure all files were generated by the current user by checking
 * whether the filename ends with the username, since this is added to local file
 * names by functions such as worker_fetch_partition-file. Files that were generated
 * by other users calling worker_fetch_partition_file directly are skipped.
 */
static void
CopyTaskFilesFromDirectory(StringInfo schemaName, StringInfo relationName,
						   StringInfo sourceDirectoryName, Oid userId)
{
	const char *directoryName = sourceDirectoryName->data;
	uint64 copiedRowTotal = 0;
	StringInfo expectedFileSuffix = makeStringInfo();

	DIR *directory = AllocateDir(directoryName);
	if (directory == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open directory \"%s\": %m", directoryName)));
	}

	appendStringInfo(expectedFileSuffix, ".%u", userId);

	struct dirent *directoryEntry = ReadDir(directory, directoryName);
	for (; directoryEntry != NULL; directoryEntry = ReadDir(directory, directoryName))
	{
		const char *baseFilename = directoryEntry->d_name;
		const char *queryString = NULL;
		uint64 copiedRowCount = 0;

		/* if system file or lingering task file, skip it */
		if (strncmp(baseFilename, ".", MAXPGPATH) == 0 ||
			strncmp(baseFilename, "..", MAXPGPATH) == 0 ||
			strstr(baseFilename, ATTEMPT_FILE_SUFFIX) != NULL)
		{
			continue;
		}

		if (!pg_str_endswith(baseFilename, expectedFileSuffix->data))
		{
			/*
			 * Someone is trying to tamper with our results. We don't throw an error
			 * here because we don't want to allow users to prevent each other from
			 * running queries.
			 */
			ereport(WARNING, (errmsg("Task file \"%s\" does not have expected suffix "
									 "\"%s\"", baseFilename, expectedFileSuffix->data)));
			continue;
		}

		StringInfo fullFilename = makeStringInfo();
		appendStringInfo(fullFilename, "%s/%s", directoryName, baseFilename);

		/* build relation object and copy statement */
		RangeVar *relation = makeRangeVar(schemaName->data, relationName->data, -1);
		CopyStmt *copyStatement = CopyStatement(relation, fullFilename->data);
		if (BinaryWorkerCopyFormat)
		{
			DefElem *copyOption = makeDefElem("format", (Node *) makeString("binary"),
											  -1);
			copyStatement->options = list_make1(copyOption);
		}

		{
			ParseState *pstate = make_parsestate(NULL);
			pstate->p_sourcetext = queryString;

			DoCopy(pstate, copyStatement, -1, -1, &copiedRowCount);

			free_parsestate(pstate);
		}

		copiedRowTotal += copiedRowCount;
		CommandCounterIncrement();
	}

	ereport(DEBUG2, (errmsg("copied " UINT64_FORMAT " rows into table: \"%s.%s\"",
							copiedRowTotal, schemaName->data, relationName->data)));

	FreeDir(directory);
}


/*
 * CopyStatement creates and initializes a copy statement to read the given
 * file's contents into the given table, using copy's standard text format.
 */
CopyStmt *
CopyStatement(RangeVar *relation, char *sourceFilename)
{
	CopyStmt *copyStatement = makeNode(CopyStmt);
	copyStatement->relation = relation;
	copyStatement->query = NULL;
	copyStatement->attlist = NIL;
	copyStatement->options = NIL;
	copyStatement->is_from = true;
	copyStatement->is_program = false;
	copyStatement->filename = sourceFilename;

	return copyStatement;
}
