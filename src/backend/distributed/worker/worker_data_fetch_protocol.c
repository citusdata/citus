/*-------------------------------------------------------------------------
 *
 * worker_data_fetch_protocol.c
 *
 * Routines for fetching remote resources from other nodes to this worker node,
 * and materializing these resources on this node if necessary.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include <unistd.h>
#include <sys/stat.h>

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_management.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_utility.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/task_tracker.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#if (PG_VERSION_NUM >= 100000)
#include "utils/regproc.h"
#include "utils/varlena.h"
#endif


/* Config variable managed via guc.c */
bool ExpireCachedShards = false;


/* Local functions forward declarations */
static void FetchRegularFileAsSuperUser(const char *nodeName, uint32 nodePort,
										StringInfo remoteFilename,
										StringInfo localFilename);
static bool ReceiveRegularFile(const char *nodeName, uint32 nodePort,
							   const char *nodeUser, StringInfo transmitCommand,
							   StringInfo filePath);
static void ReceiveResourceCleanup(int32 connectionId, const char *filename,
								   int32 fileDescriptor);
static void CitusDeleteFile(const char *filename);
static void FetchTableCommon(text *tableName, uint64 remoteTableSize,
							 ArrayType *nodeNameObject, ArrayType *nodePortObject,
							 bool (*FetchTableFunction)(const char *, uint32,
														const char *));
static uint64 LocalTableSize(Oid relationId);
static uint64 ExtractShardId(const char *tableName);
static bool FetchRegularTable(const char *nodeName, uint32 nodePort,
							  const char *tableName);
static bool FetchForeignTable(const char *nodeName, uint32 nodePort,
							  const char *tableName);
static const char * RemoteTableOwner(const char *nodeName, uint32 nodePort,
									 const char *tableName);
static StringInfo ForeignFilePath(const char *nodeName, uint32 nodePort,
								  const char *tableName);
static bool check_log_statement(List *stmt_list);
static void AlterSequenceMinMax(Oid sequenceId, char *schemaName, char *sequenceName);
static void SetDefElemArg(AlterSeqStmt *statement, const char *name, Node *arg);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_fetch_partition_file);
PG_FUNCTION_INFO_V1(worker_fetch_query_results_file);
PG_FUNCTION_INFO_V1(worker_apply_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_inter_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_sequence_command);
PG_FUNCTION_INFO_V1(worker_fetch_regular_table);
PG_FUNCTION_INFO_V1(worker_fetch_foreign_file);
PG_FUNCTION_INFO_V1(worker_append_table_to_shard);


/*
 * worker_fetch_partition_file fetches a partition file from the remote node.
 * The function assumes an upstream compute task depends on this partition file,
 * and therefore directly fetches the file into the upstream task's directory.
 */
Datum
worker_fetch_partition_file(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 partitionTaskId = PG_GETARG_UINT32(1);
	uint32 partitionFileId = PG_GETARG_UINT32(2);
	uint32 upstreamTaskId = PG_GETARG_UINT32(3);
	text *nodeNameText = PG_GETARG_TEXT_P(4);
	uint32 nodePort = PG_GETARG_UINT32(5);
	char *nodeName = NULL;

	/* remote filename is <jobId>/<partitionTaskId>/<partitionFileId> */
	StringInfo remoteDirectoryName = TaskDirectoryName(jobId, partitionTaskId);
	StringInfo remoteFilename = PartitionFilename(remoteDirectoryName, partitionFileId);

	/* local filename is <jobId>/<upstreamTaskId>/<partitionTaskId> */
	StringInfo taskDirectoryName = TaskDirectoryName(jobId, upstreamTaskId);
	StringInfo taskFilename = TaskFilename(taskDirectoryName, partitionTaskId);

	/*
	 * If we are the first function to fetch a file for the upstream task, the
	 * task directory does not exist. We then lock and create the directory.
	 */
	bool taskDirectoryExists = DirectoryExists(taskDirectoryName);

	CheckCitusVersion(ERROR);

	if (!taskDirectoryExists)
	{
		InitTaskDirectory(jobId, upstreamTaskId);
	}

	nodeName = text_to_cstring(nodeNameText);

	/* we've made sure the file names are sanitized, safe to fetch as superuser */
	FetchRegularFileAsSuperUser(nodeName, nodePort, remoteFilename, taskFilename);

	PG_RETURN_VOID();
}


/*
 * worker_fetch_query_results_file fetches a query results file from the remote
 * node. The function assumes an upstream compute task depends on this query
 * results file, and therefore directly fetches the file into the upstream
 * task's directory.
 */
Datum
worker_fetch_query_results_file(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 queryTaskId = PG_GETARG_UINT32(1);
	uint32 upstreamTaskId = PG_GETARG_UINT32(2);
	text *nodeNameText = PG_GETARG_TEXT_P(3);
	uint32 nodePort = PG_GETARG_UINT32(4);
	char *nodeName = NULL;

	/* remote filename is <jobId>/<queryTaskId> */
	StringInfo remoteDirectoryName = JobDirectoryName(jobId);
	StringInfo remoteFilename = TaskFilename(remoteDirectoryName, queryTaskId);

	/* local filename is <jobId>/<upstreamTaskId>/<queryTaskId> */
	StringInfo taskDirectoryName = TaskDirectoryName(jobId, upstreamTaskId);
	StringInfo taskFilename = TaskFilename(taskDirectoryName, queryTaskId);

	/*
	 * If we are the first function to fetch a file for the upstream task, the
	 * task directory does not exist. We then lock and create the directory.
	 */
	bool taskDirectoryExists = DirectoryExists(taskDirectoryName);

	CheckCitusVersion(ERROR);

	if (!taskDirectoryExists)
	{
		InitTaskDirectory(jobId, upstreamTaskId);
	}

	nodeName = text_to_cstring(nodeNameText);

	/* we've made sure the file names are sanitized, safe to fetch as superuser */
	FetchRegularFileAsSuperUser(nodeName, nodePort, remoteFilename, taskFilename);

	PG_RETURN_VOID();
}


/* Constructs a standardized task file path for given directory and task id. */
StringInfo
TaskFilename(StringInfo directoryName, uint32 taskId)
{
	StringInfo taskFilename = makeStringInfo();
	appendStringInfo(taskFilename, "%s/%s%0*u",
					 directoryName->data,
					 TASK_FILE_PREFIX, MIN_TASK_FILENAME_WIDTH, taskId);

	return taskFilename;
}


/*
 * FetchRegularFileAsSuperUser copies a file from a remote node in an idempotent
 * manner. It connects to the remote node as superuser to give file access.
 * Callers must make sure that the file names are sanitized.
 */
static void
FetchRegularFileAsSuperUser(const char *nodeName, uint32 nodePort,
							StringInfo remoteFilename, StringInfo localFilename)
{
	char *nodeUser = NULL;
	StringInfo attemptFilename = NULL;
	StringInfo transmitCommand = NULL;
	uint32 randomId = (uint32) random();
	bool received = false;
	int renamed = 0;

	/*
	 * We create an attempt file to signal that the file is still in transit. We
	 * further append a random id to the filename to handle the unexpected case
	 * of another process concurrently fetching the same file.
	 */
	attemptFilename = makeStringInfo();
	appendStringInfo(attemptFilename, "%s_%0*u%s", localFilename->data,
					 MIN_TASK_FILENAME_WIDTH, randomId, ATTEMPT_FILE_SUFFIX);

	transmitCommand = makeStringInfo();
	appendStringInfo(transmitCommand, TRANSMIT_REGULAR_COMMAND, remoteFilename->data);

	/* connect as superuser to give file access */
	nodeUser = CitusExtensionOwnerName();

	received = ReceiveRegularFile(nodeName, nodePort, nodeUser, transmitCommand,
								  attemptFilename);
	if (!received)
	{
		ereport(ERROR, (errmsg("could not receive file \"%s\" from %s:%u",
							   remoteFilename->data, nodeName, nodePort)));
	}

	/* atomically rename the attempt file */
	renamed = rename(attemptFilename->data, localFilename->data);
	if (renamed != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not rename file \"%s\" to \"%s\": %m",
							   attemptFilename->data, localFilename->data)));
	}
}


/*
 * ReceiveRegularFile creates a local file at the given file path, and connects
 * to remote database that has the given node name and port number. The function
 * then issues the given transmit command using client-side logic (libpq), reads
 * the remote file's contents, and appends these contents to the local file. On
 * success, the function returns success; on failure, it cleans up all resources
 * and returns false.
 */
static bool
ReceiveRegularFile(const char *nodeName, uint32 nodePort, const char *nodeUser,
				   StringInfo transmitCommand, StringInfo filePath)
{
	int32 fileDescriptor = -1;
	char filename[MAXPGPATH];
	int closed = -1;
	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	QueryStatus queryStatus = CLIENT_INVALID_QUERY;
	int32 connectionId = INVALID_CONNECTION_ID;
	char *nodeDatabase = NULL;
	bool querySent = false;
	bool queryReady = false;
	bool copyDone = false;

	/* create local file to append remote data to */
	snprintf(filename, MAXPGPATH, "%s", filePath->data);

	fileDescriptor = BasicOpenFilePerm(filename, fileFlags, fileMode);
	if (fileDescriptor < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
						  errmsg("could not open file \"%s\": %m", filePath->data)));

		return false;
	}

	/* we use the same database name on the master and worker nodes */
	nodeDatabase = get_database_name(MyDatabaseId);

	/* connect to remote node */
	connectionId = MultiClientConnect(nodeName, nodePort, nodeDatabase, nodeUser);
	if (connectionId == INVALID_CONNECTION_ID)
	{
		ReceiveResourceCleanup(connectionId, filename, fileDescriptor);

		return false;
	}

	/* send request to remote node to start transmitting data */
	querySent = MultiClientSendQuery(connectionId, transmitCommand->data);
	if (!querySent)
	{
		ReceiveResourceCleanup(connectionId, filename, fileDescriptor);

		return false;
	}

	/* loop until the remote node acknowledges our transmit request */
	while (!queryReady)
	{
		ResultStatus resultStatus = MultiClientResultStatus(connectionId);
		if (resultStatus == CLIENT_RESULT_READY)
		{
			queryReady = true;
		}
		else if (resultStatus == CLIENT_RESULT_BUSY)
		{
			/* remote node did not respond; wait for longer */
			long sleepIntervalPerCycle = RemoteTaskCheckInterval * 1000L;
			pg_usleep(sleepIntervalPerCycle);
		}
		else
		{
			ReceiveResourceCleanup(connectionId, filename, fileDescriptor);

			return false;
		}
	}

	/* check query response is as expected */
	queryStatus = MultiClientQueryStatus(connectionId);
	if (queryStatus != CLIENT_QUERY_COPY)
	{
		ReceiveResourceCleanup(connectionId, filename, fileDescriptor);

		return false;
	}

	/* loop until we receive and append all the data from remote node */
	while (!copyDone)
	{
		CopyStatus copyStatus = MultiClientCopyData(connectionId, fileDescriptor);
		if (copyStatus == CLIENT_COPY_DONE)
		{
			copyDone = true;
		}
		else if (copyStatus == CLIENT_COPY_MORE)
		{
			/* remote node will continue to send more data */
		}
		else
		{
			ReceiveResourceCleanup(connectionId, filename, fileDescriptor);

			return false;
		}
	}

	/* we are done executing; release the connection and the file handle */
	MultiClientDisconnect(connectionId);

	closed = close(fileDescriptor);
	if (closed < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
						  errmsg("could not close file \"%s\": %m", filename)));

		/* if we failed to close file, try to delete it before erroring out */
		CitusDeleteFile(filename);

		return false;
	}

	/* we successfully received the remote file */
	ereport(DEBUG2, (errmsg("received remote file \"%s\"", filename)));

	return true;
}


/*
 * ReceiveResourceCleanup gets called if an error occurs during file receiving.
 * The function closes the connection, and closes and deletes the local file.
 */
static void
ReceiveResourceCleanup(int32 connectionId, const char *filename, int32 fileDescriptor)
{
	if (connectionId != INVALID_CONNECTION_ID)
	{
		MultiClientDisconnect(connectionId);
	}

	if (fileDescriptor != -1)
	{
		int closed = -1;
		int deleted = -1;

		closed = close(fileDescriptor);
		if (closed < 0)
		{
			ereport(WARNING, (errcode_for_file_access(),
							  errmsg("could not close file \"%s\": %m", filename)));
		}

		deleted = unlink(filename);
		if (deleted != 0)
		{
			ereport(WARNING, (errcode_for_file_access(),
							  errmsg("could not delete file \"%s\": %m", filename)));
		}
	}
}


/* Deletes file with the given filename. */
static void
CitusDeleteFile(const char *filename)
{
	int deleted = unlink(filename);
	if (deleted != 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
						  errmsg("could not delete file \"%s\": %m", filename)));
	}
}


/*
 * worker_apply_shard_ddl_command extends table, index, or constraint names in
 * the given DDL command. The function then applies this extended DDL command
 * against the database.
 */
Datum
worker_apply_shard_ddl_command(PG_FUNCTION_ARGS)
{
	uint64 shardId = PG_GETARG_INT64(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *ddlCommandText = PG_GETARG_TEXT_P(2);

	char *schemaName = text_to_cstring(schemaNameText);
	const char *ddlCommand = text_to_cstring(ddlCommandText);
	Node *ddlCommandNode = ParseTreeNode(ddlCommand);

	CheckCitusVersion(ERROR);

	/* extend names in ddl command and apply extended command */
	RelayEventExtendNames(ddlCommandNode, schemaName, shardId);
	CitusProcessUtility(ddlCommandNode, ddlCommand, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	PG_RETURN_VOID();
}


/*
 * worker_apply_inter_shard_ddl_command extends table, index, or constraint names in
 * the given DDL command. The function then applies this extended DDL command
 * against the database.
 */
Datum
worker_apply_inter_shard_ddl_command(PG_FUNCTION_ARGS)
{
	uint64 leftShardId = PG_GETARG_INT64(0);
	text *leftShardSchemaNameText = PG_GETARG_TEXT_P(1);
	uint64 rightShardId = PG_GETARG_INT64(2);
	text *rightShardSchemaNameText = PG_GETARG_TEXT_P(3);
	text *ddlCommandText = PG_GETARG_TEXT_P(4);

	char *leftShardSchemaName = text_to_cstring(leftShardSchemaNameText);
	char *rightShardSchemaName = text_to_cstring(rightShardSchemaNameText);
	const char *ddlCommand = text_to_cstring(ddlCommandText);
	Node *ddlCommandNode = ParseTreeNode(ddlCommand);

	CheckCitusVersion(ERROR);

	/* extend names in ddl command and apply extended command */
	RelayEventExtendNamesForInterShardCommands(ddlCommandNode, leftShardId,
											   leftShardSchemaName, rightShardId,
											   rightShardSchemaName);
	CitusProcessUtility(ddlCommandNode, ddlCommand, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	PG_RETURN_VOID();
}


/*
 * worker_apply_sequence_command takes a CREATE SEQUENCE command string, runs the
 * CREATE SEQUENCE command then creates and runs an ALTER SEQUENCE statement
 * which adjusts the minvalue and maxvalue of the sequence such that the sequence
 * creates globally unique values.
 */
Datum
worker_apply_sequence_command(PG_FUNCTION_ARGS)
{
	text *commandText = PG_GETARG_TEXT_P(0);
	const char *commandString = text_to_cstring(commandText);
	Node *commandNode = ParseTreeNode(commandString);
	CreateSeqStmt *createSequenceStatement = NULL;
	char *sequenceName = NULL;
	char *sequenceSchema = NULL;
	Oid sequenceRelationId = InvalidOid;

	NodeTag nodeType = nodeTag(commandNode);

	CheckCitusVersion(ERROR);

	if (nodeType != T_CreateSeqStmt)
	{
		ereport(ERROR,
				(errmsg("must call worker_apply_sequence_command with a CREATE"
						" SEQUENCE command string")));
	}

	/* run the CREATE SEQUENCE command */
	CitusProcessUtility(commandNode, commandString, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);
	CommandCounterIncrement();

	createSequenceStatement = (CreateSeqStmt *) commandNode;

	sequenceName = createSequenceStatement->sequence->relname;
	sequenceSchema = createSequenceStatement->sequence->schemaname;
	createSequenceStatement = (CreateSeqStmt *) commandNode;

	sequenceRelationId = RangeVarGetRelid(createSequenceStatement->sequence,
										  AccessShareLock, false);
	Assert(sequenceRelationId != InvalidOid);

	AlterSequenceMinMax(sequenceRelationId, sequenceSchema, sequenceName);

	PG_RETURN_VOID();
}


/*
 * worker_fetch_regular_table caches the given PostgreSQL table on the local
 * node. The function caches this table by trying the given list of node names
 * and node ports in sequential order. On success, the function simply returns.
 */
Datum
worker_fetch_regular_table(PG_FUNCTION_ARGS)
{
	text *regularTableName = PG_GETARG_TEXT_P(0);
	uint64 generationStamp = PG_GETARG_INT64(1);
	ArrayType *nodeNameObject = PG_GETARG_ARRAYTYPE_P(2);
	ArrayType *nodePortObject = PG_GETARG_ARRAYTYPE_P(3);

	CheckCitusVersion(ERROR);

	/*
	 * Run common logic to fetch the remote table, and use the provided function
	 * pointer to perform the actual table fetching.
	 */
	FetchTableCommon(regularTableName, generationStamp, nodeNameObject, nodePortObject,
					 &FetchRegularTable);

	PG_RETURN_VOID();
}


/*
 * worker_fetch_foreign_file caches the given file-backed foreign table on the
 * local node. The function caches this table by trying the given list of node
 * names and node ports in sequential order. On success, the function returns.
 */
Datum
worker_fetch_foreign_file(PG_FUNCTION_ARGS)
{
	text *foreignTableName = PG_GETARG_TEXT_P(0);
	uint64 foreignFileSize = PG_GETARG_INT64(1);
	ArrayType *nodeNameObject = PG_GETARG_ARRAYTYPE_P(2);
	ArrayType *nodePortObject = PG_GETARG_ARRAYTYPE_P(3);

	CheckCitusVersion(ERROR);

	/*
	 * Run common logic to fetch the remote table, and use the provided function
	 * pointer to perform the actual table fetching.
	 */
	FetchTableCommon(foreignTableName, foreignFileSize, nodeNameObject, nodePortObject,
					 &FetchForeignTable);

	PG_RETURN_VOID();
}


/*
 * FetchTableCommon executes common logic that wraps around the actual data
 * fetching function. This common logic includes ensuring that only one process
 * tries to fetch this table at any given time, and that data fetch operations
 * are retried in case of node failures.
 */
static void
FetchTableCommon(text *tableNameText, uint64 remoteTableSize,
				 ArrayType *nodeNameObject, ArrayType *nodePortObject,
				 bool (*FetchTableFunction)(const char *, uint32, const char *))
{
	uint64 shardId = INVALID_SHARD_ID;
	Oid relationId = InvalidOid;
	List *relationNameList = NIL;
	RangeVar *relation = NULL;
	uint32 nodeIndex = 0;
	bool tableFetched = false;
	char *tableName = text_to_cstring(tableNameText);

	Datum *nodeNameArray = DeconstructArrayObject(nodeNameObject);
	Datum *nodePortArray = DeconstructArrayObject(nodePortObject);
	int32 nodeNameCount = ArrayObjectCount(nodeNameObject);
	int32 nodePortCount = ArrayObjectCount(nodePortObject);

	/* we should have the same number of node names and port numbers */
	if (nodeNameCount != nodePortCount)
	{
		ereport(ERROR, (errmsg("node name array size: %d and node port array size: %d"
							   " do not match", nodeNameCount, nodePortCount)));
	}

	/*
	 * We lock on the shardId, but do not unlock. When the function returns, and
	 * the transaction for this function commits, this lock will automatically
	 * be released. This ensures that concurrent caching commands will see the
	 * newly created table when they acquire the lock (in read committed mode).
	 */
	shardId = ExtractShardId(tableName);
	LockShardResource(shardId, AccessExclusiveLock);

	relationNameList = textToQualifiedNameList(tableNameText);
	relation = makeRangeVarFromNameList(relationNameList);
	relationId = RangeVarGetRelid(relation, NoLock, true);

	/* check if we already fetched the table */
	if (relationId != InvalidOid)
	{
		uint64 localTableSize = 0;

		if (!ExpireCachedShards)
		{
			return;
		}

		/*
		 * Check if the cached shard has the same size on disk as it has as on
		 * the placement (is up to date).
		 *
		 * Note 1: performing updates or deletes on the original shard leads to
		 * inconsistent sizes between different databases in which case the data
		 * would be fetched every time, or worse, the placement would get into
		 * a deadlock when it tries to fetch from itself while holding the lock.
		 * Therefore, this option is disabled by default.
		 *
		 * Note 2: when appending data to a shard, the size on disk only
		 * increases when a new page is added (the next 8kB block).
		 */
		localTableSize = LocalTableSize(relationId);

		if (remoteTableSize > localTableSize)
		{
			/* table is not up to date, drop the table */
			ObjectAddress tableObject = { InvalidOid, InvalidOid, 0 };

			tableObject.classId = RelationRelationId;
			tableObject.objectId = relationId;
			tableObject.objectSubId = 0;

			performDeletion(&tableObject, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
		}
		else
		{
			/* table is up to date */
			return;
		}
	}

	/* loop until we fetch the table or try all nodes */
	while (!tableFetched && (nodeIndex < nodeNameCount))
	{
		Datum nodeNameDatum = nodeNameArray[nodeIndex];
		Datum nodePortDatum = nodePortArray[nodeIndex];
		char *nodeName = TextDatumGetCString(nodeNameDatum);
		uint32 nodePort = DatumGetUInt32(nodePortDatum);

		tableFetched = (*FetchTableFunction)(nodeName, nodePort, tableName);

		nodeIndex++;
	}

	/* error out if we tried all nodes and could not fetch the table */
	if (!tableFetched)
	{
		ereport(ERROR, (errmsg("could not fetch relation: \"%s\"", tableName)));
	}
}


/* LocalTableSize returns the size on disk of the given table. */
static uint64
LocalTableSize(Oid relationId)
{
	uint64 tableSize = 0;
	char relationType = 0;
	Datum relationIdDatum = ObjectIdGetDatum(relationId);

	relationType = get_rel_relkind(relationId);
	if (RegularTable(relationId))
	{
		Datum tableSizeDatum = DirectFunctionCall1(pg_table_size, relationIdDatum);

		tableSize = DatumGetInt64(tableSizeDatum);
	}
	else if (relationType == RELKIND_FOREIGN_TABLE)
	{
		bool cstoreTable = CStoreTable(relationId);
		if (cstoreTable)
		{
			/* extract schema name of cstore */
			Oid cstoreId = get_extension_oid(CSTORE_FDW_NAME, false);
			Oid cstoreSchemaOid = get_extension_schema(cstoreId);
			const char *cstoreSchemaName = get_namespace_name(cstoreSchemaOid);

			const int tableSizeArgumentCount = 1;

			Oid tableSizeFunctionOid = FunctionOid(cstoreSchemaName,
												   CSTORE_TABLE_SIZE_FUNCTION_NAME,
												   tableSizeArgumentCount);
			Datum tableSizeDatum = OidFunctionCall1(tableSizeFunctionOid,
													relationIdDatum);

			tableSize = DatumGetInt64(tableSizeDatum);
		}
		else
		{
			char *relationName = get_rel_name(relationId);
			struct stat fileStat;

			int statOK = 0;

			StringInfo localFilePath = makeStringInfo();
			appendStringInfo(localFilePath, FOREIGN_CACHED_FILE_PATH, relationName);

			/* extract the file size using stat, analogous to pg_stat_file */
			statOK = stat(localFilePath->data, &fileStat);
			if (statOK < 0)
			{
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not stat file \"%s\": %m",
									   localFilePath->data)));
			}

			tableSize = (uint64) fileStat.st_size;
		}
	}
	else
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot get size for table \"%s\"", relationName),
						errdetail("Only regular and foreign tables are supported.")));
	}

	return tableSize;
}


/* Extracts shard id from the given table name, and returns it. */
static uint64
ExtractShardId(const char *tableName)
{
	uint64 shardId = 0;
	char *shardIdString = NULL;
	char *shardIdStringEnd = NULL;

	/* find the last underscore and increment for shardId string */
	shardIdString = strrchr(tableName, SHARD_NAME_SEPARATOR);
	if (shardIdString == NULL)
	{
		ereport(ERROR, (errmsg("could not extract shardId from table name \"%s\"",
							   tableName)));
	}
	shardIdString++;

#ifdef HAVE_STRTOULL
	errno = 0;
	shardId = strtoull(shardIdString, &shardIdStringEnd, 0);

	if (errno != 0 || (*shardIdStringEnd != '\0'))
	{
		ereport(ERROR, (errmsg("could not extract shardId from table name \"%s\"",
							   tableName)));
	}
#else
	ereport(ERROR, (errmsg("could not extract shardId from table name"),
					errhint("Your platform does not support strtoull()")));
#endif

	return shardId;
}


/*
 * FetchRegularTable fetches the given table's data using the copy out command.
 * The function then fetches the DDL commands necessary to create this table's
 * replica, and locally applies these DDL commands. Last, the function copies
 * the fetched table data into the created table; and on success, returns true.
 * On failure due to connectivity issues with remote node, the function returns
 * false. On other types of failures, the function errors out.
 */
static bool
FetchRegularTable(const char *nodeName, uint32 nodePort, const char *tableName)
{
	StringInfo localFilePath = NULL;
	StringInfo remoteCopyCommand = NULL;
	List *ddlCommandList = NIL;
	ListCell *ddlCommandCell = NULL;
	CopyStmt *localCopyCommand = NULL;
	RangeVar *localTable = NULL;
	uint64 shardId = 0;
	bool received = false;
	StringInfo queryString = NULL;
	const char *tableOwner = NULL;
	Oid tableOwnerId = InvalidOid;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	List *tableNameList = NIL;

	/* copy remote table's data to this node in an idempotent manner */
	shardId = ExtractShardId(tableName);
	localFilePath = makeStringInfo();
	appendStringInfo(localFilePath, "base/%s/%s" UINT64_FORMAT,
					 PG_JOB_CACHE_DIR, TABLE_FILE_PREFIX, shardId);

	remoteCopyCommand = makeStringInfo();
	appendStringInfo(remoteCopyCommand, COPY_OUT_COMMAND, tableName);

	received = ReceiveRegularFile(nodeName, nodePort, NULL, remoteCopyCommand,
								  localFilePath);
	if (!received)
	{
		return false;
	}

	/* fetch the ddl commands needed to create the table */
	tableOwner = RemoteTableOwner(nodeName, nodePort, tableName);
	if (tableOwner == NULL)
	{
		return false;
	}
	tableOwnerId = get_role_oid(tableOwner, false);

	/* fetch the ddl commands needed to create the table */
	ddlCommandList = TableDDLCommandList(nodeName, nodePort, tableName);
	if (ddlCommandList == NIL)
	{
		return false;
	}

	/*
	 * Apply DDL commands against the database. Note that on failure from here
	 * on, we immediately error out instead of returning false.  Have to do
	 * this as the table's owner to ensure the local table is created with
	 * compatible permissions.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(tableOwnerId, SECURITY_LOCAL_USERID_CHANGE);

	foreach(ddlCommandCell, ddlCommandList)
	{
		StringInfo ddlCommand = (StringInfo) lfirst(ddlCommandCell);
		Node *ddlCommandNode = ParseTreeNode(ddlCommand->data);

		CitusProcessUtility(ddlCommandNode, ddlCommand->data, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
		CommandCounterIncrement();
	}

	/*
	 * Copy local file into the relation. We call ProcessUtility() instead of
	 * directly calling DoCopy() because some extensions (e.g. cstore_fdw) hook
	 * into process utility to provide their custom COPY behavior.
	 */
	tableNameList = stringToQualifiedNameList(tableName);
	localTable = makeRangeVarFromNameList(tableNameList);
	localCopyCommand = CopyStatement(localTable, localFilePath->data);

	queryString = makeStringInfo();
	appendStringInfo(queryString, COPY_IN_COMMAND, tableName, localFilePath->data);

	CitusProcessUtility((Node *) localCopyCommand, queryString->data,
						PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);

	/* finally delete the temporary file we created */
	CitusDeleteFile(localFilePath->data);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return true;
}


/*
 * FetchForeignTable fetches the foreign file for the given table name from the
 * remote node. The function then fetches the DDL commands needed to create the
 * table, and applies these DDL commands locally to create the foreign table.
 * On success, the function returns true. On failure due to connectivity issues
 * with remote node, the function returns false. On failure due to applying DDL
 * commands against the local database, the function errors out.
 */
static bool
FetchForeignTable(const char *nodeName, uint32 nodePort, const char *tableName)
{
	const char *nodeUser = NULL;
	StringInfo localFilePath = NULL;
	StringInfo remoteFilePath = NULL;
	StringInfo transmitCommand = NULL;
	StringInfo alterTableCommand = NULL;
	bool received = false;
	List *ddlCommandList = NIL;
	ListCell *ddlCommandCell = NULL;

	/*
	 * Fetch a foreign file to this node in an idempotent manner. It's OK that
	 * this file name lacks the schema, as the table name will have a shard id
	 * attached to it, which is unique (so conflicts are avoided even if two
	 * tables in different schemas have the same name).
	 */
	localFilePath = makeStringInfo();
	appendStringInfo(localFilePath, FOREIGN_CACHED_FILE_PATH, tableName);

	remoteFilePath = ForeignFilePath(nodeName, nodePort, tableName);
	if (remoteFilePath == NULL)
	{
		return false;
	}

	transmitCommand = makeStringInfo();
	appendStringInfo(transmitCommand, TRANSMIT_REGULAR_COMMAND, remoteFilePath->data);

	/*
	 * We allow some arbitrary input in the file name and connect to the remote
	 * node as superuser to transmit. Therefore, we only allow calling this
	 * function when already running as superuser.
	 */
	EnsureSuperUser();
	nodeUser = CitusExtensionOwnerName();

	received = ReceiveRegularFile(nodeName, nodePort, nodeUser, transmitCommand,
								  localFilePath);
	if (!received)
	{
		return false;
	}

	/* fetch the ddl commands needed to create the table */
	ddlCommandList = TableDDLCommandList(nodeName, nodePort, tableName);
	if (ddlCommandList == NIL)
	{
		return false;
	}

	alterTableCommand = makeStringInfo();
	appendStringInfo(alterTableCommand, SET_FOREIGN_TABLE_FILENAME, tableName,
					 localFilePath->data);

	ddlCommandList = lappend(ddlCommandList, alterTableCommand);

	/*
	 * Apply DDL commands against the database. Note that on failure here, we
	 * immediately error out instead of returning false.
	 */
	foreach(ddlCommandCell, ddlCommandList)
	{
		StringInfo ddlCommand = (StringInfo) lfirst(ddlCommandCell);
		Node *ddlCommandNode = ParseTreeNode(ddlCommand->data);

		CitusProcessUtility(ddlCommandNode, ddlCommand->data, PROCESS_UTILITY_TOPLEVEL,
							NULL, None_Receiver, NULL);
		CommandCounterIncrement();
	}

	return true;
}


/*
 * RemoteTableOwner takes in the given table name, and fetches the owner of
 * the table. If an error occurs during fetching, return NULL.
 */
static const char *
RemoteTableOwner(const char *nodeName, uint32 nodePort, const char *tableName)
{
	List *ownerList = NIL;
	StringInfo queryString = NULL;
	StringInfo relationOwner;
	MultiConnection *connection = NULL;
	uint32 connectionFlag = FORCE_NEW_CONNECTION;
	PGresult *result = NULL;

	queryString = makeStringInfo();
	appendStringInfo(queryString, GET_TABLE_OWNER, tableName);
	connection = GetNodeConnection(connectionFlag, nodeName, nodePort);

	ExecuteOptionalRemoteCommand(connection, queryString->data, &result);

	ownerList = ReadFirstColumnAsText(result);
	if (list_length(ownerList) != 1)
	{
		return NULL;
	}

	relationOwner = (StringInfo) linitial(ownerList);

	return relationOwner->data;
}


/*
 * TableDDLCommandList takes in the given table name, and fetches the list of
 * DDL commands used in creating the table. If an error occurs during fetching,
 * the function returns an empty list.
 */
List *
TableDDLCommandList(const char *nodeName, uint32 nodePort, const char *tableName)
{
	List *ddlCommandList = NIL;
	StringInfo queryString = NULL;
	MultiConnection *connection = NULL;
	PGresult *result = NULL;
	uint32 connectionFlag = FORCE_NEW_CONNECTION;

	queryString = makeStringInfo();
	appendStringInfo(queryString, GET_TABLE_DDL_EVENTS, tableName);
	connection = GetNodeConnection(connectionFlag, nodeName, nodePort);

	ExecuteOptionalRemoteCommand(connection, queryString->data, &result);
	ddlCommandList = ReadFirstColumnAsText(result);

	ForgetResults(connection);
	CloseConnection(connection);

	return ddlCommandList;
}


/*
 * ForeignFilePath takes in the foreign table name, and fetches this table's
 * remote file path. If an error occurs during fetching, the function returns
 * null.
 */
static StringInfo
ForeignFilePath(const char *nodeName, uint32 nodePort, const char *tableName)
{
	List *foreignPathList = NIL;
	StringInfo foreignPathCommand = NULL;
	StringInfo foreignPath = NULL;
	MultiConnection *connection = NULL;
	PGresult *result = NULL;
	int connectionFlag = FORCE_NEW_CONNECTION;

	foreignPathCommand = makeStringInfo();
	appendStringInfo(foreignPathCommand, FOREIGN_FILE_PATH_COMMAND, tableName);
	connection = GetNodeConnection(connectionFlag, nodeName, nodePort);

	ExecuteOptionalRemoteCommand(connection, foreignPathCommand->data, &result);

	foreignPathList = ReadFirstColumnAsText(result);
	if (foreignPathList != NIL)
	{
		foreignPath = (StringInfo) linitial(foreignPathList);
	}

	return foreignPath;
}


/*
 * ExecuteRemoteQuery executes the given query, copies the query's results to a
 * sorted list, and returns this list. The function assumes that query results
 * have a single column, and asserts on that assumption. If results are empty,
 * or an error occurs during query runtime, the function returns an empty list.
 * If asUser is NULL the connection is established as the current user,
 * otherwise as the specified user.
 */
List *
ExecuteRemoteQuery(const char *nodeName, uint32 nodePort, char *runAsUser,
				   StringInfo queryString)
{
	int32 connectionId = -1;
	bool querySent = false;
	bool queryReady = false;
	bool queryOK = false;
	void *queryResult = NULL;
	int rowCount = 0;
	int rowIndex = 0;
	int columnCount = 0;
	List *resultList = NIL;

	connectionId = MultiClientConnect(nodeName, nodePort, NULL, runAsUser);
	if (connectionId == INVALID_CONNECTION_ID)
	{
		return NIL;
	}

	querySent = MultiClientSendQuery(connectionId, queryString->data);
	if (!querySent)
	{
		MultiClientDisconnect(connectionId);
		return NIL;
	}

	while (!queryReady)
	{
		ResultStatus resultStatus = MultiClientResultStatus(connectionId);
		if (resultStatus == CLIENT_RESULT_READY)
		{
			queryReady = true;
		}
		else if (resultStatus == CLIENT_RESULT_BUSY)
		{
			long sleepIntervalPerCycle = RemoteTaskCheckInterval * 1000L;
			pg_usleep(sleepIntervalPerCycle);
		}
		else
		{
			MultiClientDisconnect(connectionId);
			return NIL;
		}
	}

	queryOK = MultiClientQueryResult(connectionId, &queryResult, &rowCount, &columnCount);
	if (!queryOK)
	{
		MultiClientDisconnect(connectionId);
		return NIL;
	}

	for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		const int columnIndex = 0;
		char *rowValue = MultiClientGetValue(queryResult, rowIndex, columnIndex);

		StringInfo rowValueString = makeStringInfo();
		appendStringInfoString(rowValueString, rowValue);

		Assert(columnCount == 1);
		resultList = lappend(resultList, rowValueString);
	}

	MultiClientClearResult(queryResult);
	MultiClientDisconnect(connectionId);

	return resultList;
}


/*
 * Parses the given DDL command, and returns the tree node for parsed command.
 */
Node *
ParseTreeNode(const char *ddlCommand)
{
	Node *parseTreeNode = ParseTreeRawStmt(ddlCommand);

#if (PG_VERSION_NUM >= 100000)
	parseTreeNode = ((RawStmt *) parseTreeNode)->stmt;
#endif

	return parseTreeNode;
}


/*
 * Parses the given DDL command, and returns the tree node for parsed command.
 */
Node *
ParseTreeRawStmt(const char *ddlCommand)
{
	Node *parseTreeNode = NULL;
	List *parseTreeList = NULL;
	uint32 parseTreeCount = 0;

	parseTreeList = pg_parse_query(ddlCommand);

	/* log immediately if dictated by log statement */
	if (check_log_statement(parseTreeList))
	{
		ereport(LOG, (errmsg("statement: %s", ddlCommand), errhidestmt(true)));
	}

	parseTreeCount = list_length(parseTreeList);
	if (parseTreeCount != 1)
	{
		ereport(ERROR, (errmsg("cannot execute multiple utility events")));
	}

	/*
	 * xact.c rejects certain commands that are unsafe to run inside transaction
	 * blocks. Since we only apply commands that relate to creating tables and
	 * those commands are safe, we can safely set the ProcessUtilityContext to
	 * PROCESS_UTILITY_TOPLEVEL.
	 */
	parseTreeNode = (Node *) linitial(parseTreeList);

	return parseTreeNode;
}


/*
 * worker_append_table_to_shard fetches the given remote table's data into the
 * local file system. The function then appends this file data into the given
 * shard.
 */
Datum
worker_append_table_to_shard(PG_FUNCTION_ARGS)
{
	text *shardQualifiedNameText = PG_GETARG_TEXT_P(0);
	text *sourceQualifiedNameText = PG_GETARG_TEXT_P(1);
	text *sourceNodeNameText = PG_GETARG_TEXT_P(2);
	uint32 sourceNodePort = PG_GETARG_UINT32(3);

	List *shardQualifiedNameList = textToQualifiedNameList(shardQualifiedNameText);
	List *sourceQualifiedNameList = textToQualifiedNameList(sourceQualifiedNameText);
	char *sourceNodeName = text_to_cstring(sourceNodeNameText);

	char *shardTableName = NULL;
	char *shardSchemaName = NULL;
	char *shardQualifiedName = NULL;
	char *sourceSchemaName = NULL;
	char *sourceTableName = NULL;
	char *sourceQualifiedName = NULL;

	StringInfo localFilePath = NULL;
	StringInfo sourceCopyCommand = NULL;
	CopyStmt *localCopyCommand = NULL;
	RangeVar *localTable = NULL;
	uint64 shardId = INVALID_SHARD_ID;
	bool received = false;
	StringInfo queryString = NULL;

	CheckCitusVersion(ERROR);

	/* We extract schema names and table names from qualified names */
	DeconstructQualifiedName(shardQualifiedNameList, &shardSchemaName, &shardTableName);

	DeconstructQualifiedName(sourceQualifiedNameList, &sourceSchemaName,
							 &sourceTableName);

	/*
	 * We lock on the shardId, but do not unlock. When the function returns, and
	 * the transaction for this function commits, this lock will automatically
	 * be released. This ensures appends to a shard happen in a serial manner.
	 */
	shardId = ExtractShardId(shardTableName);
	LockShardResource(shardId, AccessExclusiveLock);

	/* copy remote table's data to this node */
	localFilePath = makeStringInfo();
	appendStringInfo(localFilePath, "base/%s/%s" UINT64_FORMAT,
					 PG_JOB_CACHE_DIR, TABLE_FILE_PREFIX, shardId);

	sourceQualifiedName = quote_qualified_identifier(sourceSchemaName, sourceTableName);
	sourceCopyCommand = makeStringInfo();
	appendStringInfo(sourceCopyCommand, COPY_OUT_COMMAND, sourceQualifiedName);

	received = ReceiveRegularFile(sourceNodeName, sourceNodePort, NULL, sourceCopyCommand,
								  localFilePath);
	if (!received)
	{
		ereport(ERROR, (errmsg("could not copy table \"%s\" from \"%s:%u\"",
							   sourceTableName, sourceNodeName, sourceNodePort)));
	}

	/* copy local file into the given shard */
	localTable = makeRangeVar(shardSchemaName, shardTableName, -1);
	localCopyCommand = CopyStatement(localTable, localFilePath->data);

	shardQualifiedName = quote_qualified_identifier(shardSchemaName,
													shardTableName);

	queryString = makeStringInfo();
	appendStringInfo(queryString, COPY_IN_COMMAND, shardQualifiedName,
					 localFilePath->data);

	CitusProcessUtility((Node *) localCopyCommand, queryString->data,
						PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);

	/* finally delete the temporary file we created */
	CitusDeleteFile(localFilePath->data);

	PG_RETURN_VOID();
}


/*
 * check_log_statement is a copy of postgres' check_log_statement function and
 * returns whether a statement ought to be logged or not.
 */
static bool
check_log_statement(List *statementList)
{
	ListCell *statementCell;

	if (log_statement == LOGSTMT_NONE)
	{
		return false;
	}

	if (log_statement == LOGSTMT_ALL)
	{
		return true;
	}

	/* else we have to inspect the statement(s) to see whether to log */
	foreach(statementCell, statementList)
	{
		Node *statement = (Node *) lfirst(statementCell);

		if (GetCommandLogLevel(statement) <= log_statement)
		{
			return true;
		}
	}

	return false;
}


/*
 * AlterSequenceMinMax arranges the min and max value of the given sequence. The function
 * creates ALTER SEQUENCE statemenet which sets the start, minvalue and maxvalue of
 * the given sequence.
 *
 * The function provides the uniqueness by shifting the start of the sequence by
 * GetLocalGroupId() << 48 + 1 and sets a maxvalue which stops it from passing out any
 * values greater than: (GetLocalGroupID() + 1) << 48.
 *
 * This is to ensure every group of workers passes out values from a unique range,
 * and therefore that all values generated for the sequence are globally unique.
 */
static void
AlterSequenceMinMax(Oid sequenceId, char *schemaName, char *sequenceName)
{
	Form_pg_sequence sequenceData = pg_get_sequencedef(sequenceId);
	int64 startValue = 0;
	int64 maxValue = 0;
#if (PG_VERSION_NUM >= 100000)
	int64 sequenceMaxValue = sequenceData->seqmax;
	int64 sequenceMinValue = sequenceData->seqmin;
#else
	int64 sequenceMaxValue = sequenceData->max_value;
	int64 sequenceMinValue = sequenceData->min_value;
#endif


	/* calculate min/max values that the sequence can generate in this worker */
	startValue = (((int64) GetLocalGroupId()) << 48) + 1;
	maxValue = startValue + ((int64) 1 << 48);

	/*
	 * We alter the sequence if the previously set min and max values are not equal to
	 * their correct values. This happens when the sequence has been created
	 * during shard, before the current worker having the metadata.
	 */
	if (sequenceMinValue != startValue || sequenceMaxValue != maxValue)
	{
		StringInfo startNumericString = makeStringInfo();
		StringInfo maxNumericString = makeStringInfo();
		Node *startFloatArg = NULL;
		Node *maxFloatArg = NULL;
		AlterSeqStmt *alterSequenceStatement = makeNode(AlterSeqStmt);
		const char *dummyString = "-";

		alterSequenceStatement->sequence = makeRangeVar(schemaName, sequenceName, -1);

		/*
		 * DefElem->arg can only hold literal ints up to int4, in order to represent
		 * larger numbers we need to construct a float represented as a string.
		 */
		appendStringInfo(startNumericString, INT64_FORMAT, startValue);
		startFloatArg = (Node *) makeFloat(startNumericString->data);

		appendStringInfo(maxNumericString, INT64_FORMAT, maxValue);
		maxFloatArg = (Node *) makeFloat(maxNumericString->data);

		SetDefElemArg(alterSequenceStatement, "start", startFloatArg);
		SetDefElemArg(alterSequenceStatement, "minvalue", startFloatArg);
		SetDefElemArg(alterSequenceStatement, "maxvalue", maxFloatArg);

		SetDefElemArg(alterSequenceStatement, "restart", startFloatArg);

		/* since the command is an AlterSeqStmt, a dummy command string works fine */
		CitusProcessUtility((Node *) alterSequenceStatement, dummyString,
							PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);
	}
}


/*
 * SetDefElemArg scans through all the DefElem's of an AlterSeqStmt and
 * and sets the arg of the one with a defname of name to arg.
 *
 * If a DefElem with the given defname does not exist it is created and
 * added to the AlterSeqStmt.
 */
static void
SetDefElemArg(AlterSeqStmt *statement, const char *name, Node *arg)
{
	DefElem *defElem = NULL;
	ListCell *optionCell = NULL;

	foreach(optionCell, statement->options)
	{
		defElem = (DefElem *) lfirst(optionCell);

		if (strcmp(defElem->defname, name) == 0)
		{
			pfree(defElem->arg);
			defElem->arg = arg;
			return;
		}
	}

#if (PG_VERSION_NUM >= 100000)
	defElem = makeDefElem((char *) name, arg, -1);
#else
	defElem = makeDefElem((char *) name, arg);
#endif

	statement->options = lappend(statement->options, defElem);
}
