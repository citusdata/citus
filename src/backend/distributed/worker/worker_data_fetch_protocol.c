/*-------------------------------------------------------------------------
 *
 * worker_data_fetch_protocol.c
 *
 * Routines for fetching remote resources from other nodes to this worker node,
 * and materializing these resources on this node if necessary.
 *
 * Copyright (c) Citus Data, Inc.
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
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_server_executor.h"
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
#include "utils/regproc.h"
#include "utils/varlena.h"


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
static bool check_log_statement(List *stmt_list);
static void AlterSequenceMinMax(Oid sequenceId, char *schemaName, char *sequenceName,
								Oid sequenceTypeId);
static void SetDefElemArg(AlterSeqStmt *statement, const char *name, Node *arg);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_fetch_partition_file);
PG_FUNCTION_INFO_V1(worker_apply_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_inter_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_sequence_command);
PG_FUNCTION_INFO_V1(worker_append_table_to_shard);

/*
 * Following UDFs are stub functions, you can check their comments for more
 * detail.
 */
PG_FUNCTION_INFO_V1(worker_fetch_query_results_file);
PG_FUNCTION_INFO_V1(worker_fetch_regular_table);
PG_FUNCTION_INFO_V1(worker_fetch_foreign_file);
PG_FUNCTION_INFO_V1(master_expire_table_cache);


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
	StringInfo taskFilename = UserTaskFilename(taskDirectoryName, partitionTaskId);

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
 * UserTaskFilename returns a full file path for a task file including the
 * current user ID as a suffix.
 */
StringInfo
UserTaskFilename(StringInfo directoryName, uint32 taskId)
{
	StringInfo taskFilename = TaskFilename(directoryName, taskId);

	appendStringInfo(taskFilename, ".%u", GetUserId());

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
	char *userName = CurrentUserName();
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
	appendStringInfo(transmitCommand, TRANSMIT_WITH_USER_COMMAND, remoteFilename->data,
					 quote_literal_cstr(userName));

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
	nodeDatabase = CurrentDatabaseName();

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
		CopyStatus copyStatus = MultiClientCopyData(connectionId, fileDescriptor, NULL);
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
	Oid sequenceTypeId = PG_GETARG_OID(1);
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

	AlterSequenceMinMax(sequenceRelationId, sequenceSchema, sequenceName, sequenceTypeId);

	PG_RETURN_VOID();
}


/*
 * ExtractShardIdFromTableName tries to extract shard id from the given table name,
 * and returns the shard id if table name is formatted as shard name.
 * Else, the function returns INVALID_SHARD_ID.
 */
uint64
ExtractShardIdFromTableName(const char *tableName, bool missingOk)
{
	uint64 shardId = 0;
	char *shardIdString = NULL;
	char *shardIdStringEnd = NULL;

	/* find the last underscore and increment for shardId string */
	shardIdString = strrchr(tableName, SHARD_NAME_SEPARATOR);
	if (shardIdString == NULL && !missingOk)
	{
		ereport(ERROR, (errmsg("could not extract shardId from table name \"%s\"",
							   tableName)));
	}
	else if (shardIdString == NULL && missingOk)
	{
		return INVALID_SHARD_ID;
	}

	shardIdString++;

	errno = 0;
	shardId = pg_strtouint64(shardIdString, &shardIdStringEnd, 0);

	if (errno != 0 || (*shardIdStringEnd != '\0'))
	{
		if (!missingOk)
		{
			ereport(ERROR, (errmsg("could not extract shardId from table name \"%s\"",
								   tableName)));
		}
		else
		{
			return INVALID_SHARD_ID;
		}
	}

	return shardId;
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

	PQclear(result);
	ForgetResults(connection);
	CloseConnection(connection);

	return ddlCommandList;
}


/*
 * Parses the given DDL command, and returns the tree node for parsed command.
 */
Node *
ParseTreeNode(const char *ddlCommand)
{
	Node *parseTreeNode = ParseTreeRawStmt(ddlCommand);

	parseTreeNode = ((RawStmt *) parseTreeNode)->stmt;

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
		ereport(LOG, (errmsg("statement: %s", ApplyLogRedaction(ddlCommand)),
					  errhidestmt(true)));
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
	Oid sourceShardRelationId = InvalidOid;
	Oid sourceSchemaId = InvalidOid;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

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
	shardId = ExtractShardIdFromTableName(shardTableName, false);
	LockShardResource(shardId, AccessExclusiveLock);

	/* copy remote table's data to this node */
	localFilePath = makeStringInfo();
	appendStringInfo(localFilePath, "base/%s/%s" UINT64_FORMAT,
					 PG_JOB_CACHE_DIR, TABLE_FILE_PREFIX, shardId);

	sourceQualifiedName = quote_qualified_identifier(sourceSchemaName, sourceTableName);
	sourceCopyCommand = makeStringInfo();

	/*
	 * Partitioned tables do not support "COPY table TO STDOUT". Thus, we use
	 * "COPY (SELECT * FROM table) TO STDOUT" for partitioned tables.
	 *
	 * If the schema name is not explicitly set, we use the public schema.
	 */
	sourceSchemaName = sourceSchemaName ? sourceSchemaName : "public";
	sourceSchemaId = get_namespace_oid(sourceSchemaName, false);
	sourceShardRelationId = get_relname_relid(sourceTableName, sourceSchemaId);
	if (PartitionedTableNoLock(sourceShardRelationId))
	{
		appendStringInfo(sourceCopyCommand, COPY_SELECT_ALL_OUT_COMMAND,
						 sourceQualifiedName);
	}
	else
	{
		appendStringInfo(sourceCopyCommand, COPY_OUT_COMMAND, sourceQualifiedName);
	}

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

	/* make sure we are allowed to execute the COPY command */
	CheckCopyPermissions(localCopyCommand);

	/* need superuser to copy from files */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	CitusProcessUtility((Node *) localCopyCommand, queryString->data,
						PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

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
 * For serial we only have 32 bits and therefore shift by 28, and for smallserial
 * we only have 16 bits and therefore shift by 12.
 *
 * This is to ensure every group of workers passes out values from a unique range,
 * and therefore that all values generated for the sequence are globally unique.
 */
static void
AlterSequenceMinMax(Oid sequenceId, char *schemaName, char *sequenceName,
					Oid sequenceTypeId)
{
	Form_pg_sequence sequenceData = pg_get_sequencedef(sequenceId);
	int64 startValue = 0;
	int64 maxValue = 0;
	int64 sequenceMaxValue = sequenceData->seqmax;
	int64 sequenceMinValue = sequenceData->seqmin;
	int valueBitLength = 48;

	/* for smaller types, put the group ID into the first 4 bits */
	if (sequenceTypeId == INT4OID)
	{
		valueBitLength = 28;
		sequenceMaxValue = INT_MAX;
	}
	else if (sequenceTypeId == INT2OID)
	{
		valueBitLength = 12;
		sequenceMaxValue = SHRT_MAX;
	}

	/* calculate min/max values that the sequence can generate in this worker */
	startValue = (((int64) GetLocalGroupId()) << valueBitLength) + 1;
	maxValue = startValue + ((int64) 1 << valueBitLength);

	/*
	 * We alter the sequence if the previously set min and max values are not equal to
	 * their correct values.
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

	defElem = makeDefElem((char *) name, arg, -1);

	statement->options = lappend(statement->options, defElem);
}


/*
 * worker_fetch_query_results_file is a stub UDF to allow the function object
 * to be re-created during upgrades. We should keep this around until we drop
 * support for Postgres 11, since Postgres 11 is the highest version for which
 * this object may have been created.
 */
Datum
worker_fetch_query_results_file(PG_FUNCTION_ARGS)
{
	ereport(DEBUG2, (errmsg("this function is deprecated and no longer is used")));
	PG_RETURN_VOID();
}


/*
 * worker_fetch_regular_table UDF is a stub UDF to install Citus flawlessly.
 * Otherwise we need to delete them from our sql files, which is confusing
 */
Datum
worker_fetch_regular_table(PG_FUNCTION_ARGS)
{
	ereport(DEBUG2, (errmsg("this function is deprecated and no longer is used")));
	PG_RETURN_VOID();
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
 * master_expire_table_cache UDF is a stub UDF to install Citus flawlessly.
 * Otherwise we need to delete them from our sql files, which is confusing
 */
Datum
master_expire_table_cache(PG_FUNCTION_ARGS)
{
	ereport(DEBUG2, (errmsg("this function is deprecated and no longer is used")));
	PG_RETURN_VOID();
}
