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
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_server_executor.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"

#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/varlena.h"


/* Local functions forward declarations */
static bool ReceiveRegularFile(const char *nodeName, uint32 nodePort,
							   const char *nodeUser, StringInfo transmitCommand,
							   StringInfo filePath);
static void ReceiveResourceCleanup(int32 connectionId, const char *filename,
								   int32 fileDescriptor);
static CopyStmt * CopyStatement(RangeVar *relation, char *sourceFilename);
static void CitusDeleteFile(const char *filename);
static bool check_log_statement(List *stmt_list);
static void AlterSequenceMinMax(Oid sequenceId, char *schemaName, char *sequenceName,
								Oid sequenceTypeId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_apply_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_inter_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_sequence_command);
PG_FUNCTION_INFO_V1(worker_append_table_to_shard);
PG_FUNCTION_INFO_V1(worker_nextval);


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
	char filename[MAXPGPATH];
	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	bool queryReady = false;
	bool copyDone = false;

	/* create local file to append remote data to */
	strlcpy(filename, filePath->data, MAXPGPATH);

	int32 fileDescriptor = BasicOpenFilePerm(filename, fileFlags, fileMode);
	if (fileDescriptor < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
						  errmsg("could not open file \"%s\": %m", filePath->data)));

		return false;
	}

	/* we use the same database name on the master and worker nodes */
	const char *nodeDatabase = CurrentDatabaseName();

	/* connect to remote node */
	int32 connectionId = MultiClientConnect(nodeName, nodePort, nodeDatabase, nodeUser);
	if (connectionId == INVALID_CONNECTION_ID)
	{
		ReceiveResourceCleanup(connectionId, filename, fileDescriptor);

		return false;
	}

	/* send request to remote node to start transmitting data */
	bool querySent = MultiClientSendQuery(connectionId, transmitCommand->data);
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
	QueryStatus queryStatus = MultiClientQueryStatus(connectionId);
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

	int closed = close(fileDescriptor);
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
		int closed = close(fileDescriptor);
		if (closed < 0)
		{
			ereport(WARNING, (errcode_for_file_access(),
							  errmsg("could not close file \"%s\": %m", filename)));
		}

		int deleted = unlink(filename);
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
	CheckCitusVersion(ERROR);

	uint64 shardId = PG_GETARG_INT64(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *ddlCommandText = PG_GETARG_TEXT_P(2);

	char *schemaName = text_to_cstring(schemaNameText);
	const char *ddlCommand = text_to_cstring(ddlCommandText);
	Node *ddlCommandNode = ParseTreeNode(ddlCommand);

	/* extend names in ddl command and apply extended command */
	RelayEventExtendNames(ddlCommandNode, schemaName, shardId);
	ProcessUtilityParseTree(ddlCommandNode, ddlCommand, PROCESS_UTILITY_QUERY, NULL,
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
	CheckCitusVersion(ERROR);

	uint64 leftShardId = PG_GETARG_INT64(0);
	text *leftShardSchemaNameText = PG_GETARG_TEXT_P(1);
	uint64 rightShardId = PG_GETARG_INT64(2);
	text *rightShardSchemaNameText = PG_GETARG_TEXT_P(3);
	text *ddlCommandText = PG_GETARG_TEXT_P(4);

	char *leftShardSchemaName = text_to_cstring(leftShardSchemaNameText);
	char *rightShardSchemaName = text_to_cstring(rightShardSchemaNameText);
	const char *ddlCommand = text_to_cstring(ddlCommandText);
	Node *ddlCommandNode = ParseTreeNode(ddlCommand);

	/* extend names in ddl command and apply extended command */
	RelayEventExtendNamesForInterShardCommands(ddlCommandNode, leftShardId,
											   leftShardSchemaName, rightShardId,
											   rightShardSchemaName);
	ProcessUtilityParseTree(ddlCommandNode, ddlCommand, PROCESS_UTILITY_QUERY, NULL,
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
	CheckCitusVersion(ERROR);

	text *commandText = PG_GETARG_TEXT_P(0);
	Oid sequenceTypeId = PG_GETARG_OID(1);
	const char *commandString = text_to_cstring(commandText);
	Node *commandNode = ParseTreeNode(commandString);

	NodeTag nodeType = nodeTag(commandNode);

	if (nodeType != T_CreateSeqStmt)
	{
		ereport(ERROR,
				(errmsg("must call worker_apply_sequence_command with a CREATE"
						" SEQUENCE command string")));
	}

	/*
	 * If sequence with the same name exist for different type, it must have been
	 * stayed on that node after a rollbacked create_distributed_table operation.
	 * We must change it's name first to create the sequence with the correct type.
	 */
	CreateSeqStmt *createSequenceStatement = (CreateSeqStmt *) commandNode;
	RenameExistingSequenceWithDifferentTypeIfExists(createSequenceStatement->sequence,
													sequenceTypeId);

	/* run the CREATE SEQUENCE command */
	ProcessUtilityParseTree(commandNode, commandString, PROCESS_UTILITY_QUERY, NULL,
							None_Receiver, NULL);
	CommandCounterIncrement();

	Oid sequenceRelationId = RangeVarGetRelid(createSequenceStatement->sequence,
											  AccessShareLock, false);
	char *sequenceName = createSequenceStatement->sequence->relname;
	char *sequenceSchema = createSequenceStatement->sequence->schemaname;

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
	char *shardIdStringEnd = NULL;

	/* find the last underscore and increment for shardId string */
	char *shardIdString = strrchr(tableName, SHARD_NAME_SEPARATOR);
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
	uint64 shardId = strtou64(shardIdString, &shardIdStringEnd, 0);

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
	List *parseTreeList = pg_parse_query(ddlCommand);

	/* log immediately if dictated by log statement */
	if (check_log_statement(parseTreeList))
	{
		ereport(LOG, (errmsg("statement: %s", ApplyLogRedaction(ddlCommand)),
					  errhidestmt(true)));
	}

	uint32 parseTreeCount = list_length(parseTreeList);
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
	Node *parseTreeNode = (Node *) linitial(parseTreeList);

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
	CheckCitusVersion(ERROR);

	text *shardQualifiedNameText = PG_GETARG_TEXT_P(0);
	text *sourceQualifiedNameText = PG_GETARG_TEXT_P(1);
	text *sourceNodeNameText = PG_GETARG_TEXT_P(2);
	uint32 sourceNodePort = PG_GETARG_UINT32(3);

	List *shardQualifiedNameList = textToQualifiedNameList(shardQualifiedNameText);
	List *sourceQualifiedNameList = textToQualifiedNameList(sourceQualifiedNameText);
	char *sourceNodeName = text_to_cstring(sourceNodeNameText);

	char *shardTableName = NULL;
	char *shardSchemaName = NULL;
	char *sourceSchemaName = NULL;
	char *sourceTableName = NULL;

	/* We extract schema names and table names from qualified names */
	DeconstructQualifiedName(shardQualifiedNameList, &shardSchemaName, &shardTableName);

	DeconstructQualifiedName(sourceQualifiedNameList, &sourceSchemaName,
							 &sourceTableName);

	/*
	 * We lock on the shardId, but do not unlock. When the function returns, and
	 * the transaction for this function commits, this lock will automatically
	 * be released. This ensures appends to a shard happen in a serial manner.
	 */
	uint64 shardId = ExtractShardIdFromTableName(shardTableName, false);
	LockShardResource(shardId, AccessExclusiveLock);

	/*
	 * Copy into intermediate results directory, which is automatically cleaned on
	 * error.
	 */
	StringInfo localFilePath = makeStringInfo();
	appendStringInfo(localFilePath, "%s/worker_append_table_to_shard_" UINT64_FORMAT,
					 CreateIntermediateResultsDirectory(), shardId);

	char *sourceQualifiedName = quote_qualified_identifier(sourceSchemaName,
														   sourceTableName);
	StringInfo sourceCopyCommand = makeStringInfo();

	/*
	 * Partitioned tables do not support "COPY table TO STDOUT". Thus, we use
	 * "COPY (SELECT * FROM table) TO STDOUT" for partitioned tables.
	 *
	 * If the schema name is not explicitly set, we use the public schema.
	 */
	sourceSchemaName = sourceSchemaName ? sourceSchemaName : "public";
	Oid sourceSchemaId = get_namespace_oid(sourceSchemaName, false);
	Oid sourceShardRelationId = get_relname_relid(sourceTableName, sourceSchemaId);
	if (PartitionedTableNoLock(sourceShardRelationId))
	{
		appendStringInfo(sourceCopyCommand, COPY_SELECT_ALL_OUT_COMMAND,
						 sourceQualifiedName);
	}
	else
	{
		appendStringInfo(sourceCopyCommand, COPY_OUT_COMMAND, sourceQualifiedName);
	}

	char *userName = CurrentUserName();
	bool received = ReceiveRegularFile(sourceNodeName, sourceNodePort, userName,
									   sourceCopyCommand,
									   localFilePath);
	if (!received)
	{
		ereport(ERROR, (errmsg("could not copy table \"%s\" from \"%s:%u\"",
							   sourceTableName, sourceNodeName, sourceNodePort)));
	}

	/* copy local file into the given shard */
	RangeVar *localTable = makeRangeVar(shardSchemaName, shardTableName, -1);
	CopyStmt *localCopyCommand = CopyStatement(localTable, localFilePath->data);

	char *shardQualifiedName = quote_qualified_identifier(shardSchemaName,
														  shardTableName);

	StringInfo queryString = makeStringInfo();
	appendStringInfo(queryString, COPY_IN_COMMAND, shardQualifiedName,
					 localFilePath->data);

	/* make sure we are allowed to execute the COPY command */
	CheckCopyPermissions(localCopyCommand);

	Relation shardRelation = table_openrv(localCopyCommand->relation, RowExclusiveLock);

	/* mimic check from copy.c */
	if (XactReadOnly && !shardRelation->rd_islocaltemp)
	{
		PreventCommandIfReadOnly("COPY FROM");
	}

	ParseState *parseState = make_parsestate(NULL);
	(void) addRangeTableEntryForRelation(parseState, shardRelation, RowExclusiveLock,
										 NULL, false, false);

	CopyFromState copyState = BeginCopyFrom_compat(parseState,
												   shardRelation,
												   NULL,
												   localCopyCommand->filename,
												   localCopyCommand->is_program,
												   NULL,
												   localCopyCommand->attlist,
												   localCopyCommand->options);


	CopyFrom(copyState);
	EndCopyFrom(copyState);

	free_parsestate(parseState);

	/* finally delete the temporary file we created */
	CitusDeleteFile(localFilePath->data);
	table_close(shardRelation, NoLock);

	PG_RETURN_VOID();
}


/*
 * CopyStatement creates and initializes a copy statement to read the given
 * file's contents into the given table, using copy's standard text format.
 */
static CopyStmt *
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


/*
 * worker_nextval calculates nextval() in worker nodes
 * for int and smallint column default types
 * TODO: not error out but get the proper nextval()
 */
Datum
worker_nextval(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg(
						"nextval(sequence) calls in worker nodes are not supported"
						" for column defaults of type int or smallint")));
	PG_RETURN_INT32(0);
}


/*
 * check_log_statement is a copy of postgres' check_log_statement function and
 * returns whether a statement ought to be logged or not.
 */
static bool
check_log_statement(List *statementList)
{
	if (log_statement == LOGSTMT_NONE)
	{
		return false;
	}

	if (log_statement == LOGSTMT_ALL)
	{
		return true;
	}

	/* else we have to inspect the statement(s) to see whether to log */
	Node *statement = NULL;
	foreach_ptr(statement, statementList)
	{
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
	int64 sequenceMaxValue = sequenceData->seqmax;
	int64 sequenceMinValue = sequenceData->seqmin;
	int valueBitLength = 48;

	/*
	 * For int and smallint, we don't currently support insertion from workers
	 * Check issue #5126 and PR #5254 for details.
	 * https://github.com/citusdata/citus/issues/5126
	 * So, no need to alter sequence min/max for now
	 * We call setval(sequence, maxvalue) such that manually using
	 * nextval(sequence) in the workers will error out as well.
	 */
	if (sequenceTypeId != INT8OID)
	{
		DirectFunctionCall2(setval_oid,
							ObjectIdGetDatum(sequenceId),
							Int64GetDatum(sequenceMaxValue));
		return;
	}

	/* calculate min/max values that the sequence can generate in this worker */
	int64 startValue = (((int64) GetLocalGroupId()) << valueBitLength) + 1;
	int64 maxValue = startValue + ((int64) 1 << valueBitLength);

	/*
	 * We alter the sequence if the previously set min and max values are not equal to
	 * their correct values.
	 */
	if (sequenceMinValue != startValue || sequenceMaxValue != maxValue)
	{
		StringInfo startNumericString = makeStringInfo();
		StringInfo maxNumericString = makeStringInfo();
		AlterSeqStmt *alterSequenceStatement = makeNode(AlterSeqStmt);
		const char *dummyString = "-";

		alterSequenceStatement->sequence = makeRangeVar(schemaName, sequenceName, -1);

		/*
		 * DefElem->arg can only hold literal ints up to int4, in order to represent
		 * larger numbers we need to construct a float represented as a string.
		 */
		appendStringInfo(startNumericString, INT64_FORMAT, startValue);
		Node *startFloatArg = (Node *) makeFloat(startNumericString->data);

		appendStringInfo(maxNumericString, INT64_FORMAT, maxValue);
		Node *maxFloatArg = (Node *) makeFloat(maxNumericString->data);

		SetDefElemArg(alterSequenceStatement, "start", startFloatArg);
		SetDefElemArg(alterSequenceStatement, "minvalue", startFloatArg);
		SetDefElemArg(alterSequenceStatement, "maxvalue", maxFloatArg);

		SetDefElemArg(alterSequenceStatement, "restart", startFloatArg);

		/* since the command is an AlterSeqStmt, a dummy command string works fine */
		ProcessUtilityParseTree((Node *) alterSequenceStatement, dummyString,
								PROCESS_UTILITY_QUERY, NULL, None_Receiver, NULL);
	}
}


/*
 * SetDefElemArg scans through all the DefElem's of an AlterSeqStmt and
 * and sets the arg of the one with a defname of name to arg.
 *
 * If a DefElem with the given defname does not exist it is created and
 * added to the AlterSeqStmt.
 */
void
SetDefElemArg(AlterSeqStmt *statement, const char *name, Node *arg)
{
	DefElem *defElem = NULL;
	foreach_ptr(defElem, statement->options)
	{
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
