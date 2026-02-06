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

#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/sequence.h"
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

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_server_executor.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/version_compat.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_protocol.h"


/* Local functions forward declarations */
static bool check_log_statement(List *stmt_list);
static void AlterSequenceMinMax(Oid sequenceId, char *schemaName, char *sequenceName,
								Oid sequenceTypeId);
static void SetLocalEnableDDLPropagation(bool state);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_apply_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_inter_shard_ddl_command);
PG_FUNCTION_INFO_V1(worker_apply_sequence_command);
PG_FUNCTION_INFO_V1(worker_adjust_identity_column_seq_ranges);
PG_FUNCTION_INFO_V1(citus_internal_adjust_identity_column_seq_settings);
PG_FUNCTION_INFO_V1(worker_append_table_to_shard);
PG_FUNCTION_INFO_V1(worker_nextval);


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
 * worker_adjust_identity_column_seq_ranges implements the legacy
 * worker_adjust_identity_column_seq_ranges() UDF. It is kept for backward
 * compatibility when an operation is initiated from a node that is not yet
 * upgraded and still assumes the presence of this UDF version on remote nodes,
 * calling it on nodes that have already been upgraded to a newer Citus version.
 * Keeping this implementation allows upgraded nodes to continue responding to
 * such calls.
 */
Datum
worker_adjust_identity_column_seq_ranges(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid tableRelationId = PG_GETARG_OID(0);

	EnsureTableOwner(tableRelationId);

	Relation tableRelation = relation_open(tableRelationId, AccessShareLock);
	TupleDesc tableTupleDesc = RelationGetDescr(tableRelation);

	bool missingSequenceOk = false;

	for (int attributeIndex = 0; attributeIndex < tableTupleDesc->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tableTupleDesc,
														attributeIndex);

		/* skip dropped columns */
		if (attributeForm->attisdropped)
		{
			continue;
		}

		if (attributeForm->attidentity)
		{
			Oid sequenceOid = getIdentitySequence(identitySequenceRelation_compat(
													  tableRelation),
												  attributeForm->attnum,
												  missingSequenceOk);

			Oid sequenceSchemaOid = get_rel_namespace(sequenceOid);
			char *sequenceSchemaName = get_namespace_name(sequenceSchemaOid);
			char *sequenceName = get_rel_name(sequenceOid);
			Oid sequenceTypeId = pg_get_sequencedef(sequenceOid)->seqtypid;

			AlterSequenceMinMax(sequenceOid, sequenceSchemaName, sequenceName,
								sequenceTypeId);
		}
	}

	relation_close(tableRelation, NoLock);

	PG_RETURN_VOID();
}


/*
 * citus_internal_adjust_identity_column_seq_settings takes a sequence oid;
 * and if this's a worker node, then runs an ALTER SEQUENCE statement to adjust
 * the minvalue and maxvalue of it so that the sequence creates globally unique
 * values. When called on coordinator, the function sets the sequence's last value
 * to the given last value.
 */
Datum
citus_internal_adjust_identity_column_seq_settings(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid sequenceId = PG_GETARG_OID(0);
	int64 lastValue = PG_GETARG_INT64(1);

	EnsureTableOwner(sequenceId);

	/*
	 * While altering a sequence, avoid propagating the DDL to other nodes
	 * if it's already marked as distributed.
	 */
	bool oldEnableDDLPropagation = EnableDDLPropagation;
	SetLocalEnableDDLPropagation(false);

	Oid sequenceSchemaOid = get_rel_namespace(sequenceId);
	char *sequenceSchemaName = get_namespace_name(sequenceSchemaOid);
	char *sequenceName = get_rel_name(sequenceId);
	Oid sequenceTypeId = pg_get_sequencedef(sequenceId)->seqtypid;

	if (IsCoordinator())
	{
		DirectFunctionCall2(setval_oid,
							ObjectIdGetDatum(sequenceId),
							Int64GetDatum(lastValue));
	}
	else
	{
		AlterSequenceMinMax(sequenceId, sequenceSchemaName, sequenceName,
							sequenceTypeId);
	}

	SetLocalEnableDDLPropagation(oldEnableDDLPropagation);

	PG_RETURN_VOID();
}


/*
 * AdjustDependentSeqRangesOnLocalNode takes a table oid, finds all sequences
 * the table depends on, and runs AlterSequenceMinMax() for each.
 *
 * Note that this doesn't adjust sequence ranges for identity columns by design,
 * see comments written for the call made to GetAllDependenciesForObject() in
 * the function body for more details.
 */
void
AdjustDependentSeqRangesOnLocalNode(Oid relationId)
{
	/*
	 * While altering a sequence, avoid propagating the DDL to other nodes
	 * if it's already marked as distributed.
	 */
	bool oldEnableDDLPropagation = EnableDDLPropagation;
	SetLocalEnableDDLPropagation(false);

	ObjectAddress address = { 0 };
	ObjectAddressSubSet(address, RelationRelationId, relationId, 0);

	/*
	 * We use GetAllDependenciesForObject() instead of GetDependenciesForObject()
	 * because we want to collect the sequences even if they're already marked
	 * as distributed. This is because, today this function is called after most
	 * of the work to distribute a table is done.
	 *
	 * Also note that as GetAllDependenciesForObject() uses ExpandCitusSupportedTypes(),
	 * while it can capture the sequences used by serial columns, it explicitly
	 * discards sequences used by identity columns.
	 */
	List *dependencies = GetAllDependenciesForObject(&address);
	ObjectAddress *dependency = NULL;
	foreach_declared_ptr(dependency, dependencies)
	{
		if (!getObjectClass(dependency) == OCLASS_CLASS ||
			get_rel_relkind(dependency->objectId) != RELKIND_SEQUENCE)
		{
			continue;
		}

		Oid sequenceOid = dependency->objectId;
		Oid sequenceSchemaOid = get_rel_namespace(sequenceOid);
		char *sequenceSchemaName = get_namespace_name(sequenceSchemaOid);
		char *sequenceName = get_rel_name(sequenceOid);
		Oid sequenceTypeId = pg_get_sequencedef(sequenceOid)->seqtypid;

		AlterSequenceMinMax(sequenceOid, sequenceSchemaName, sequenceName,
							sequenceTypeId);
	}

	SetLocalEnableDDLPropagation(oldEnableDDLPropagation);
}


/*
 * AdjustIdentityColumnSeqRangeOnLocalNode takes a table oid, finds all identity
 * columns on the table, and runs AlterSequenceMinMax() for each underlying sequence.
 */
void
AdjustIdentityColumnSeqRangesOnLocalNode(Oid relationId)
{
	/*
	 * While altering a sequence, avoid propagating the DDL to other nodes
	 * if it's already marked as distributed.
	 */
	bool oldEnableDDLPropagation = EnableDDLPropagation;
	SetLocalEnableDDLPropagation(false);

	Relation tableRelation = relation_open(relationId, AccessShareLock);
	TupleDesc tableTupleDesc = RelationGetDescr(tableRelation);

	bool missingSequenceOk = false;

	for (int attributeIndex = 0; attributeIndex < tableTupleDesc->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tableTupleDesc,
														attributeIndex);

		/* skip dropped columns */
		if (attributeForm->attisdropped)
		{
			continue;
		}

		if (attributeForm->attidentity)
		{
			Oid sequenceOid = getIdentitySequence(identitySequenceRelation_compat(
													  tableRelation),
												  attributeForm->attnum,
												  missingSequenceOk);

			Oid sequenceSchemaOid = get_rel_namespace(sequenceOid);
			char *sequenceSchemaName = get_namespace_name(sequenceSchemaOid);
			char *sequenceName = get_rel_name(sequenceOid);
			Oid sequenceTypeId = pg_get_sequencedef(sequenceOid)->seqtypid;

			AlterSequenceMinMax(sequenceOid, sequenceSchemaName, sequenceName,
								sequenceTypeId);
		}
	}

	relation_close(tableRelation, NoLock);

	SetLocalEnableDDLPropagation(oldEnableDDLPropagation);
}


/*
 * SetNextValColumnDefaultsToWorkerNextValOnLocalNode takes a table oid,
 * finds all int / smallint based columns with nextval() default
 * expressions on the table, and runs an ALTER COLUMN statement for each
 * column to change the default expression to use worker_nextval() instead
 * of nextval().
 */
void
SetNextValColumnDefaultsToWorkerNextValOnLocalNode(Oid relationId)
{
	/*
	 * While altering a sequence, avoid propagating the DDL to other nodes
	 * if it's already marked as distributed.
	 */
	bool oldEnableDDLPropagation = EnableDDLPropagation;
	SetLocalEnableDDLPropagation(false);

	Relation tableRelation = relation_open(relationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(tableRelation);
	relation_close(tableRelation, AccessShareLock);

	TupleConstr *tupleConstraints = tupleDescriptor->constr;
	AttrNumber defaultValueIndex = 0;
	for (int attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
		 attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor,
														attributeIndex);

		if (attributeForm->attisdropped || !attributeForm->atthasdef)
		{
			continue;
		}

		Assert(tupleConstraints != NULL);

		AttrDefault *defaultValueList = tupleConstraints->defval;
		Assert(defaultValueList != NULL);

		AttrDefault *defaultValue = &(defaultValueList[defaultValueIndex]);
		defaultValueIndex++;

		Assert(defaultValue->adnum == (attributeIndex + 1));
		Assert(defaultValueIndex <= tupleConstraints->num_defval);

		if (attributeForm->attgenerated)
		{
			continue;
		}

		/* convert expression to node tree */
		Node *defaultNode = (Node *) stringToNode(defaultValue->adbin);

		if (!contain_nextval_expression_walker(defaultNode, NULL))
		{
			continue;
		}

		Oid seqOid = GetSequenceOid(relationId, defaultValue->adnum);
		if (seqOid == InvalidOid || pg_get_sequencedef(seqOid)->seqtypid == INT8OID)
		{
			continue;
		}

		/*
		 * We use worker_nextval for int and smallint types.
		 * Check issue #5126 and PR #5254 for details.
		 * https://github.com/citusdata/citus/issues/5126
		 */
		bool missingOk = false;
		char *command =
			GetAlterColumnWithNextvalDefaultCmd(seqOid, relationId,
												NameStr(attributeForm->attname),
												missingOk);

		ExecuteAndLogUtilityCommand(command);
	}

	SetLocalEnableDDLPropagation(oldEnableDDLPropagation);
}


/*
 * SetLocalEnableDDLPropagation is simply a C interface for setting
 * the following:
 *      SET LOCAL citus.enable_ddl_propagation = 'on'|'off';
 */
static void
SetLocalEnableDDLPropagation(bool state)
{
	set_config_option("citus.enable_ddl_propagation", state == true ? "on" : "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
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

	/*
	 * Support the legacy version of this UDF. This is for the sake of backward
	 * compatibility when an operation is initiated from a node that is not yet
	 * upgraded and still assumes the presence of the older UDF version on remote nodes,
	 * calling it on nodes that have already been upgraded to a newer Citus version.
	 * Keeping this implementation allows upgraded nodes to continue responding to
	 * such calls.
	 */
	bool lastValueProvided = false;
	int64 lastValue = 0;
	if (PG_NARGS() == 3)
	{
		lastValueProvided = true;
		lastValue = PG_GETARG_INT64(2);
	}

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
	 * We must change its name first to create the sequence with the correct type.
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

	if (IsCoordinator())
	{
		/*
		 * This cannot really happen but still check.
		 *
		 * This is because, in the older versions of Citus, we were never calling
		 * this UDF on the coordinator node. For this reason, if this is executed
		 * against the coordinator, then the node initiating the operation should
		 * actually be assuming the new version of this UDF. In that case, last_value
		 * must always be provided.
		 */
		if (!lastValueProvided)
		{
			ereport(ERROR,
					(errmsg("last value must be provided when adjusting sequence "
							"setting on coordinator")));
		}

		DirectFunctionCall2(setval_oid,
							ObjectIdGetDatum(sequenceRelationId),
							Int64GetDatum(lastValue));
	}
	else
	{
		AlterSequenceMinMax(sequenceRelationId, sequenceSchema, sequenceName,
							sequenceTypeId);
	}


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
		ereport(LOG, (errmsg("statement: %s", ddlCommand),
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
 * worker_append_table_to_shard is deprecated.
 */
Datum
worker_append_table_to_shard(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("worker_append_table_to_shard has been deprecated")));
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
	foreach_declared_ptr(statement, statementList)
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
	foreach_declared_ptr(defElem, statement->options)
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
