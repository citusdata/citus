/*-------------------------------------------------------------------------
 *
 * node_protocol.c
 *	  Routines for requesting information from the master node for creating or
 *	  updating shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include <string.h>

#include "access/attnum.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "commands/sequence.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/namespace_utils.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/varlena.h"

#include "columnar/cstore_tableam.h"

/* Shard related configuration */
int ShardCount = 32;
int ShardReplicationFactor = 1; /* desired replication factor for shards */
int ShardMaxSize = 1048576;     /* maximum size in KB one shard can grow to */
int ShardPlacementPolicy = SHARD_PLACEMENT_ROUND_ROBIN;
int NextShardId = 0;
int NextPlacementId = 0;

static List * GetTableReplicaIdentityCommand(Oid relationId);
static Datum WorkerNodeGetDatum(WorkerNode *workerNode, TupleDesc tupleDescriptor);
static void GatherIndexAndConstraintDefinitionList(Form_pg_index indexForm,
												   List **indexDDLEventList);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_get_table_metadata);
PG_FUNCTION_INFO_V1(master_get_table_ddl_events);
PG_FUNCTION_INFO_V1(master_get_new_shardid);
PG_FUNCTION_INFO_V1(master_get_new_placementid);
PG_FUNCTION_INFO_V1(master_get_active_worker_nodes);
PG_FUNCTION_INFO_V1(master_get_round_robin_candidate_nodes);
PG_FUNCTION_INFO_V1(master_stage_shard_row);
PG_FUNCTION_INFO_V1(master_stage_shard_placement_row);


/*
 * master_get_table_metadata takes in a relation name, and returns partition
 * related metadata for the relation. These metadata are grouped and returned in
 * a tuple, and are used by the caller when creating new shards. The function
 * errors if given relation does not exist, or is not partitioned.
 */
Datum
master_get_table_metadata(PG_FUNCTION_ARGS)
{
	text *relationName = PG_GETARG_TEXT_P(0);
	Oid relationId = ResolveRelationId(relationName, false);

	Datum partitionKeyExpr = 0;
	Datum partitionKey = 0;
	TupleDesc metadataDescriptor = NULL;
	Datum values[TABLE_METADATA_FIELDS];
	bool isNulls[TABLE_METADATA_FIELDS];

	CheckCitusVersion(ERROR);

	/* find partition tuple for partitioned relation */
	CitusTableCacheEntry *partitionEntry = GetCitusTableCacheEntry(relationId);

	/* create tuple descriptor for return value */
	TypeFuncClass resultTypeClass = get_call_result_type(fcinfo, NULL,
														 &metadataDescriptor);
	if (resultTypeClass != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR, (errmsg("return type must be a row type")));
	}

	/* form heap tuple for table metadata */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	char *partitionKeyString = partitionEntry->partitionKeyString;

	/* reference tables do not have partition key */
	if (partitionKeyString == NULL)
	{
		partitionKey = PointerGetDatum(NULL);
		isNulls[3] = true;
	}
	else
	{
		/* get decompiled expression tree for partition key */
		partitionKeyExpr =
			PointerGetDatum(cstring_to_text(partitionEntry->partitionKeyString));
		partitionKey = DirectFunctionCall2(pg_get_expr, partitionKeyExpr,
										   ObjectIdGetDatum(relationId));
	}

	uint64 shardMaxSizeInBytes = (int64) ShardMaxSize * 1024L;

	/* get storage type */
	char shardStorageType = ShardStorageType(relationId);

	values[0] = ObjectIdGetDatum(relationId);
	values[1] = shardStorageType;
	values[2] = partitionEntry->partitionMethod;
	values[3] = partitionKey;
	values[4] = Int32GetDatum(ShardReplicationFactor);
	values[5] = Int64GetDatum(shardMaxSizeInBytes);
	values[6] = Int32GetDatum(ShardPlacementPolicy);

	HeapTuple metadataTuple = heap_form_tuple(metadataDescriptor, values, isNulls);
	Datum metadataDatum = HeapTupleGetDatum(metadataTuple);

	PG_RETURN_DATUM(metadataDatum);
}


/*
 * CStoreTable returns true if the given relationId belongs to a foreign cstore
 * table, otherwise it returns false.
 */
bool
CStoreTable(Oid relationId)
{
	bool cstoreTable = false;

	char relationKind = get_rel_relkind(relationId);
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ForeignTable *foreignTable = GetForeignTable(relationId);
		ForeignServer *server = GetForeignServer(foreignTable->serverid);
		ForeignDataWrapper *foreignDataWrapper = GetForeignDataWrapper(server->fdwid);

		if (strncmp(foreignDataWrapper->fdwname, CSTORE_FDW_NAME, NAMEDATALEN) == 0)
		{
			cstoreTable = true;
		}
	}

	return cstoreTable;
}


/*
 * master_get_table_ddl_events takes in a relation name, and returns the set of
 * DDL commands needed to reconstruct the relation. The returned DDL commands
 * are similar in flavor to schema definitions that pgdump returns. The function
 * errors if given relation does not exist.
 */
Datum
master_get_table_ddl_events(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	ListCell *tableDDLEventCell = NULL;

	CheckCitusVersion(ERROR);

	/*
	 * On the very first call to this function, we first use the given relation
	 * name to get to the relation. We then recreate the list of DDL statements
	 * issued for this relation, and save the first statement's position in the
	 * function context.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		text *relationName = PG_GETARG_TEXT_P(0);
		Oid relationId = ResolveRelationId(relationName, false);
		bool includeSequenceDefaults = true;


		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		MemoryContext oldContext = MemoryContextSwitchTo(
			functionContext->multi_call_memory_ctx);

		/* allocate DDL statements, and then save position in DDL statements */
		List *tableDDLEventList = GetFullTableCreationCommands(relationId,
															   includeSequenceDefaults);
		tableDDLEventCell = list_head(tableDDLEventList);
		ListCellAndListWrapper *wrapper = palloc0(sizeof(ListCellAndListWrapper));
		wrapper->list = tableDDLEventList;
		wrapper->listCell = tableDDLEventCell;
		functionContext->user_fctx = wrapper;

		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * On every call to this function, we get the current position in the
	 * statement list. We then iterate to the next position in the list and
	 * return the current statement, if we have not yet reached the end of
	 * list.
	 */
	functionContext = SRF_PERCALL_SETUP();

	ListCellAndListWrapper *wrapper =
		(ListCellAndListWrapper *) functionContext->user_fctx;
	if (wrapper->listCell != NULL)
	{
		TableDDLCommand *ddlStatement = (TableDDLCommand *) lfirst(wrapper->listCell);
		Assert(CitusIsA(ddlStatement, TableDDLCommand));
		text *ddlStatementText = cstring_to_text(GetTableDDLCommand(ddlStatement));

		wrapper->listCell = lnext_compat(wrapper->list, wrapper->listCell);

		SRF_RETURN_NEXT(functionContext, PointerGetDatum(ddlStatementText));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * master_get_new_shardid is a user facing wrapper function around GetNextShardId()
 * which allocates and returns a unique shardId for the shard to be created.
 *
 * NB: This can be called by any user; for now we have decided that that's
 * ok. We might want to restrict this to users part of a specific role or such
 * at some later point.
 */
Datum
master_get_new_shardid(PG_FUNCTION_ARGS)
{
	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	uint64 shardId = GetNextShardId();
	Datum shardIdDatum = Int64GetDatum(shardId);

	PG_RETURN_DATUM(shardIdDatum);
}


/*
 * GetNextShardId allocates and returns a unique shardId for the shard to be
 * created. This allocation occurs both in shared memory and in write ahead
 * logs; writing to logs avoids the risk of having shardId collisions.
 *
 * Please note that the caller is still responsible for finalizing shard data
 * and the shardId with the master node.
 */
uint64
GetNextShardId()
{
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	uint64 shardId = 0;

	/*
	 * In regression tests, we would like to generate shard IDs consistently
	 * even if the tests run in parallel. Instead of the sequence, we can use
	 * the next_shard_id GUC to specify which shard ID the current session should
	 * generate next. The GUC is automatically increased by 1 every time a new
	 * shard ID is generated.
	 */
	if (NextShardId > 0)
	{
		shardId = NextShardId;
		NextShardId += 1;

		return shardId;
	}

	text *sequenceName = cstring_to_text(SHARDID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	Datum shardIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	shardId = DatumGetInt64(shardIdDatum);

	return shardId;
}


/*
 * master_get_new_placementid is a user facing wrapper function around
 * GetNextPlacementId() which allocates and returns a unique placement id for the
 * placement to be created.
 *
 * NB: This can be called by any user; for now we have decided that that's
 * ok. We might want to restrict this to users part of a specific role or such
 * at some later point.
 */
Datum
master_get_new_placementid(PG_FUNCTION_ARGS)
{
	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	uint64 placementId = GetNextPlacementId();
	Datum placementIdDatum = Int64GetDatum(placementId);

	PG_RETURN_DATUM(placementIdDatum);
}


/*
 * GetNextPlacementId allocates and returns a unique placementId for
 * the placement to be created. This allocation occurs both in shared memory
 * and in write ahead logs; writing to logs avoids the risk of having shardId
 * collisions.
 *
 * NB: This can be called by any user; for now we have decided that that's
 * ok. We might want to restrict this to users part of a specific role or such
 * at some later point.
 */
uint64
GetNextPlacementId(void)
{
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	uint64 placementId = 0;

	/*
	 * In regression tests, we would like to generate placement IDs consistently
	 * even if the tests run in parallel. Instead of the sequence, we can use
	 * the next_placement_id GUC to specify which shard ID the current session
	 * should generate next. The GUC is automatically increased by 1 every time
	 * a new placement ID is generated.
	 */
	if (NextPlacementId > 0)
	{
		placementId = NextPlacementId;
		NextPlacementId += 1;

		return placementId;
	}

	text *sequenceName = cstring_to_text(PLACEMENTID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName, false);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique placement id from sequence */
	Datum placementIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	placementId = DatumGetInt64(placementIdDatum);

	return placementId;
}


/*
 * master_get_round_robin_candidate_nodes is a stub UDF to make pg_upgrade
 * work flawlessly while upgrading servers from 6.1. This implementation
 * will be removed after the UDF dropped on the sql side properly.
 */
Datum
master_get_round_robin_candidate_nodes(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("this function is deprecated and no longer is used")));
}


/*
 * master_stage_shard_row is a stub UDF to make pg_upgrade
 * work flawlessly while upgrading servers from 6.1. This implementation
 * will be removed after the UDF dropped on the sql side properly.
 */
Datum
master_stage_shard_row(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("this function is deprecated and no longer is used")));
}


/*
 * master_stage_shard_placement_row is a stub UDF to make pg_upgrade
 * work flawlessly while upgrading servers from 6.1. This implementation
 * will be removed after the UDF dropped on the sql side properly.
 */
Datum
master_stage_shard_placement_row(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("this function is deprecated and no longer is used")));
}


/*
 * master_get_active_worker_nodes returns a set of active worker host names and
 * port numbers in deterministic order. Currently we assume that all worker
 * nodes in pg_dist_node are active.
 */
Datum
master_get_active_worker_nodes(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	uint32 workerNodeIndex = 0;
	uint32 workerNodeCount = 0;

	CheckCitusVersion(ERROR);

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc tupleDescriptor = NULL;

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		MemoryContext oldContext = MemoryContextSwitchTo(
			functionContext->multi_call_memory_ctx);

		List *workerNodeList = ActiveReadableNonCoordinatorNodeList();
		workerNodeCount = (uint32) list_length(workerNodeList);

		functionContext->user_fctx = workerNodeList;
		functionContext->max_calls = workerNodeCount;

		/*
		 * This tuple descriptor must match the output parameters declared for
		 * the function in pg_proc.
		 */
#if PG_VERSION_NUM < PG_VERSION_12
		tupleDescriptor = CreateTemplateTupleDesc(WORKER_NODE_FIELDS, false);
#else
		tupleDescriptor = CreateTemplateTupleDesc(WORKER_NODE_FIELDS);
#endif
		TupleDescInitEntry(tupleDescriptor, (AttrNumber) 1, "node_name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupleDescriptor, (AttrNumber) 2, "node_port",
						   INT8OID, -1, 0);

		functionContext->tuple_desc = BlessTupleDesc(tupleDescriptor);

		MemoryContextSwitchTo(oldContext);
	}

	functionContext = SRF_PERCALL_SETUP();
	workerNodeIndex = functionContext->call_cntr;
	workerNodeCount = functionContext->max_calls;

	if (workerNodeIndex < workerNodeCount)
	{
		List *workerNodeList = functionContext->user_fctx;
		WorkerNode *workerNode = list_nth(workerNodeList, workerNodeIndex);

		Datum workerNodeDatum = WorkerNodeGetDatum(workerNode,
												   functionContext->tuple_desc);

		SRF_RETURN_NEXT(functionContext, workerNodeDatum);
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/* Finds the relationId from a potentially qualified relation name. */
Oid
ResolveRelationId(text *relationName, bool missingOk)
{
	/* resolve relationId from passed in schema and relation name */
	List *relationNameList = textToQualifiedNameList(relationName);
	RangeVar *relation = makeRangeVarFromNameList(relationNameList);
	Oid relationId = RangeVarGetRelid(relation, NoLock, missingOk);

	return relationId;
}


/*
 * GetFullTableCreationCommands takes in a relationId, includeSequenceDefaults flag,
 * and returns the list of DDL commands needed to reconstruct the relation.
 * When the flag includeSequenceDefaults is set, the function also creates
 * DEFAULT clauses for columns getting their default values from a sequence.
 * These DDL commands are all palloced; and include the table's schema
 * definition, optional column storage and statistics definitions, and index
 * constraint and trigger definitions.
 */
List *
GetFullTableCreationCommands(Oid relationId, bool includeSequenceDefaults)
{
	List *tableDDLEventList = NIL;

	List *preLoadCreationCommandList =
		GetPreLoadTableCreationCommands(relationId, includeSequenceDefaults);

	tableDDLEventList = list_concat(tableDDLEventList, preLoadCreationCommandList);

	List *postLoadCreationCommandList =
		GetPostLoadTableCreationCommands(relationId);

	tableDDLEventList = list_concat(tableDDLEventList, postLoadCreationCommandList);

	return tableDDLEventList;
}


/*
 * GetPostLoadTableCreationCommands takes in a relationId and returns the list
 * of DDL commands that should be applied after loading the data.
 */
List *
GetPostLoadTableCreationCommands(Oid relationId)
{
	List *tableDDLEventList = NIL;

	List *indexAndConstraintCommandList = GetTableIndexAndConstraintCommands(relationId);
	tableDDLEventList = list_concat(tableDDLEventList, indexAndConstraintCommandList);

	List *replicaIdentityEvents = GetTableReplicaIdentityCommand(relationId);
	tableDDLEventList = list_concat(tableDDLEventList, replicaIdentityEvents);

	List *triggerCommands = GetExplicitTriggerCommandList(relationId);
	tableDDLEventList = list_concat(tableDDLEventList, triggerCommands);

	List *statisticsCommands = GetExplicitStatisticsCommandList(relationId);
	tableDDLEventList = list_concat(tableDDLEventList, statisticsCommands);

	return tableDDLEventList;
}


/*
 * GetTableReplicaIdentityCommand returns the list of DDL commands to
 * (re)define the replica identity choice for a given table.
 */
static List *
GetTableReplicaIdentityCommand(Oid relationId)
{
	List *replicaIdentityCreateCommandList = NIL;

	/*
	 * We skip non-relations because postgres does not support
	 * ALTER TABLE .. REPLICA IDENTITY on non-relations.
	 */
	char relationKind = get_rel_relkind(relationId);
	if (relationKind != RELKIND_RELATION)
	{
		return NIL;
	}

	char *replicaIdentityCreateCommand = pg_get_replica_identity_command(relationId);

	if (replicaIdentityCreateCommand)
	{
		replicaIdentityCreateCommandList = lappend(
			replicaIdentityCreateCommandList,
			makeTableDDLCommandString(replicaIdentityCreateCommand));
	}

	return replicaIdentityCreateCommandList;
}


/*
 * GetPreLoadTableCreationCommands takes in a relationId, and returns the list of DDL
 * commands needed to reconstruct the relation, excluding indexes and constraints,
 * to facilitate faster data load.
 */
List *
GetPreLoadTableCreationCommands(Oid relationId, bool includeSequenceDefaults)
{
	List *tableDDLEventList = NIL;

	PushOverrideEmptySearchPath(CurrentMemoryContext);

	/* if foreign table, fetch extension and server definitions */
	char tableType = get_rel_relkind(relationId);
	if (tableType == RELKIND_FOREIGN_TABLE)
	{
		char *extensionDef = pg_get_extensiondef_string(relationId);
		char *serverDef = pg_get_serverdef_string(relationId);

		if (extensionDef != NULL)
		{
			tableDDLEventList = lappend(tableDDLEventList,
										makeTableDDLCommandString(extensionDef));
		}
		tableDDLEventList = lappend(tableDDLEventList,
									makeTableDDLCommandString(serverDef));
	}

	/* fetch table schema and column option definitions */
	char *tableSchemaDef = pg_get_tableschemadef_string(relationId,
														includeSequenceDefaults);
	char *tableColumnOptionsDef = pg_get_tablecolumnoptionsdef_string(relationId);

	tableDDLEventList = lappend(tableDDLEventList, makeTableDDLCommandString(
									tableSchemaDef));
	if (tableColumnOptionsDef != NULL)
	{
		tableDDLEventList = lappend(tableDDLEventList, makeTableDDLCommandString(
										tableColumnOptionsDef));
	}

#if PG_VERSION_NUM >= 120000

	/* add columnar options for cstore tables */
	if (IsCStoreTableAmTable(relationId))
	{
		TableDDLCommand *cstoreOptionsDDL = ColumnarGetTableOptionsDDL(relationId);
		if (cstoreOptionsDDL != NULL)
		{
			tableDDLEventList = lappend(tableDDLEventList, cstoreOptionsDDL);
		}
	}
#endif

	char *tableOwnerDef = TableOwnerResetCommand(relationId);
	if (tableOwnerDef != NULL)
	{
		tableDDLEventList = lappend(tableDDLEventList, makeTableDDLCommandString(
										tableOwnerDef));
	}

	List *policyCommands = CreatePolicyCommands(relationId);
	tableDDLEventList = list_concat(tableDDLEventList, policyCommands);

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return tableDDLEventList;
}


/*
 * GetTableIndexAndConstraintCommands returns the list of DDL commands to
 * (re)create indexes and constraints for a given table.
 */
List *
GetTableIndexAndConstraintCommands(Oid relationId)
{
	return ExecuteFunctionOnEachTableIndex(relationId,
										   GatherIndexAndConstraintDefinitionList);
}


/*
 * GatherIndexAndConstraintDefinitionList adds the DDL command for the given index.
 */
static void
GatherIndexAndConstraintDefinitionList(Form_pg_index indexForm, List **indexDDLEventList)
{
	Oid indexId = indexForm->indexrelid;
	char *statementDef = NULL;

	bool indexImpliedByConstraint = IndexImpliedByAConstraint(indexForm);

	/* get the corresponding constraint or index statement */
	if (indexImpliedByConstraint)
	{
		Oid constraintId = get_index_constraint(indexId);
		Assert(constraintId != InvalidOid);

		statementDef = pg_get_constraintdef_command(constraintId);
	}
	else
	{
		statementDef = pg_get_indexdef_string(indexId);
	}

	/* append found constraint or index definition to the list */
	*indexDDLEventList = lappend(*indexDDLEventList, makeTableDDLCommandString(
									 statementDef));

	/* if table is clustered on this index, append definition to the list */
	if (indexForm->indisclustered)
	{
		char *clusteredDef = pg_get_indexclusterdef_string(indexId);
		Assert(clusteredDef != NULL);

		*indexDDLEventList = lappend(*indexDDLEventList, makeTableDDLCommandString(
										 clusteredDef));
	}
}


/*
 * IndexImpliedByAConstraint is a helper function to be used while scanning
 * pg_index. It returns true if the index identified by the given indexForm is
 * implied by a constraint. Note that caller is responsible for passing a valid
 * indexFrom, which means an alive heap tuple which is of form Form_pg_index.
 */
bool
IndexImpliedByAConstraint(Form_pg_index indexForm)
{
	Assert(indexForm != NULL);

	bool indexImpliedByConstraint = false;

	/*
	 * A primary key index is always created by a constraint statement.
	 * A unique key index or exclusion index is created by a constraint
	 * if and only if the index has a corresponding constraint entry in
	 * pg_depend. Any other index form is never associated with a constraint.
	 */
	if (indexForm->indisprimary)
	{
		indexImpliedByConstraint = true;
	}
	else if (indexForm->indisunique || indexForm->indisexclusion)
	{
		Oid constraintId = get_index_constraint(indexForm->indexrelid);

		indexImpliedByConstraint = OidIsValid(constraintId);
	}

	return indexImpliedByConstraint;
}


/*
 * ShardStorageType returns the shard storage type according to relation type.
 */
char
ShardStorageType(Oid relationId)
{
	char shardStorageType = 0;

	char relationType = get_rel_relkind(relationId);
	if (RegularTable(relationId))
	{
		shardStorageType = SHARD_STORAGE_TABLE;
	}
	else if (relationType == RELKIND_FOREIGN_TABLE)
	{
		bool cstoreTable = CStoreTable(relationId);
		if (cstoreTable)
		{
			shardStorageType = SHARD_STORAGE_COLUMNAR;
		}
		else
		{
			shardStorageType = SHARD_STORAGE_FOREIGN;
		}
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unexpected relation type: %c", relationType)));
	}

	return shardStorageType;
}


/*
 * IsCoordinator function returns true if this node is identified as the
 * schema/coordinator/master node of the cluster.
 */
bool
IsCoordinator(void)
{
	return (GetLocalGroupId() == COORDINATOR_GROUP_ID);
}


/*
 * WorkerNodeGetDatum converts the worker node passed to it into its datum
 * representation. To do this, the function first creates the heap tuple from
 * the worker node name and port. Then, the function converts the heap tuple
 * into a datum and returns it.
 */
static Datum
WorkerNodeGetDatum(WorkerNode *workerNode, TupleDesc tupleDescriptor)
{
	Datum values[WORKER_NODE_FIELDS];
	bool isNulls[WORKER_NODE_FIELDS];

	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[0] = CStringGetTextDatum(workerNode->workerName);
	values[1] = Int64GetDatum((int64) workerNode->workerPort);

	HeapTuple workerNodeTuple = heap_form_tuple(tupleDescriptor, values, isNulls);
	Datum workerNodeDatum = HeapTupleGetDatum(workerNodeTuple);

	return workerNodeDatum;
}


/*
 * DistributedTableReplicationIsEnabled returns true if distributed table shards
 * are replicated according to ShardReplicationFactor.
 */
bool
DistributedTableReplicationIsEnabled()
{
	return (ShardReplicationFactor > 1);
}


/*
 * makeTableDDLCommandString creates a TableDDLCommand based on a constant string. If the
 * TableDDLCommand is turned into a sharded table command the constant will be wrapped in
 * worker_apply_shard_ddl_command with the target shardId. If the command applies to an
 * un-sharded table (eg. mx) the command is applied as is.
 */
TableDDLCommand *
makeTableDDLCommandString(char *commandStr)
{
	TableDDLCommand *command = CitusMakeNode(TableDDLCommand);

	command->type = TABLE_DDL_COMMAND_STRING;
	command->commandStr = commandStr;

	return command;
}


/*
 * makeTableDDLCommandString creates an implementation of TableDDLCommand that creates the
 * final sql command based on function pointers being passed.
 */
TableDDLCommand *
makeTableDDLCommandFunction(TableDDLFunction function,
							TableDDLShardedFunction shardedFunction,
							void *context)
{
	TableDDLCommand *command = CitusMakeNode(TableDDLCommand);

	/*
	 * Function pointers are called later without verifying them not being NULL. Guard
	 * developers from making a mistake with them directly when they could be made.
	 */
	Assert(function != NULL);
	Assert(shardedFunction != NULL);

	command->type = TABLE_DDL_COMMAND_FUNCTION;
	command->function.function = function;
	command->function.shardedFunction = shardedFunction;
	command->function.context = context;

	return command;
}


/*
 * GetShardedTableDDLCommandString is the internal function for TableDDLCommand objects
 * created with makeTableDDLCommandString.
 */
static char *
GetShardedTableDDLCommandString(TableDDLCommand *command, uint64 shardId,
								char *schemaName)
{
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	Assert(command->type == TABLE_DDL_COMMAND_STRING);

	char *escapedDDLCommand = quote_literal_cstr(command->commandStr);

	if (schemaName != NULL && strcmp(schemaName, "public") != 0)
	{
		char *escapedSchemaName = quote_literal_cstr(schemaName);
		appendStringInfo(&buf, WORKER_APPLY_SHARD_DDL_COMMAND, shardId, escapedSchemaName,
						 escapedDDLCommand);
	}
	else
	{
		appendStringInfo(&buf, WORKER_APPLY_SHARD_DDL_COMMAND_WITHOUT_SCHEMA, shardId,
						 escapedDDLCommand);
	}

	return buf.data;
}


/*
 * GetTableDDLCommandString is the internal function for TableDDLCommand objects created
 * with makeTableDDLCommandString to return the non-sharded version of the ddl command.
 */
static char *
GetTableDDLCommandString(TableDDLCommand *command)
{
	Assert(command->type == TABLE_DDL_COMMAND_STRING);
	return command->commandStr;
}


/*
 * GetShardedTableDDLCommand returns the ddl command expressed by this TableDDLCommand
 * where all applicable names are transformed into the names for a shard identified by
 * shardId
 *
 * schemaName is deprecated but used for TableDDLCommandString. All other implementations
 * will need to rely solely on the shardId.
 */
char *
GetShardedTableDDLCommand(TableDDLCommand *command, uint64 shardId, char *schemaName)
{
	switch (command->type)
	{
		case TABLE_DDL_COMMAND_STRING:
		{
			return GetShardedTableDDLCommandString(command, shardId, schemaName);
		}

		case TABLE_DDL_COMMAND_FUNCTION:
		{
			return command->function.shardedFunction(shardId, command->function.context);
		}
	}

	/* unreachable: compiler should warn/error when not all cases are covered above */
	ereport(ERROR, (errmsg("unsupported TableDDLCommand: %d", command->type)));
}


/*
 * GetTableDDLCommand returns the ddl command expressed by this TableDDLCommand where all
 * table names are targeting the base table, not any shards.
 */
char *
GetTableDDLCommand(TableDDLCommand *command)
{
	switch (command->type)
	{
		case TABLE_DDL_COMMAND_STRING:
		{
			return GetTableDDLCommandString(command);
		}

		case TABLE_DDL_COMMAND_FUNCTION:
		{
			return command->function.function(command->function.context);
		}
	}

	/* unreachable: compiler should warn/error when not all cases are covered above */
	ereport(ERROR, (errmsg("unsupported TableDDLCommand: %d", command->type)));
}
