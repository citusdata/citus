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

#include <string.h>

#include "postgres.h"

#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

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
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
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

#include "pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/namespace_utils.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/shared_library_init.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"

/* Shard related configuration */
int ShardCount = 32;
int ShardReplicationFactor = 1; /* desired replication factor for shards */
int NextShardId = 0;
int NextPlacementId = 0;

static void GatherIndexAndConstraintDefinitionListExcludingReplicaIdentity(Form_pg_index
																		   indexForm,
																		   List **
																		   indexDDLEventList,
																		   int indexFlags);
static Datum WorkerNodeGetDatum(WorkerNode *workerNode, TupleDesc tupleDescriptor);

static char * CitusCreateAlterColumnarTableSet(char *qualifiedRelationName,
											   const ColumnarOptions *options);
static char * GetTableDDLCommandColumnar(void *context);
static TableDDLCommand * ColumnarGetTableOptionsDDL(Oid relationId);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_get_table_metadata);
PG_FUNCTION_INFO_V1(master_get_table_ddl_events);
PG_FUNCTION_INFO_V1(master_get_new_shardid);
PG_FUNCTION_INFO_V1(master_get_new_placementid);
PG_FUNCTION_INFO_V1(master_get_active_worker_nodes);
PG_FUNCTION_INFO_V1(citus_get_active_worker_nodes);
PG_FUNCTION_INFO_V1(master_get_round_robin_candidate_nodes);
PG_FUNCTION_INFO_V1(master_stage_shard_row);
PG_FUNCTION_INFO_V1(master_stage_shard_placement_row);


/*
 * master_get_table_metadata is a deprecated UDF.
 */
Datum
master_get_table_metadata(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("master_get_table_metadata is deprecated")));
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
	CheckCitusVersion(ERROR);

	FuncCallContext *functionContext = NULL;
	ListCell *tableDDLEventCell = NULL;

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
		IncludeSequenceDefaults includeSequenceDefaults = NEXTVAL_SEQUENCE_DEFAULTS;
		IncludeIdentities includeIdentityDefaults = INCLUDE_IDENTITY;


		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		MemoryContext oldContext = MemoryContextSwitchTo(
			functionContext->multi_call_memory_ctx);

		/* allocate DDL statements, and then save position in DDL statements */
		bool creatingShellTableOnRemoteNode = false;
		List *tableDDLEventList = GetFullTableCreationCommands(relationId,
															   includeSequenceDefaults,
															   includeIdentityDefaults,
															   creatingShellTableOnRemoteNode);
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

		wrapper->listCell = lnext(wrapper->list, wrapper->listCell);

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
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

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
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	uint64 placementId = GetNextPlacementId();
	Datum placementIdDatum = Int64GetDatum(placementId);

	PG_RETURN_DATUM(placementIdDatum);
}


/*
 * GetNextPlacementId allocates and returns a unique placementId for
 * the placement to be created. This allocation occurs both in shared memory
 * and in write ahead logs; writing to logs avoids the risk of having placementId
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
 * citus_get_active_worker_nodes returns a set of active worker host names and
 * port numbers in deterministic order. Currently we assume that all worker
 * nodes in pg_dist_node are active.
 */
Datum
citus_get_active_worker_nodes(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	FuncCallContext *functionContext = NULL;
	uint32 workerNodeCount = 0;

	if (SRF_IS_FIRSTCALL())
	{
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
		TupleDesc tupleDescriptor = CreateTemplateTupleDesc(WORKER_NODE_FIELDS);
		TupleDescInitEntry(tupleDescriptor, (AttrNumber) 1, "node_name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupleDescriptor, (AttrNumber) 2, "node_port",
						   INT8OID, -1, 0);

		functionContext->tuple_desc = BlessTupleDesc(tupleDescriptor);

		MemoryContextSwitchTo(oldContext);
	}

	functionContext = SRF_PERCALL_SETUP();
	uint32 workerNodeIndex = functionContext->call_cntr;
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


/*
 * master_get_active_worker_nodes is a wrapper function for old UDF name.
 */
Datum
master_get_active_worker_nodes(PG_FUNCTION_ARGS)
{
	return citus_get_active_worker_nodes(fcinfo);
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
 * GetFullTableCreationCommands takes in a relationId, includeSequenceDefaults,
 * and returns the list of DDL commands needed to reconstruct the relation.
 * When includeSequenceDefaults is NEXTVAL_SEQUENCE_DEFAULTS, the function also creates
 * DEFAULT clauses for columns getting their default values from a sequence.
 * When it's WORKER_NEXTVAL_SEQUENCE_DEFAULTS, the function creates the DEFAULT
 * clause using worker_nextval('sequence') and not nextval('sequence')
 * These DDL commands are all palloced; and include the table's schema
 * definition, optional column storage and statistics definitions, and index
 * constraint and trigger definitions.
 * When IncludeIdentities is NO_IDENTITY, the function does not include identity column
 * specifications. When it's INCLUDE_IDENTITY it creates GENERATED .. AS IDENTIY clauses.
 */
List *
GetFullTableCreationCommands(Oid relationId,
							 IncludeSequenceDefaults includeSequenceDefaults,
							 IncludeIdentities includeIdentityDefaults,
							 bool creatingShellTableOnRemoteNode)
{
	List *tableDDLEventList = NIL;

	List *preLoadCreationCommandList =
		GetPreLoadTableCreationCommands(relationId, includeSequenceDefaults,
										includeIdentityDefaults, NULL);

	tableDDLEventList = list_concat(tableDDLEventList, preLoadCreationCommandList);

	List *postLoadCreationCommandList =
		GetPostLoadTableCreationCommands(relationId, true, true);

	if (creatingShellTableOnRemoteNode)
	{
		/*
		 * While creating shell tables, we need to associate dependencies between
		 * sequences and the relation. We also need to add truncate trigger for it
		 * if it is not the foreign table.
		 */
		List *sequenceDependencyCommandList = SequenceDependencyCommandList(relationId);
		tableDDLEventList = list_concat(tableDDLEventList, sequenceDependencyCommandList);

		if (!IsForeignTable(relationId))
		{
			TableDDLCommand *truncateTriggerCommand = TruncateTriggerCreateCommand(
				relationId);
			tableDDLEventList = lappend(tableDDLEventList,
										truncateTriggerCommand);
		}

		/*
		 * For identity column sequences, we only need to modify
		 * their min/max values to produce unique values on the worker nodes.
		 */
		List *identitySequenceDependencyCommandList =
			IdentitySequenceDependencyCommandList(relationId);
		tableDDLEventList = list_concat(tableDDLEventList,
										identitySequenceDependencyCommandList);
	}

	tableDDLEventList = list_concat(tableDDLEventList, postLoadCreationCommandList);

	return tableDDLEventList;
}


/*
 * GetPostLoadTableCreationCommands takes in a relationId and returns the list
 * of DDL commands that should be applied after loading the data.
 */
List *
GetPostLoadTableCreationCommands(Oid relationId, bool includeIndexes,
								 bool includeReplicaIdentity)
{
	List *tableDDLEventList = NIL;

	/*
	 * Include all the commands (e.g., create index, set index clustered
	 * and set index statistics) regarding the indexes. Note that
	 * running all these commands in parallel might fail as the
	 * latter two depends on the first one. So, the caller should
	 * execute the commands sequentially.
	 */
	int indexFlags = INCLUDE_INDEX_ALL_STATEMENTS;

	if (includeIndexes && includeReplicaIdentity)
	{
		List *indexAndConstraintCommandList =
			GetTableIndexAndConstraintCommands(relationId, indexFlags);
		tableDDLEventList = list_concat(tableDDLEventList, indexAndConstraintCommandList);
	}
	else if (includeIndexes && !includeReplicaIdentity)
	{
		/*
		 * Do not include the indexes/constraints that backs
		 * replica identity, if any.
		 */
		List *indexAndConstraintCommandList =
			GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(relationId,
																	   indexFlags);
		tableDDLEventList = list_concat(tableDDLEventList, indexAndConstraintCommandList);
	}

	if (includeReplicaIdentity)
	{
		List *replicaIdentityEvents = GetTableReplicaIdentityCommand(relationId);
		tableDDLEventList = list_concat(tableDDLEventList, replicaIdentityEvents);
	}

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
List *
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
GetPreLoadTableCreationCommands(Oid relationId,
								IncludeSequenceDefaults includeSequenceDefaults,
								IncludeIdentities includeIdentityDefaults,
								char *accessMethod)
{
	List *tableDDLEventList = NIL;

	int saveNestLevel = PushEmptySearchPath();

	/* fetch table schema and column option definitions */
	char *tableSchemaDef = pg_get_tableschemadef_string(relationId,
														includeSequenceDefaults,
														includeIdentityDefaults,
														accessMethod);
	char *tableColumnOptionsDef = pg_get_tablecolumnoptionsdef_string(relationId);

	tableDDLEventList = lappend(tableDDLEventList, makeTableDDLCommandString(
									tableSchemaDef));
	if (tableColumnOptionsDef != NULL)
	{
		tableDDLEventList = lappend(tableDDLEventList, makeTableDDLCommandString(
										tableColumnOptionsDef));
	}


	/* add columnar options for cstore tables */
	if (accessMethod == NULL && extern_IsColumnarTableAmTable(relationId))
	{
		TableDDLCommand *cstoreOptionsDDL = ColumnarGetTableOptionsDDL(relationId);
		if (cstoreOptionsDDL != NULL)
		{
			tableDDLEventList = lappend(tableDDLEventList, cstoreOptionsDDL);
		}
	}

	List *tableACLList = pg_get_table_grants(relationId);
	if (tableACLList != NIL)
	{
		char *tableACLCommand = NULL;
		foreach_declared_ptr(tableACLCommand, tableACLList)
		{
			tableDDLEventList = lappend(tableDDLEventList,
										makeTableDDLCommandString(tableACLCommand));
		}
	}

	char *tableOwnerDef = TableOwnerResetCommand(relationId);
	if (tableOwnerDef != NULL)
	{
		tableDDLEventList = lappend(tableDDLEventList, makeTableDDLCommandString(
										tableOwnerDef));
	}

	List *tableRowLevelSecurityCommands = GetTableRowLevelSecurityCommands(relationId);
	tableDDLEventList = list_concat(tableDDLEventList, tableRowLevelSecurityCommands);

	List *policyCommands = CreatePolicyCommands(relationId);
	tableDDLEventList = list_concat(tableDDLEventList, policyCommands);

	/* revert back to original search_path */
	PopEmptySearchPath(saveNestLevel);

	return tableDDLEventList;
}


/*
 * GetTableIndexAndConstraintCommands returns the list of DDL commands to
 * (re)create indexes and constraints for a given table.
 */
List *
GetTableIndexAndConstraintCommands(Oid relationId, int indexFlags)
{
	return ExecuteFunctionOnEachTableIndex(relationId,
										   GatherIndexAndConstraintDefinitionList,
										   indexFlags);
}


/*
 * GetTableIndexAndConstraintCommands returns the list of DDL commands to
 * (re)create indexes and constraints for a given table.
 */
List *
GetTableIndexAndConstraintCommandsExcludingReplicaIdentity(Oid relationId, int indexFlags)
{
	return ExecuteFunctionOnEachTableIndex(relationId,
										   GatherIndexAndConstraintDefinitionListExcludingReplicaIdentity,
										   indexFlags);
}


/*
 * GatherIndexAndConstraintDefinitionListExcludingReplicaIdentity is a wrapper around
 * GatherIndexAndConstraintDefinitionList(), which only excludes the indexes or
 * constraints that back the replica identity.
 */
static void
GatherIndexAndConstraintDefinitionListExcludingReplicaIdentity(Form_pg_index indexForm,
															   List **indexDDLEventList,
															   int indexFlags)
{
	Oid relationId = indexForm->indrelid;
	Relation relation = table_open(relationId, AccessShareLock);

	Oid replicaIdentityIndex = GetRelationIdentityOrPK(relation);

	if (replicaIdentityIndex == indexForm->indexrelid)
	{
		/* this index is backing the replica identity, so skip */
		table_close(relation, NoLock);
		return;
	}

	GatherIndexAndConstraintDefinitionList(indexForm, indexDDLEventList, indexFlags);

	table_close(relation, NoLock);
}


/*
 * Get replica identity index or if it is not defined a primary key.
 *
 * If neither is defined, returns InvalidOid.
 *
 * Inspired from postgres/src/backend/replication/logical/worker.c
 */
Oid
GetRelationIdentityOrPK(Relation rel)
{
	Oid idxoid = RelationGetReplicaIndex(rel);

	if (!OidIsValid(idxoid))
	{
		idxoid = RelationGetPrimaryKeyIndex(rel);
	}

	return idxoid;
}


/*
 * GatherIndexAndConstraintDefinitionList adds the DDL command for the given index.
 */
void
GatherIndexAndConstraintDefinitionList(Form_pg_index indexForm, List **indexDDLEventList,
									   int indexFlags)
{
	/* generate fully-qualified names */
	int saveNestLevel = PushEmptySearchPath();

	Oid indexId = indexForm->indexrelid;
	bool indexImpliedByConstraint = IndexImpliedByAConstraint(indexForm);

	/* get the corresponding constraint or index statement */
	if (indexImpliedByConstraint)
	{
		if (indexFlags & INCLUDE_CREATE_CONSTRAINT_STATEMENTS)
		{
			Oid constraintId = get_index_constraint(indexId);
			Assert(constraintId != InvalidOid);

			/* include constraints backed by indexes only when explicitly asked */
			char *statementDef = pg_get_constraintdef_command(constraintId);
			*indexDDLEventList =
				lappend(*indexDDLEventList,
						makeTableDDLCommandString(statementDef));
		}
	}
	else if (indexFlags & INCLUDE_CREATE_INDEX_STATEMENTS)
	{
		/*
		 * Include indexes that are not backing constraints only when
		 * explicitly asked.
		 */
		char *statementDef = pg_get_indexdef_string(indexId);
		*indexDDLEventList = lappend(*indexDDLEventList,
									 makeTableDDLCommandString(statementDef));
	}

	/* if table is clustered on this index, append definition to the list */
	if ((indexFlags & INCLUDE_INDEX_CLUSTERED_STATEMENTS) &&
		indexForm->indisclustered)
	{
		char *clusteredDef = pg_get_indexclusterdef_string(indexId);
		Assert(clusteredDef != NULL);

		*indexDDLEventList = lappend(*indexDDLEventList, makeTableDDLCommandString(
										 clusteredDef));
	}

	/* we need alter index commands for altered targets on expression indexes */
	if (indexFlags & INCLUDE_INDEX_STATISTICS_STATEMENTTS)
	{
		List *alterIndexStatisticsCommands = GetAlterIndexStatisticsCommands(indexId);
		*indexDDLEventList = list_concat(*indexDDLEventList,
										 alterIndexStatisticsCommands);
	}

	/* revert back to original search_path */
	PopEmptySearchPath(saveNestLevel);
}


/*
 * GetTableRowLevelSecurityCommands takes in a relationId, and returns the list of
 * commands needed to reconstruct the row level security policy.
 */
List *
GetTableRowLevelSecurityCommands(Oid relationId)
{
	List *rowLevelSecurityCommandList = NIL;

	List *rowLevelSecurityEnableCommands = pg_get_row_level_security_commands(relationId);

	char *rowLevelSecurityCommand = NULL;
	foreach_declared_ptr(rowLevelSecurityCommand, rowLevelSecurityEnableCommands)
	{
		rowLevelSecurityCommandList = lappend(
			rowLevelSecurityCommandList,
			makeTableDDLCommandString(rowLevelSecurityCommand));
	}

	return rowLevelSecurityCommandList;
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
		shardStorageType = SHARD_STORAGE_FOREIGN;
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


/*
 * CitusCreateAlterColumnarTableSet generates a portable
 */
static char *
CitusCreateAlterColumnarTableSet(char *qualifiedRelationName,
								 const ColumnarOptions *options)
{
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf,
					 "ALTER TABLE %s SET ("
					 "columnar.chunk_group_row_limit = %d, "
					 "columnar.stripe_row_limit = %lu, "
					 "columnar.compression_level = %d, "
					 "columnar.compression = %s);",
					 qualifiedRelationName,
					 options->chunkRowCount,
					 options->stripeRowCount,
					 options->compressionLevel,
					 quote_literal_cstr(extern_CompressionTypeStr(
											options->compressionType)));

	return buf.data;
}


/*
 * GetTableDDLCommandColumnar is an internal function used to turn a
 * ColumnarTableDDLContext stored on the context of a TableDDLCommandFunction into a sql
 * command that will be executed against a table. The resulting command will set the
 * options of the table to the same options as the relation on the coordinator.
 */
static char *
GetTableDDLCommandColumnar(void *context)
{
	ColumnarTableDDLContext *tableDDLContext = (ColumnarTableDDLContext *) context;

	char *qualifiedShardName = quote_qualified_identifier(tableDDLContext->schemaName,
														  tableDDLContext->relationName);

	return CitusCreateAlterColumnarTableSet(qualifiedShardName,
											&tableDDLContext->options);
}


/*
 * GetShardedTableDDLCommandColumnar is an internal function used to turn a
 * ColumnarTableDDLContext stored on the context of a TableDDLCommandFunction into a sql
 * command that will be executed against a shard. The resulting command will set the
 * options of the shard to the same options as the relation the shard is based on.
 */
char *
GetShardedTableDDLCommandColumnar(uint64 shardId, void *context)
{
	ColumnarTableDDLContext *tableDDLContext = (ColumnarTableDDLContext *) context;

	/*
	 * AppendShardId is destructive of the original cahr *, given we want to serialize
	 * more than once we copy it before appending the shard id.
	 */
	char *relationName = pstrdup(tableDDLContext->relationName);
	AppendShardIdToName(&relationName, shardId);

	char *qualifiedShardName = quote_qualified_identifier(tableDDLContext->schemaName,
														  relationName);

	return CitusCreateAlterColumnarTableSet(qualifiedShardName,
											&tableDDLContext->options);
}


/*
 * ColumnarGetCustomTableOptionsDDL returns a TableDDLCommand representing a command that
 * will apply the passed columnar options to the relation identified by relationId on a
 * new table or shard.
 */
TableDDLCommand *
ColumnarGetCustomTableOptionsDDL(char *schemaName, char *relationName,
								 ColumnarOptions options)
{
	ColumnarTableDDLContext *context = (ColumnarTableDDLContext *) palloc0(
		sizeof(ColumnarTableDDLContext));

	/* build the context */
	context->schemaName = schemaName;
	context->relationName = relationName;
	context->options = options;

	/* create TableDDLCommand based on the context build above */
	return makeTableDDLCommandFunction(
		GetTableDDLCommandColumnar,
		GetShardedTableDDLCommandColumnar,
		context);
}


/*
 * ColumnarGetTableOptionsDDL returns a TableDDLCommand representing a command that will
 * apply the columnar options currently applicable to the relation identified by
 * relationId on a new table or shard.
 */
static TableDDLCommand *
ColumnarGetTableOptionsDDL(Oid relationId)
{
	Oid namespaceId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(namespaceId);
	char *relationName = get_rel_name(relationId);

	ColumnarOptions options = { 0 };
	extern_ReadColumnarOptions(relationId, &options);

	return ColumnarGetCustomTableOptionsDDL(schemaName, relationName, options);
}
