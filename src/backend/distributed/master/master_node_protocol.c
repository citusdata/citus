/*-------------------------------------------------------------------------
 *
 * master_node_protocol.c
 *	  Routines for requesting information from the master node for creating or
 *	  updating shards.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/worker_manager.h"
#include "foreign/foreign.h"
#include "libpq/ip.h"
#include "libpq/libpq-be.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#if (PG_VERSION_NUM >= 90500)
#include "utils/ruleutils.h"
#endif
#include "utils/syscache.h"
#include "utils/tqual.h"


/* Shard related configuration */
int ShardReplicationFactor = 2; /* desired replication factor for shards */
int ShardMaxSize = 1048576;     /* maximum size in KB one shard can grow to */
int ShardPlacementPolicy = SHARD_PLACEMENT_ROUND_ROBIN;


static Datum WorkerNodeGetDatum(WorkerNode *workerNode, TupleDesc tupleDescriptor);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_get_table_metadata);
PG_FUNCTION_INFO_V1(master_get_table_ddl_events);
PG_FUNCTION_INFO_V1(master_get_new_shardid);
PG_FUNCTION_INFO_V1(master_get_local_first_candidate_nodes);
PG_FUNCTION_INFO_V1(master_get_round_robin_candidate_nodes);
PG_FUNCTION_INFO_V1(master_get_active_worker_nodes);


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
	Oid relationId = ResolveRelationId(relationName);

	DistTableCacheEntry *partitionEntry = NULL;
	TypeFuncClass resultTypeClass = 0;
	Datum partitionKeyExpr = 0;
	Datum partitionKey = 0;
	Datum metadataDatum = 0;
	HeapTuple metadataTuple = NULL;
	TupleDesc metadataDescriptor = NULL;
	uint64 shardMaxSizeInBytes = 0;
	char relationType = 0;
	char storageType = 0;
	Datum values[TABLE_METADATA_FIELDS];
	bool isNulls[TABLE_METADATA_FIELDS];

	/* find partition tuple for partitioned relation */
	partitionEntry = DistributedTableCacheEntry(relationId);

	/* create tuple descriptor for return value */
	resultTypeClass = get_call_result_type(fcinfo, NULL, &metadataDescriptor);
	if (resultTypeClass != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR, (errmsg("return type must be a row type")));
	}

	/* get decompiled expression tree for partition key */
	partitionKeyExpr =
		PointerGetDatum(cstring_to_text(partitionEntry->partitionKeyString));
	partitionKey = DirectFunctionCall2(pg_get_expr, partitionKeyExpr,
									   ObjectIdGetDatum(relationId));

	/* form heap tuple for table metadata */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	shardMaxSizeInBytes = (int64) ShardMaxSize * 1024L;

	/* get storage type */
	relationType = get_rel_relkind(relationId);
	if (relationType == RELKIND_RELATION)
	{
		storageType = SHARD_STORAGE_TABLE;
	}
	else if (relationType == RELKIND_FOREIGN_TABLE)
	{
		bool cstoreTable = CStoreTable(relationId);
		if (cstoreTable)
		{
			storageType = SHARD_STORAGE_COLUMNAR;
		}
		else
		{
			storageType = SHARD_STORAGE_FOREIGN;
		}
	}

	values[0] = ObjectIdGetDatum(relationId);
	values[1] = storageType;
	values[2] = partitionEntry->partitionMethod;
	values[3] = partitionKey;
	values[4] = Int32GetDatum(ShardReplicationFactor);
	values[5] = Int64GetDatum(shardMaxSizeInBytes);
	values[6] = Int32GetDatum(ShardPlacementPolicy);

	metadataTuple = heap_form_tuple(metadataDescriptor, values, isNulls);
	metadataDatum = HeapTupleGetDatum(metadataTuple);

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

	/*
	 * On the very first call to this function, we first use the given relation
	 * name to get to the relation. We then recreate the list of DDL statements
	 * issued for this relation, and save the first statement's position in the
	 * function context.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		text *relationName = PG_GETARG_TEXT_P(0);
		Oid relationId = ResolveRelationId(relationName);

		MemoryContext oldContext = NULL;
		List *tableDDLEventList = NIL;

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);

		/* allocate DDL statements, and then save position in DDL statements */
		tableDDLEventList = GetTableDDLEvents(relationId);
		tableDDLEventCell = list_head(tableDDLEventList);

		functionContext->user_fctx = tableDDLEventCell;

		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * On every call to this function, we get the current position in the
	 * statement list. We then iterate to the next position in the list and
	 * return the current statement, if we have not yet reached the end of
	 * list.
	 */
	functionContext = SRF_PERCALL_SETUP();

	tableDDLEventCell = (ListCell *) functionContext->user_fctx;
	if (tableDDLEventCell != NULL)
	{
		char *ddlStatement = (char *) lfirst(tableDDLEventCell);
		text *ddlStatementText = cstring_to_text(ddlStatement);

		functionContext->user_fctx = lnext(tableDDLEventCell);

		SRF_RETURN_NEXT(functionContext, PointerGetDatum(ddlStatementText));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * master_get_new_shardid allocates and returns a unique shardId for the shard
 * to be created. This allocation occurs both in shared memory and in write
 * ahead logs; writing to logs avoids the risk of having shardId collisions.
 *
 * Please note that the caller is still responsible for finalizing shard data
 * and the shardId with the master node. Further note that this function relies
 * on an internal sequence created in initdb to generate unique identifiers.
 *
 * NB: This can be called by any user; for now we have decided that that's
 * ok. We might want to restrict this to users part of a specific role or such
 * at some later point.
 */
Datum
master_get_new_shardid(PG_FUNCTION_ARGS)
{
	text *sequenceName = cstring_to_text(SHARDID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum shardIdDatum = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	shardIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	PG_RETURN_DATUM(shardIdDatum);
}


/*
 * master_get_local_first_candidate_nodes returns a set of candidate host names
 * and port numbers on which to place new shards. The function makes sure to
 * always allocate the first candidate node as the node the caller is connecting
 * from; and allocates additional nodes until the shard replication factor is
 * met. The function errors if the caller's remote node name is not found in the
 * membership list, or if the number of available nodes falls short of the
 * replication factor.
 */
Datum
master_get_local_first_candidate_nodes(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	uint32 desiredNodeCount = 0;
	uint32 currentNodeCount = 0;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldContext = NULL;
		TupleDesc tupleDescriptor = NULL;
		uint32 liveNodeCount = 0;
		bool hasOid = false;

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);

		functionContext->user_fctx = NIL;
		functionContext->max_calls = ShardReplicationFactor;

		/* if enough live nodes, return an extra candidate node as backup */
		liveNodeCount = WorkerGetLiveNodeCount();
		if (liveNodeCount > ShardReplicationFactor)
		{
			functionContext->max_calls = ShardReplicationFactor + 1;
		}

		/*
		 * This tuple descriptor must match the output parameters declared for
		 * the function in pg_proc.
		 */
		tupleDescriptor = CreateTemplateTupleDesc(CANDIDATE_NODE_FIELDS, hasOid);
		TupleDescInitEntry(tupleDescriptor, (AttrNumber) 1, "node_name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupleDescriptor, (AttrNumber) 2, "node_port",
						   INT8OID, -1, 0);

		functionContext->tuple_desc = BlessTupleDesc(tupleDescriptor);

		MemoryContextSwitchTo(oldContext);
	}

	functionContext = SRF_PERCALL_SETUP();
	desiredNodeCount = functionContext->max_calls;
	currentNodeCount = functionContext->call_cntr;

	if (currentNodeCount < desiredNodeCount)
	{
		MemoryContext oldContext = NULL;
		List *currentNodeList = NIL;
		WorkerNode *candidateNode = NULL;
		Datum candidateDatum = 0;

		/* switch to memory context appropriate for multiple function calls */
		oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);
		currentNodeList = functionContext->user_fctx;

		candidateNode = WorkerGetLocalFirstCandidateNode(currentNodeList);
		if (candidateNode == NULL)
		{
			ereport(ERROR, (errmsg("could only find %u of %u required nodes",
								   currentNodeCount, desiredNodeCount)));
		}

		currentNodeList = lappend(currentNodeList, candidateNode);
		functionContext->user_fctx = currentNodeList;

		MemoryContextSwitchTo(oldContext);

		candidateDatum = WorkerNodeGetDatum(candidateNode, functionContext->tuple_desc);

		SRF_RETURN_NEXT(functionContext, candidateDatum);
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * master_get_round_robin_candidate_nodes returns a set of candidate host names
 * and port numbers on which to place new shards. The function uses the round
 * robin policy to choose the nodes and tries to ensure that there is an even
 * distribution of shards across the worker nodes. This function errors out if
 * the number of available nodes falls short of the replication factor.
 */
Datum
master_get_round_robin_candidate_nodes(PG_FUNCTION_ARGS)
{
	uint64 shardId = PG_GETARG_INT64(0);
	FuncCallContext *functionContext = NULL;
	uint32 desiredNodeCount = 0;
	uint32 currentNodeCount = 0;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldContext = NULL;
		TupleDesc tupleDescriptor = NULL;
		List *workerNodeList = NIL;
		TypeFuncClass resultTypeClass = 0;
		uint32 workerNodeCount = 0;

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);

		/* get the worker node list and sort it for determinism */
		workerNodeList = WorkerNodeList();
		workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

		functionContext->user_fctx = workerNodeList;
		functionContext->max_calls = ShardReplicationFactor;

		/* if we enough live nodes, return an extra candidate node as backup */
		workerNodeCount = (uint32) list_length(workerNodeList);
		if (workerNodeCount > ShardReplicationFactor)
		{
			functionContext->max_calls = ShardReplicationFactor + 1;
		}

		/* create tuple descriptor for return value */
		resultTypeClass = get_call_result_type(fcinfo, NULL, &tupleDescriptor);
		if (resultTypeClass != TYPEFUNC_COMPOSITE)
		{
			ereport(ERROR, (errmsg("return type must be a row type")));
		}

		functionContext->tuple_desc = tupleDescriptor;

		MemoryContextSwitchTo(oldContext);
	}

	functionContext = SRF_PERCALL_SETUP();
	desiredNodeCount = functionContext->max_calls;
	currentNodeCount = functionContext->call_cntr;

	if (currentNodeCount < desiredNodeCount)
	{
		List *workerNodeList = functionContext->user_fctx;
		WorkerNode *candidateNode = NULL;
		Datum candidateDatum = 0;

		candidateNode = WorkerGetRoundRobinCandidateNode(workerNodeList, shardId,
														 currentNodeCount);
		if (candidateNode == NULL)
		{
			ereport(ERROR, (errmsg("could only find %u of %u required nodes",
								   currentNodeCount, desiredNodeCount)));
		}

		candidateDatum = WorkerNodeGetDatum(candidateNode, functionContext->tuple_desc);

		SRF_RETURN_NEXT(functionContext, candidateDatum);
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * master_get_active_worker_nodes returns a set of active worker host names and
 * port numbers in deterministic order. Currently we assume that all worker
 * nodes in pg_worker_list.conf are active.
 */
Datum
master_get_active_worker_nodes(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	uint32 workerNodeIndex = 0;
	uint32 workerNodeCount = 0;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldContext = NULL;
		List *workerNodeList = NIL;
		uint32 workerNodeCount = 0;
		TupleDesc tupleDescriptor = NULL;
		bool hasOid = false;

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);

		workerNodeList = WorkerNodeList();
		workerNodeCount = (uint32) list_length(workerNodeList);

		functionContext->user_fctx = workerNodeList;
		functionContext->max_calls = workerNodeCount;

		/*
		 * This tuple descriptor must match the output parameters declared for
		 * the function in pg_proc.
		 */
		tupleDescriptor = CreateTemplateTupleDesc(WORKER_NODE_FIELDS, hasOid);
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
ResolveRelationId(text *relationName)
{
	List *relationNameList = NIL;
	RangeVar *relation = NULL;
	Oid relationId = InvalidOid;
	bool failOK = false;        /* error if relation cannot be found */

	/* resolve relationId from passed in schema and relation name */
	relationNameList = textToQualifiedNameList(relationName);
	relation = makeRangeVarFromNameList(relationNameList);
	relationId = RangeVarGetRelid(relation, NoLock, failOK);

	return relationId;
}


/*
 * GetTableDDLEvents takes in a relationId, and returns the list of DDL commands
 * needed to reconstruct the relation. These DDL commands are all palloced; and
 * include the table's schema definition, optional column storage and statistics
 * definitions, and index and constraint defitions.
 */
List *
GetTableDDLEvents(Oid relationId)
{
	List *tableDDLEventList = NIL;
	char tableType = 0;
	char *tableSchemaDef = NULL;
	char *tableColumnOptionsDef = NULL;
	char *schemaName = NULL;
	Oid schemaId = InvalidOid;

	Relation pgIndex = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	/* if foreign table, fetch extension and server definitions */
	tableType = get_rel_relkind(relationId);
	if (tableType == RELKIND_FOREIGN_TABLE)
	{
		char *extensionDef = pg_get_extensiondef_string(relationId);
		char *serverDef = pg_get_serverdef_string(relationId);

		if (extensionDef != NULL)
		{
			tableDDLEventList = lappend(tableDDLEventList, extensionDef);
		}
		tableDDLEventList = lappend(tableDDLEventList, serverDef);
	}

	/* create schema if the table is not in the default namespace (public) */
	schemaId = get_rel_namespace(relationId);
	schemaName = get_namespace_name(schemaId);
	if (strncmp(schemaName, "public", NAMEDATALEN) != 0)
	{
		StringInfo schemaNameDef = makeStringInfo();
		appendStringInfo(schemaNameDef, CREATE_SCHEMA_COMMAND, schemaName);

		tableDDLEventList = lappend(tableDDLEventList, schemaNameDef->data);
	}

	/* fetch table schema and column option definitions */
	tableSchemaDef = pg_get_tableschemadef_string(relationId);
	tableColumnOptionsDef = pg_get_tablecolumnoptionsdef_string(relationId);

	tableDDLEventList = lappend(tableDDLEventList, tableSchemaDef);
	if (tableColumnOptionsDef != NULL)
	{
		tableDDLEventList = lappend(tableDDLEventList, tableColumnOptionsDef);
	}

	/* open system catalog and scan all indexes that belong to this table */
	pgIndex = heap_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	scanDescriptor = systable_beginscan(pgIndex,
										IndexIndrelidIndexId, true, /* indexOK */
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(heapTuple);
		Oid indexId = indexForm->indexrelid;
		bool isConstraint = false;
		char *statementDef = NULL;

		/*
		 * A primary key index is always created by a constraint statement.
		 * A unique key index is created by a constraint if and only if the
		 * index has a corresponding constraint entry in pg_depend. Any other
		 * index form is never associated with a constraint.
		 */
		if (indexForm->indisprimary)
		{
			isConstraint = true;
		}
		else if (indexForm->indisunique)
		{
			Oid constraintId = get_index_constraint(indexId);
			isConstraint = OidIsValid(constraintId);
		}
		else
		{
			isConstraint = false;
		}

		/* get the corresponding constraint or index statement */
		if (isConstraint)
		{
			Oid constraintId = get_index_constraint(indexId);
			Assert(constraintId != InvalidOid);

#if (PG_VERSION_NUM >= 90500)
			statementDef = pg_get_constraintdef_command(constraintId);
#else
			statementDef = pg_get_constraintdef_string(constraintId);
#endif
		}
		else
		{
			statementDef = pg_get_indexdef_string(indexId);
		}

		/* append found constraint or index definition to the list */
		tableDDLEventList = lappend(tableDDLEventList, statementDef);

		/* if table is clustered on this index, append definition to the list */
		if (indexForm->indisclustered)
		{
			char *clusteredDef = pg_get_indexclusterdef_string(indexId);
			Assert(clusteredDef != NULL);

			tableDDLEventList = lappend(tableDDLEventList, clusteredDef);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgIndex, AccessShareLock);

	return tableDDLEventList;
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
	HeapTuple workerNodeTuple = NULL;
	Datum workerNodeDatum = 0;

	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[0] = CStringGetTextDatum(workerNode->workerName);
	values[1] = Int64GetDatum((int64) workerNode->workerPort);

	workerNodeTuple = heap_form_tuple(tupleDescriptor, values, isNulls);
	workerNodeDatum = HeapTupleGetDatum(workerNodeTuple);

	return workerNodeDatum;
}
