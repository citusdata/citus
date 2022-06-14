/*-------------------------------------------------------------------------
 *
 * test/shard_rebalancer.c
 *
 * This file contains functions used for unit testing the planning part of the
 * shard rebalancer.
 *
 * Copyright (c) 2014-2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"

#include "safe_lib.h"

#include "catalog/pg_type.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/relay_utility.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* static declarations for json conversion */
static List * JsonArrayToShardPlacementTestInfoList(
	ArrayType *shardPlacementJsonArrayObject);
static List * JsonArrayToWorkerTestInfoList(ArrayType *workerNodeJsonArrayObject);
static bool JsonFieldValueBoolDefault(Datum jsonDocument, const char *key,
									  bool defaultValue);
static uint32 JsonFieldValueUInt32Default(Datum jsonDocument, const char *key,
										  uint32 defaultValue);
static uint64 JsonFieldValueUInt64Default(Datum jsonDocument, const char *key,
										  uint64 defaultValue);
static char * JsonFieldValueString(Datum jsonDocument, const char *key);
static ArrayType * PlacementUpdateListToJsonArray(List *placementUpdateList);
static bool ShardAllowedOnNode(uint64 shardId, WorkerNode *workerNode, void *context);
static float NodeCapacity(WorkerNode *workerNode, void *context);
static ShardCost GetShardCost(uint64 shardId, void *context);


PG_FUNCTION_INFO_V1(shard_placement_rebalance_array);
PG_FUNCTION_INFO_V1(shard_placement_replication_array);
PG_FUNCTION_INFO_V1(worker_node_responsive);
PG_FUNCTION_INFO_V1(run_try_drop_marked_shards);

typedef struct ShardPlacementTestInfo
{
	ShardPlacement *placement;
	uint64 cost;
	bool nextColocationGroup;
} ShardPlacementTestInfo;

typedef struct WorkerTestInfo
{
	WorkerNode *node;
	List *disallowedShardIds;
	float capacity;
} WorkerTestInfo;

typedef struct RebalancePlanContext
{
	List *workerTestInfoList;
	List *shardPlacementTestInfoList;
} RebalancePlacementContext;

/*
 * run_try_drop_marked_shards is a wrapper to run TryDropOrphanedShards.
 */
Datum
run_try_drop_marked_shards(PG_FUNCTION_ARGS)
{
	bool waitForLocks = false;
	TryDropOrphanedShards(waitForLocks);
	PG_RETURN_VOID();
}


/*
 * IsActiveTestShardPlacement checks if the dummy shard placement created in tests
 * are labelled as active. Note that this function does not check if the worker is also
 * active, because the dummy test workers are not registered as actual workers.
 */
static inline bool
IsActiveTestShardPlacement(ShardPlacement *shardPlacement)
{
	return shardPlacement->shardState == SHARD_STATE_ACTIVE;
}


/*
 * shard_placement_rebalance_array returns a list of operations which can make a
 * cluster consisting of given shard placements and worker nodes balanced with
 * respect to the given threshold. Threshold is a value between 0 and 1 which
 * determines the evenness in shard distribution. When threshold is 0, then all
 * nodes should have equal number of shards. As threshold increases, cluster's
 * evenness requirements decrease, and we can rebalance the cluster using less
 * operations.
 */
Datum
shard_placement_rebalance_array(PG_FUNCTION_ARGS)
{
	ArrayType *workerNodeJsonArray = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *shardPlacementJsonArray = PG_GETARG_ARRAYTYPE_P(1);
	float threshold = PG_GETARG_FLOAT4(2);
	int32 maxShardMoves = PG_GETARG_INT32(3);
	bool drainOnly = PG_GETARG_BOOL(4);
	float utilizationImproventThreshold = PG_GETARG_FLOAT4(5);

	List *workerNodeList = NIL;
	List *shardPlacementListList = NIL;
	List *shardPlacementList = NIL;
	WorkerTestInfo *workerTestInfo = NULL;
	ShardPlacementTestInfo *shardPlacementTestInfo = NULL;
	RebalancePlanFunctions rebalancePlanFunctions = {
		.shardAllowedOnNode = ShardAllowedOnNode,
		.nodeCapacity = NodeCapacity,
		.shardCost = GetShardCost,
	};
	RebalancePlacementContext context = {
		.workerTestInfoList = NULL,
	};

	context.workerTestInfoList = JsonArrayToWorkerTestInfoList(workerNodeJsonArray);
	context.shardPlacementTestInfoList = JsonArrayToShardPlacementTestInfoList(
		shardPlacementJsonArray);

	/* we don't need original arrays any more, so we free them to save memory */
	pfree(workerNodeJsonArray);
	pfree(shardPlacementJsonArray);

	/* map workerTestInfoList to a list of its WorkerNodes */
	foreach_ptr(workerTestInfo, context.workerTestInfoList)
	{
		workerNodeList = lappend(workerNodeList, workerTestInfo->node);
	}

	/* map shardPlacementTestInfoList to a list of list of its ShardPlacements */
	foreach_ptr(shardPlacementTestInfo, context.shardPlacementTestInfoList)
	{
		if (shardPlacementTestInfo->nextColocationGroup)
		{
			shardPlacementList = SortList(shardPlacementList, CompareShardPlacements);
			shardPlacementListList = lappend(shardPlacementListList,
											 FilterShardPlacementList(shardPlacementList,
																	  IsActiveTestShardPlacement));
			shardPlacementList = NIL;
		}
		shardPlacementList = lappend(shardPlacementList,
									 shardPlacementTestInfo->placement);
	}
	shardPlacementList = SortList(shardPlacementList, CompareShardPlacements);
	shardPlacementListList = lappend(shardPlacementListList, shardPlacementList);

	rebalancePlanFunctions.context = &context;

	/* sort the lists to make the function more deterministic */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	List *placementUpdateList = RebalancePlacementUpdates(workerNodeList,
														  shardPlacementListList,
														  threshold,
														  maxShardMoves,
														  drainOnly,
														  utilizationImproventThreshold,
														  &rebalancePlanFunctions);
	ArrayType *placementUpdateJsonArray = PlacementUpdateListToJsonArray(
		placementUpdateList);

	PG_RETURN_ARRAYTYPE_P(placementUpdateJsonArray);
}


/*
 * ShardAllowedOnNode is the function that checks if shard is allowed to be on
 * a worker when running the shard rebalancer unit tests.
 */
static bool
ShardAllowedOnNode(uint64 shardId, WorkerNode *workerNode, void *voidContext)
{
	RebalancePlacementContext *context = voidContext;
	WorkerTestInfo *workerTestInfo = NULL;
	uint64 *disallowedShardIdPtr = NULL;
	foreach_ptr(workerTestInfo, context->workerTestInfoList)
	{
		if (workerTestInfo->node == workerNode)
		{
			break;
		}
	}
	Assert(workerTestInfo != NULL);

	foreach_ptr(disallowedShardIdPtr, workerTestInfo->disallowedShardIds)
	{
		if (shardId == *disallowedShardIdPtr)
		{
			return false;
		}
	}
	return true;
}


/*
 * NodeCapacity is the function that gets the capacity of a worker when running
 * the shard rebalancer unit tests.
 */
static float
NodeCapacity(WorkerNode *workerNode, void *voidContext)
{
	RebalancePlacementContext *context = voidContext;
	WorkerTestInfo *workerTestInfo = NULL;
	foreach_ptr(workerTestInfo, context->workerTestInfoList)
	{
		if (workerTestInfo->node == workerNode)
		{
			break;
		}
	}
	Assert(workerTestInfo != NULL);
	return workerTestInfo->capacity;
}


/*
 * GetShardCost is the function that gets the ShardCost of a shard when running
 * the shard rebalancer unit tests.
 */
static ShardCost
GetShardCost(uint64 shardId, void *voidContext)
{
	RebalancePlacementContext *context = voidContext;
	ShardCost shardCost;
	memset_struct_0(shardCost);
	shardCost.shardId = shardId;

	ShardPlacementTestInfo *shardPlacementTestInfo = NULL;
	foreach_ptr(shardPlacementTestInfo, context->shardPlacementTestInfoList)
	{
		if (shardPlacementTestInfo->placement->shardId == shardId)
		{
			break;
		}
	}
	Assert(shardPlacementTestInfo != NULL);
	shardCost.cost = shardPlacementTestInfo->cost;
	return shardCost;
}


/*
 * shard_placement_replication_array returns a list of operations which will
 * replicate under-replicated shards in a cluster consisting of given shard
 * placements and worker nodes. A shard is under-replicated if it has less
 * active placements than the given shard replication factor.
 */
Datum
shard_placement_replication_array(PG_FUNCTION_ARGS)
{
	ArrayType *workerNodeJsonArray = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *shardPlacementJsonArray = PG_GETARG_ARRAYTYPE_P(1);
	uint32 shardReplicationFactor = PG_GETARG_INT32(2);

	List *workerNodeList = NIL;
	List *shardPlacementList = NIL;
	WorkerTestInfo *workerTestInfo = NULL;
	ShardPlacementTestInfo *shardPlacementTestInfo = NULL;

	/* validate shard replication factor */
	if (shardReplicationFactor < SHARD_REPLICATION_FACTOR_MINIMUM ||
		shardReplicationFactor > SHARD_REPLICATION_FACTOR_MAXIMUM)
	{
		ereport(ERROR, (errmsg("invalid shard replication factor"),
						errhint("Shard replication factor must be an integer "
								"between %d and %d", SHARD_REPLICATION_FACTOR_MINIMUM,
								SHARD_REPLICATION_FACTOR_MAXIMUM)));
	}

	List *workerTestInfoList = JsonArrayToWorkerTestInfoList(workerNodeJsonArray);
	List *shardPlacementTestInfoList = JsonArrayToShardPlacementTestInfoList(
		shardPlacementJsonArray);

	/* we don't need original arrays any more, so we free them to save memory */
	pfree(workerNodeJsonArray);
	pfree(shardPlacementJsonArray);

	foreach_ptr(workerTestInfo, workerTestInfoList)
	{
		workerNodeList = lappend(workerNodeList, workerTestInfo->node);
	}

	foreach_ptr(shardPlacementTestInfo, shardPlacementTestInfoList)
	{
		shardPlacementList = lappend(shardPlacementList,
									 shardPlacementTestInfo->placement);
	}

	List *activeShardPlacementList = FilterShardPlacementList(shardPlacementList,
															  IsActiveTestShardPlacement);

	/* sort the lists to make the function more deterministic */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);
	activeShardPlacementList = SortList(activeShardPlacementList, CompareShardPlacements);

	List *placementUpdateList = ReplicationPlacementUpdates(workerNodeList,
															activeShardPlacementList,
															shardReplicationFactor);
	ArrayType *placementUpdateJsonArray = PlacementUpdateListToJsonArray(
		placementUpdateList);

	PG_RETURN_ARRAYTYPE_P(placementUpdateJsonArray);
}


/*
 * JsonArrayToShardPlacementTestInfoList converts the given shard placement json array
 * to a list of ShardPlacement structs.
 */
static List *
JsonArrayToShardPlacementTestInfoList(ArrayType *shardPlacementJsonArrayObject)
{
	List *shardPlacementTestInfoList = NIL;
	Datum *shardPlacementJsonArray = NULL;
	int placementCount = 0;

	/*
	 * Memory is not automatically freed when we call UDFs using DirectFunctionCall.
	 * We call these functions in functionCallContext, so we can free the memory
	 * once they return.
	 */
	MemoryContext functionCallContext = AllocSetContextCreate(CurrentMemoryContext,
															  "Function Call Context",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);

	deconstruct_array(shardPlacementJsonArrayObject, JSONOID, -1, false, 'i',
					  &shardPlacementJsonArray, NULL, &placementCount);

	for (int placementIndex = 0; placementIndex < placementCount; placementIndex++)
	{
		Datum placementJson = shardPlacementJsonArray[placementIndex];
		ShardPlacementTestInfo *placementTestInfo = palloc0(
			sizeof(ShardPlacementTestInfo));

		MemoryContext oldContext = MemoryContextSwitchTo(functionCallContext);

		uint64 shardId = JsonFieldValueUInt64Default(
			placementJson, FIELD_NAME_SHARD_ID, placementIndex + 1);
		uint64 shardLength = JsonFieldValueUInt64Default(
			placementJson, FIELD_NAME_SHARD_LENGTH, 1);
		int shardState = JsonFieldValueUInt32Default(
			placementJson, FIELD_NAME_SHARD_STATE, SHARD_STATE_ACTIVE);
		char *nodeName = JsonFieldValueString(placementJson, FIELD_NAME_NODE_NAME);
		if (nodeName == NULL)
		{
			ereport(ERROR, (errmsg(FIELD_NAME_NODE_NAME " needs be set")));
		}
		int nodePort = JsonFieldValueUInt32Default(
			placementJson, FIELD_NAME_NODE_PORT, 5432);
		uint64 placementId = JsonFieldValueUInt64Default(
			placementJson, FIELD_NAME_PLACEMENT_ID, placementIndex + 1);

		uint64 cost = JsonFieldValueUInt64Default(placementJson, "cost", 1);
		bool nextColocationGroup =
			JsonFieldValueBoolDefault(placementJson, "next_colocation", false);

		MemoryContextSwitchTo(oldContext);

		placementTestInfo->placement = palloc0(sizeof(ShardPlacement));
		placementTestInfo->placement->shardId = shardId;
		placementTestInfo->placement->shardLength = shardLength;
		placementTestInfo->placement->shardState = shardState;
		placementTestInfo->placement->nodeName = pstrdup(nodeName);
		placementTestInfo->placement->nodePort = nodePort;
		placementTestInfo->placement->placementId = placementId;
		placementTestInfo->cost = cost;
		placementTestInfo->nextColocationGroup = nextColocationGroup;

		/*
		 * We have copied whatever we needed from the UDF calls, so we can free
		 * the memory allocated by them.
		 */
		MemoryContextReset(functionCallContext);


		shardPlacementTestInfoList = lappend(shardPlacementTestInfoList,
											 placementTestInfo);
	}

	pfree(shardPlacementJsonArray);

	return shardPlacementTestInfoList;
}


/*
 * JsonArrayToWorkerNodeList converts the given worker node json array to a list
 * of WorkerNode structs.
 */
static List *
JsonArrayToWorkerTestInfoList(ArrayType *workerNodeJsonArrayObject)
{
	List *workerTestInfoList = NIL;
	Datum *workerNodeJsonArray = NULL;
	int workerNodeCount = 0;

	deconstruct_array(workerNodeJsonArrayObject, JSONOID, -1, false, 'i',
					  &workerNodeJsonArray, NULL, &workerNodeCount);

	for (int workerNodeIndex = 0; workerNodeIndex < workerNodeCount; workerNodeIndex++)
	{
		Datum workerNodeJson = workerNodeJsonArray[workerNodeIndex];
		char *workerName = JsonFieldValueString(workerNodeJson, FIELD_NAME_WORKER_NAME);
		if (workerName == NULL)
		{
			ereport(ERROR, (errmsg(FIELD_NAME_WORKER_NAME " needs be set")));
		}
		uint32 workerPort = JsonFieldValueUInt32Default(workerNodeJson,
														FIELD_NAME_WORKER_PORT, 5432);
		List *disallowedShardIdList = NIL;


		WorkerTestInfo *workerTestInfo = palloc0(sizeof(WorkerTestInfo));
		WorkerNode *workerNode = palloc0(sizeof(WorkerNode));
		strncpy_s(workerNode->workerName, sizeof(workerNode->workerName), workerName,
				  WORKER_LENGTH);
		workerNode->nodeId = workerNodeIndex;
		workerNode->workerPort = workerPort;
		workerNode->shouldHaveShards = true;
		workerNode->nodeRole = PrimaryNodeRoleId();
		workerTestInfo->node = workerNode;

		workerTestInfo->capacity = JsonFieldValueUInt64Default(workerNodeJson,
															   "capacity", 1);

		workerNode->isActive = JsonFieldValueBoolDefault(workerNodeJson,
														 "isActive", true);

		workerTestInfoList = lappend(workerTestInfoList, workerTestInfo);
		char *disallowedShardsString = JsonFieldValueString(
			workerNodeJson, "disallowed_shards");

		if (disallowedShardsString == NULL)
		{
			continue;
		}

		char *strtokPosition = NULL;
		char *shardString = strtok_r(disallowedShardsString, ",", &strtokPosition);
		while (shardString != NULL)
		{
			uint64 *shardInt = palloc0(sizeof(uint64));
			*shardInt = SafeStringToUint64(shardString);
			disallowedShardIdList = lappend(disallowedShardIdList, shardInt);
			shardString = strtok_r(NULL, ",", &strtokPosition);
		}
		workerTestInfo->disallowedShardIds = disallowedShardIdList;
	}

	return workerTestInfoList;
}


/*
 * JsonFieldValueBoolDefault gets the value of the given key in the given json
 * document and returns it as a boolean. If the field does not exist in the
 * JSON it returns defaultValue.
 */
static bool
JsonFieldValueBoolDefault(Datum jsonDocument, const char *key, bool defaultValue)
{
	char *valueString = JsonFieldValueString(jsonDocument, key);
	if (valueString == NULL)
	{
		return defaultValue;
	}
	Datum valueBoolDatum = DirectFunctionCall1(boolin, CStringGetDatum(valueString));

	return DatumGetBool(valueBoolDatum);
}


/*
 * JsonFieldValueUInt32Default gets the value of the given key in the given json
 * document and returns it as an unsigned 32-bit integer. If the field does not
 * exist in the JSON it returns defaultValue.
 */
static uint32
JsonFieldValueUInt32Default(Datum jsonDocument, const char *key, uint32 defaultValue)
{
	char *valueString = JsonFieldValueString(jsonDocument, key);
	if (valueString == NULL)
	{
		return defaultValue;
	}
	Datum valueInt4Datum = DirectFunctionCall1(int4in, CStringGetDatum(valueString));

	uint32 valueUInt32 = DatumGetInt32(valueInt4Datum);
	return valueUInt32;
}


/*
 * JsonFieldValueUInt64 gets the value of the given key in the given json
 * document and returns it as an unsigned 64-bit integer. If the field does not
 * exist in the JSON it returns defaultValue.
 */
static uint64
JsonFieldValueUInt64Default(Datum jsonDocument, const char *key, uint64 defaultValue)
{
	char *valueString = JsonFieldValueString(jsonDocument, key);
	if (valueString == NULL)
	{
		return defaultValue;
	}
	Datum valueInt8Datum = DirectFunctionCall1(int8in, CStringGetDatum(valueString));

	uint64 valueUInt64 = DatumGetInt64(valueInt8Datum);
	return valueUInt64;
}


/*
 * DirectFunctionalCall2Null is a version of DirectFunctionCall2 that can
 * return NULL. It still does not support NULL arguments though.
 */
static Datum
DirectFunctionCall2Null(PGFunction func, bool *isnull, Datum arg1, Datum arg2)
{
	LOCAL_FCINFO(fcinfo, 2);

	InitFunctionCallInfoData(*fcinfo, NULL, 2, InvalidOid, NULL, NULL);

	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;
	fcinfo->args[1].value = arg2;
	fcinfo->args[1].isnull = false;

	Datum result = (*func)(fcinfo);
	if (fcinfo->isnull)
	{
		*isnull = true;
		return 0;
	}

	*isnull = false;
	return result;
}


/*
 * JsonFieldValueString gets the value of the given key in the given json
 * document and returns it as a string. If the field does not exist in the JSON
 * it returns NULL.
 */
static char *
JsonFieldValueString(Datum jsonDocument, const char *key)
{
	bool isnull = false;
	Datum keyDatum = PointerGetDatum(cstring_to_text(key));

	Datum valueTextDatum = DirectFunctionCall2Null(
		json_object_field_text, &isnull, jsonDocument, keyDatum);
	if (isnull)
	{
		return NULL;
	}

	char *valueString = text_to_cstring(DatumGetTextP(valueTextDatum));
	return valueString;
}


/*
 * PlacementUpdateListToJsonArray converts the given list of placement update
 * data to a json array.
 */
static ArrayType *
PlacementUpdateListToJsonArray(List *placementUpdateList)
{
	ListCell *placementUpdateCell = NULL;
	int placementUpdateIndex = 0;

	int placementUpdateCount = list_length(placementUpdateList);
	Datum *placementUpdateJsonArray = palloc0(placementUpdateCount * sizeof(Datum));

	foreach(placementUpdateCell, placementUpdateList)
	{
		PlacementUpdateEvent *placementUpdateEvent = lfirst(placementUpdateCell);
		WorkerNode *sourceNode = placementUpdateEvent->sourceNode;
		WorkerNode *targetNode = placementUpdateEvent->targetNode;

		StringInfo escapedSourceName = makeStringInfo();
		escape_json(escapedSourceName, sourceNode->workerName);

		StringInfo escapedTargetName = makeStringInfo();
		escape_json(escapedTargetName, targetNode->workerName);

		StringInfo placementUpdateJsonString = makeStringInfo();
		appendStringInfo(placementUpdateJsonString, PLACEMENT_UPDATE_JSON_FORMAT,
						 placementUpdateEvent->updateType, placementUpdateEvent->shardId,
						 escapedSourceName->data, sourceNode->workerPort,
						 escapedTargetName->data, targetNode->workerPort);

		Datum placementUpdateStringDatum = CStringGetDatum(
			placementUpdateJsonString->data);
		Datum placementUpdateJsonDatum = DirectFunctionCall1(json_in,
															 placementUpdateStringDatum);

		placementUpdateJsonArray[placementUpdateIndex] = placementUpdateJsonDatum;
		placementUpdateIndex++;
	}

	ArrayType *placementUpdateObject = construct_array(placementUpdateJsonArray,
													   placementUpdateCount, JSONOID,
													   -1, false, 'i');

	return placementUpdateObject;
}


/*
 * worker_node_responsive returns true if the given worker node is responsive.
 * Otherwise, it returns false.
 */
Datum
worker_node_responsive(PG_FUNCTION_ARGS)
{
	text *workerNameText = PG_GETARG_TEXT_PP(0);
	uint32 workerPort = PG_GETARG_INT32(1);
	int connectionFlag = FORCE_NEW_CONNECTION;

	bool workerNodeResponsive = false;
	const char *workerName = text_to_cstring(workerNameText);

	MultiConnection *connection = GetNodeConnection(connectionFlag, workerName,
													workerPort);

	if (connection != NULL && connection->pgConn != NULL)
	{
		if (PQstatus(connection->pgConn) == CONNECTION_OK)
		{
			workerNodeResponsive = true;
		}

		CloseConnection(connection);
	}

	PG_RETURN_BOOL(workerNodeResponsive);
}
