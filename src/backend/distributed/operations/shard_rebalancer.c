/*-------------------------------------------------------------------------
 *
 * shard_rebalancer.c
 *
 * Function definitions for the shard rebalancer tool.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "libpq-fe.h"

#include <math.h>

#include "distributed/pg_version_constants.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "distributed/argutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/enterprise.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_progress.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_rebalance_strategy.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/repair_shards.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_cleaner.h"
#include "distributed/tuplestore.h"
#include "distributed/utils/array_type.h"
#include "distributed/worker_protocol.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM >= PG_VERSION_13
#include "common/hashfn.h"
#endif


/* RebalanceOptions are the options used to control the rebalance algorithm */
typedef struct RebalanceOptions
{
	List *relationIdList;
	float4 threshold;
	int32 maxShardMoves;
	ArrayType *excludedShardArray;
	bool drainOnly;
	float4 improvementThreshold;
	Form_pg_dist_rebalance_strategy rebalanceStrategy;
} RebalanceOptions;


/*
 * RebalanceState is used to keep the internal state of the rebalance
 * algorithm in one place.
 */
typedef struct RebalanceState
{
	/*
	 * placementsHash contains the current state of all shard placements, it
	 * is initialized from pg_dist_placement and is then modified based on the
	 * found shard moves.
	 */
	HTAB *placementsHash;

	/*
	 * placementUpdateList contains all of the updates that have been done to
	 * reach the current state of placementsHash.
	 */
	List *placementUpdateList;
	RebalancePlanFunctions *functions;

	/*
	 * fillStateListDesc contains all NodeFillStates ordered from full nodes to
	 * empty nodes.
	 */
	List *fillStateListDesc;

	/*
	 * fillStateListAsc contains all NodeFillStates ordered from empty nodes to
	 * full nodes.
	 */
	List *fillStateListAsc;

	/*
	 * disallowedPlacementList contains all placements that currently exist,
	 * but are not allowed according to the shardAllowedOnNode function.
	 */
	List *disallowedPlacementList;

	/*
	 * totalCost is the cost of all the shards in the cluster added together.
	 */
	float4 totalCost;

	/*
	 * totalCapacity is the capacity of all the nodes in the cluster added
	 * together.
	 */
	float4 totalCapacity;

	/*
	 * ignoredMoves is the number of moves that were ignored. This is used to
	 * limit the amount of loglines we send.
	 */
	int64 ignoredMoves;
} RebalanceState;


/* RebalanceContext stores the context for the function callbacks */
typedef struct RebalanceContext
{
	FmgrInfo shardCostUDF;
	FmgrInfo nodeCapacityUDF;
	FmgrInfo shardAllowedOnNodeUDF;
} RebalanceContext;

/* WorkerHashKey contains hostname and port to be used as a key in a hash */
typedef struct WorkerHashKey
{
	char hostname[MAX_NODE_LENGTH];
	int port;
} WorkerHashKey;

/* WorkerShardIds represents a set of shardIds grouped by worker */
typedef struct WorkerShardIds
{
	WorkerHashKey worker;

	/* This is a uint64 hashset representing the shard ids for a specific worker */
	HTAB *shardIds;
} WorkerShardIds;

/* ShardStatistics contains statistics about a shard */
typedef struct ShardStatistics
{
	uint64 shardId;

	/* The shard its size in bytes. */
	uint64 totalSize;
} ShardStatistics;

/*
 * WorkerShardStatistics represents a set of statistics about shards,
 * grouped by worker.
 */
typedef struct WorkerShardStatistics
{
	WorkerHashKey worker;

	/*
	 * Statistics for each shard on this worker:
	 * key: shardId
	 * value: ShardStatistics
	 */
	HTAB *statistics;
} WorkerShardStatistics;


/* static declarations for main logic */
static int ShardActivePlacementCount(HTAB *activePlacementsHash, uint64 shardId,
									 List *activeWorkerNodeList);
static void UpdateShardPlacement(PlacementUpdateEvent *placementUpdateEvent,
								 List *responsiveNodeList, Oid shardReplicationModeOid);

/* static declarations for main logic's utility functions */
static HTAB * ShardPlacementsListToHash(List *shardPlacementList);
static bool PlacementsHashFind(HTAB *placementsHash, uint64 shardId,
							   WorkerNode *workerNode);
static void PlacementsHashEnter(HTAB *placementsHash, uint64 shardId,
								WorkerNode *workerNode);
static void PlacementsHashRemove(HTAB *placementsHash, uint64 shardId,
								 WorkerNode *workerNode);
static int PlacementsHashCompare(const void *lhsKey, const void *rhsKey, Size keySize);
static uint32 PlacementsHashHashCode(const void *key, Size keySize);
static bool WorkerNodeListContains(List *workerNodeList, const char *workerName,
								   uint32 workerPort);
static void UpdateColocatedShardPlacementProgress(uint64 shardId, char *sourceName,
												  int sourcePort, uint64 progress);
static bool IsPlacementOnWorkerNode(ShardPlacement *placement, WorkerNode *workerNode);
static NodeFillState * FindFillStateForPlacement(RebalanceState *state,
												 ShardPlacement *placement);
static RebalanceState * InitRebalanceState(List *workerNodeList, List *shardPlacementList,
										   RebalancePlanFunctions *functions);
static void MoveShardsAwayFromDisallowedNodes(RebalanceState *state);
static bool FindAndMoveShardCost(float4 utilizationLowerBound,
								 float4 utilizationUpperBound,
								 float4 improvementThreshold,
								 RebalanceState *state);
static NodeFillState * FindAllowedTargetFillState(RebalanceState *state, uint64 shardId);
static void MoveShardCost(NodeFillState *sourceFillState, NodeFillState *targetFillState,
						  ShardCost *shardCost, RebalanceState *state);
static int CompareNodeFillStateAsc(const void *void1, const void *void2);
static int CompareNodeFillStateDesc(const void *void1, const void *void2);
static int CompareShardCostAsc(const void *void1, const void *void2);
static int CompareShardCostDesc(const void *void1, const void *void2);
static int CompareDisallowedPlacementAsc(const void *void1, const void *void2);
static int CompareDisallowedPlacementDesc(const void *void1, const void *void2);
static bool ShardAllowedOnNode(uint64 shardId, WorkerNode *workerNode, void *context);
static float4 NodeCapacity(WorkerNode *workerNode, void *context);
static ShardCost GetShardCost(uint64 shardId, void *context);
static List * NonColocatedDistRelationIdList(void);
static void RebalanceTableShards(RebalanceOptions *options, Oid shardReplicationModeOid);
static void AcquireColocationLock(Oid relationId, const char *operationName);
static void ExecutePlacementUpdates(List *placementUpdateList, Oid
									shardReplicationModeOid, char *noticeOperation);
static float4 CalculateUtilization(float4 totalCost, float4 capacity);
static Form_pg_dist_rebalance_strategy GetRebalanceStrategy(Name name);
static void EnsureShardCostUDF(Oid functionOid);
static void EnsureNodeCapacityUDF(Oid functionOid);
static void EnsureShardAllowedOnNodeUDF(Oid functionOid);
static void ConflictShardPlacementUpdateOnlyWithIsolationTesting(uint64 shardId);
static HTAB * BuildWorkerShardStatisticsHash(PlacementUpdateEventProgress *steps,
											 int stepCount);
static HTAB * GetShardStatistics(MultiConnection *connection, HTAB *shardIds);
static HTAB * GetMovedShardIdsByWorker(PlacementUpdateEventProgress *steps,
									   int stepCount, bool fromSource);
static uint64 WorkerShardSize(HTAB *workerShardStatistics,
							  char *workerName, int workerPort, uint64 shardId);
static void AddToWorkerShardIdSet(HTAB *shardsByWorker, char *workerName, int workerPort,
								  uint64 shardId);
static HTAB * BuildShardSizesHash(ProgressMonitorData *monitor, HTAB *shardStatistics);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(rebalance_table_shards);
PG_FUNCTION_INFO_V1(replicate_table_shards);
PG_FUNCTION_INFO_V1(get_rebalance_table_shards_plan);
PG_FUNCTION_INFO_V1(get_rebalance_progress);
PG_FUNCTION_INFO_V1(citus_drain_node);
PG_FUNCTION_INFO_V1(master_drain_node);
PG_FUNCTION_INFO_V1(citus_shard_cost_by_disk_size);
PG_FUNCTION_INFO_V1(citus_validate_rebalance_strategy_functions);
PG_FUNCTION_INFO_V1(pg_dist_rebalance_strategy_enterprise_check);

bool RunningUnderIsolationTest = false;
int MaxRebalancerLoggedIgnoredMoves = 5;

/*
 * This is randomly generated hardcoded number. It's used as the first part of
 * the advisory lock identifier that's used during isolation tests. See the
 * comments on ConflictShardPlacementUpdateOnlyWithIsolationTesting, for more
 * information.
 */
#define SHARD_PLACEMENT_UPDATE_ADVISORY_LOCK_FIRST_KEY 29279


#ifdef USE_ASSERT_CHECKING

/*
 * Check that all the invariants of the state hold.
 */
static void
CheckRebalanceStateInvariants(const RebalanceState *state)
{
	NodeFillState *fillState = NULL;
	NodeFillState *prevFillState = NULL;
	int fillStateIndex = 0;
	int fillStateLength = list_length(state->fillStateListAsc);

	Assert(state != NULL);
	Assert(list_length(state->fillStateListAsc) == list_length(state->fillStateListDesc));
	foreach_ptr(fillState, state->fillStateListAsc)
	{
		float4 totalCost = 0;
		ShardCost *shardCost = NULL;
		ShardCost *prevShardCost = NULL;
		if (prevFillState != NULL)
		{
			/* Check that the previous fill state is more empty than this one */
			bool higherUtilization = fillState->utilization > prevFillState->utilization;
			bool sameUtilization = fillState->utilization == prevFillState->utilization;
			bool lowerOrSameCapacity = fillState->capacity <= prevFillState->capacity;
			Assert(higherUtilization || (sameUtilization && lowerOrSameCapacity));
		}

		/* Check that fillStateListDesc is the reversed version of fillStateListAsc */
		Assert(list_nth(state->fillStateListDesc, fillStateLength - fillStateIndex - 1) ==
			   fillState);


		foreach_ptr(shardCost, fillState->shardCostListDesc)
		{
			if (prevShardCost != NULL)
			{
				/* Check that shard costs are sorted in descending order */
				Assert(shardCost->cost <= prevShardCost->cost);
			}
			totalCost += shardCost->cost;
		}

		/* Check that utilization field is up to date. */
		Assert(fillState->utilization == CalculateUtilization(fillState->totalCost,
															  fillState->capacity));

		/*
		 * Check that fillState->totalCost is within 0.1% difference of
		 * sum(fillState->shardCostListDesc->cost)
		 * We cannot compare exactly, because these numbers are floats and
		 * fillState->totalCost is modified by doing + and - on it. So instead
		 * we check that the numbers are roughly the same.
		 */
		float4 absoluteDifferenceBetweenTotalCosts =
			fabsf(fillState->totalCost - totalCost);
		float4 maximumAbsoluteValueOfTotalCosts =
			fmaxf(fabsf(fillState->totalCost), fabsf(totalCost));
		Assert(absoluteDifferenceBetweenTotalCosts <= maximumAbsoluteValueOfTotalCosts /
			   1000);

		prevFillState = fillState;
		fillStateIndex++;
	}
}


#else
#define CheckRebalanceStateInvariants(l) ((void) 0)
#endif                          /* USE_ASSERT_CHECKING */

/*
 * BigIntArrayDatumContains checks if the array contains the given number.
 */
static bool
BigIntArrayDatumContains(Datum *array, int arrayLength, uint64 toFind)
{
	for (int i = 0; i < arrayLength; i++)
	{
		if (DatumGetInt64(array[i]) == toFind)
		{
			return true;
		}
	}
	return false;
}


/*
 * FullShardPlacementList returns a List containing all the shard placements of
 * a specific table (excluding the excludedShardArray)
 */
static List *
FullShardPlacementList(Oid relationId, ArrayType *excludedShardArray)
{
	List *shardPlacementList = NIL;
	CitusTableCacheEntry *citusTableCacheEntry = GetCitusTableCacheEntry(relationId);
	int shardIntervalArrayLength = citusTableCacheEntry->shardIntervalArrayLength;
	int excludedShardIdCount = ArrayObjectCount(excludedShardArray);
	Datum *excludedShardArrayDatum = DeconstructArrayObject(excludedShardArray);

	for (int shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		ShardInterval *shardInterval =
			citusTableCacheEntry->sortedShardIntervalArray[shardIndex];
		GroupShardPlacement *placementArray =
			citusTableCacheEntry->arrayOfPlacementArrays[shardIndex];
		int numberOfPlacements =
			citusTableCacheEntry->arrayOfPlacementArrayLengths[shardIndex];

		if (BigIntArrayDatumContains(excludedShardArrayDatum, excludedShardIdCount,
									 shardInterval->shardId))
		{
			continue;
		}

		for (int placementIndex = 0; placementIndex < numberOfPlacements;
			 placementIndex++)
		{
			GroupShardPlacement *groupPlacement = &placementArray[placementIndex];
			WorkerNode *worker = LookupNodeForGroup(groupPlacement->groupId);
			ShardPlacement *placement = CitusMakeNode(ShardPlacement);
			placement->shardId = groupPlacement->shardId;
			placement->shardLength = groupPlacement->shardLength;
			placement->shardState = groupPlacement->shardState;
			placement->nodeId = worker->nodeId;
			placement->nodeName = pstrdup(worker->workerName);
			placement->nodePort = worker->workerPort;
			placement->placementId = groupPlacement->placementId;

			shardPlacementList = lappend(shardPlacementList, placement);
		}
	}
	return SortList(shardPlacementList, CompareShardPlacements);
}


/*
 * SortedActiveWorkers returns all the active workers like
 * ActiveReadableNodeList, but sorted.
 */
static List *
SortedActiveWorkers()
{
	List *activeWorkerList = ActiveReadableNodeList();
	return SortList(activeWorkerList, CompareWorkerNodes);
}


/*
 * GetRebalanceSteps returns a List of PlacementUpdateEvents that are needed to
 * rebalance a list of tables.
 */
static List *
GetRebalanceSteps(RebalanceOptions *options)
{
	EnsureShardCostUDF(options->rebalanceStrategy->shardCostFunction);
	EnsureNodeCapacityUDF(options->rebalanceStrategy->nodeCapacityFunction);
	EnsureShardAllowedOnNodeUDF(options->rebalanceStrategy->shardAllowedOnNodeFunction);

	RebalanceContext context;
	memset(&context, 0, sizeof(RebalanceContext));
	fmgr_info(options->rebalanceStrategy->shardCostFunction, &context.shardCostUDF);
	fmgr_info(options->rebalanceStrategy->nodeCapacityFunction, &context.nodeCapacityUDF);
	fmgr_info(options->rebalanceStrategy->shardAllowedOnNodeFunction,
			  &context.shardAllowedOnNodeUDF);

	RebalancePlanFunctions rebalancePlanFunctions = {
		.shardAllowedOnNode = ShardAllowedOnNode,
		.nodeCapacity = NodeCapacity,
		.shardCost = GetShardCost,
		.context = &context,
	};

	/* sort the lists to make the function more deterministic */
	List *activeWorkerList = SortedActiveWorkers();
	List *activeShardPlacementListList = NIL;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, options->relationIdList)
	{
		List *shardPlacementList = FullShardPlacementList(relationId,
														  options->excludedShardArray);
		List *activeShardPlacementListForRelation =
			FilterShardPlacementList(shardPlacementList, IsActiveShardPlacement);
		activeShardPlacementListList =
			lappend(activeShardPlacementListList, activeShardPlacementListForRelation);
	}

	if (options->threshold < options->rebalanceStrategy->minimumThreshold)
	{
		ereport(WARNING, (errmsg(
							  "the given threshold is lower than the minimum "
							  "threshold allowed by the rebalance strategy, "
							  "using the minimum allowed threshold instead"
							  ),
						  errdetail("Using threshold of %.2f",
									options->rebalanceStrategy->minimumThreshold
									)
						  ));
		options->threshold = options->rebalanceStrategy->minimumThreshold;
	}

	return RebalancePlacementUpdates(activeWorkerList,
									 activeShardPlacementListList,
									 options->threshold,
									 options->maxShardMoves,
									 options->drainOnly,
									 options->improvementThreshold,
									 &rebalancePlanFunctions);
}


/*
 * ShardAllowedOnNode determines if shard is allowed on a specific worker node.
 */
static bool
ShardAllowedOnNode(uint64 shardId, WorkerNode *workerNode, void *voidContext)
{
	if (!workerNode->shouldHaveShards)
	{
		return false;
	}

	RebalanceContext *context = voidContext;
	Datum allowed = FunctionCall2(&context->shardAllowedOnNodeUDF, shardId,
								  workerNode->nodeId);
	return DatumGetBool(allowed);
}


/*
 * NodeCapacity returns the relative capacity of a node. A node with capacity 2
 * can contain twice as many shards as a node with capacity 1. The actual
 * capacity can be a number grounded in reality, like the disk size, number of
 * cores, but it doesn't have to be.
 */
static float4
NodeCapacity(WorkerNode *workerNode, void *voidContext)
{
	if (!workerNode->shouldHaveShards)
	{
		return 0;
	}

	RebalanceContext *context = voidContext;
	Datum capacity = FunctionCall1(&context->nodeCapacityUDF, workerNode->nodeId);
	return DatumGetFloat4(capacity);
}


/*
 * GetShardCost returns the cost of the given shard. A shard with cost 2 will
 * be weighted as heavily as two shards with cost 1. This cost number can be a
 * number grounded in reality, like the shard size on disk, but it doesn't have
 * to be.
 */
static ShardCost
GetShardCost(uint64 shardId, void *voidContext)
{
	ShardCost shardCost = { 0 };
	shardCost.shardId = shardId;
	RebalanceContext *context = voidContext;
	Datum shardCostDatum = FunctionCall1(&context->shardCostUDF, UInt64GetDatum(shardId));
	shardCost.cost = DatumGetFloat4(shardCostDatum);
	return shardCost;
}


/*
 * citus_shard_cost_by_disk_size gets the cost for a shard based on the disk
 * size of the shard on a worker. The worker to check the disk size is
 * determined by choosing the first active placement for the shard. The disk
 * size is calculated using pg_total_relation_size, so it includes indexes.
 *
 * SQL signature:
 * citus_shard_cost_by_disk_size(shardid bigint) returns float4
 */
Datum
citus_shard_cost_by_disk_size(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	uint64 shardId = PG_GETARG_INT64(0);
	bool missingOk = false;
	ShardPlacement *shardPlacement = ActiveShardPlacement(shardId, missingOk);

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CostByDiscSizeContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	List *colocatedShardList = ColocatedNonPartitionShardIntervalList(shardInterval);

	uint64 colocationSizeInBytes = ShardListSizeInBytes(colocatedShardList,
														shardPlacement->nodeName,
														shardPlacement->nodePort);

	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(localContext);

	if (colocationSizeInBytes <= 0)
	{
		PG_RETURN_FLOAT4(1);
	}

	PG_RETURN_FLOAT4(colocationSizeInBytes);
}


/*
 * GetColocatedRebalanceSteps takes a List of PlacementUpdateEvents and creates
 * a new List of containing those and all the updates for colocated shards.
 */
static List *
GetColocatedRebalanceSteps(List *placementUpdateList)
{
	ListCell *placementUpdateCell = NULL;
	List *colocatedUpdateList = NIL;

	foreach(placementUpdateCell, placementUpdateList)
	{
		PlacementUpdateEvent *placementUpdate = lfirst(placementUpdateCell);
		ShardInterval *shardInterval = LoadShardInterval(placementUpdate->shardId);
		List *colocatedShardList = ColocatedShardIntervalList(shardInterval);
		ListCell *colocatedShardCell = NULL;

		foreach(colocatedShardCell, colocatedShardList)
		{
			ShardInterval *colocatedShard = lfirst(colocatedShardCell);
			PlacementUpdateEvent *colocatedUpdate = palloc0(sizeof(PlacementUpdateEvent));

			colocatedUpdate->shardId = colocatedShard->shardId;
			colocatedUpdate->sourceNode = placementUpdate->sourceNode;
			colocatedUpdate->targetNode = placementUpdate->targetNode;
			colocatedUpdate->updateType = placementUpdate->updateType;

			colocatedUpdateList = lappend(colocatedUpdateList, colocatedUpdate);
		}
	}

	return colocatedUpdateList;
}


/*
 * AcquireColocationLock tries to acquire a lock for rebalance/replication. If
 * this is it not possible it fails instantly because this means another
 * rebalance/replication is currently happening. This would really mess up
 * planning.
 */
static void
AcquireColocationLock(Oid relationId, const char *operationName)
{
	uint32 lockId = relationId;
	LOCKTAG tag;

	CitusTableCacheEntry *citusTableCacheEntry = GetCitusTableCacheEntry(relationId);
	if (citusTableCacheEntry->colocationId != INVALID_COLOCATION_ID)
	{
		lockId = citusTableCacheEntry->colocationId;
	}

	SET_LOCKTAG_REBALANCE_COLOCATION(tag, (int64) lockId);

	LockAcquireResult lockAcquired = LockAcquire(&tag, ExclusiveLock, false, true);
	if (!lockAcquired)
	{
		ereport(ERROR, (errmsg("could not acquire the lock required to %s %s",
							   operationName, generate_qualified_relation_name(
								   relationId))));
	}
}


/*
 * GetResponsiveWorkerList returns a List of workers that respond to new
 * connection requests.
 */
static List *
GetResponsiveWorkerList()
{
	List *activeWorkerList = ActiveReadableNodeList();
	ListCell *activeWorkerCell = NULL;
	List *responsiveWorkerList = NIL;

	foreach(activeWorkerCell, activeWorkerList)
	{
		WorkerNode *worker = lfirst(activeWorkerCell);
		int connectionFlag = FORCE_NEW_CONNECTION;

		MultiConnection *connection = GetNodeConnection(connectionFlag,
														worker->workerName,
														worker->workerPort);

		if (connection != NULL && connection->pgConn != NULL)
		{
			if (PQstatus(connection->pgConn) == CONNECTION_OK)
			{
				responsiveWorkerList = lappend(responsiveWorkerList, worker);
			}

			CloseConnection(connection);
		}
	}
	return responsiveWorkerList;
}


/*
 * ExecutePlacementUpdates copies or moves a shard placement by calling the
 * corresponding functions in Citus in a separate subtransaction for each
 * update.
 */
static void
ExecutePlacementUpdates(List *placementUpdateList, Oid shardReplicationModeOid,
						char *noticeOperation)
{
	List *responsiveWorkerList = GetResponsiveWorkerList();

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "ExecutePlacementLoopContext",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	ListCell *placementUpdateCell = NULL;

	char shardReplicationMode = LookupShardTransferMode(shardReplicationModeOid);
	if (shardReplicationMode == TRANSFER_MODE_FORCE_LOGICAL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("the force_logical transfer mode is currently "
							   "unsupported")));
	}

	DropOrphanedShardsInSeparateTransaction();

	foreach(placementUpdateCell, placementUpdateList)
	{
		PlacementUpdateEvent *placementUpdate = lfirst(placementUpdateCell);
		ereport(NOTICE, (errmsg(
							 "%s shard %lu from %s:%u to %s:%u ...",
							 noticeOperation,
							 placementUpdate->shardId,
							 placementUpdate->sourceNode->workerName,
							 placementUpdate->sourceNode->workerPort,
							 placementUpdate->targetNode->workerName,
							 placementUpdate->targetNode->workerPort
							 )));
		UpdateShardPlacement(placementUpdate, responsiveWorkerList,
							 shardReplicationModeOid);
		MemoryContextReset(localContext);
	}
	MemoryContextSwitchTo(oldContext);
}


/*
 * SetupRebalanceMonitor initializes the dynamic shared memory required for storing the
 * progress information of a rebalance process. The function takes a List of
 * PlacementUpdateEvents for all shards that will be moved (including colocated
 * ones) and the relation id of the target table. The dynamic shared memory
 * portion consists of a RebalanceMonitorHeader and multiple
 * PlacementUpdateEventProgress, one for each planned shard placement move. The
 * dsm_handle of the created segment is savedin the progress of the current backend so
 * that it can be read by external agents such as get_rebalance_progress function by
 * calling pg_stat_get_progress_info UDF. Since currently only VACUUM commands are
 * officially allowed as the command type, we describe ourselves as a VACUUM command and
 * in order to distinguish a rebalancer progress from regular VACUUM progresses, we put
 * a magic number to the first progress field as an indicator. Finally we return the
 * dsm handle so that it can be used for updating the progress and cleaning things up.
 */
static void
SetupRebalanceMonitor(List *placementUpdateList, Oid relationId)
{
	List *colocatedUpdateList = GetColocatedRebalanceSteps(placementUpdateList);
	ListCell *colocatedUpdateCell = NULL;

	dsm_handle dsmHandle;
	ProgressMonitorData *monitor = CreateProgressMonitor(
		list_length(colocatedUpdateList),
		sizeof(PlacementUpdateEventProgress),
		&dsmHandle);
	PlacementUpdateEventProgress *rebalanceSteps = ProgressMonitorSteps(monitor);

	int32 eventIndex = 0;
	foreach(colocatedUpdateCell, colocatedUpdateList)
	{
		PlacementUpdateEvent *colocatedUpdate = lfirst(colocatedUpdateCell);
		PlacementUpdateEventProgress *event = rebalanceSteps + eventIndex;

		strlcpy(event->sourceName, colocatedUpdate->sourceNode->workerName, 255);
		strlcpy(event->targetName, colocatedUpdate->targetNode->workerName, 255);

		event->shardId = colocatedUpdate->shardId;
		event->sourcePort = colocatedUpdate->sourceNode->workerPort;
		event->targetPort = colocatedUpdate->targetNode->workerPort;
		pg_atomic_init_u64(&event->progress, REBALANCE_PROGRESS_WAITING);

		eventIndex++;
	}
	RegisterProgressMonitor(REBALANCE_ACTIVITY_MAGIC_NUMBER, relationId, dsmHandle);
}


/*
 * rebalance_table_shards rebalances the shards across the workers.
 *
 * SQL signature:
 *
 * rebalance_table_shards(
 *     relation regclass,
 *     threshold float4,
 *     max_shard_moves int,
 *     excluded_shard_list bigint[],
 *     shard_transfer_mode citus.shard_transfer_mode,
 *     drain_only boolean,
 *     rebalance_strategy name
 * ) RETURNS VOID
 */
Datum
rebalance_table_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	List *relationIdList = NIL;
	if (!PG_ARGISNULL(0))
	{
		Oid relationId = PG_GETARG_OID(0);
		ErrorIfMoveUnsupportedTableType(relationId);
		relationIdList = list_make1_oid(relationId);
	}
	else
	{
		/*
		 * Note that we don't need to do any checks to error out for
		 * citus local tables here as NonColocatedDistRelationIdList
		 * already doesn't return non-distributed tables.
		 */
		relationIdList = NonColocatedDistRelationIdList();
	}

	PG_ENSURE_ARGNOTNULL(2, "max_shard_moves");
	PG_ENSURE_ARGNOTNULL(3, "excluded_shard_list");
	PG_ENSURE_ARGNOTNULL(4, "shard_transfer_mode");
	PG_ENSURE_ARGNOTNULL(5, "drain_only");

	Form_pg_dist_rebalance_strategy strategy = GetRebalanceStrategy(
		PG_GETARG_NAME_OR_NULL(6));
	RebalanceOptions options = {
		.relationIdList = relationIdList,
		.threshold = PG_GETARG_FLOAT4_OR_DEFAULT(1, strategy->defaultThreshold),
		.maxShardMoves = PG_GETARG_INT32(2),
		.excludedShardArray = PG_GETARG_ARRAYTYPE_P(3),
		.drainOnly = PG_GETARG_BOOL(5),
		.rebalanceStrategy = strategy,
		.improvementThreshold = strategy->improvementThreshold,
	};
	Oid shardTransferModeOid = PG_GETARG_OID(4);
	RebalanceTableShards(&options, shardTransferModeOid);
	PG_RETURN_VOID();
}


/*
 * GetRebalanceStrategy returns the rebalance strategy from
 * pg_dist_rebalance_strategy matching the given name. If name is NULL it
 * returns the default rebalance strategy from pg_dist_rebalance_strategy.
 */
static Form_pg_dist_rebalance_strategy
GetRebalanceStrategy(Name name)
{
	Relation pgDistRebalanceStrategy = table_open(DistRebalanceStrategyRelationId(),
												  AccessShareLock);

	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	if (name == NULL)
	{
		/* WHERE default_strategy=true */
		ScanKeyInit(&scanKey[0], Anum_pg_dist_rebalance_strategy_default_strategy,
					BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(true));
	}
	else
	{
		/* WHERE name=$name */
		ScanKeyInit(&scanKey[0], Anum_pg_dist_rebalance_strategy_name,
					BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(name));
	}
	SysScanDesc scanDescriptor = systable_beginscan(pgDistRebalanceStrategy,
													InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		if (name == NULL)
		{
			ereport(ERROR, (errmsg(
								"no rebalance_strategy was provided, but there is also no default strategy set")));
		}
		ereport(ERROR, (errmsg("could not find rebalance strategy with name %s",
							   (char *) name)));
	}

	Form_pg_dist_rebalance_strategy strategy =
		(Form_pg_dist_rebalance_strategy) GETSTRUCT(heapTuple);
	Form_pg_dist_rebalance_strategy strategy_copy =
		palloc0(sizeof(FormData_pg_dist_rebalance_strategy));

	/* Copy data over by dereferencing */
	*strategy_copy = *strategy;


	systable_endscan(scanDescriptor);
	table_close(pgDistRebalanceStrategy, NoLock);

	return strategy_copy;
}


/*
 * citus_drain_node drains a node by setting shouldhaveshards to false and
 * running the rebalancer after in drain_only mode.
 */
Datum
citus_drain_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	PG_ENSURE_ARGNOTNULL(0, "nodename");
	PG_ENSURE_ARGNOTNULL(1, "nodeport");
	PG_ENSURE_ARGNOTNULL(2, "shard_transfer_mode");

	text *nodeNameText = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	Oid shardTransferModeOid = PG_GETARG_OID(2);
	Form_pg_dist_rebalance_strategy strategy = GetRebalanceStrategy(
		PG_GETARG_NAME_OR_NULL(3));
	RebalanceOptions options = {
		.relationIdList = NonColocatedDistRelationIdList(),
		.threshold = strategy->defaultThreshold,
		.maxShardMoves = 0,
		.excludedShardArray = construct_empty_array(INT4OID),
		.drainOnly = true,
		.rebalanceStrategy = strategy,
	};

	char *nodeName = text_to_cstring(nodeNameText);

	/*
	 * This is done in a separate session. This way it's not undone if the
	 * draining fails midway through.
	 */
	ExecuteRebalancerCommandInSeparateTransaction(psprintf(
													  "SELECT master_set_node_property(%s, %i, 'shouldhaveshards', false)",
													  quote_literal_cstr(nodeName),
													  nodePort));

	RebalanceTableShards(&options, shardTransferModeOid);

	PG_RETURN_VOID();
}


/*
 * replicate_table_shards replicates under-replicated shards of the specified
 * table.
 */
Datum
replicate_table_shards(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	Oid relationId = PG_GETARG_OID(0);
	uint32 shardReplicationFactor = PG_GETARG_INT32(1);
	int32 maxShardCopies = PG_GETARG_INT32(2);
	ArrayType *excludedShardArray = PG_GETARG_ARRAYTYPE_P(3);
	Oid shardReplicationModeOid = PG_GETARG_OID(4);

	char transferMode = LookupShardTransferMode(shardReplicationModeOid);
	EnsureReferenceTablesExistOnAllNodesExtended(transferMode);

	AcquireColocationLock(relationId, "replicate");

	List *activeWorkerList = SortedActiveWorkers();
	List *shardPlacementList = FullShardPlacementList(relationId, excludedShardArray);
	List *activeShardPlacementList = FilterShardPlacementList(shardPlacementList,
															  IsActiveShardPlacement);

	List *placementUpdateList = ReplicationPlacementUpdates(activeWorkerList,
															activeShardPlacementList,
															shardReplicationFactor);
	placementUpdateList = list_truncate(placementUpdateList, maxShardCopies);

	ExecutePlacementUpdates(placementUpdateList, shardReplicationModeOid, "Copying");

	PG_RETURN_VOID();
}


/*
 * master_drain_node is a wrapper function for old UDF name.
 */
Datum
master_drain_node(PG_FUNCTION_ARGS)
{
	return citus_drain_node(fcinfo);
}


/*
 * get_rebalance_table_shards_plan function calculates the shard move steps
 * required for the rebalance operations including the ones for colocated
 * tables.
 *
 * SQL signature:
 *
 * get_rebalance_table_shards_plan(
 *     relation regclass,
 *     threshold float4,
 *     max_shard_moves int,
 *     excluded_shard_list bigint[],
 *     drain_only boolean,
 *     rebalance_strategy name
 * )
 */
Datum
get_rebalance_table_shards_plan(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	List *relationIdList = NIL;
	if (!PG_ARGISNULL(0))
	{
		Oid relationId = PG_GETARG_OID(0);
		ErrorIfMoveUnsupportedTableType(relationId);

		relationIdList = list_make1_oid(relationId);
	}
	else
	{
		/*
		 * Note that we don't need to do any checks to error out for
		 * citus local tables here as NonColocatedDistRelationIdList
		 * already doesn't return non-distributed tables.
		 */
		relationIdList = NonColocatedDistRelationIdList();
	}

	PG_ENSURE_ARGNOTNULL(2, "max_shard_moves");
	PG_ENSURE_ARGNOTNULL(3, "excluded_shard_list");
	PG_ENSURE_ARGNOTNULL(4, "drain_only");

	Form_pg_dist_rebalance_strategy strategy = GetRebalanceStrategy(
		PG_GETARG_NAME_OR_NULL(5));
	RebalanceOptions options = {
		.relationIdList = relationIdList,
		.threshold = PG_GETARG_FLOAT4_OR_DEFAULT(1, strategy->defaultThreshold),
		.maxShardMoves = PG_GETARG_INT32(2),
		.excludedShardArray = PG_GETARG_ARRAYTYPE_P(3),
		.drainOnly = PG_GETARG_BOOL(4),
		.rebalanceStrategy = strategy,
		.improvementThreshold = PG_GETARG_FLOAT4_OR_DEFAULT(
			6, strategy->improvementThreshold),
	};


	List *placementUpdateList = GetRebalanceSteps(&options);
	List *colocatedUpdateList = GetColocatedRebalanceSteps(placementUpdateList);
	ListCell *colocatedUpdateCell = NULL;

	TupleDesc tupdesc;
	Tuplestorestate *tupstore = SetupTuplestore(fcinfo, &tupdesc);

	foreach(colocatedUpdateCell, colocatedUpdateList)
	{
		PlacementUpdateEvent *colocatedUpdate = lfirst(colocatedUpdateCell);
		Datum values[7];
		bool nulls[7];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(RelationIdForShard(colocatedUpdate->shardId));
		values[1] = UInt64GetDatum(colocatedUpdate->shardId);
		values[2] = UInt64GetDatum(ShardLength(colocatedUpdate->shardId));
		values[3] = PointerGetDatum(cstring_to_text(
										colocatedUpdate->sourceNode->workerName));
		values[4] = UInt32GetDatum(colocatedUpdate->sourceNode->workerPort);
		values[5] = PointerGetDatum(cstring_to_text(
										colocatedUpdate->targetNode->workerName));
		values[6] = UInt32GetDatum(colocatedUpdate->targetNode->workerPort);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}


/*
 * get_rebalance_progress collects information about the ongoing rebalance operations and
 * returns the concatenated list of steps involved in the operations, along with their
 * progress information. Currently the progress field can take 4 integer values
 * (-1: error, 0: waiting, 1: moving, 2: moved). The progress field is of type bigint
 * because we may implement a more granular, byte-level progress as a future improvement.
 */
Datum
get_rebalance_progress(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	List *segmentList = NIL;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore = SetupTuplestore(fcinfo, &tupdesc);

	/* get the addresses of all current rebalance monitors */
	List *rebalanceMonitorList = ProgressMonitorList(REBALANCE_ACTIVITY_MAGIC_NUMBER,
													 &segmentList);

	ProgressMonitorData *monitor = NULL;
	foreach_ptr(monitor, rebalanceMonitorList)
	{
		PlacementUpdateEventProgress *placementUpdateEvents = ProgressMonitorSteps(
			monitor);
		HTAB *shardStatistics = BuildWorkerShardStatisticsHash(placementUpdateEvents,
															   monitor->stepCount);
		HTAB *shardSizes = BuildShardSizesHash(monitor, shardStatistics);
		for (int eventIndex = 0; eventIndex < monitor->stepCount; eventIndex++)
		{
			PlacementUpdateEventProgress *step = placementUpdateEvents + eventIndex;
			uint64 shardId = step->shardId;
			ShardInterval *shardInterval = LoadShardInterval(shardId);

			uint64 sourceSize = WorkerShardSize(shardStatistics, step->sourceName,
												step->sourcePort, shardId);
			uint64 targetSize = WorkerShardSize(shardStatistics, step->targetName,
												step->targetPort, shardId);

			uint64 shardSize = 0;
			ShardStatistics *shardSizesStat =
				hash_search(shardSizes, &shardId, HASH_FIND, NULL);
			if (shardSizesStat)
			{
				shardSize = shardSizesStat->totalSize;
			}

			Datum values[11];
			bool nulls[11];

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[0] = monitor->processId;
			values[1] = ObjectIdGetDatum(shardInterval->relationId);
			values[2] = UInt64GetDatum(shardId);
			values[3] = UInt64GetDatum(shardSize);
			values[4] = PointerGetDatum(cstring_to_text(step->sourceName));
			values[5] = UInt32GetDatum(step->sourcePort);
			values[6] = PointerGetDatum(cstring_to_text(step->targetName));
			values[7] = UInt32GetDatum(step->targetPort);
			values[8] = UInt64GetDatum(pg_atomic_read_u64(&step->progress));
			values[9] = UInt64GetDatum(sourceSize);
			values[10] = UInt64GetDatum(targetSize);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	DetachFromDSMSegments(segmentList);

	return (Datum) 0;
}


/*
 * BuildShardSizesHash creates a hash that maps a shardid to its full size
 * within the cluster. It does this by using the rebalance progress monitor
 * state to find the node the shard is currently on. It then looks up the shard
 * size in the shardStatistics hashmap for this node.
 */
static HTAB *
BuildShardSizesHash(ProgressMonitorData *monitor, HTAB *shardStatistics)
{
	HASHCTL info = {
		.keysize = sizeof(uint64),
		.entrysize = sizeof(ShardStatistics),
		.hcxt = CurrentMemoryContext
	};

	HTAB *shardSizes = hash_create(
		"ShardSizeHash", 32, &info,
		HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	PlacementUpdateEventProgress *placementUpdateEvents = ProgressMonitorSteps(monitor);

	for (int eventIndex = 0; eventIndex < monitor->stepCount; eventIndex++)
	{
		PlacementUpdateEventProgress *step = placementUpdateEvents + eventIndex;

		uint64 shardId = step->shardId;
		uint64 shardSize = 0;
		uint64 backupShardSize = 0;
		uint64 progress = pg_atomic_read_u64(&step->progress);

		uint64 sourceSize = WorkerShardSize(shardStatistics, step->sourceName,
											step->sourcePort, shardId);
		uint64 targetSize = WorkerShardSize(shardStatistics, step->targetName,
											step->targetPort, shardId);

		if (progress == REBALANCE_PROGRESS_WAITING ||
			progress == REBALANCE_PROGRESS_MOVING)
		{
			/*
			 * If we are not done with the move, the correct shard size is the
			 * size on the source.
			 */
			shardSize = sourceSize;
			backupShardSize = targetSize;
		}
		else if (progress == REBALANCE_PROGRESS_MOVED)
		{
			/*
			 * If we are done with the move, the correct shard size is the size
			 * on the target
			 */
			shardSize = targetSize;
			backupShardSize = sourceSize;
		}

		if (shardSize == 0)
		{
			if (backupShardSize == 0)
			{
				/*
				 * We don't have any useful shard size. This can happen when a
				 * shard is moved multiple times and it is not present on
				 * either of these nodes. Probably the shard is on a worker
				 * related to another event. In the weird case that this shard
				 * is on the nodes and actually is size 0, we will have no
				 * entry in the hashmap. When fetching from it we always
				 * default to 0 if no entry is found, so that's fine.
				 */
				continue;
			}

			/*
			 * Because of the way we fetch shard sizes they are from a slightly
			 * earlier moment than the progress state we just read from shared
			 * memory. Usually this is no problem, but there exist some race
			 * conditions where this matters. For example, for very quick moves
			 * it is possible that even though a step is now reported as MOVED,
			 * when we read the shard sizes the move had not even started yet.
			 * This in turn can mean that the target size is 0 while the source
			 * size is not. We try to handle such rare edge cases by falling
			 * back on the other shard size if that one is not 0.
			 */
			shardSize = backupShardSize;
		}


		ShardStatistics *currentWorkerStatistics =
			hash_search(shardSizes, &shardId, HASH_ENTER, NULL);
		currentWorkerStatistics->totalSize = shardSize;
	}
	return shardSizes;
}


/*
 * WorkerShardSize returns the size of a shard in bytes on a worker, based on
 * the workerShardStatisticsHash.
 */
static uint64
WorkerShardSize(HTAB *workerShardStatisticsHash, char *workerName, int workerPort,
				uint64 shardId)
{
	WorkerHashKey workerKey = { 0 };
	strlcpy(workerKey.hostname, workerName, MAX_NODE_LENGTH);
	workerKey.port = workerPort;

	WorkerShardStatistics *workerStats =
		hash_search(workerShardStatisticsHash, &workerKey, HASH_FIND, NULL);
	if (!workerStats)
	{
		return 0;
	}

	ShardStatistics *shardStats =
		hash_search(workerStats->statistics, &shardId, HASH_FIND, NULL);
	if (!shardStats)
	{
		return 0;
	}
	return shardStats->totalSize;
}


/*
 * BuildWorkerShardStatisticsHash returns a shard id -> shard statistics hash containing
 * sizes of shards on the source node and destination node.
 */
static HTAB *
BuildWorkerShardStatisticsHash(PlacementUpdateEventProgress *steps, int stepCount)
{
	HTAB *shardsByWorker = GetMovedShardIdsByWorker(steps, stepCount, true);

	HASHCTL info = {
		.keysize = sizeof(WorkerHashKey),
		.entrysize = sizeof(WorkerShardStatistics),
		.hcxt = CurrentMemoryContext
	};

	HTAB *workerShardStatistics = hash_create("WorkerShardStatistics", 32, &info,
											  HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	WorkerShardIds *entry = NULL;

	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardsByWorker);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		int connectionFlags = 0;
		MultiConnection *connection = GetNodeConnection(connectionFlags,
														entry->worker.hostname,
														entry->worker.port);

		HTAB *statistics =
			GetShardStatistics(connection, entry->shardIds);

		WorkerHashKey workerKey = { 0 };
		strlcpy(workerKey.hostname, entry->worker.hostname, MAX_NODE_LENGTH);
		workerKey.port = entry->worker.port;

		WorkerShardStatistics *moveStat =
			hash_search(workerShardStatistics, &entry->worker, HASH_ENTER, NULL);
		moveStat->statistics = statistics;
	}

	return workerShardStatistics;
}


/*
 * GetShardStatistics fetches the statics for the given shard ids over the
 * given connection. It returns a hashmap where the keys are the shard ids and
 * the values are the statistics.
 */
static HTAB *
GetShardStatistics(MultiConnection *connection, HTAB *shardIds)
{
	StringInfo query = makeStringInfo();

	appendStringInfoString(
		query,
		"WITH shard_names (shard_id, schema_name, table_name) AS ((VALUES ");

	bool isFirst = true;
	uint64 *shardIdPtr = NULL;
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardIds);
	while ((shardIdPtr = hash_seq_search(&status)) != NULL)
	{
		uint64 shardId = *shardIdPtr;
		ShardInterval *shardInterval = LoadShardInterval(shardId);
		Oid relationId = shardInterval->relationId;
		char *shardName = get_rel_name(relationId);

		AppendShardIdToName(&shardName, shardId);

		Oid schemaId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(schemaId);
		if (!isFirst)
		{
			appendStringInfo(query, ", ");
		}

		appendStringInfo(query, "(" UINT64_FORMAT ",%s,%s)",
						 shardId,
						 quote_literal_cstr(schemaName),
						 quote_literal_cstr(shardName));

		isFirst = false;
	}

	appendStringInfoString(query, "))");
	appendStringInfoString(
		query,
		" SELECT shard_id, coalesce(pg_total_relation_size(tables.relid),0)"

		/* for each shard in shardIds */
		" FROM shard_names"

		/* check if its name can be found in pg_class, if so return size */
		" LEFT JOIN"
		" (SELECT c.oid AS relid, c.relname, n.nspname"
		" FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace) tables"
		" ON tables.relname = shard_names.table_name AND"
		" tables.nspname = shard_names.schema_name ");

	PGresult *result = NULL;
	int queryResult = ExecuteOptionalRemoteCommand(connection, query->data, &result);
	if (queryResult != RESPONSE_OKAY)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot get the size because of a connection error")));
	}

	int rowCount = PQntuples(result);
	int colCount = PQnfields(result);

	/* This is not expected to ever happen, but we check just to be sure */
	if (colCount < 2)
	{
		ereport(ERROR, (errmsg("unexpected number of columns returned by: %s",
							   query->data)));
	}

	HASHCTL info = {
		.keysize = sizeof(uint64),
		.entrysize = sizeof(ShardStatistics),
		.hcxt = CurrentMemoryContext
	};

	HTAB *shardStatistics = hash_create("ShardStatisticsHash", 32, &info,
										HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		char *shardIdString = PQgetvalue(result, rowIndex, 0);
		uint64 shardId = strtou64(shardIdString, NULL, 10);
		char *sizeString = PQgetvalue(result, rowIndex, 1);
		uint64 totalSize = strtou64(sizeString, NULL, 10);

		ShardStatistics *statistics =
			hash_search(shardStatistics, &shardId, HASH_ENTER, NULL);
		statistics->totalSize = totalSize;
	}

	PQclear(result);

	bool raiseErrors = true;
	ClearResults(connection, raiseErrors);

	return shardStatistics;
}


/*
 * GetMovedShardIdsByWorker groups the shard ids in the provided steps by
 * worker. It returns a hashmap that contains a set of these shard ids.
 */
static HTAB *
GetMovedShardIdsByWorker(PlacementUpdateEventProgress *steps, int stepCount,
						 bool fromSource)
{
	HASHCTL info = {
		.keysize = sizeof(WorkerHashKey),
		.entrysize = sizeof(WorkerShardIds),
		.hcxt = CurrentMemoryContext
	};

	HTAB *shardsByWorker = hash_create("GetRebalanceStepsByWorker", 32, &info,
									   HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	for (int stepIndex = 0; stepIndex < stepCount; stepIndex++)
	{
		PlacementUpdateEventProgress *step = &(steps[stepIndex]);

		AddToWorkerShardIdSet(shardsByWorker, step->sourceName, step->sourcePort,
							  step->shardId);

		if (pg_atomic_read_u64(&step->progress) == REBALANCE_PROGRESS_WAITING)
		{
			/*
			 * shard move has not started so we don't need target stats for
			 * this shard
			 */
			continue;
		}

		AddToWorkerShardIdSet(shardsByWorker, step->targetName, step->targetPort,
							  step->shardId);
	}

	return shardsByWorker;
}


/*
 * AddToWorkerShardIdSet adds the shard id to the shard id set for the
 * specified worker in the shardsByWorker hashmap.
 */
static void
AddToWorkerShardIdSet(HTAB *shardsByWorker, char *workerName, int workerPort,
					  uint64 shardId)
{
	WorkerHashKey workerKey = { 0 };

	strlcpy(workerKey.hostname, workerName, MAX_NODE_LENGTH);
	workerKey.port = workerPort;

	bool isFound = false;
	WorkerShardIds *workerShardIds =
		hash_search(shardsByWorker, &workerKey, HASH_ENTER, &isFound);
	if (!isFound)
	{
		HASHCTL info = {
			.keysize = sizeof(uint64),
			.entrysize = sizeof(uint64),
			.hcxt = CurrentMemoryContext
		};

		workerShardIds->shardIds = hash_create(
			"WorkerShardIdsSet", 32, &info,
			HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	}

	hash_search(workerShardIds->shardIds, &shardId, HASH_ENTER, NULL);
}


/*
 * NonColocatedDistRelationIdList returns a list of distributed table oids, one
 * for each existing colocation group.
 */
static List *
NonColocatedDistRelationIdList(void)
{
	List *relationIdList = NIL;
	List *allCitusTablesList = CitusTableTypeIdList(ANY_CITUS_TABLE_TYPE);
	Oid tableId = InvalidOid;

	/* allocate sufficient capacity for O(1) expected look-up time */
	int capacity = (int) (list_length(allCitusTablesList) / 0.75) + 1;
	int flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS;
	HASHCTL info = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(Oid),
		.hcxt = CurrentMemoryContext
	};

	HTAB *alreadySelectedColocationIds = hash_create("RebalanceColocationIdSet",
													 capacity, &info, flags);
	foreach_oid(tableId, allCitusTablesList)
	{
		bool foundInSet = false;
		CitusTableCacheEntry *citusTableCacheEntry = GetCitusTableCacheEntry(
			tableId);

		if (!IsCitusTableTypeCacheEntry(citusTableCacheEntry, DISTRIBUTED_TABLE))
		{
			/*
			 * We're only interested in distributed tables, should ignore
			 * reference tables and citus local tables.
			 */
			continue;
		}

		if (citusTableCacheEntry->colocationId != INVALID_COLOCATION_ID)
		{
			hash_search(alreadySelectedColocationIds,
						&citusTableCacheEntry->colocationId, HASH_ENTER,
						&foundInSet);
			if (foundInSet)
			{
				continue;
			}
		}
		relationIdList = lappend_oid(relationIdList, tableId);
	}
	return relationIdList;
}


/*
 * RebalanceTableShards rebalances the shards for the relations inside the
 * relationIdList across the different workers.
 */
static void
RebalanceTableShards(RebalanceOptions *options, Oid shardReplicationModeOid)
{
	char transferMode = LookupShardTransferMode(shardReplicationModeOid);
	EnsureReferenceTablesExistOnAllNodesExtended(transferMode);

	if (list_length(options->relationIdList) == 0)
	{
		return;
	}

	Oid relationId = InvalidOid;
	char *operationName = "rebalance";
	if (options->drainOnly)
	{
		operationName = "move";
	}

	foreach_oid(relationId, options->relationIdList)
	{
		AcquireColocationLock(relationId, operationName);
	}

	List *placementUpdateList = GetRebalanceSteps(options);

	if (list_length(placementUpdateList) == 0)
	{
		return;
	}

	/*
	 * This uses the first relationId from the list, it's only used for display
	 * purposes so it does not really matter which to show
	 */
	SetupRebalanceMonitor(placementUpdateList, linitial_oid(options->relationIdList));
	ExecutePlacementUpdates(placementUpdateList, shardReplicationModeOid, "Moving");
	FinalizeCurrentProgressMonitor();
}


/*
 * ConflictShardPlacementUpdateOnlyWithIsolationTesting is only useful for
 * testing and should not be called by any code-path except for
 * UpdateShardPlacement().
 *
 * To be able to test the rebalance monitor functionality correctly, we need to
 * be able to pause the rebalancer at a specific place in time. We cannot do
 * this by block the shard move itself someway (e.g. by calling truncate on the
 * distributed table). The reason for this is that we do the shard move in a
 * newly opened connection. This causes our isolation tester block detection to
 * not realise that the rebalance_table_shards call is blocked.
 *
 * So instead, before opening a connection we lock an advisory lock that's
 * based on the shard id (shard id mod 1000). By locking this advisory lock in
 * a different session we can block the rebalancer in a way that the isolation
 * tester block detection is able to detect.
 */
static void
ConflictShardPlacementUpdateOnlyWithIsolationTesting(uint64 shardId)
{
	LOCKTAG tag;
	const bool sessionLock = false;
	const bool dontWait = false;

	if (RunningUnderIsolationTest)
	{
		/* we've picked a random lock */
		SET_LOCKTAG_ADVISORY(tag, MyDatabaseId,
							 SHARD_PLACEMENT_UPDATE_ADVISORY_LOCK_FIRST_KEY,
							 shardId % 1000, 2);

		(void) LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
	}
}


/*
 * UpdateShardPlacement copies or moves a shard placement by calling
 * the corresponding functions in Citus in a subtransaction.
 */
static void
UpdateShardPlacement(PlacementUpdateEvent *placementUpdateEvent,
					 List *responsiveNodeList, Oid shardReplicationModeOid)
{
	PlacementUpdateType updateType = placementUpdateEvent->updateType;
	uint64 shardId = placementUpdateEvent->shardId;
	WorkerNode *sourceNode = placementUpdateEvent->sourceNode;
	WorkerNode *targetNode = placementUpdateEvent->targetNode;
	const char *doRepair = "false";

	Datum shardTranferModeLabelDatum =
		DirectFunctionCall1(enum_out, shardReplicationModeOid);
	char *shardTranferModeLabel = DatumGetCString(shardTranferModeLabelDatum);

	StringInfo placementUpdateCommand = makeStringInfo();

	/* if target node is not responsive, don't continue */
	bool targetResponsive = WorkerNodeListContains(responsiveNodeList,
												   targetNode->workerName,
												   targetNode->workerPort);
	if (!targetResponsive)
	{
		ereport(ERROR, (errmsg("target node %s:%d is not responsive",
							   targetNode->workerName,
							   targetNode->workerPort)));
	}

	/* if source node is not responsive, don't continue */
	bool sourceResponsive = WorkerNodeListContains(responsiveNodeList,
												   sourceNode->workerName,
												   sourceNode->workerPort);
	if (!sourceResponsive)
	{
		ereport(ERROR, (errmsg("source node %s:%d is not responsive",
							   sourceNode->workerName,
							   sourceNode->workerPort)));
	}

	if (updateType == PLACEMENT_UPDATE_MOVE)
	{
		appendStringInfo(placementUpdateCommand,
						 "SELECT citus_move_shard_placement(%ld,%s,%u,%s,%u,%s)",
						 shardId,
						 quote_literal_cstr(sourceNode->workerName),
						 sourceNode->workerPort,
						 quote_literal_cstr(targetNode->workerName),
						 targetNode->workerPort,
						 quote_literal_cstr(shardTranferModeLabel));
	}
	else if (updateType == PLACEMENT_UPDATE_COPY)
	{
		appendStringInfo(placementUpdateCommand,
						 "SELECT citus_copy_shard_placement(%ld,%s,%u,%s,%u,%s,%s)",
						 shardId,
						 quote_literal_cstr(sourceNode->workerName),
						 sourceNode->workerPort,
						 quote_literal_cstr(targetNode->workerName),
						 targetNode->workerPort,
						 doRepair,
						 quote_literal_cstr(shardTranferModeLabel));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("only moving or copying shards is supported")));
	}

	UpdateColocatedShardPlacementProgress(shardId,
										  sourceNode->workerName,
										  sourceNode->workerPort,
										  REBALANCE_PROGRESS_MOVING);

	ConflictShardPlacementUpdateOnlyWithIsolationTesting(shardId);

	/*
	 * In case of failure, we throw an error such that rebalance_table_shards
	 * fails early.
	 */
	ExecuteRebalancerCommandInSeparateTransaction(placementUpdateCommand->data);

	UpdateColocatedShardPlacementProgress(shardId,
										  sourceNode->workerName,
										  sourceNode->workerPort,
										  REBALANCE_PROGRESS_MOVED);
}


/*
 * ExecuteCriticalCommandInSeparateTransaction runs a command in a separate
 * transaction that is commited right away. This is useful for things that you
 * don't want to rollback when the current transaction is rolled back.
 */
void
ExecuteRebalancerCommandInSeparateTransaction(char *command)
{
	int connectionFlag = FORCE_NEW_CONNECTION;
	MultiConnection *connection = GetNodeConnection(connectionFlag, LocalHostName,
													PostPortNumber);
	StringInfo setApplicationName = makeStringInfo();
	appendStringInfo(setApplicationName, "SET application_name TO %s",
					 CITUS_REBALANCER_NAME);

	ExecuteCriticalRemoteCommand(connection, setApplicationName->data);
	ExecuteCriticalRemoteCommand(connection, command);

	CloseConnection(connection);
}


/*
 * RebalancePlacementUpdates returns a list of placement updates which makes the
 * cluster balanced. We move shards to these nodes until all nodes become utilized.
 * We consider a node under-utilized if it has less than floor((1.0 - threshold) *
 * placementCountAverage) shard placements. In each iteration we choose the node
 * with maximum number of shard placements as the source, and we choose the node
 * with minimum number of shard placements as the target. Then we choose a shard
 * which is placed in the source node but not in the target node as the shard to
 * move.
 *
 * The activeShardPlacementListList argument contains a list of lists of active shard
 * placements. Each of these lists are balanced independently. This is used to
 * make sure different colocation groups are balanced separately, so each list
 * contains the placements of a colocation group.
 */
List *
RebalancePlacementUpdates(List *workerNodeList, List *activeShardPlacementListList,
						  double threshold,
						  int32 maxShardMoves,
						  bool drainOnly,
						  float4 improvementThreshold,
						  RebalancePlanFunctions *functions)
{
	List *rebalanceStates = NIL;
	RebalanceState *state = NULL;
	List *shardPlacementList = NIL;
	List *placementUpdateList = NIL;

	foreach_ptr(shardPlacementList, activeShardPlacementListList)
	{
		state = InitRebalanceState(workerNodeList, shardPlacementList,
								   functions);
		rebalanceStates = lappend(rebalanceStates, state);
	}

	foreach_ptr(state, rebalanceStates)
	{
		state->placementUpdateList = placementUpdateList;
		MoveShardsAwayFromDisallowedNodes(state);
		placementUpdateList = state->placementUpdateList;
	}

	if (!drainOnly)
	{
		foreach_ptr(state, rebalanceStates)
		{
			state->placementUpdateList = placementUpdateList;

			/* calculate lower bound for placement count */
			float4 averageUtilization = (state->totalCost / state->totalCapacity);
			float4 utilizationLowerBound = ((1.0 - threshold) * averageUtilization);
			float4 utilizationUpperBound = ((1.0 + threshold) * averageUtilization);

			bool moreMovesAvailable = true;
			while (list_length(state->placementUpdateList) < maxShardMoves &&
				   moreMovesAvailable)
			{
				moreMovesAvailable = FindAndMoveShardCost(
					utilizationLowerBound,
					utilizationUpperBound,
					improvementThreshold,
					state);
			}
			placementUpdateList = state->placementUpdateList;

			if (moreMovesAvailable)
			{
				ereport(NOTICE, (errmsg(
									 "Stopped searching before we were out of moves. "
									 "Please rerun the rebalancer after it's finished "
									 "for a more optimal placement.")));
				break;
			}
		}
	}

	foreach_ptr(state, rebalanceStates)
	{
		hash_destroy(state->placementsHash);
	}

	int64 ignoredMoves = 0;
	foreach_ptr(state, rebalanceStates)
	{
		ignoredMoves += state->ignoredMoves;
	}

	if (ignoredMoves > 0)
	{
		if (MaxRebalancerLoggedIgnoredMoves == -1 ||
			ignoredMoves <= MaxRebalancerLoggedIgnoredMoves)
		{
			ereport(NOTICE, (
						errmsg(
							"Ignored %ld moves, all of which are shown in notices above",
							ignoredMoves
							),
						errhint(
							"If you do want these moves to happen, try changing improvement_threshold to a lower value than what it is now (%g).",
							improvementThreshold)
						));
		}
		else
		{
			ereport(NOTICE, (
						errmsg(
							"Ignored %ld moves, %d of which are shown in notices above",
							ignoredMoves,
							MaxRebalancerLoggedIgnoredMoves
							),
						errhint(
							"If you do want these moves to happen, try changing improvement_threshold to a lower value than what it is now (%g).",
							improvementThreshold)
						));
		}
	}
	return placementUpdateList;
}


/*
 * InitRebalanceState sets up a RebalanceState for it's arguments. The
 * RebalanceState contains the information needed to calculate shard moves.
 */
static RebalanceState *
InitRebalanceState(List *workerNodeList, List *shardPlacementList,
				   RebalancePlanFunctions *functions)
{
	ShardPlacement *placement = NULL;
	HASH_SEQ_STATUS status;
	WorkerNode *workerNode = NULL;

	RebalanceState *state = palloc0(sizeof(RebalanceState));
	state->functions = functions;
	state->placementsHash = ShardPlacementsListToHash(shardPlacementList);

	/* create empty fill state for all of the worker nodes */
	foreach_ptr(workerNode, workerNodeList)
	{
		NodeFillState *fillState = palloc0(sizeof(NodeFillState));
		fillState->node = workerNode;
		fillState->capacity = functions->nodeCapacity(workerNode, functions->context);

		/*
		 * Set the utilization here although the totalCost is not set yet. This
		 * is needed to set the utilization to INFINITY when the capacity is 0.
		 */
		fillState->utilization = CalculateUtilization(fillState->totalCost,
													  fillState->capacity);
		state->fillStateListAsc = lappend(state->fillStateListAsc, fillState);
		state->fillStateListDesc = lappend(state->fillStateListDesc, fillState);
		state->totalCapacity += fillState->capacity;
	}

	/* Fill the fill states for all of the worker nodes based on the placements */
	foreach_htab(placement, &status, state->placementsHash)
	{
		ShardCost *shardCost = palloc0(sizeof(ShardCost));
		NodeFillState *fillState = FindFillStateForPlacement(state, placement);

		Assert(fillState != NULL);

		*shardCost = functions->shardCost(placement->shardId, functions->context);

		fillState->totalCost += shardCost->cost;
		fillState->utilization = CalculateUtilization(fillState->totalCost,
													  fillState->capacity);
		fillState->shardCostListDesc = lappend(fillState->shardCostListDesc,
											   shardCost);
		fillState->shardCostListDesc = SortList(fillState->shardCostListDesc,
												CompareShardCostDesc);

		state->totalCost += shardCost->cost;

		if (!functions->shardAllowedOnNode(placement->shardId, fillState->node,
										   functions->context))
		{
			DisallowedPlacement *disallowed = palloc0(sizeof(DisallowedPlacement));
			disallowed->shardCost = shardCost;
			disallowed->fillState = fillState;
			state->disallowedPlacementList = lappend(state->disallowedPlacementList,
													 disallowed);
		}
	}
	foreach_htab_cleanup(placement, &status);

	state->fillStateListAsc = SortList(state->fillStateListAsc, CompareNodeFillStateAsc);
	state->fillStateListDesc = SortList(state->fillStateListDesc,
										CompareNodeFillStateDesc);
	CheckRebalanceStateInvariants(state);

	return state;
}


/*
 * CalculateUtilization returns INFINITY when capacity is 0 and
 * totalCost/capacity otherwise.
 */
static float4
CalculateUtilization(float4 totalCost, float4 capacity)
{
	if (capacity <= 0)
	{
		return INFINITY;
	}
	return totalCost / capacity;
}


/*
 * FindFillStateForPlacement finds the fillState for the workernode that
 * matches the placement.
 */
static NodeFillState *
FindFillStateForPlacement(RebalanceState *state, ShardPlacement *placement)
{
	NodeFillState *fillState = NULL;

	/* Find the correct fill state to add the placement to and do that */
	foreach_ptr(fillState, state->fillStateListAsc)
	{
		if (IsPlacementOnWorkerNode(placement, fillState->node))
		{
			return fillState;
		}
	}
	return NULL;
}


/*
 * IsPlacementOnWorkerNode checks if the shard placement is for to the given
 * workenode.
 */
static bool
IsPlacementOnWorkerNode(ShardPlacement *placement, WorkerNode *workerNode)
{
	if (strncmp(workerNode->workerName, placement->nodeName, WORKER_LENGTH) != 0)
	{
		return false;
	}
	return workerNode->workerPort == placement->nodePort;
}


/*
 * CompareNodeFillStateAsc can be used to sort fill states from empty to full.
 */
static int
CompareNodeFillStateAsc(const void *void1, const void *void2)
{
	const NodeFillState *a = *((const NodeFillState **) void1);
	const NodeFillState *b = *((const NodeFillState **) void2);
	if (a->utilization < b->utilization)
	{
		return -1;
	}
	if (a->utilization > b->utilization)
	{
		return 1;
	}

	/*
	 * If utilization prefer nodes with more capacity, since utilization will
	 * grow slower on those
	 */
	if (a->capacity > b->capacity)
	{
		return -1;
	}
	if (a->capacity < b->capacity)
	{
		return 1;
	}

	/* Finally differentiate by node id */
	if (a->node->nodeId < b->node->nodeId)
	{
		return -1;
	}
	return a->node->nodeId > b->node->nodeId;
}


/*
 * CompareNodeFillStateDesc can be used to sort fill states from full to empty.
 */
static int
CompareNodeFillStateDesc(const void *a, const void *b)
{
	return -CompareNodeFillStateAsc(a, b);
}


/*
 * CompareShardCostAsc can be used to sort shard costs from low cost to high
 * cost.
 */
static int
CompareShardCostAsc(const void *void1, const void *void2)
{
	const ShardCost *a = *((const ShardCost **) void1);
	const ShardCost *b = *((const ShardCost **) void2);
	if (a->cost < b->cost)
	{
		return -1;
	}
	if (a->cost > b->cost)
	{
		return 1;
	}

	/* make compare function (more) stable for tests */
	if (a->shardId > b->shardId)
	{
		return -1;
	}
	return a->shardId < b->shardId;
}


/*
 * CompareShardCostDesc can be used to sort shard costs from high cost to low
 * cost.
 */
static int
CompareShardCostDesc(const void *a, const void *b)
{
	return -CompareShardCostAsc(a, b);
}


/*
 * MoveShardsAwayFromDisallowedNodes returns a list of placement updates that
 * move any shards that are not allowed on their current node to a node that
 * they are allowed on.
 */
static void
MoveShardsAwayFromDisallowedNodes(RebalanceState *state)
{
	DisallowedPlacement *disallowedPlacement = NULL;

	state->disallowedPlacementList = SortList(state->disallowedPlacementList,
											  CompareDisallowedPlacementDesc);

	/* Move shards off of nodes they are not allowed on */
	foreach_ptr(disallowedPlacement, state->disallowedPlacementList)
	{
		NodeFillState *targetFillState = FindAllowedTargetFillState(
			state, disallowedPlacement->shardCost->shardId);
		if (targetFillState == NULL)
		{
			ereport(WARNING, (errmsg(
								  "Not allowed to move shard " UINT64_FORMAT
								  " anywhere from %s:%d",
								  disallowedPlacement->shardCost->shardId,
								  disallowedPlacement->fillState->node->workerName,
								  disallowedPlacement->fillState->node->workerPort
								  )));
			continue;
		}
		MoveShardCost(disallowedPlacement->fillState,
					  targetFillState,
					  disallowedPlacement->shardCost,
					  state);
	}
}


/*
 * CompareDisallowedPlacementAsc can be used to sort disallowed placements from
 * low cost to high cost.
 */
static int
CompareDisallowedPlacementAsc(const void *void1, const void *void2)
{
	const DisallowedPlacement *a = *((const DisallowedPlacement **) void1);
	const DisallowedPlacement *b = *((const DisallowedPlacement **) void2);
	return CompareShardCostAsc(&(a->shardCost), &(b->shardCost));
}


/*
 * CompareDisallowedPlacementDesc can be used to sort disallowed placements from
 * high cost to low cost.
 */
static int
CompareDisallowedPlacementDesc(const void *a, const void *b)
{
	return -CompareDisallowedPlacementAsc(a, b);
}


/*
 * FindAllowedTargetFillState finds the first fill state in fillStateListAsc
 * where the shard can be moved to.
 */
static NodeFillState *
FindAllowedTargetFillState(RebalanceState *state, uint64 shardId)
{
	NodeFillState *targetFillState = NULL;
	foreach_ptr(targetFillState, state->fillStateListAsc)
	{
		bool hasShard = PlacementsHashFind(
			state->placementsHash,
			shardId,
			targetFillState->node);
		if (!hasShard && state->functions->shardAllowedOnNode(
				shardId,
				targetFillState->node,
				state->functions->context))
		{
			return targetFillState;
		}
	}
	return NULL;
}


/*
 * MoveShardCost moves a shardcost from the source to the target fill states
 * and updates the RebalanceState accordingly. What it does in detail is:
 * 1. add a placement update to state->placementUpdateList
 * 2. update state->placementsHash
 * 3. update totalcost, utilization and shardCostListDesc in source and target
 * 4. resort state->fillStateListAsc/Desc
 */
static void
MoveShardCost(NodeFillState *sourceFillState,
			  NodeFillState *targetFillState,
			  ShardCost *shardCost,
			  RebalanceState *state)
{
	uint64 shardIdToMove = shardCost->shardId;

	/* construct the placement update */
	PlacementUpdateEvent *placementUpdateEvent = palloc0(sizeof(PlacementUpdateEvent));
	placementUpdateEvent->updateType = PLACEMENT_UPDATE_MOVE;
	placementUpdateEvent->shardId = shardIdToMove;
	placementUpdateEvent->sourceNode = sourceFillState->node;
	placementUpdateEvent->targetNode = targetFillState->node;

	/* record the placement update */
	state->placementUpdateList = lappend(state->placementUpdateList,
										 placementUpdateEvent);

	/* update the placements hash and the node shard lists */
	PlacementsHashRemove(state->placementsHash, shardIdToMove, sourceFillState->node);
	PlacementsHashEnter(state->placementsHash, shardIdToMove, targetFillState->node);

	sourceFillState->totalCost -= shardCost->cost;
	sourceFillState->utilization = CalculateUtilization(sourceFillState->totalCost,
														sourceFillState->capacity);
	sourceFillState->shardCostListDesc = list_delete_ptr(
		sourceFillState->shardCostListDesc,
		shardCost);

	targetFillState->totalCost += shardCost->cost;
	targetFillState->utilization = CalculateUtilization(targetFillState->totalCost,
														targetFillState->capacity);
	targetFillState->shardCostListDesc = lappend(targetFillState->shardCostListDesc,
												 shardCost);
	targetFillState->shardCostListDesc = SortList(targetFillState->shardCostListDesc,
												  CompareShardCostDesc);

	state->fillStateListAsc = SortList(state->fillStateListAsc, CompareNodeFillStateAsc);
	state->fillStateListDesc = SortList(state->fillStateListDesc,
										CompareNodeFillStateDesc);
	CheckRebalanceStateInvariants(state);
}


/*
 * FindAndMoveShardCost is the main rebalancing algorithm. This takes the
 * current state and returns a list with a new move appended that improves the
 * balance of shards. The algorithm is greedy and will use the first new move
 * that improves the balance. It finds nodes by trying to move a shard from the
 * most utilized node (highest utilization) to the emptiest node (lowest
 * utilization). If no moves are possible it will try the second emptiest node
 * until it tried all of them. Then it wil try the second fullest node. If it
 * was able to find a move it will return true and false if it couldn't.
 *
 * This algorithm won't necessarily result in the best possible balance. Getting
 * the best balance is an NP problem, so it's not feasible to go for the best
 * balance. This algorithm was chosen because of the following reasons:
 * 1. Literature research showed that similar problems would get within 2X of
 *    the optimal balance with a greedy algoritm.
 * 2. Every move will always improve the balance. So if the user stops a
 *    rebalance midway through, they will never be in a worse situation than
 *    before.
 * 3. It's pretty easy to reason about.
 * 4. It's simple to implement.
 *
 * utilizationLowerBound and utilizationUpperBound are used to indicate what
 * the target utilization range of all nodes is. If they are within this range,
 * then balance is good enough. If all nodes are in this range then the cluster
 * is considered balanced and no more moves are done. This is mostly useful for
 * the by_disk_size rebalance strategy. If we wouldn't have this then the
 * rebalancer could become flappy in certain cases.
 *
 * improvementThreshold is a threshold that can be used to ignore moves when
 * they only improve the balance a little relative to the cost of the shard.
 * Again this is mostly useful for the by_disk_size rebalance strategy.
 * Without this threshold the rebalancer would move a shard of 1TB when this
 * move only improves the cluster by 10GB.
 */
static bool
FindAndMoveShardCost(float4 utilizationLowerBound,
					 float4 utilizationUpperBound,
					 float4 improvementThreshold,
					 RebalanceState *state)
{
	NodeFillState *sourceFillState = NULL;
	NodeFillState *targetFillState = NULL;

	/*
	 * find a source node for the move, starting at the node with the highest
	 * utilization
	 */
	foreach_ptr(sourceFillState, state->fillStateListDesc)
	{
		/* Don't move shards away from nodes that are already too empty, we're
		 * done searching */
		if (sourceFillState->utilization <= utilizationLowerBound)
		{
			return false;
		}

		/* find a target node for the move, starting at the node with the
		 * lowest utilization */
		foreach_ptr(targetFillState, state->fillStateListAsc)
		{
			ShardCost *shardCost = NULL;

			/* Don't add more shards to nodes that are already at the upper
			 * bound. We should try the next source node now because further
			 * target nodes will also be above the upper bound */
			if (targetFillState->utilization >= utilizationUpperBound)
			{
				break;
			}

			/* Don't move a shard between nodes that both have decent
			 * utilization. We should try the next source node now because
			 * further target nodes will also have have decent utilization */
			if (targetFillState->utilization >= utilizationLowerBound &&
				sourceFillState->utilization <= utilizationUpperBound)
			{
				break;
			}

			/* find a shardcost that can be moved between between nodes that
			 * makes the cost distribution more equal */
			foreach_ptr(shardCost, sourceFillState->shardCostListDesc)
			{
				bool targetHasShard = PlacementsHashFind(state->placementsHash,
														 shardCost->shardId,
														 targetFillState->node);
				float4 newTargetTotalCost = targetFillState->totalCost + shardCost->cost;
				float4 newTargetUtilization = CalculateUtilization(
					newTargetTotalCost,
					targetFillState->capacity);
				float4 newSourceTotalCost = sourceFillState->totalCost - shardCost->cost;
				float4 newSourceUtilization = CalculateUtilization(
					newSourceTotalCost,
					sourceFillState->capacity);

				/* Skip shards that already are on the node */
				if (targetHasShard)
				{
					continue;
				}

				/* Skip shards that already are not allowed on the node */
				if (!state->functions->shardAllowedOnNode(shardCost->shardId,
														  targetFillState->node,
														  state->functions->context))
				{
					continue;
				}

				/*
				 * If the target is still less utilized than the source, then
				 * this is clearly a good move. And if they are equally
				 * utilized too.
				 */
				if (newTargetUtilization <= newSourceUtilization)
				{
					MoveShardCost(sourceFillState, targetFillState,
								  shardCost, state);
					return true;
				}

				/*
				 * The target is now more utilized than the source. So we need
				 * to determine if the move is a net positive for the overall
				 * cost distribution. This means that the new highest
				 * utilization of source and target is lower than the previous
				 * highest, or the highest utilization is the same, but the
				 * lowest increased.
				 */
				if (newTargetUtilization > sourceFillState->utilization)
				{
					continue;
				}
				if (newTargetUtilization == sourceFillState->utilization &&
					newSourceUtilization <= targetFillState->utilization
					)
				{
					/*
					 * this can trigger when capacity of the nodes is not the
					 * same. Example (also a test):
					 * - node with capacity 3
					 * - node with capacity 1
					 * - 3 shards with cost 1
					 * Best distribution would be 2 shards on node with
					 * capacity 3 and one on node with capacity 1
					 */
					continue;
				}

				/*
				 * fmaxf and fminf here are only needed for cases when nodes
				 * have different capacities. If they are the same, then both
				 * arguments are equal.
				 */
				float4 utilizationImprovement = fmaxf(
					sourceFillState->utilization - newTargetUtilization,
					newSourceUtilization - targetFillState->utilization
					);
				float4 utilizationAddedByShard = fminf(
					newTargetUtilization - targetFillState->utilization,
					sourceFillState->utilization - newSourceUtilization
					);

				/*
				 * If the shard causes a lot of utilization, but the
				 * improvement which is gained by moving it is small, then we
				 * ignore the move. Probably there are other shards that are
				 * better candidates, and in any case it's probably not worth
				 * the effort to move the this shard.
				 *
				 * One of the main cases this tries to avoid is the rebalancer
				 * moving a very large shard with the "by_disk_size" strategy
				 * when that only gives a small benefit in data distribution.
				 */
				float4 normalizedUtilizationImprovement = utilizationImprovement /
														  utilizationAddedByShard;
				if (normalizedUtilizationImprovement < improvementThreshold)
				{
					state->ignoredMoves++;
					if (MaxRebalancerLoggedIgnoredMoves == -1 ||
						state->ignoredMoves <= MaxRebalancerLoggedIgnoredMoves)
					{
						ereport(NOTICE, (
									errmsg(
										"Ignoring move of shard %ld from %s:%d to %s:%d, because the move only brings a small improvement relative to the shard its size",
										shardCost->shardId,
										sourceFillState->node->workerName,
										sourceFillState->node->workerPort,
										targetFillState->node->workerName,
										targetFillState->node->workerPort
										),
									errdetail(
										"The balance improvement of %g is lower than the improvement_threshold of %g",
										normalizedUtilizationImprovement,
										improvementThreshold
										)
									));
					}
					continue;
				}
				MoveShardCost(sourceFillState, targetFillState,
							  shardCost, state);
				return true;
			}
		}
	}
	return false;
}


/*
 * ReplicationPlacementUpdates returns a list of placement updates which
 * replicates shard placements that need re-replication. To do this, the
 * function loops over the active shard placements, and for each shard placement
 * which needs to be re-replicated, it chooses an active worker node with
 * smallest number of shards as the target node.
 */
List *
ReplicationPlacementUpdates(List *workerNodeList, List *activeShardPlacementList,
							int shardReplicationFactor)
{
	List *placementUpdateList = NIL;
	ListCell *shardPlacementCell = NULL;
	uint32 workerNodeIndex = 0;
	HTAB *placementsHash = ShardPlacementsListToHash(activeShardPlacementList);
	uint32 workerNodeCount = list_length(workerNodeList);

	/* get number of shards per node */
	uint32 *shardCountArray = palloc0(workerNodeCount * sizeof(uint32));
	foreach(shardPlacementCell, activeShardPlacementList)
	{
		ShardPlacement *placement = lfirst(shardPlacementCell);

		for (workerNodeIndex = 0; workerNodeIndex < workerNodeCount; workerNodeIndex++)
		{
			WorkerNode *node = list_nth(workerNodeList, workerNodeIndex);
			if (strncmp(node->workerName, placement->nodeName, WORKER_LENGTH) == 0 &&
				node->workerPort == placement->nodePort)
			{
				shardCountArray[workerNodeIndex]++;
				break;
			}
		}
	}

	foreach(shardPlacementCell, activeShardPlacementList)
	{
		WorkerNode *sourceNode = NULL;
		WorkerNode *targetNode = NULL;
		uint32 targetNodeShardCount = UINT_MAX;
		uint32 targetNodeIndex = 0;

		ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
		uint64 shardId = placement->shardId;

		/* skip the shard placement if it has enough replications */
		int activePlacementCount = ShardActivePlacementCount(placementsHash, shardId,
															 workerNodeList);
		if (activePlacementCount >= shardReplicationFactor)
		{
			continue;
		}

		/*
		 * We can copy the shard from any active worker node that contains the
		 * shard.
		 */
		for (workerNodeIndex = 0; workerNodeIndex < workerNodeCount; workerNodeIndex++)
		{
			WorkerNode *workerNode = list_nth(workerNodeList, workerNodeIndex);

			bool placementExists = PlacementsHashFind(placementsHash, shardId,
													  workerNode);
			if (placementExists)
			{
				sourceNode = workerNode;
				break;
			}
		}

		/*
		 * If we couldn't find any worker node which contains the shard, then
		 * all copies of the shard are list and we should error out.
		 */
		if (sourceNode == NULL)
		{
			ereport(ERROR, (errmsg("could not find a source for shard " UINT64_FORMAT,
								   shardId)));
		}

		/*
		 * We can copy the shard to any worker node that doesn't contain the shard.
		 * Among such worker nodes, we choose the worker node with minimum shard
		 * count as the target.
		 */
		for (workerNodeIndex = 0; workerNodeIndex < workerNodeCount; workerNodeIndex++)
		{
			WorkerNode *workerNode = list_nth(workerNodeList, workerNodeIndex);

			if (!NodeCanHaveDistTablePlacements(workerNode))
			{
				/* never replicate placements to nodes that should not have placements */
				continue;
			}

			/* skip this node if it already contains the shard */
			bool placementExists = PlacementsHashFind(placementsHash, shardId,
													  workerNode);
			if (placementExists)
			{
				continue;
			}

			/* compare and change the target node */
			if (shardCountArray[workerNodeIndex] < targetNodeShardCount)
			{
				targetNode = workerNode;
				targetNodeShardCount = shardCountArray[workerNodeIndex];
				targetNodeIndex = workerNodeIndex;
			}
		}

		/*
		 * If there is no worker node which doesn't contain the shard, then the
		 * shard replication factor is greater than number of worker nodes, and
		 * we should error out.
		 */
		if (targetNode == NULL)
		{
			ereport(ERROR, (errmsg("could not find a target for shard " UINT64_FORMAT,
								   shardId)));
		}

		/* construct the placement update */
		PlacementUpdateEvent *placementUpdateEvent = palloc0(
			sizeof(PlacementUpdateEvent));
		placementUpdateEvent->updateType = PLACEMENT_UPDATE_COPY;
		placementUpdateEvent->shardId = shardId;
		placementUpdateEvent->sourceNode = sourceNode;
		placementUpdateEvent->targetNode = targetNode;

		/* record the placement update */
		placementUpdateList = lappend(placementUpdateList, placementUpdateEvent);

		/* update the placements hash and the shard count array */
		PlacementsHashEnter(placementsHash, shardId, targetNode);
		shardCountArray[targetNodeIndex]++;
	}

	hash_destroy(placementsHash);

	return placementUpdateList;
}


/*
 * ShardActivePlacementCount returns the number of active placements for the
 * given shard which are placed at the active worker nodes.
 */
static int
ShardActivePlacementCount(HTAB *activePlacementsHash, uint64 shardId,
						  List *activeWorkerNodeList)
{
	int shardActivePlacementCount = 0;
	ListCell *workerNodeCell = NULL;

	foreach(workerNodeCell, activeWorkerNodeList)
	{
		WorkerNode *workerNode = lfirst(workerNodeCell);
		bool placementExists = PlacementsHashFind(activePlacementsHash, shardId,
												  workerNode);
		if (placementExists)
		{
			shardActivePlacementCount++;
		}
	}

	return shardActivePlacementCount;
}


/*
 * ShardPlacementsListToHash creates and returns a hash set from a shard
 * placement list.
 */
static HTAB *
ShardPlacementsListToHash(List *shardPlacementList)
{
	ListCell *shardPlacementCell = NULL;
	HASHCTL info;
	int shardPlacementCount = list_length(shardPlacementList);

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ShardPlacement);
	info.entrysize = sizeof(ShardPlacement);
	info.hash = PlacementsHashHashCode;
	info.match = PlacementsHashCompare;
	int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	HTAB *shardPlacementsHash = hash_create("ActivePlacements Hash",
											shardPlacementCount, &info, hashFlags);

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		void *hashKey = (void *) shardPlacement;
		hash_search(shardPlacementsHash, hashKey, HASH_ENTER, NULL);
	}

	return shardPlacementsHash;
}


/*
 * PlacementsHashFind returns true if there exists a shard placement with the
 * given workerNode and shard id in the given placements hash, otherwise it
 * returns false.
 */
static bool
PlacementsHashFind(HTAB *placementsHash, uint64 shardId, WorkerNode *workerNode)
{
	bool placementFound = false;

	ShardPlacement shardPlacement;
	memset(&shardPlacement, 0, sizeof(shardPlacement));

	shardPlacement.shardId = shardId;
	shardPlacement.nodeName = workerNode->workerName;
	shardPlacement.nodePort = workerNode->workerPort;

	void *hashKey = (void *) (&shardPlacement);
	hash_search(placementsHash, hashKey, HASH_FIND, &placementFound);

	return placementFound;
}


/*
 * PlacementsHashEnter enters a shard placement for the given worker node and
 * shard id to the given placements hash.
 */
static void
PlacementsHashEnter(HTAB *placementsHash, uint64 shardId, WorkerNode *workerNode)
{
	ShardPlacement shardPlacement;
	memset(&shardPlacement, 0, sizeof(shardPlacement));

	shardPlacement.shardId = shardId;
	shardPlacement.nodeName = workerNode->workerName;
	shardPlacement.nodePort = workerNode->workerPort;

	void *hashKey = (void *) (&shardPlacement);
	hash_search(placementsHash, hashKey, HASH_ENTER, NULL);
}


/*
 * PlacementsHashRemove removes the shard placement for the given worker node and
 * shard id from the given placements hash.
 */
static void
PlacementsHashRemove(HTAB *placementsHash, uint64 shardId, WorkerNode *workerNode)
{
	ShardPlacement shardPlacement;
	memset(&shardPlacement, 0, sizeof(shardPlacement));

	shardPlacement.shardId = shardId;
	shardPlacement.nodeName = workerNode->workerName;
	shardPlacement.nodePort = workerNode->workerPort;

	void *hashKey = (void *) (&shardPlacement);
	hash_search(placementsHash, hashKey, HASH_REMOVE, NULL);
}


/*
 * PlacementsHashCompare compares two shard placements using shard id, node name,
 * and node port number.
 */
static int
PlacementsHashCompare(const void *lhsKey, const void *rhsKey, Size keySize)
{
	const ShardPlacement *placementLhs = (const ShardPlacement *) lhsKey;
	const ShardPlacement *placementRhs = (const ShardPlacement *) rhsKey;

	int shardIdCompare = 0;

	/* first, compare by shard id */
	if (placementLhs->shardId < placementRhs->shardId)
	{
		shardIdCompare = -1;
	}
	else if (placementLhs->shardId > placementRhs->shardId)
	{
		shardIdCompare = 1;
	}

	if (shardIdCompare != 0)
	{
		return shardIdCompare;
	}

	/* then, compare by node name */
	int nodeNameCompare = strncmp(placementLhs->nodeName, placementRhs->nodeName,
								  WORKER_LENGTH);
	if (nodeNameCompare != 0)
	{
		return nodeNameCompare;
	}

	/* finally, compare by node port */
	int nodePortCompare = placementLhs->nodePort - placementRhs->nodePort;
	return nodePortCompare;
}


/*
 * PlacementsHashHashCode computes the hash code for a shard placement from the
 * placement's shard id, node name, and node port number.
 */
static uint32
PlacementsHashHashCode(const void *key, Size keySize)
{
	const ShardPlacement *placement = (const ShardPlacement *) key;
	const uint64 *shardId = &(placement->shardId);
	const char *nodeName = placement->nodeName;
	const uint32 *nodePort = &(placement->nodePort);

	/* standard hash function outlined in Effective Java, Item 8 */
	uint32 result = 17;
	result = 37 * result + tag_hash(shardId, sizeof(uint64));
	result = 37 * result + string_hash(nodeName, WORKER_LENGTH);
	result = 37 * result + tag_hash(nodePort, sizeof(uint32));

	return result;
}


/* WorkerNodeListContains checks if the worker node exists in the given list. */
static bool
WorkerNodeListContains(List *workerNodeList, const char *workerName, uint32 workerPort)
{
	bool workerNodeListContains = false;
	ListCell *workerNodeCell = NULL;

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		if ((strncmp(workerNode->workerName, workerName, WORKER_LENGTH) == 0) &&
			(workerNode->workerPort == workerPort))
		{
			workerNodeListContains = true;
			break;
		}
	}

	return workerNodeListContains;
}


/*
 * UpdateColocatedShardPlacementProgress updates the progress of the given placement,
 * along with its colocated placements, to the given state.
 */
static void
UpdateColocatedShardPlacementProgress(uint64 shardId, char *sourceName, int sourcePort,
									  uint64 progress)
{
	ProgressMonitorData *header = GetCurrentProgressMonitor();

	if (header != NULL)
	{
		PlacementUpdateEventProgress *steps = ProgressMonitorSteps(header);
		ListCell *colocatedShardIntervalCell = NULL;

		ShardInterval *shardInterval = LoadShardInterval(shardId);
		List *colocatedShardIntervalList = ColocatedShardIntervalList(shardInterval);

		for (int moveIndex = 0; moveIndex < header->stepCount; moveIndex++)
		{
			PlacementUpdateEventProgress *step = steps + moveIndex;
			uint64 currentShardId = step->shardId;
			bool colocatedShard = false;

			foreach(colocatedShardIntervalCell, colocatedShardIntervalList)
			{
				ShardInterval *candidateShard = lfirst(colocatedShardIntervalCell);
				if (candidateShard->shardId == currentShardId)
				{
					colocatedShard = true;
					break;
				}
			}

			if (colocatedShard &&
				strcmp(step->sourceName, sourceName) == 0 &&
				step->sourcePort == sourcePort)
			{
				pg_atomic_write_u64(&step->progress, progress);
			}
		}
	}
}


/*
 * pg_dist_rebalance_strategy_enterprise_check is a now removed function, but
 * to avoid issues during upgrades a C stub is kept.
 */
Datum
pg_dist_rebalance_strategy_enterprise_check(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}


/*
 * citus_validate_rebalance_strategy_functions checks all the functions for
 * their correct signature.
 *
 * SQL signature:
 *
 * citus_validate_rebalance_strategy_functions(
 *     shard_cost_function regproc,
 *     node_capacity_function regproc,
 *     shard_allowed_on_node_function regproc,
 * ) RETURNS VOID
 */
Datum
citus_validate_rebalance_strategy_functions(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureShardCostUDF(PG_GETARG_OID(0));
	EnsureNodeCapacityUDF(PG_GETARG_OID(1));
	EnsureShardAllowedOnNodeUDF(PG_GETARG_OID(2));
	PG_RETURN_VOID();
}


/*
 * EnsureShardCostUDF checks that the UDF matching the oid has the correct
 * signature to be used as a ShardCost function. The expected signature is:
 *
 * shard_cost(shardid bigint) returns float4
 */
static void
EnsureShardCostUDF(Oid functionOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		ereport(ERROR, (errmsg("cache lookup failed for shard_cost_function with oid %u",
							   functionOid)));
	}
	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(proctup);
	char *name = NameStr(procForm->proname);
	if (procForm->pronargs != 1)
	{
		ereport(ERROR, (errmsg("signature for shard_cost_function is incorrect"),
						errdetail(
							"number of arguments of %s should be 1, not %i",
							name, procForm->pronargs)));
	}
	if (procForm->proargtypes.values[0] != INT8OID)
	{
		ereport(ERROR, (errmsg("signature for shard_cost_function is incorrect"),
						errdetail(
							"argument type of %s should be bigint", name)));
	}
	if (procForm->prorettype != FLOAT4OID)
	{
		ereport(ERROR, (errmsg("signature for shard_cost_function is incorrect"),
						errdetail("return type of %s should be real", name)));
	}
	ReleaseSysCache(proctup);
}


/*
 * EnsureNodeCapacityUDF checks that the UDF matching the oid has the correct
 * signature to be used as a NodeCapacity function. The expected signature is:
 *
 * node_capacity(nodeid int) returns float4
 */
static void
EnsureNodeCapacityUDF(Oid functionOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		ereport(ERROR, (errmsg(
							"cache lookup failed for node_capacity_function with oid %u",
							functionOid)));
	}
	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(proctup);
	char *name = NameStr(procForm->proname);
	if (procForm->pronargs != 1)
	{
		ereport(ERROR, (errmsg("signature for node_capacity_function is incorrect"),
						errdetail(
							"number of arguments of %s should be 1, not %i",
							name, procForm->pronargs)));
	}
	if (procForm->proargtypes.values[0] != INT4OID)
	{
		ereport(ERROR, (errmsg("signature for node_capacity_function is incorrect"),
						errdetail("argument type of %s should be int", name)));
	}
	if (procForm->prorettype != FLOAT4OID)
	{
		ereport(ERROR, (errmsg("signature for node_capacity_function is incorrect"),
						errdetail("return type of %s should be real", name)));
	}
	ReleaseSysCache(proctup);
}


/*
 * EnsureShardAllowedOnNodeUDF checks that the UDF matching the oid has the correct
 * signature to be used as a NodeCapacity function. The expected signature is:
 *
 * shard_allowed_on_node(shardid bigint, nodeid int) returns boolean
 */
static void
EnsureShardAllowedOnNodeUDF(Oid functionOid)
{
	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
	if (!HeapTupleIsValid(proctup))
	{
		ereport(ERROR, (errmsg(
							"cache lookup failed for shard_allowed_on_node_function with oid %u",
							functionOid)));
	}
	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(proctup);
	char *name = NameStr(procForm->proname);
	if (procForm->pronargs != 2)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"number of arguments of %s should be 2, not %i",
							name, procForm->pronargs)));
	}
	if (procForm->proargtypes.values[0] != INT8OID)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"type of first argument of %s should be bigint", name)));
	}
	if (procForm->proargtypes.values[1] != INT4OID)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"type of second argument of %s should be int", name)));
	}
	if (procForm->prorettype != BOOLOID)
	{
		ereport(ERROR, (errmsg(
							"signature for shard_allowed_on_node_function is incorrect"),
						errdetail(
							"return type of %s should be boolean", name)));
	}
	ReleaseSysCache(proctup);
}
