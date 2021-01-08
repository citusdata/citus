/*-------------------------------------------------------------------------
 *
 * shard_rebalancer.h
 *
 * Type and function declarations for the shard rebalancer tool.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARD_REBALANCER_H
#define SHARD_REBALANCER_H

#include "postgres.h"

#include "fmgr.h"
#include "nodes/pg_list.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/worker_manager.h"


/* Limits for function parameters */
#define SHARD_REPLICATION_FACTOR_MINIMUM 1
#define SHARD_REPLICATION_FACTOR_MAXIMUM 100

/* Definitions for metadata update commands */
#define INSERT_SHARD_PLACEMENT_COMMAND "INSERT INTO pg_dist_shard_placement VALUES(" \
	UINT64_FORMAT ", %d, " UINT64_FORMAT ", '%s', %d)"
#define DELETE_SHARD_PLACEMENT_COMMAND "DELETE FROM pg_dist_shard_placement WHERE " \
									   "shardid=" UINT64_FORMAT \
	" AND nodename='%s' AND nodeport=%d"

/*
 * Definitions for shard placement json field names. These names should match
 * the column names in pg_dist_shard_placement.
 */
#define FIELD_NAME_SHARD_ID "shardid"
#define FIELD_NAME_SHARD_LENGTH "shardlength"
#define FIELD_NAME_SHARD_STATE "shardstate"
#define FIELD_NAME_NODE_NAME "nodename"
#define FIELD_NAME_NODE_PORT "nodeport"
#define FIELD_NAME_PLACEMENT_ID "placementid"

/*
 * Definitions for worker node json field names. These names should match the
 * column names in master_get_active_worker_nodes().
 */
#define FIELD_NAME_WORKER_NAME "node_name"
#define FIELD_NAME_WORKER_PORT "node_port"

/* Definitions for placement update json field names */
#define FIELD_NAME_UPDATE_TYPE "updatetype"
#define FIELD_NAME_SOURCE_NAME "sourcename"
#define FIELD_NAME_SOURCE_PORT "sourceport"
#define FIELD_NAME_TARGET_NAME "targetname"
#define FIELD_NAME_TARGET_PORT "targetport"

/* *INDENT-OFF* */
/* Definition for format of placement update json document */
#define PLACEMENT_UPDATE_JSON_FORMAT \
"{"\
   "\"" FIELD_NAME_UPDATE_TYPE "\":%d,"\
   "\"" FIELD_NAME_SHARD_ID "\":" UINT64_FORMAT ","\
   "\"" FIELD_NAME_SOURCE_NAME "\":%s,"\
   "\"" FIELD_NAME_SOURCE_PORT "\":%d,"\
   "\"" FIELD_NAME_TARGET_NAME "\":%s,"\
   "\"" FIELD_NAME_TARGET_PORT "\":%d"\
"}"

/* *INDENT-ON* */

#define REBALANCE_ACTIVITY_MAGIC_NUMBER 1337
#define REBALANCE_PROGRESS_ERROR -1
#define REBALANCE_PROGRESS_WAITING 0
#define REBALANCE_PROGRESS_MOVING 1
#define REBALANCE_PROGRESS_MOVED 2

/* Enumeration that defines different placement update types */
typedef enum
{
	PLACEMENT_UPDATE_INVALID_FIRST = 0,
	PLACEMENT_UPDATE_MOVE = 1,
	PLACEMENT_UPDATE_COPY = 2
} PlacementUpdateType;


/*
 * PlacementUpdateEvent represents a logical unit of work that copies or
 * moves a shard placement.
 */
typedef struct PlacementUpdateEvent
{
	PlacementUpdateType updateType;
	uint64 shardId;
	WorkerNode *sourceNode;
	WorkerNode *targetNode;
} PlacementUpdateEvent;


typedef struct PlacementUpdateEventProgress
{
	uint64 shardId;
	char sourceName[255];
	int sourcePort;
	char targetName[255];
	int targetPort;
	uint64 shardSize;
	uint64 progress;
} PlacementUpdateEventProgress;

typedef struct NodeFillState
{
	WorkerNode *node;
	float4 capacity;
	float4 totalCost;
	float4 utilization;
	List *shardCostListDesc;
} NodeFillState;

typedef struct ShardCost
{
	uint64 shardId;
	float4 cost;
} ShardCost;

typedef struct DisallowedPlacement
{
	ShardCost *shardCost;
	NodeFillState *fillState;
} DisallowedPlacement;

typedef struct RebalancePlanFunctions
{
	bool (*shardAllowedOnNode)(uint64 shardId, WorkerNode *workerNode, void *context);
	float4 (*nodeCapacity)(WorkerNode *workerNode, void *context);
	ShardCost (*shardCost)(uint64 shardId, void *context);
	void *context;
} RebalancePlanFunctions;

/* External function declarations */
extern Datum shard_placement_rebalance_array(PG_FUNCTION_ARGS);
extern Datum shard_placement_replication_array(PG_FUNCTION_ARGS);
extern Datum worker_node_responsive(PG_FUNCTION_ARGS);
extern Datum update_shard_placement(PG_FUNCTION_ARGS);
extern Datum init_rebalance_monitor(PG_FUNCTION_ARGS);
extern Datum finalize_rebalance_monitor(PG_FUNCTION_ARGS);
extern Datum get_rebalance_progress(PG_FUNCTION_ARGS);

extern List * RebalancePlacementUpdates(List *workerNodeList, List *shardPlacementList,
										double threshold,
										int32 maxShardMoves,
										bool drainOnly,
										RebalancePlanFunctions *rebalancePlanFunctions);
extern List * ReplicationPlacementUpdates(List *workerNodeList, List *shardPlacementList,
										  int shardReplicationFactor);


#endif   /* SHARD_REBALANCER_H */
