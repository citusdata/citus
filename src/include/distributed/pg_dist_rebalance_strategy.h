/*-------------------------------------------------------------------------
 *
 * pg_dist_rebalance_strategy.h
 *	  definition of the "rebalance strategy" relation (pg_dist_rebalance_strategy).
 *
 * This table contains all the available strategies for rebalancing.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_REBALANCE_STRATEGY_H
#define PG_DIST_REBALANCE_STRATEGY_H

#include "postgres.h"


/* ----------------
 *		pg_dist_shard definition.
 * ----------------
 */
typedef struct FormData_pg_dist_rebalance_strategy
{
	NameData name;        /* user readable name of the strategy */
	bool default_strategy;         /* if this strategy is the default strategy */
	Oid shardCostFunction;         /* function to calculate the shard cost */
	Oid nodeCapacityFunction;        /* function to get the capacity of a node */
	Oid shardAllowedOnNodeFunction;  /* function to check if shard is allowed on node */
	float4 defaultThreshold;         /* default threshold that is used */
	float4 minimumThreshold;         /* minimum threshold that is allowed */
	float4 improvementThreshold;     /* the shard size threshold that is used */
} FormData_pg_dist_rebalance_strategy;

/* ----------------
 *      Form_pg_dist_shards corresponds to a pointer to a tuple with
 *      the format of pg_dist_shards relation.
 * ----------------
 */
typedef FormData_pg_dist_rebalance_strategy *Form_pg_dist_rebalance_strategy;

/* ----------------
 *      compiler constants for pg_dist_rebalance_strategy
 * ----------------
 */
#define Natts_pg_dist_rebalance_strategy 7
#define Anum_pg_dist_rebalance_strategy_name 1
#define Anum_pg_dist_rebalance_strategy_default_strategy 2
#define Anum_pg_dist_rebalance_strategy_shard_cost_function 3
#define Anum_pg_dist_rebalance_strategy_node_capacity_function 4
#define Anum_pg_dist_rebalance_strategy_shard_allowed_on_node_function 5
#define Anum_pg_dist_rebalance_strategy_default_threshold 6
#define Anum_pg_dist_rebalance_strategy_minimum_threshold 7

#endif   /* PG_DIST_REBALANCE_STRATEGY_H */
