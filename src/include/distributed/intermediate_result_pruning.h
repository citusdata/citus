/*-------------------------------------------------------------------------
 *
 * intermediate_result_pruning.h
 *   Functions for pruning intermediate result broadcasting.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef INTERMEDIATE_RESULT_PRUNING_H
#define INTERMEDIATE_RESULT_PRUNING_H

#include "distributed/subplan_execution.h"

/*
 * UINT32_MAX is reserved in pg_dist_node, so we can use it safely.
 */
#define LOCAL_NODE_ID UINT32_MAX

/*
 * If you want to connect to the current node use `LocalHostName`, which is a GUC, instead
 * of the hardcoded loopback hostname. Only if you really need the loopback hostname use
 * this define.
 */
#define LOCAL_HOST_NAME "localhost"

extern bool LogIntermediateResults;

extern List * FindSubPlanUsages(DistributedPlan *plan);
extern List * FindAllWorkerNodesUsingSubplan(HTAB *intermediateResultsHash,
											 char *resultId);
extern HTAB * MakeIntermediateResultHTAB(void);
extern void RecordSubplanExecutionsOnNodes(HTAB *intermediateResultsHash,
										   DistributedPlan *distributedPlan);
extern IntermediateResultsHashEntry * SearchIntermediateResult(HTAB *resultsHash,
															   char *resultId);

#endif /* INTERMEDIATE_RESULT_PRUNING_H */
