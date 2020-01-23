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

#define LOCAL_NODE_ID 555

extern bool LogIntermediateResults;

extern List * FindSubPlansUsedInNode(Node *node);
extern List * FindAllWorkerNodesUsingSubplan(HTAB *intermediateResultsHash,
											 char *resultId);
extern HTAB * MakeIntermediateResultHTAB(void);
extern void RecordSubplanExecutionsOnNodes(HTAB *intermediateResultsHash,
										   DistributedPlan *distributedPlan);
extern IntermediateResultsHashEntry * SearchIntermediateResult(HTAB
															   *intermediateResultsHash,
															   char *resultId);


#endif /* INTERMEDIATE_RESULT_PRUNING_H */
