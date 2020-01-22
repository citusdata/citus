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
 * TODO: In theory, a node can have the nodeID UINT32_MAX,
 * but in practice that doesn't sound realistic. In addition
 * to that, pg_dist_node_nodeid_seq would error out after
 * getting to UINT32_MAX as that's the limit. Thus, we'd have
 * a bigger problem than this.
 */
#define LOCAL_NODE_ID UINT32_MAX

extern bool LogIntermediateResults;

extern List * FindSubPlansUsedInNode(Node *node);
extern List * FindAllWorkerNodesUsingSubplan(HTAB *intermediateResultsHash,
											 char *resultId);
extern HTAB * MakeIntermediateResultHTAB(void);
extern void RecordSubplanExecutionsOnNodes(HTAB *intermediateResultsHash,
										   DistributedPlan *distributedPlan);
extern IntermediateResultsHashEntry * SearchIntermediateResult(HTAB *resultsHash,
															   char *resultId);


#endif /* INTERMEDIATE_RESULT_PRUNING_H */
