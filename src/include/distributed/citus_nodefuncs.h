/*-------------------------------------------------------------------------
 *
 * citus_nodefuncs.h
 *	  Node (de-)serialization support for Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_NODEFUNCS_H
#define CITUS_NODEFUNCS_H

#include "distributed/multi_physical_planner.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

/* citus_nodefuncs.c */
extern void SetRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
								 char *fragmentSchemaName, char *fragmentTableName,
								 List *tableIdList, List *funcColumnNames,
								 List *funcColumnTypes, List *funcColumnTypeMods,
								 List *funcCollations);
extern void ModifyRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
									char *fragmentSchemaName, char *fragmentTableName,
									List *tableIdList);
extern void ExtractRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind *rteKind,
									 char **fragmentSchemaName, char **fragmentTableName,
									 List **tableIdList);
extern CitusRTEKind GetRangeTblKind(RangeTblEntry *rte);

extern void RegisterNodes(void);

#define READFUNC_ARGS struct ExtensibleNode *node
#define OUTFUNC_ARGS StringInfo str, const struct ExtensibleNode *raw_node
#define COPYFUNC_ARGS struct ExtensibleNode *target_node, const struct \
	ExtensibleNode *source_node

extern void ReadUnsupportedCitusNode(READFUNC_ARGS);

extern void OutJob(OUTFUNC_ARGS);
extern void OutDistributedPlan(OUTFUNC_ARGS);
extern void OutDistributedSubPlan(OUTFUNC_ARGS);
extern void OutUsedDistributedSubPlan(OUTFUNC_ARGS);
extern void OutShardInterval(OUTFUNC_ARGS);
extern void OutMapMergeJob(OUTFUNC_ARGS);
extern void OutShardPlacement(OUTFUNC_ARGS);
extern void OutRelationShard(OUTFUNC_ARGS);
extern void OutRelationRowLock(OUTFUNC_ARGS);
extern void OutTask(OUTFUNC_ARGS);
extern void OutLocalPlannedStatement(OUTFUNC_ARGS);
extern void OutDeferredErrorMessage(OUTFUNC_ARGS);
extern void OutGroupShardPlacement(OUTFUNC_ARGS);

extern void OutMultiNode(OUTFUNC_ARGS);
extern void OutMultiTreeRoot(OUTFUNC_ARGS);
extern void OutMultiProject(OUTFUNC_ARGS);
extern void OutMultiCollect(OUTFUNC_ARGS);
extern void OutMultiSelect(OUTFUNC_ARGS);
extern void OutMultiTable(OUTFUNC_ARGS);
extern void OutMultiJoin(OUTFUNC_ARGS);
extern void OutMultiPartition(OUTFUNC_ARGS);
extern void OutMultiCartesianProduct(OUTFUNC_ARGS);
extern void OutMultiExtendedOp(OUTFUNC_ARGS);
extern void OutTableDDLCommand(OUTFUNC_ARGS);

extern void CopyNodeJob(COPYFUNC_ARGS);
extern void CopyNodeDistributedPlan(COPYFUNC_ARGS);
extern void CopyNodeDistributedSubPlan(COPYFUNC_ARGS);
extern void CopyNodeUsedDistributedSubPlan(COPYFUNC_ARGS);
extern void CopyNodeShardInterval(COPYFUNC_ARGS);
extern void CopyNodeMapMergeJob(COPYFUNC_ARGS);
extern void CopyNodeShardPlacement(COPYFUNC_ARGS);
extern void CopyNodeGroupShardPlacement(COPYFUNC_ARGS);
extern void CopyNodeRelationShard(COPYFUNC_ARGS);
extern void CopyNodeRelationRowLock(COPYFUNC_ARGS);
extern void CopyNodeTask(COPYFUNC_ARGS);
extern void CopyNodeLocalPlannedStatement(COPYFUNC_ARGS);
extern void CopyNodeTaskQuery(COPYFUNC_ARGS);
extern void CopyNodeDeferredErrorMessage(COPYFUNC_ARGS);

#endif /* CITUS_NODEFUNCS_H */
