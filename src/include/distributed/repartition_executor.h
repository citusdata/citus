/*-------------------------------------------------------------------------
 *
 * repartition_executor.h
 *
 * Declarations for public functions and types related to repartition of
 * select query results.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REPARTITION_EXECUTOR_H
#define REPARTITION_EXECUTOR_H

extern bool EnableRepartitionedInsertSelect;

extern int DistributionColumnIndex(List *insertTargetList, Var *distributionColumn);
extern List * GenerateTaskListWithColocatedIntermediateResults(Oid targetRelationId,
															   Query *
															   modifyQueryViaCoordinatorOrRepartition,
															   char *resultIdPrefix);
extern List * GenerateTaskListWithRedistributedResults(
	Query *modifyQueryViaCoordinatorOrRepartition,
	CitusTableCacheEntry *
	targetRelation,
	List **redistributedResults,
	bool useBinaryFormat);
extern bool IsSupportedRedistributionTarget(Oid targetRelationId);
extern bool IsRedistributablePlan(Plan *selectPlan);

#endif /* REPARTITION_EXECUTOR_H */
