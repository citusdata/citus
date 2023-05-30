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

extern int DistributionColumnIndex(List *insertTargetList, Var *partitionColumn);
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

#endif /* REPARTITION_EXECUTOR_H */
