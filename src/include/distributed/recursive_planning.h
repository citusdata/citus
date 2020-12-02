/*-------------------------------------------------------------------------
 *
 * recursive_planning.h
 *	  General Citus planner code.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef RECURSIVE_PLANNING_H
#define RECURSIVE_PLANNING_H

#include "distributed/pg_version_constants.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif

/*
 * RecursivePlanningContext is used to recursively plan subqueries
 * and CTEs, pull results to the coordinator, and push it back into
 * the workers.
 */
typedef struct RecursivePlanningContext
{
	int level;
	uint64 planId;
	bool allDistributionKeysInQueryAreEqual; /* used for some optimizations */
	List *subPlanList;
	PlannerRestrictionContext *plannerRestrictionContext;
} RecursivePlanningContext;


extern List * GenerateSubplansForSubqueriesAndCTEs(uint64 planId, Query *originalQuery,
												   PlannerRestrictionContext *
												   plannerRestrictionContext);
extern char * GenerateResultId(uint64 planId, uint32 subPlanId);
extern Query * BuildSubPlanResultQuery(List *targetEntryList, List *columnAliasList,
									   char *resultId);
extern Query * BuildReadIntermediateResultsArrayQuery(List *targetEntryList,
													  List *columnAliasList,
													  List *resultIdList,
													  bool useBinaryCopyFormat);
extern bool GeneratingSubplans(void);
extern bool ContainsLocalTableDistributedTableJoin(List *rangeTableList);
extern void ReplaceRTERelationWithRteSubquery(RangeTblEntry *rangeTableEntry,
											  List *restrictionList,
											  List *requiredAttrNumbers,
											  RecursivePlanningContext *context);
extern bool ContainsLocalTableSubqueryJoin(List *rangeTableList, Oid resultRelationId);
extern bool ContainsTableToBeConvertedToSubquery(List *rangeTableList, Oid
												 resultRelationId);
extern bool IsRecursivelyPlannableRelation(RangeTblEntry *rangeTableEntry);
extern bool IsRelationLocalTableOrMatView(Oid relationId);
#endif /* RECURSIVE_PLANNING_H */
