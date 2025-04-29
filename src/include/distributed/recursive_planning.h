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

#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"

#include "pg_version_constants.h"

#include "distributed/errormessage.h"
#include "distributed/log_utils.h"
#include "distributed/relation_restriction_equivalence.h"

typedef struct RecursivePlanningContextInternal RecursivePlanningContext;

typedef struct RangeTblEntryIndex
{
	RangeTblEntry *rangeTableEntry;
	Index rteIndex;
}RangeTblEntryIndex;

extern PlannerRestrictionContext * GetPlannerRestrictionContext(
	RecursivePlanningContext *recursivePlanningContext);
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
extern Query * BuildEmptyResultQuery(List *targetEntryList, char *resultId);
extern bool GeneratingSubplans(void);
extern bool ContainsLocalTableDistributedTableJoin(List *rangeTableList);
extern void ReplaceRTERelationWithRteSubquery(RangeTblEntry *rangeTableEntry,
											  List *requiredAttrNumbers,
											  RecursivePlanningContext *context,
											  RTEPermissionInfo *perminfo);
extern bool IsRecursivelyPlannableRelation(RangeTblEntry *rangeTableEntry);
extern bool IsRelationLocalTableOrMatView(Oid relationId);
extern bool ContainsReferencesToOuterQuery(Query *query);
extern void UpdateVarNosInNode(Node *node, Index newVarNo);
extern bool IsPushdownSafeForRTEInLeftJoin(RangeTblEntry *rte);


#endif /* RECURSIVE_PLANNING_H */
