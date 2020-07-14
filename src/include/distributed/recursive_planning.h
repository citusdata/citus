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

/* managed via guc.c */
typedef enum
{
	LOCAL_JOIN_POLICY_NEVER = 0,
	LOCAL_JOIN_POLICY_PULL_LOCAL = 1,
	LOCAL_JOIN_POLICY_PULL_DISTRIBUTED = 2,
	LOCAL_JOIN_POLICY_AUTO = 3,
} LocalJoinPolicy;

extern int LocalTableJoinPolicy;


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


#endif /* RECURSIVE_PLANNING_H */
