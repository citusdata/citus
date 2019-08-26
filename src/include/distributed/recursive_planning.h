/*-------------------------------------------------------------------------
 *
 * recursive_planning.h
 *	  General Citus planner code.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef RECURSIVE_PLANNING_H
#define RECURSIVE_PLANNING_H


#include "distributed/errormessage.h"
#include "distributed/relation_restriction_equivalence.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif


extern List * GenerateSubplansForSubqueriesAndCTEs(uint64 planId, Query *originalQuery,
												   PlannerRestrictionContext *
												   plannerRestrictionContext);
extern char * GenerateResultId(uint64 planId, uint32 subPlanId);
extern Query * BuildSubPlanResultQuery(List *targetEntryList, List *columnAliasList,
									   char *resultId);


#endif /* RECURSIVE_PLANNING_H */
