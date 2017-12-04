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
#include "nodes/relation.h"


extern DeferredErrorMessage * RecursivelyPlanSubqueriesAndCTEs(Query *query,
															   PlannerRestrictionContext *
															   plannerRestrictionContext,
															   uint64 planId,
															   List **subPlanList);
extern char * GenerateResultId(uint64 planId, uint32 subPlanId);


#endif /* RECURSIVE_PLANNING_H */
