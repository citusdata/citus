/*-------------------------------------------------------------------------
 *
 * query_colocation_checker.h
 *	  General Citus planner code.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef QUERY_COLOCATION_CHECKER_H
#define QUERY_COLOCATION_CHECKER_H


#include "distributed/distributed_planner.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"


/*
 * ColocatedJoinChecker is a helper structure that is used to decide
 * whether any subqueries should be recursively planned due joins non
 * colocated joins.
 */
typedef struct ColocatedJoinChecker
{
	Query *subquery;
	List *anchorAttributeEquivalences;
	List *anchorRelationRestrictionList;
	PlannerRestrictionContext *subqueryPlannerRestriction;
} ColocatedJoinChecker;


extern ColocatedJoinChecker CreateColocatedJoinChecker(Query *subquery,
													   PlannerRestrictionContext *
													   restrictionContext);
extern bool SubqueryColocated(Query *subquery, ColocatedJoinChecker *context);
extern Query * WrapRteRelationIntoSubquery(RangeTblEntry *rteRelation,
										   List *requiredAttributes);
extern List * CreateAllTargetListForRelation(Oid relationId, List *requiredAttributes);

#endif /* QUERY_COLOCATION_CHECKER_H */
