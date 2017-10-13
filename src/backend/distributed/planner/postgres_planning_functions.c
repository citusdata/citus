/*-------------------------------------------------------------------------
 *
 * postgres_planning_function.c
 *    Includes planning routines copied from
 *    src/backend/optimizer/plan/createplan.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This needs to be closely in sync with the core code.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/multi_master_planner.h"
#include "nodes/plannodes.h"
#include "optimizer/tlist.h"


/*
 * make_unique_from_sortclauses creates and returns a unique node
 * from provided distinct clause list.
 * The functions is copied from postgresql from
 * src/backend/optimizer/plan/createplan.c.
 *
 * distinctList is a list of SortGroupClauses, identifying the targetlist items
 * that should be considered by the Unique filter.  The input path must
 * already be sorted accordingly.
 */
Unique *
make_unique_from_sortclauses(Plan *lefttree, List *distinctList)
{
	Unique *node = makeNode(Unique);
	Plan *plan = &node->plan;
	int numCols = list_length(distinctList);
	int keyno = 0;
	AttrNumber *uniqColIdx;
	Oid *uniqOperators;
	ListCell *slitem;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * convert SortGroupClause list into arrays of attr indexes and equality
	 * operators, as wanted by executor
	 */
	Assert(numCols > 0);
	uniqColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
	uniqOperators = (Oid *) palloc(sizeof(Oid) * numCols);

	foreach(slitem, distinctList)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(slitem);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

		uniqColIdx[keyno] = tle->resno;
		uniqOperators[keyno] = sortcl->eqop;
		Assert(OidIsValid(uniqOperators[keyno]));
		keyno++;
	}

	node->numCols = numCols;
	node->uniqColIdx = uniqColIdx;
	node->uniqOperators = uniqOperators;

	return node;
}
