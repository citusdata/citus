/*-------------------------------------------------------------------------
 *
 * query_colocation_checker.c implements the logic for determining
 * whether any subqueries in a given query are co-located (e.g.,
 * distribution keys of the relations inside subqueries are equal).
 *
 * The main logic behind non colocated subquery joins is that we pick
 * an anchor range table entry and check for distribution key equality
 * of any other subqueries in the given query. If for a given subquery,
 * we cannot find distribution key equality with the anchor rte, we
 * recursively plan that subquery.
 *
 * We also used a hacky solution for picking relations as the anchor range
 * table entries. The hack is that we wrap them into a subquery. This is only
 * necessary since some of the attribute equivalence checks are based on
 * queries rather than range table entries.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/rel.h"

#include "pg_version_constants.h"

#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_planner.h" /* only to access utility functions */
#include "distributed/pg_dist_partition.h"
#include "distributed/query_colocation_checker.h"
#include "distributed/relation_restriction_equivalence.h"


static RangeTblEntry * AnchorRte(Query *subquery);
static List * UnionRelationRestrictionLists(List *firstRelationList,
											List *secondRelationList);
static List * CreateDummyTargetList(Oid relationId, List *requiredAttributes);
static TargetEntry * CreateTargetEntryForColumn(Form_pg_attribute attributeTuple, Index
												rteIndex,
												int attributeNumber, int resno);
static TargetEntry * CreateTargetEntryForNullCol(Form_pg_attribute attributeTuple, int
												 resno);
static TargetEntry * CreateUnusedTargetEntry(int resno);

/*
 * CreateColocatedJoinChecker is a helper function that simply calculates
 * a ColocatedJoinChecker with the given input and returns it.
 */
ColocatedJoinChecker
CreateColocatedJoinChecker(Query *subquery, PlannerRestrictionContext *restrictionContext)
{
	ColocatedJoinChecker colocatedJoinChecker = { 0 };

	Query *anchorSubquery = NULL;

	/* we couldn't pick an anchor subquery, no need to continue */
	RangeTblEntry *anchorRangeTblEntry = AnchorRte(subquery);
	if (anchorRangeTblEntry == NULL)
	{
		colocatedJoinChecker.anchorRelationRestrictionList = NIL;

		return colocatedJoinChecker;
	}

	if (anchorRangeTblEntry->rtekind == RTE_RELATION)
	{
		/*
		 * If we get a relation as our anchor, wrap into a subquery. The only
		 * reason that we wrap the relation into a subquery is that some of the utility
		 * functions (i.e., FilterPlannerRestrictionForQuery()) rely on queries
		 * not relations.
		 */
#if PG_VERSION_NUM >= PG_VERSION_16
		RTEPermissionInfo *perminfo = NULL;
		if (anchorRangeTblEntry->perminfoindex)
		{
			perminfo = getRTEPermissionInfo(subquery->rteperminfos, anchorRangeTblEntry);
		}
		anchorSubquery = WrapRteRelationIntoSubquery(anchorRangeTblEntry, NIL, perminfo);
#else
		anchorSubquery = WrapRteRelationIntoSubquery(anchorRangeTblEntry, NIL, NULL);
#endif
	}
	else if (anchorRangeTblEntry->rtekind == RTE_SUBQUERY)
	{
		anchorSubquery = anchorRangeTblEntry->subquery;
	}
	else
	{
		/* we don't expect any other RTE type here */
		pg_unreachable();
	}

	PlannerRestrictionContext *anchorPlannerRestrictionContext =
		FilterPlannerRestrictionForQuery(restrictionContext, anchorSubquery);
	RelationRestrictionContext *anchorRelationRestrictionContext =
		anchorPlannerRestrictionContext->relationRestrictionContext;
	List *anchorRestrictionEquivalences =
		GenerateAllAttributeEquivalences(anchorPlannerRestrictionContext);

	/* fill the non colocated planning context */
	colocatedJoinChecker.subquery = subquery;
	colocatedJoinChecker.subqueryPlannerRestriction = restrictionContext;

	colocatedJoinChecker.anchorRelationRestrictionList =
		anchorRelationRestrictionContext->relationRestrictionList;
	colocatedJoinChecker.anchorAttributeEquivalences = anchorRestrictionEquivalences;

	return colocatedJoinChecker;
}


/*
 * AnchorRte gets a query and searches for a relation or a subquery within
 * the join tree of the query such that we can use it as our anchor range
 * table entry during our non colocated subquery planning.
 *
 * The function returns NULL if it cannot find a proper range table entry for our
 * purposes. See the function for the details.
 */
static RangeTblEntry *
AnchorRte(Query *subquery)
{
	FromExpr *joinTree = subquery->jointree;
	Relids joinRelIds = get_relids_in_jointree_compat((Node *) joinTree, false, false);
	int currentRTEIndex = -1;
	RangeTblEntry *anchorRangeTblEntry = NULL;

	/*
	 * Pick a random anchor relation or subquery (i.e., the first) for now. We
	 * might consider picking a better rte as the anchor. For example, we could
	 * iterate on the joinRelIds, and check which rteIndex has more distribution
	 * key equiality with rteIndexes. For the time being, the current primitive
	 * approach helps us in many cases.
	 */
	while ((currentRTEIndex = bms_next_member(joinRelIds, currentRTEIndex)) >= 0)
	{
		RangeTblEntry *currentRte = rt_fetch(currentRTEIndex, subquery->rtable);

		/*
		 * We always prefer distributed relations if we can find any. The
		 * reason is that Citus is currently able to recursively plan
		 * subqueries, but not relations.
		 *
		 * For the subqueries, make sure that the subquery contains at least one
		 * distributed table and doesn't have a set operation.
		 *
		 * TODO: The set operation restriction might sound weird, but, the restriction
		 * equivalence generation functions ignore set operations. We should
		 * integrate the logic in SafeToPushdownUnionSubquery() to
		 * GenerateAllAttributeEquivalences() such that the latter becomes aware of
		 * the set operations.
		 */
		if (anchorRangeTblEntry == NULL && currentRte->rtekind == RTE_SUBQUERY &&
			FindNodeMatchingCheckFunction((Node *) currentRte->subquery,
										  IsDistributedTableRTE) &&
			currentRte->subquery->setOperations == NULL &&
			!ContainsUnionSubquery(currentRte->subquery))
		{
			/* found a subquery, keep it if we cannot find a relation */
			anchorRangeTblEntry = currentRte;
		}
		else if (currentRte->rtekind == RTE_RELATION)
		{
			Oid relationId = currentRte->relid;

			if (!IsCitusTableType(relationId, DISTRIBUTED_TABLE))
			{
				/*
				 * We're not interested in non distributed relations.
				 */
				continue;
			}

			anchorRangeTblEntry = currentRte;
			break;
		}
	}

	return anchorRangeTblEntry;
}


/*
 * SubqueryColocated returns true if the input subquery has a distribution
 * key equality with the anchor subquery. In other words, we refer the
 * distribution key equality of relations as "colocation" in this context.
 */
bool
SubqueryColocated(Query *subquery, ColocatedJoinChecker *checker)
{
	List *anchorRelationRestrictionList = checker->anchorRelationRestrictionList;
	List *anchorAttributeEquivalences = checker->anchorAttributeEquivalences;

	PlannerRestrictionContext *restrictionContext = checker->subqueryPlannerRestriction;
	PlannerRestrictionContext *filteredPlannerContext =
		FilterPlannerRestrictionForQuery(restrictionContext, subquery);
	List *filteredRestrictionList =
		filteredPlannerContext->relationRestrictionContext->relationRestrictionList;


	/*
	 * There are no relations in the input subquery, such as a subquery
	 * that consist of only intermediate results or without FROM
	 * clause or subquery in WHERE clause anded with FALSE.
	 *
	 * Note that for the subquery in WHERE clause, the input original
	 * subquery (a.k.a., which didn't go through standard_planner()) may
	 * contain distributed relations, but postgres is smart enough to
	 * not generate the restriction information. That's the reason for
	 * not asserting non-existence of distributed relations.
	 */
	if (list_length(filteredRestrictionList) == 0)
	{
		return true;
	}

	/*
	 * We merge the relation restrictions of the input subquery and the anchor
	 * restrictions to form a temporary relation restriction context. The aim of
	 * forming this temporary context is to check whether the context contains
	 * distribution key equality or not.
	 */
	List *unionedRelationRestrictionList =
		UnionRelationRestrictionLists(anchorRelationRestrictionList,
									  filteredRestrictionList);

	/*
	 * We already have the attributeEquivalences, thus, only need to prepare
	 * the planner restrictions with unioned relations for our purpose of
	 * distribution key equality. Note that we don't need to calculate the
	 * join restrictions, we're already relying on the attributeEquivalences
	 * provided by the context.
	 */
	RelationRestrictionContext *unionedRelationRestrictionContext = palloc0(
		sizeof(RelationRestrictionContext));
	unionedRelationRestrictionContext->relationRestrictionList =
		unionedRelationRestrictionList;

	PlannerRestrictionContext *unionedPlannerRestrictionContext = palloc0(
		sizeof(PlannerRestrictionContext));
	unionedPlannerRestrictionContext->relationRestrictionContext =
		unionedRelationRestrictionContext;

	if (!RestrictionEquivalenceForPartitionKeysViaEquivalences(
			unionedPlannerRestrictionContext,
			anchorAttributeEquivalences))
	{
		return false;
	}

	return true;
}


/*
 * WrapRteRelationIntoSubquery wraps the given relation range table entry
 * in a newly constructed "(SELECT * FROM table_name as anchor_relation)" query.
 *
 * Note that the query returned by this function does not contain any filters or
 * projections. The returned query should be used cautiosly and it is mostly
 * designed for generating a stub query.
 */
Query *
WrapRteRelationIntoSubquery(RangeTblEntry *rteRelation,
							List *requiredAttributes,
							RTEPermissionInfo *perminfo)
{
	Query *subquery = makeNode(Query);
	RangeTblRef *newRangeTableRef = makeNode(RangeTblRef);

	subquery->commandType = CMD_SELECT;

	/* we copy the input rteRelation to preserve the rteIdentity */
	RangeTblEntry *newRangeTableEntry = copyObject(rteRelation);
	subquery->rtable = list_make1(newRangeTableEntry);

#if PG_VERSION_NUM >= PG_VERSION_16
	if (perminfo)
	{
		newRangeTableEntry->perminfoindex = 1;
		subquery->rteperminfos = list_make1(perminfo);
	}
#endif

	/* set the FROM expression to the subquery */
	newRangeTableRef = makeNode(RangeTblRef);
	newRangeTableRef->rtindex = SINGLE_RTE_INDEX;
	subquery->jointree = makeFromExpr(list_make1(newRangeTableRef), NULL);

	subquery->targetList =
		CreateFilteredTargetListForRelation(rteRelation->relid, requiredAttributes);

	if (list_length(subquery->targetList) == 0)
	{
		/*
		 * in case there is no required column, we assign one dummy NULL target entry
		 * to the subquery targetList so that it has at least one target.
		 * (targetlist should have at least one element)
		 */
		subquery->targetList = CreateDummyTargetList(rteRelation->relid,
													 requiredAttributes);
	}

	return subquery;
}


/*
 * CreateAllTargetListForRelation creates a target list which contains all the columns
 * of the given relation. If the column is not in required columns, then it is added
 * as a NULL column.
 */
List *
CreateAllTargetListForRelation(Oid relationId, List *requiredAttributes)
{
	Relation relation = relation_open(relationId, AccessShareLock);
	int numberOfAttributes = RelationGetNumberOfAttributes(relation);

	List *targetList = NIL;
	int varAttrNo = 1;

	for (int attrNum = 1; attrNum <= numberOfAttributes; attrNum++)
	{
		Form_pg_attribute attributeTuple =
			TupleDescAttr(relation->rd_att, attrNum - 1);

		int resNo = attrNum;

		if (attributeTuple->attisdropped)
		{
			/*
			 * For dropped columns, we generate a dummy null column because
			 * varattno in relation and subquery are different things, however if
			 * we put the NULL columns to the subquery for the dropped columns,
			 * they will point to the same variable.
			 */
			TargetEntry *nullTargetEntry = CreateUnusedTargetEntry(resNo);
			targetList = lappend(targetList, nullTargetEntry);
			continue;
		}

		if (!list_member_int(requiredAttributes, attrNum))
		{
			TargetEntry *nullTargetEntry =
				CreateTargetEntryForNullCol(attributeTuple, resNo);
			targetList = lappend(targetList, nullTargetEntry);
		}
		else
		{
			TargetEntry *targetEntry =
				CreateTargetEntryForColumn(attributeTuple, SINGLE_RTE_INDEX, varAttrNo++,
										   resNo);
			targetList = lappend(targetList, targetEntry);
		}
	}

	relation_close(relation, NoLock);
	return targetList;
}


/*
 * CreateFilteredTargetListForRelation creates a target list which contains
 * only the required columns of the given relation. If there is not required
 * columns then a dummy NULL column is put as the only entry.
 */
List *
CreateFilteredTargetListForRelation(Oid relationId, List *requiredAttributes)
{
	Relation relation = relation_open(relationId, AccessShareLock);
	int numberOfAttributes = RelationGetNumberOfAttributes(relation);

	List *targetList = NIL;
	int resultNo = 1;
	for (int attrNum = 1; attrNum <= numberOfAttributes; attrNum++)
	{
		Form_pg_attribute attributeTuple =
			TupleDescAttr(relation->rd_att, attrNum - 1);

		if (list_member_int(requiredAttributes, attrNum))
		{
			/* In the subquery with only required attribute numbers, the result no
			 * corresponds to the ordinal index of it in targetList.
			 */
			TargetEntry *targetEntry =
				CreateTargetEntryForColumn(attributeTuple, SINGLE_RTE_INDEX, attrNum,
										   resultNo++);
			targetList = lappend(targetList, targetEntry);
		}
	}
	relation_close(relation, NoLock);
	return targetList;
}


/*
 * CreateDummyTargetList creates a target list which contains only a
 * NULL entry.
 */
static List *
CreateDummyTargetList(Oid relationId, List *requiredAttributes)
{
	int resno = 1;
	TargetEntry *dummyTargetEntry = CreateUnusedTargetEntry(resno);
	return list_make1(dummyTargetEntry);
}


/*
 * CreateTargetEntryForColumn creates a target entry for the given
 * column.
 */
static TargetEntry *
CreateTargetEntryForColumn(Form_pg_attribute attributeTuple, Index rteIndex,
						   int attributeNumber, int resno)
{
	Var *targetColumn =
		makeVar(rteIndex, attributeNumber, attributeTuple->atttypid,
				attributeTuple->atttypmod, attributeTuple->attcollation, 0);
	TargetEntry *targetEntry =
		makeTargetEntry((Expr *) targetColumn, resno,
						pstrdup(attributeTuple->attname.data), false);
	return targetEntry;
}


/*
 * CreateTargetEntryForNullCol creates a target entry that has a NULL expression.
 */
static TargetEntry *
CreateTargetEntryForNullCol(Form_pg_attribute attributeTuple, int resno)
{
	Expr *nullExpr = (Expr *) makeNullConst(attributeTuple->atttypid,
											attributeTuple->atttypmod,
											attributeTuple->attcollation);
	char *resName = attributeTuple->attname.data;
	TargetEntry *targetEntry =
		makeTargetEntry(nullExpr, resno, pstrdup(resName), false);
	return targetEntry;
}


/*
 * CreateUnusedTargetEntry creates a dummy target entry which is not used
 * in postgres query.
 */
static TargetEntry *
CreateUnusedTargetEntry(int resno)
{
	StringInfo colname = makeStringInfo();
	appendStringInfo(colname, "dummy-%d", resno);
	Expr *nullExpr = (Expr *) makeNullConst(INT4OID,
											0,
											InvalidOid);
	TargetEntry *targetEntry =
		makeTargetEntry(nullExpr, resno, colname->data, false);
	return targetEntry;
}


/*
 * UnionRelationRestrictionLists merges two relation restriction lists
 * and returns a newly allocated list. The merged relation restriction
 * list doesn't contain any duplicate elements.
 */
static List *
UnionRelationRestrictionLists(List *firstRelationList, List *secondRelationList)
{
	List *unionedRelationRestrictionList = NULL;
	ListCell *relationRestrictionCell = NULL;
	Relids rteIdentities = NULL;

	/* list_concat destructively modifies the first list, thus copy it */
	firstRelationList = list_copy(firstRelationList);
	List *allRestrictionList = list_concat(firstRelationList, secondRelationList);

	foreach(relationRestrictionCell, allRestrictionList)
	{
		RelationRestriction *restriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		int rteIdentity = GetRTEIdentity(restriction->rte);

		/* already have the same rte, skip */
		if (bms_is_member(rteIdentity, rteIdentities))
		{
			continue;
		}

		unionedRelationRestrictionList =
			lappend(unionedRelationRestrictionList, restriction);

		rteIdentities = bms_add_member(rteIdentities, rteIdentity);
	}

	RelationRestrictionContext *unionedRestrictionContext = palloc0(
		sizeof(RelationRestrictionContext));
	unionedRestrictionContext->relationRestrictionList = unionedRelationRestrictionList;

	return unionedRelationRestrictionList;
}
