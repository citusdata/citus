/*-------------------------------------------------------------------------
 *
 * extract_equality_filters_from_query.c
 *
 * This file contains functions to parse queries and return details.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include <stddef.h>

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "distributed/shard_pruning.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "nodes/value.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/palloc.h"


/*
 * EqualityFilter represents a <column1> = <constant> or <column1> = <column2>
 * filter. Its intent is to return the relation ID and attribute number;
 */
typedef struct EqualityFilter
{
	/* relation ID of column1 */
	Oid leftRelationId;

	/* attribute number of column1 in the relation */
	int leftAttno;

	/* relation ID of column2 or InvalidOid of comparing to a Const */
	Oid rightRelationId;

	/* attribute number of column2 in the relation or InvalidAttrNumber */
	int rightAttno;
} EqualityFilter;

/* ReplaceNullsContext gets passed down in the ReplaceNulls walker */
typedef struct ReplaceNullsContext
{
	/* oid of pg_catalog.citus_internal_null_wrapper() */
	Oid nullWrapperFunctionId;
} ReplaceNullsContext;


static Node * ReplaceNulls(Node *node, ReplaceNullsContext *context);
static bool VarNullWrapperOpExprClause(Expr *clause, Oid wrapperFunctionId, Var **column);
static List * AddEqualityFilterToSet(List *equalityFilters, EqualityFilter *filter);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(extract_equality_filters_from_query);


/*
 * extract_equality_filters_from_query returns all the equality filters in a
 * query.
 */
Datum
extract_equality_filters_from_query(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	char *queryString = text_to_cstring(PG_GETARG_TEXT_P(0));

	MemoryContext oldContext = CurrentMemoryContext;
	ResourceOwner oldOwner = CurrentResourceOwner;

	/* capture filters via hooks */
	PlannerRestrictionContext *plannerRestrictionContext =
		CreateAndPushPlannerRestrictionContext();

	/* use a subtransaction to correctly handle failures */
	BeginInternalSubTransaction(NULL);

	/*
	 * Use PG_TRY to make sure we pop the planner restriction context
	 * and be robust to planner errors due to current limitations.
	 */
	PG_TRY();
	{
		RawStmt *rawStmt = (RawStmt *) ParseTreeRawStmt(queryString);
		Query *query = RewriteRawQueryStmt(rawStmt, queryString, NULL, 0);

		CmdType commandType = query->commandType;
		if (commandType != CMD_SELECT && commandType != CMD_INSERT &&
			commandType != CMD_UPDATE && commandType != CMD_DELETE)
		{
			ereport(ERROR, (errmsg("can only process SELECT/INSERT/UPDATE/DELETE "
								   "commands")));
		}

		/* find the OID of pg_catalog.citus_internal_null_wrapper */
		List *nameList = list_make2(makeString("pg_catalog"),
									makeString("citus_internal_null_wrapper"));
		Oid paramOids[1] = { ANYELEMENTOID };
		Oid wrapperFunctionId = LookupFuncName(nameList, 1, paramOids, false);

		/* replace NULL with pg_catalog.citus_internal_null_wrapper(NULL) */
		ReplaceNullsContext replaceNullsContext;
		replaceNullsContext.nullWrapperFunctionId = wrapperFunctionId;

		query = (Query *) ReplaceNulls((Node *) query, &replaceNullsContext);

		/* prepare the query by annotating RTEs */
		List *rangeTableList = ExtractRangeTableEntryList(query);
		AssignRTEIdentities(rangeTableList, 1);

		int cursorOptions = 0;

		/* run the planner to extract filters via hooks */
		standard_planner_compat(query, cursorOptions, NULL);

		RelationRestrictionContext *restrictionContext =
			plannerRestrictionContext->relationRestrictionContext;

		List *equalityFilters = NIL;

		RelationRestriction *relationRestriction = NULL;
		foreach_ptr(relationRestriction,
					restrictionContext->relationRestrictionList)
		{
			List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
			List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
			Expr *clause = NULL;

			foreach_ptr(clause, restrictClauseList)
			{
				Var *leftVar = NULL;
				Const *rightConst = NULL;

				if (!NodeIsEqualsOpExpr((Node *) clause))
				{
					/* not an equals expression */
					continue;
				}

				if (!VarConstOpExprClause((OpExpr *) clause, &leftVar, &rightConst) &&
					!VarNullWrapperOpExprClause(clause, wrapperFunctionId, &leftVar))
				{
					/* not of the form <column> = <constant> */
					continue;
				}

				if (leftVar->varattno <= InvalidAttrNumber)
				{
					/* not a regular column */
					continue;
				}

				/* found a <column> = <constant> expression */
				EqualityFilter *constFilter = palloc0(sizeof(EqualityFilter));

				constFilter->leftRelationId = relationRestriction->relationId;
				constFilter->leftAttno = leftVar->varattno;
				constFilter->rightRelationId = InvalidOid;
				constFilter->rightAttno = InvalidAttrNumber;

				equalityFilters = AddEqualityFilterToSet(equalityFilters, constFilter);
			}
		}

		JoinRestrictionContext *joinRestrictionContext =
			plannerRestrictionContext->joinRestrictionContext;
		JoinRestriction *joinRestriction = NULL;

		foreach_ptr(joinRestriction, joinRestrictionContext->joinRestrictionList)
		{
			PlannerInfo *plannerInfo = joinRestriction->plannerInfo;
			RestrictInfo *restrictionInfo = NULL;

			foreach_ptr(restrictionInfo, joinRestriction->joinRestrictInfoList)
			{
				Expr *restrictionClause = restrictionInfo->clause;
				Var *leftVar = NULL;
				Var *rightVar = NULL;

				if (!IsColumnEquiJoinClause(restrictionClause, &leftVar, &rightVar))
				{
					/* not a regular equi-join */
					continue;
				}

				if (leftVar->varattno <= InvalidAttrNumber ||
					rightVar->varattno <= InvalidAttrNumber)
				{
					/* at least one of the vars is not a regular column */
					continue;
				}

				RangeTblEntry *leftRTE = plannerInfo->simple_rte_array[leftVar->varno];
				if (leftRTE->rtekind != RTE_RELATION)
				{
					/* left column does not belong to a relation */
					continue;
				}

				RangeTblEntry *rightRTE = plannerInfo->simple_rte_array[rightVar->varno];
				if (rightRTE->rtekind != RTE_RELATION)
				{
					/* right column does not belong to a relation */
					continue;
				}

				/* found a <column1> = <column2> expression */
				EqualityFilter *joinFilter = palloc0(sizeof(EqualityFilter));

				joinFilter->leftRelationId = leftRTE->relid;
				joinFilter->leftAttno = leftVar->varattno;
				joinFilter->rightRelationId = rightRTE->relid;
				joinFilter->rightAttno = rightVar->varattno;

				equalityFilters = AddEqualityFilterToSet(equalityFilters, joinFilter);
			}
		}

		/* return all the filters via the tuple store */
		EqualityFilter *equalityFilter = NULL;
		foreach_ptr(equalityFilter, equalityFilters)
		{
			Datum values[4];
			bool isNulls[4];

			memset(values, 0, sizeof(values));
			memset(isNulls, 0, sizeof(isNulls));

			values[0] = ObjectIdGetDatum(equalityFilter->leftRelationId);
			values[1] = Int32GetDatum(equalityFilter->leftAttno);

			if (equalityFilter->rightRelationId != InvalidOid)
			{
				/* join clause */
				values[2] = ObjectIdGetDatum(equalityFilter->rightRelationId);
				values[3] = Int32GetDatum(equalityFilter->rightAttno);
			}
			else
			{
				/* constant clause */
				isNulls[2] = true;
				isNulls[3] = true;
			}

			tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
		}

		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldContext);
		ErrorData *edata = CopyErrorData();

		ereport(NOTICE, (errcode(edata->sqlerrcode),
						 errmsg("could not analyze query: %s", edata->message)));

		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldContext);
		CurrentResourceOwner = oldOwner;
	}
	PG_END_TRY();

	PopPlannerRestrictionContext();

	PG_RETURN_VOID();
}


/*
 * ReplaceNulls replaces occurrences of <column> = NULL with a dummy value
 * or a call to citus_internal_null_wrapper in order to preserve the
 * expression in the planner.
 *
 * We prefer to use the dummy value when possible because it allows the planner to be
 * infer equality filters across joins.
 */
static Node *
ReplaceNulls(Node *node, ReplaceNullsContext *context)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Const))
	{
		Const *constNode = (Const *) node;

		if (!constNode->constisnull)
		{
			return node;
		}

		Const *newConst = copyObject(constNode);

		switch (constNode->consttype)
		{
			/* numeric types map to 0 */
			case CHAROID:
			case BOOLOID:
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case FLOAT4OID:
			case FLOAT8OID:
			case NUMERICOID:
			case OIDOID:
			case REGPROCOID:
			case REGPROCEDUREOID:
			case REGOPEROID:
			case REGOPERATOROID:
			case REGCLASSOID:
			case REGTYPEOID:
			case REGCONFIGOID:
			case REGDICTIONARYOID:
			case REGROLEOID:
			case REGNAMESPACEOID:
			{
				newConst->constisnull = false;
				newConst->constvalue = (Datum) 0;
				return (Node *) newConst;
			}

			case BPCHAROID:
			case VARCHAROID:
			case TEXTOID:
			{
				newConst->constisnull = false;
				newConst->constvalue = CStringGetTextDatum("");
				return (Node *) newConst;
			}

			/* other types use a wrapper */
			default:
			{
				/* build the call to citus_internal_null_wrapper */
				FuncExpr *funcExpr = makeNode(FuncExpr);
				funcExpr->funcid = context->nullWrapperFunctionId;
				funcExpr->funcretset = false;
				funcExpr->funcvariadic = false;
				funcExpr->funcformat = 0;
				funcExpr->funccollid = 0;
				funcExpr->inputcollid = 0;
				funcExpr->location = -1;

				/* pass NULL as an argument */
				funcExpr->args = list_make1(newConst);

				return (Node *) funcExpr;
			}
		}
	}
	else if (IsA(node, Query))
	{
		return (Node *) query_tree_mutator((Query *) node, ReplaceNulls,
										   context, 0);
	}

	return expression_tree_mutator(node, ReplaceNulls, context);
}


/*
 * VarNullWrapperOpExprClause checks whether an expression is of the
 * form <column> = pg_catalog.citus_internal_null_wrapper(..) and
 * returns the column.
 */
static bool
VarNullWrapperOpExprClause(Expr *clause, Oid wrapperFunctionId, Var **column)
{
	Node *leftOperand;
	Node *rightOperand;

	if (!BinaryOpExpression((Expr *) clause, &leftOperand, &rightOperand))
	{
		return false;
	}

	if (IsA(leftOperand, Var) && IsA(rightOperand, FuncExpr))
	{
		FuncExpr *funcExpr = (FuncExpr *) rightOperand;

		if (funcExpr->funcid == wrapperFunctionId)
		{
			*column = (Var *) leftOperand;
			return true;
		}
	}
	else if (IsA(leftOperand, FuncExpr) && IsA(rightOperand, Var))
	{
		FuncExpr *funcExpr = (FuncExpr *) leftOperand;

		if (funcExpr->funcid == wrapperFunctionId)
		{
			*column = (Var *) rightOperand;
			return true;
		}
	}

	return false;
}


/*
 * AddEqualityFilterToSet adds an equality filter to the list if it does
 * not already exist and returns the new set.
 */
static List *
AddEqualityFilterToSet(List *equalityFilterSet, EqualityFilter *newFilter)
{
	EqualityFilter *existingFilter = NULL;

	foreach_ptr(existingFilter, equalityFilterSet)
	{
		if (memcmp(existingFilter, newFilter, sizeof(EqualityFilter)) == 0)
		{
			/* filter already exists */
			return equalityFilterSet;
		}
	}

	return lappend(equalityFilterSet, newFilter);
}
