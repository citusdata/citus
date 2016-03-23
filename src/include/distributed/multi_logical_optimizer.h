/*-------------------------------------------------------------------------
 *
 * multi_logical_optimizer.h
 *	  Type and function declarations for optimizing multi-relation based logical
 *	  plans.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_LOGICAL_OPTIMIZER_H
#define MULTI_LOGICAL_OPTIMIZER_H

#include "distributed/master_metadata_utility.h"
#include "distributed/multi_logical_planner.h"


/* Definitions local to logical plan optimizer */
#define DIVISION_OPER_NAME "/"
#define DISABLE_LIMIT_APPROXIMATION -1
#define DISABLE_DISTINCT_APPROXIMATION 0.0
#define ARRAY_CAT_AGGREGATE_NAME "array_cat_agg"
#define WORKER_COLUMN_FORMAT "worker_column_%d"

/* Definitions related to count(distinct) approximations */
#define HLL_EXTENSION_NAME "hll"
#define HLL_TYPE_NAME "hll"
#define HLL_HASH_INTEGER_FUNC_NAME "hll_hash_integer"
#define HLL_HASH_BIGINT_FUNC_NAME "hll_hash_bigint"
#define HLL_HASH_TEXT_FUNC_NAME "hll_hash_text"
#define HLL_HASH_ANY_FUNC_NAME "hll_hash_any"
#define HLL_ADD_AGGREGATE_NAME "hll_add_agg"
#define HLL_UNION_AGGREGATE_NAME "hll_union_agg"
#define HLL_CARDINALITY_FUNC_NAME "hll_cardinality"


/*
 * AggregateType represents an aggregate function's type, where the function is
 * used in the context of a query. We use this function type to determine how to
 * modify the plan when creating the logical distributed plan.
 *
 * Please note that the order of values in this enumeration is tied to the order
 * of elements in the following AggregateNames array. This order needs to be
 * preserved.
 */
typedef enum
{
	AGGREGATE_INVALID_FIRST = 0,
	AGGREGATE_AVERAGE = 1,
	AGGREGATE_MIN = 2,
	AGGREGATE_MAX = 3,
	AGGREGATE_SUM = 4,
	AGGREGATE_COUNT = 5,
	AGGREGATE_ARRAY_AGG = 6
} AggregateType;


/*
 * PushDownStatus indicates whether a node can be pushed down below its child
 * using the commutative and distributive relational algebraic properties.
 */
typedef enum
{
	PUSH_DOWN_INVALID_FIRST = 0,
	PUSH_DOWN_VALID = 1,
	PUSH_DOWN_NOT_VALID = 2,
	PUSH_DOWN_SPECIAL_CONDITIONS = 3
} PushDownStatus;


/*
 * PullUpStatus indicates whether a node can be pulled up above its parent using
 * the commutative and factorizable relational algebraic properties.
 */
typedef enum
{
	PULL_UP_INVALID_FIRST = 0,
	PULL_UP_VALID = 1,
	PULL_UP_NOT_VALID = 2
} PullUpStatus;


/*
 * AggregateNames is an array that stores cstring names for aggregate functions;
 * these cstring names act as an intermediary when mapping aggregate function
 * oids to AggregateType enumerations. For this mapping to occur, we use the
 * aggregate function oid to find the corresponding cstring name in pg_proc. We
 * then compare that name against entries in this array, and return the
 * appropriate AggregateType value.
 *
 * Please note that the order of elements in this array is tied to the order of
 * values in the preceding AggregateType enum. This order needs to be preserved.
 */
static const char *const AggregateNames[] = {
	"invalid", "avg", "min", "max", "sum",
	"count", "array_agg"
};


/* Config variable managed via guc.c */
extern int LimitClauseRowFetchCount;
extern double CountDistinctErrorRate;


/* Function declaration for optimizing logical plans */
extern void MultiLogicalPlanOptimize(MultiTreeRoot *multiTree);

/* Function declaration for getting partition method for the given relation */
extern char PartitionMethod(Oid relationId);

/* Function declaration for getting oid for the given function name */
extern Oid FunctionOid(const char *functionName, int argumentCount);

/* Function declaration for helper functions in subquery pushdown */
extern List * SubqueryMultiTableList(MultiNode *multiNode);
extern List * GroupTargetEntryList(List *groupClauseList, List *targetEntryList);
extern bool ExtractQueryWalker(Node *node, List **queryList);
extern bool LeafQuery(Query *queryTree);
extern List * PartitionColumnOpExpressionList(Query *query);
extern List * ReplaceColumnsInOpExpressionList(List *opExpressionList, Var *newColumn);


#endif   /* MULTI_LOGICAL_OPTIMIZER_H */
