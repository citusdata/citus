/*-------------------------------------------------------------------------
 *
 * shard_pruning.c
 *   Shard pruning related code.
 *
 * The goal of shard pruning is to find a minimal (super)set of shards that
 * need to be queried to find rows matching the expression in a query.
 *
 * In PruneShards we first make a compact representation of the given
 * query logical tree. This tree represents boolean operators and its
 * associated valid constraints (expression nodes) and whether boolean
 * operator has associated unknown constraints. This allows essentially
 * unknown constraints to be replaced by a simple placeholder flag.
 *
 * For example query: WHERE (hash_col IN (1,2)) AND (other_col=1 OR other_col=2)
 * Gets transformed by steps:
 * 1. AND(hash_col IN (1,2), OR(X, X))
 * 2. AND(hash_col IN (1,2), OR(X))
 * 3. AND(hash_col IN (1,2), X)
 * Where X represents any set of unrecognized unprunable constraint(s).
 *
 * Above allows the following pruning machinery to understand that
 * the target shard is determined solely by constraint: hash_col IN (1,2).
 * Here it does not matter what X is as its ANDed by a valid constraint.
 * Pruning machinery will fail, returning all shards, if it encounters
 * eg. OR(hash_col=1, X) as this condition does not limit the target shards.
 *
 * PruneShards secondly computes a simplified disjunctive normal form (DNF)
 * of the logical tree as a list of pruning instances. Each pruning instance
 * contains all AND-ed constraints on the partition column. An OR expression
 * will result in two or more new pruning instances being added for the
 * subexpressions. The "parent" instance is marked isPartial and ignored
 * during pruning.
 *
 * We use the distributive property for constraints of the form P AND (Q OR R)
 * to rewrite it to (P AND Q) OR (P AND R) by copying constraints from parent
 * to "child" pruning instances. However, we do not distribute nested
 * expressions. While (P OR Q) AND (R OR S) is logically equivalent to (P AND
 * R) OR (P AND S) OR (Q AND R) OR (Q AND S), in our implementation it becomes
 * P OR Q OR R OR S. This is acceptable since this will always result in a
 * superset of shards. If this proves to be a issue in practice, a more
 * complete algorithm could be implemented.
 *
 * We then evaluate each non-partial pruning instance in the disjunction
 * through the following, increasingly expensive, steps:
 *
 * 1) If there is a constant equality constraint on the partition column, and
 *    no overlapping shards exist, find the shard interval in which the
 *    constant falls
 *
 * 2) If there is a hash range constraint on the partition column, find the
 *    shard interval matching the range
 *
 * 3) If there are range constraints (e.g. (a > 0 AND a < 10)) on the
 *    partition column, find the shard intervals that overlap with the range
 *
 * 4) If there are overlapping shards, exhaustively search all shards that are
 *    not excluded by constraints
 *
 * Finally, the union of the shards found by each pruning instance is
 * returned.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "distributed/shard_pruning.h"

#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "distributed/metadata_cache.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "distributed/log_utils.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "utils/arrayaccess.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"


/*
 * Tree node for compact representation of the given query logical tree.
 * Represent a single boolean operator node and its associated
 * valid constraints (expression nodes) and invalid constraint flag.
 */
typedef struct PruningTreeNode
{
	/* Indicates is this AND/OR boolean operator */
	BoolExprType boolop;

	/* Does this boolean operator have unknown/unprunable constraint(s) */
	bool hasInvalidConstraints;

	/* List of recognized valid prunable constraints of this boolean opearator */
	List *validConstraints;

	/* Child boolean producing operators. Parents are always different from their children */
	List *childBooleanNodes;
} PruningTreeNode;

/*
 * Context used for expression_tree_walker
 */
typedef struct PruningTreeBuildContext
{
	Var *partitionColumn;
	PruningTreeNode *current;
} PruningTreeBuildContext;

/*
 * A pruning instance is a set of ANDed constraints on a partition key.
 */
typedef struct PruningInstance
{
	/* Does this instance contain any prunable expressions? */
	bool hasValidConstraint;

	/*
	 * This constraint never evaluates to true, i.e. pruning does not have to
	 * be performed.
	 */
	bool evaluatesToFalse;

	/*
	 * Constraints on the partition column value. If multiple values are
	 * found the more restrictive one should be stored here. Even for
	 * a hash-partitioned table, actual column-values are stored here, *not*
	 * hashed values.
	 */
	Const *lessConsts;
	Const *lessEqualConsts;
	Const *equalConsts;
	Const *greaterEqualConsts;
	Const *greaterConsts;

	/*
	 * Constraint using a pre-hashed column value. The constant will store the
	 * hashed value, not the original value of the restriction.
	 */
	Const *hashedEqualConsts;

	/*
	 * Has this PruningInstance been added to
	 * ClauseWalkerContext->pruningInstances? This is not done immediately,
	 * but the first time a constraint (independent of us being able to handle
	 * that constraint) is found.
	 */
	bool addedToPruningInstances;

	/*
	 * When OR clauses are found, the non-ORed part (think of a < 3 AND (a > 5
	 * OR a > 7)) of the expression is stored in one PruningInstance which is
	 * then copied for the ORed expressions. The original is marked as
	 * isPartial, to avoid being used for pruning.
	 */
	bool isPartial;
} PruningInstance;


/*
 * Partial instances that need to be finished building. This is used to
 * collect all ANDed restrictions, before looking into ORed expressions.
 */
typedef struct PendingPruningInstance
{
	PruningInstance *instance;
	PruningTreeNode *continueAt;
} PendingPruningInstance;

#if PG_VERSION_NUM >= 120000
typedef union \
{ \
	FunctionCallInfoBaseData fcinfo; \
	/* ensure enough space for nargs args is available */ \
	char fcinfo_data[SizeForFunctionCallInfo(2)]; \
} FunctionCall2InfoData;
#else
typedef FunctionCallInfoData FunctionCall2InfoData;
#endif

/*
 * Data necessary to perform a single PruneShards().
 */
typedef struct ClauseWalkerContext
{
	Var *partitionColumn;
	char partitionMethod;

	/* ORed list of pruning targets */
	List *pruningInstances;

	/*
	 * Partially built PruningInstances, that need to be completed by doing a
	 * separate PrunableExpressionsWalker() pass.
	 */
	List *pendingInstances;

	/* PruningInstance currently being built, all eligible constraints are added here */
	PruningInstance *currentPruningInstance;

	/*
	 * Information about function calls we need to perform. Re-using the same
	 * FunctionCall2InfoData, instead of using FunctionCall2Coll, is often
	 * cheaper.
	 */
	FunctionCall2InfoData compareValueFunctionCall;
	FunctionCall2InfoData compareIntervalFunctionCall;
} ClauseWalkerContext;

static bool BuildPruningTree(Node *node, PruningTreeBuildContext *context);
static void SimplifyPruningTree(PruningTreeNode *node, PruningTreeNode *parent);
static void PrunableExpressions(PruningTreeNode *node, ClauseWalkerContext *context);
static void PrunableExpressionsWalker(PruningTreeNode *node,
									  ClauseWalkerContext *context);
static bool IsValidPartitionKeyRestriction(OpExpr *opClause);
static void AddPartitionKeyRestrictionToInstance(ClauseWalkerContext *context,
												 OpExpr *opClause, Var *varClause,
												 Const *constantClause);
static bool VarConstOpExprClause(OpExpr *opClause, Var *partitionColumn,
								 Var **varClause, Const **constantClause);
static Const * TransformPartitionRestrictionValue(Var *partitionColumn,
												  Const *restrictionValue);
static void AddSAOPartitionKeyRestrictionToInstance(ClauseWalkerContext *context,
													ScalarArrayOpExpr *
													arrayOperatorExpression);
static bool SAORestrictions(ScalarArrayOpExpr *arrayOperatorExpression,
							Var *partitionColumn,
							List **requestedRestrictions);
static bool IsValidHashRestriction(OpExpr *opClause);
static void AddHashRestrictionToInstance(ClauseWalkerContext *context, OpExpr *opClause,
										 Var *varClause, Const *constantClause);
static void AddNewConjuction(ClauseWalkerContext *context, PruningTreeNode *node);
static PruningInstance * CopyPartialPruningInstance(PruningInstance *sourceInstance);
static List * ShardArrayToList(ShardInterval **shardArray, int length);
static List * DeepCopyShardIntervalList(List *originalShardIntervalList);
static int PerformValueCompare(FunctionCallInfo compareFunctionCall, Datum a,
							   Datum b);
static int PerformCompare(FunctionCallInfo compareFunctionCall);

static List * PruneOne(CitusTableCacheEntry *cacheEntry, ClauseWalkerContext *context,
					   PruningInstance *prune);
static List * PruneWithBoundaries(CitusTableCacheEntry *cacheEntry,
								  ClauseWalkerContext *context,
								  PruningInstance *prune);
static List * ExhaustivePrune(CitusTableCacheEntry *cacheEntry,
							  ClauseWalkerContext *context,
							  PruningInstance *prune);
static bool ExhaustivePruneOne(ShardInterval *curInterval,
							   ClauseWalkerContext *context,
							   PruningInstance *prune);
static int UpperShardBoundary(Datum partitionColumnValue,
							  ShardInterval **shardIntervalCache,
							  int shardCount, FunctionCallInfo compareFunction,
							  bool includeMin);
static int LowerShardBoundary(Datum partitionColumnValue,
							  ShardInterval **shardIntervalCache,
							  int shardCount, FunctionCallInfo compareFunction,
							  bool includeMax);
static PruningTreeNode * CreatePruningNode(BoolExprType boolop);
static OpExpr * SAORestrictionArrayEqualityOp(ScalarArrayOpExpr *arrayOperatorExpression,
											  Var *partitionColumn);
static void DebugLogNode(char *fmt, Node *node, List *deparseCtx);
static void DebugLogPruningInstance(PruningInstance *pruning, List *deparseCtx);
static int ConstraintCount(PruningTreeNode *node);


/*
 * PruneShards returns all shards from a distributed table that cannot be
 * proven to be eliminated by whereClauseList.
 *
 * For reference tables, the function simply returns the single shard that the
 * table has.
 *
 * When there is a single <partition column> = <constant> filter in the where
 * clause list, the constant is written to the partitionValueConst pointer.
 */
List *
PruneShards(Oid relationId, Index rangeTableId, List *whereClauseList,
			Const **partitionValueConst)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	int shardCount = cacheEntry->shardIntervalArrayLength;
	char partitionMethod = cacheEntry->partitionMethod;
	ClauseWalkerContext context = { 0 };
	ListCell *pruneCell;
	List *prunedList = NIL;
	bool foundRestriction = false;
	bool foundPartitionColumnValue = false;
	Const *singlePartitionValueConst = NULL;

	/* there are no shards to return */
	if (shardCount == 0)
	{
		return NIL;
	}

	/* always return empty result if WHERE clause is of the form: false (AND ..) */
	if (ContainsFalseClause(whereClauseList))
	{
		return NIL;
	}

	/* short circuit for reference tables */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		prunedList = ShardArrayToList(cacheEntry->sortedShardIntervalArray,
									  cacheEntry->shardIntervalArrayLength);
		return DeepCopyShardIntervalList(prunedList);
	}


	context.partitionMethod = partitionMethod;
	context.partitionColumn = PartitionColumn(relationId, rangeTableId);
	context.currentPruningInstance = palloc0(sizeof(PruningInstance));

	if (cacheEntry->shardIntervalCompareFunction)
	{
		/* initiate function call info once (allows comparators to cache metadata) */
		InitFunctionCallInfoData(*(FunctionCallInfo) &
								 context.compareIntervalFunctionCall,
								 cacheEntry->shardIntervalCompareFunction,
								 2, cacheEntry->partitionColumn->varcollid, NULL, NULL);
	}
	else
	{
		ereport(ERROR, (errmsg("shard pruning not possible without "
							   "a shard interval comparator")));
	}

	if (cacheEntry->shardColumnCompareFunction)
	{
		/* initiate function call info once (allows comparators to cache metadata) */
		InitFunctionCallInfoData(*(FunctionCallInfo) &
								 context.compareValueFunctionCall,
								 cacheEntry->shardColumnCompareFunction,
								 2, cacheEntry->partitionColumn->varcollid, NULL, NULL);
	}
	else
	{
		ereport(ERROR, (errmsg("shard pruning not possible without "
							   "a partition column comparator")));
	}

	PruningTreeNode *tree = CreatePruningNode(AND_EXPR);

	PruningTreeBuildContext treeBuildContext = { 0 };
	treeBuildContext.current = tree;
	treeBuildContext.partitionColumn = PartitionColumn(relationId, rangeTableId);

	/* Build logical tree of prunable restrictions and invalid restrictions */
	BuildPruningTree((Node *) whereClauseList, &treeBuildContext);

	/* Simplify logic tree of prunable restrictions */
	SimplifyPruningTree(tree, NULL);

	/* Figure out what we can prune on */
	PrunableExpressions(tree, &context);

	List *debugLoggedPruningInstances = NIL;

	/*
	 * Prune using each of the PrunableInstances we found, and OR results
	 * together.
	 */
	foreach(pruneCell, context.pruningInstances)
	{
		PruningInstance *prune = (PruningInstance *) lfirst(pruneCell);

		/*
		 * If this is a partial instance, a fully built one has also been
		 * added. Skip.
		 */
		if (prune->isPartial)
		{
			continue;
		}

		/*
		 * If the current instance has no prunable expressions, we'll have to
		 * return all shards. No point in continuing pruning in that case.
		 */
		if (!prune->hasValidConstraint)
		{
			foundRestriction = false;
			break;
		}

		if (context.partitionMethod == DISTRIBUTE_BY_HASH)
		{
			if (!prune->evaluatesToFalse && !prune->equalConsts &&
				!prune->hashedEqualConsts)
			{
				/* if hash-partitioned and no equals constraints, return all shards */
				foundRestriction = false;
				break;
			}
			else if (partitionValueConst != NULL && prune->equalConsts != NULL)
			{
				if (!foundPartitionColumnValue)
				{
					/* remember the partition column value */
					singlePartitionValueConst = prune->equalConsts;
					foundPartitionColumnValue = true;
				}
				else if (singlePartitionValueConst == NULL)
				{
					/* already found multiple partition column values */
				}
				else if (!equal(prune->equalConsts, singlePartitionValueConst))
				{
					/* found multiple partition column values */
					singlePartitionValueConst = NULL;
				}
			}
		}

		List *pruneOneList = PruneOne(cacheEntry, &context, prune);

		if (prunedList)
		{
			/*
			 * We can use list_union_ptr, which is a lot faster than doing
			 * comparing shards by value, because all the ShardIntervals are
			 * guaranteed to be from
			 * CitusTableCacheEntry->sortedShardIntervalArray (thus having the
			 * same pointer values).
			 */
			prunedList = list_union_ptr(prunedList, pruneOneList);
		}
		else
		{
			prunedList = pruneOneList;
		}
		foundRestriction = true;

		if (IsLoggableLevel(DEBUG3) && pruneOneList)
		{
			debugLoggedPruningInstances = lappend(debugLoggedPruningInstances, prune);
		}
	}

	/* found no valid restriction, build list of all shards */
	if (!foundRestriction)
	{
		prunedList = ShardArrayToList(cacheEntry->sortedShardIntervalArray,
									  cacheEntry->shardIntervalArrayLength);
	}

	if (IsLoggableLevel(DEBUG3))
	{
		if (foundRestriction && debugLoggedPruningInstances != NIL)
		{
			List *deparseCtx = deparse_context_for("unknown", relationId);
			foreach(pruneCell, debugLoggedPruningInstances)
			{
				PruningInstance *prune = (PruningInstance *) lfirst(pruneCell);
				DebugLogPruningInstance(prune, deparseCtx);
			}
		}
		else
		{
			ereport(DEBUG3, (errmsg("no valid constraints found")));
		}

		ereport(DEBUG3, (errmsg("shard count: %d", list_length(prunedList))));
	}

	/* if requested, copy the partition value constant */
	if (partitionValueConst != NULL)
	{
		if (singlePartitionValueConst != NULL)
		{
			*partitionValueConst = copyObject(singlePartitionValueConst);
		}
		else
		{
			*partitionValueConst = NULL;
		}
	}

	/*
	 * Deep copy list, so it's independent of the CitusTableCacheEntry
	 * contents.
	 */
	return DeepCopyShardIntervalList(prunedList);
}


/*
 * IsValidConditionNode checks whether node is a valid constraint for pruning.
 */
static bool
IsValidConditionNode(Node *node, Var *partitionColumn)
{
	if (IsA(node, OpExpr))
	{
		OpExpr *opClause = (OpExpr *) node;
		Var *varClause = NULL;
		if (VarConstOpExprClause(opClause, partitionColumn, &varClause, NULL))
		{
			if (equal(varClause, partitionColumn))
			{
				return IsValidPartitionKeyRestriction(opClause);
			}
			else if (varClause->varattno == RESERVED_HASHED_COLUMN_ID)
			{
				return IsValidHashRestriction(opClause);
			}
		}

		return false;
	}
	else if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *arrayOperatorExpression = (ScalarArrayOpExpr *) node;
		return SAORestrictions(arrayOperatorExpression, partitionColumn, NULL);
	}
	else
	{
		return false;
	}
}


/*
 * BuildPruningTree builds a logical tree of constraints for pruning.
 */
static bool
BuildPruningTree(Node *node, PruningTreeBuildContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, List))
	{
		return expression_tree_walker(node, BuildPruningTree, context);
	}
	else if (IsA(node, BoolExpr))
	{
		BoolExpr *boolExpr = (BoolExpr *) node;

		if (boolExpr->boolop == NOT_EXPR)
		{
			/*
			 * With Var-Const conditions we should not encounter NOT_EXPR nodes.
			 * Postgres standard planner applies De Morgan's laws to remove them.
			 * We still encounter them with subqueries inside NOT, for example with:
			 * WHERE id NOT IN (SELECT id FROM something).
			 * We treat these as invalid constraints for pruning when we encounter them.
			 */
			context->current->hasInvalidConstraints = true;

			return false;
		}
		else if (context->current->boolop != boolExpr->boolop)
		{
			PruningTreeNode *child = CreatePruningNode(boolExpr->boolop);

			context->current->childBooleanNodes = lappend(
				context->current->childBooleanNodes, child);

			PruningTreeBuildContext newContext = { 0 };
			newContext.partitionColumn = context->partitionColumn;
			newContext.current = child;

			return expression_tree_walker((Node *) boolExpr->args,
										  BuildPruningTree, &newContext);
		}
		else
		{
			return expression_tree_walker(node, BuildPruningTree, context);
		}
	}
	else if (IsValidConditionNode(node, context->partitionColumn))
	{
		context->current->validConstraints = lappend(context->current->validConstraints,
													 node);

		return false;
	}
	else
	{
		context->current->hasInvalidConstraints = true;

		return false;
	}
}


/*
 * SimplifyPruningTree reduces logical tree of valid and invalid constraints for pruning.
 * The goal is to remove any node having just a single constraint associated with it.
 * This constraint is assigned to the parent logical node.
 *
 * For example 'AND(hash_col = 1, OR(X))' gets simplified to 'AND(hash_col = 1, X)',
 * where X is any unknown condition.
 */
static void
SimplifyPruningTree(PruningTreeNode *node, PruningTreeNode *parent)
{
	/* Copy list of children as its mutated inside the loop */
	List *childBooleanNodes = list_copy(node->childBooleanNodes);

	ListCell *cell;
	foreach(cell, childBooleanNodes)
	{
		PruningTreeNode *child = (PruningTreeNode *) lfirst(cell);
		SimplifyPruningTree(child, node);
	}

	if (!parent)
	{
		/* Root is always ANDed expressions */
		Assert(node->boolop == AND_EXPR);
		return;
	}

	/* Boolean operator with single (recognized/unknown) constraint gets simplified */
	if (ConstraintCount(node) <= 1)
	{
		Assert(node->childBooleanNodes == NIL);
		parent->validConstraints = list_concat(parent->validConstraints,
											   node->validConstraints);
		parent->hasInvalidConstraints = parent->hasInvalidConstraints ||
										node->hasInvalidConstraints;

		/* Remove current node from parent. Its constraint was assigned to the parent above */
		parent->childBooleanNodes = list_delete_ptr(parent->childBooleanNodes, node);
	}
}


/*
 * ContainsFalseClause returns whether the flattened where clause list
 * contains false as a clause.
 */
bool
ContainsFalseClause(List *whereClauseList)
{
	bool containsFalseClause = false;
	ListCell *clauseCell = NULL;

	foreach(clauseCell, whereClauseList)
	{
		Node *clause = (Node *) lfirst(clauseCell);

		if (IsA(clause, Const))
		{
			Const *constant = (Const *) clause;
			if (constant->consttype == BOOLOID && !DatumGetBool(constant->constvalue))
			{
				containsFalseClause = true;
				break;
			}
		}
	}

	return containsFalseClause;
}


/*
 * PrunableExpressions builds a list of all prunable expressions in node,
 * storing them in context->pruningInstances.
 */
static void
PrunableExpressions(PruningTreeNode *tree, ClauseWalkerContext *context)
{
	/*
	 * Build initial list of prunable expressions. As long as only,
	 * implicitly or explicitly, ANDed expressions are found, this perform a
	 * depth-first search. When an ORed expression is found, the current
	 * PruningInstance is added to context->pruningInstances (once for each
	 * ORed expression), then the tree-traversal is continued without
	 * recursing. Once at the top-level again, we'll process all pending
	 * expressions - that allows us to find all ANDed expressions, before
	 * recursing into an ORed expression.
	 */
	PrunableExpressionsWalker(tree, context);

	/*
	 * Process all pending instances. While processing, new ones might be
	 * added to the list, so don't use foreach().
	 *
	 * Check the places in PruningInstanceWalker that push onto
	 * context->pendingInstances why construction of the PruningInstance might
	 * be pending.
	 *
	 * We copy the partial PruningInstance, and continue adding information by
	 * calling PrunableExpressionsWalker() on the copy, continuing at the
	 * node stored in PendingPruningInstance->continueAt.
	 */
	while (context->pendingInstances != NIL)
	{
		PendingPruningInstance *instance =
			(PendingPruningInstance *) linitial(context->pendingInstances);
		PruningInstance *newPrune = CopyPartialPruningInstance(instance->instance);

		context->pendingInstances = list_delete_first(context->pendingInstances);

		context->currentPruningInstance = newPrune;
		PrunableExpressionsWalker(instance->continueAt, context);
		context->currentPruningInstance = NULL;
	}
}


/*
 * PrunableExpressionsWalker() is the main work horse for
 * PrunableExpressions().
 */
static void
PrunableExpressionsWalker(PruningTreeNode *node, ClauseWalkerContext *context)
{
	ListCell *cell = NULL;

	if (node == NULL)
	{
		return;
	}

	if (node->boolop == OR_EXPR)
	{
		/*
		 * "Queue" partial pruning instances. This is used to convert
		 * expressions like (A AND (B OR C) AND D) into (A AND B AND D),
		 * (A AND C AND D), with A, B, C, D being restrictions. When the
		 * OR is encountered, a reference to the partially built
		 * PruningInstance (containing A at this point), is added to
		 * context->pendingInstances once for B and once for C. Once a
		 * full tree-walk completed, PrunableExpressions() will complete
		 * the pending instances, which'll now also know about restriction
		 * D, by calling PrunableExpressionsWalker() once for B and once
		 * for C.
		 */

		if (node->hasInvalidConstraints)
		{
			PruningTreeNode *child = CreatePruningNode(AND_EXPR);
			child->hasInvalidConstraints = true;

			AddNewConjuction(context, child);
		}

		foreach(cell, node->validConstraints)
		{
			Node *constraint = (Node *) lfirst(cell);

			PruningTreeNode *child = CreatePruningNode(AND_EXPR);
			child->validConstraints = list_make1(constraint);

			AddNewConjuction(context, child);
		}

		foreach(cell, node->childBooleanNodes)
		{
			PruningTreeNode *child = (PruningTreeNode *) lfirst(cell);
			Assert(child->boolop == AND_EXPR);
			AddNewConjuction(context, child);
		}

		return;
	}

	Assert(node->boolop == AND_EXPR);

	foreach(cell, node->validConstraints)
	{
		Node *constraint = (Node *) lfirst(cell);

		if (IsA(constraint, OpExpr))
		{
			OpExpr *opClause = (OpExpr *) constraint;
			PruningInstance *prune = context->currentPruningInstance;
			Var *varClause = NULL;
			Const *constantClause = NULL;

			if (!prune->addedToPruningInstances)
			{
				context->pruningInstances = lappend(context->pruningInstances, prune);
				prune->addedToPruningInstances = true;
			}

			if (VarConstOpExprClause(opClause, context->partitionColumn, &varClause,
									 &constantClause))
			{
				if (equal(varClause, context->partitionColumn))
				{
					/*
					 * Found a restriction on the partition column itself. Update the
					 * current constraint with the new information.
					 */
					AddPartitionKeyRestrictionToInstance(context, opClause, varClause,
														 constantClause);
				}
				else if (varClause->varattno == RESERVED_HASHED_COLUMN_ID)
				{
					/*
					 * Found restriction that directly specifies the boundaries of a
					 * hashed column.
					 */
					AddHashRestrictionToInstance(context, opClause, varClause,
												 constantClause);
				}
				else
				{
					/* We encounter here only valid constraints */
					Assert(false);
				}
			}
			else
			{
				/* We encounter here only valid constraints */
				Assert(false);
			}
		}
		else if (IsA(constraint, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *arrayOperatorExpression = (ScalarArrayOpExpr *) constraint;
			AddSAOPartitionKeyRestrictionToInstance(context, arrayOperatorExpression);
		}
		else
		{
			/* We encounter here only valid constraints */
			Assert(false);
		}
	}

	if (node->hasInvalidConstraints)
	{
		PruningInstance *prune = context->currentPruningInstance;

		/*
		 * Mark unknown expression as added, so we'll fail pruning if there's no ANDed
		 * restrictions that we know how to deal with.
		 */
		if (!prune->addedToPruningInstances)
		{
			context->pruningInstances = lappend(context->pruningInstances, prune);
			prune->addedToPruningInstances = true;
		}
	}

	foreach(cell, node->childBooleanNodes)
	{
		PruningTreeNode *child = (PruningTreeNode *) lfirst(cell);
		Assert(child->boolop == OR_EXPR);
		PrunableExpressionsWalker(child, context);
	}
}


/*
 * VarConstOpExprClause check whether an expression is a valid comparison of a Var to a Const.
 * Also obtaining the var with constant when valid.
 */
static bool
VarConstOpExprClause(OpExpr *opClause, Var *partitionColumn, Var **varClause,
					 Const **constantClause)
{
	Var *foundVarClause = NULL;
	Const *foundConstantClause = NULL;

	Node *leftOperand;
	Node *rightOperand;
	if (!BinaryOpExpression((Expr *) opClause, &leftOperand, &rightOperand))
	{
		return false;
	}

	if (IsA(rightOperand, Const) && IsA(leftOperand, Var))
	{
		foundVarClause = (Var *) leftOperand;
		foundConstantClause = (Const *) rightOperand;
	}
	else if (IsA(leftOperand, Const) && IsA(rightOperand, Var))
	{
		foundVarClause = (Var *) rightOperand;
		foundConstantClause = (Const *) leftOperand;
	}
	else
	{
		return false;
	}

	if (varClause)
	{
		*varClause = foundVarClause;
	}
	if (constantClause)
	{
		*constantClause = foundConstantClause;
	}
	return true;
}


/*
 * AddSAOPartitionKeyRestrictionToInstance adds partcol = arrayelem operator
 * restriction to the current pruning instance for each element of the array. These
 * restrictions are added to pruning instance to prune shards based on IN/=ANY
 * constraints.
 */
static void
AddSAOPartitionKeyRestrictionToInstance(ClauseWalkerContext *context,
										ScalarArrayOpExpr *arrayOperatorExpression)
{
	List *restrictions = NULL;
	bool validSAORestriction PG_USED_FOR_ASSERTS_ONLY =
		SAORestrictions(arrayOperatorExpression, context->partitionColumn, &restrictions);

	Assert(validSAORestriction);

	PruningTreeNode *node = CreatePruningNode(OR_EXPR);
	node->validConstraints = restrictions;
	AddNewConjuction(context, node);
}


/*
 * SAORestrictions checks whether an SAO constraint is valid.
 * Also obtains equality restrictions.
 */
static bool
SAORestrictions(ScalarArrayOpExpr *arrayOperatorExpression, Var *partitionColumn,
				List **requestedRestrictions)
{
	Node *leftOpExpression = linitial(arrayOperatorExpression->args);
	Node *strippedLeftOpExpression = strip_implicit_coercions(leftOpExpression);
	bool usingEqualityOperator = OperatorImplementsEquality(
		arrayOperatorExpression->opno);
	Expr *arrayArgument = (Expr *) lsecond(arrayOperatorExpression->args);

	/* checking for partcol = ANY(const, value, s); or partcol IN (const,b,c); */
	if (usingEqualityOperator && strippedLeftOpExpression != NULL &&
		equal(strippedLeftOpExpression, partitionColumn) &&
		IsA(arrayArgument, Const))
	{
		Const *arrayConst = (Const *) arrayArgument;
		int16 typlen = 0;
		bool typbyval = false;
		char typalign = '\0';
		Datum arrayElement = 0;
		Datum inArray = arrayConst->constvalue;
		bool isNull = false;
		bool foundValid = false;

		/* check for the NULL right-hand expression*/
		if (inArray == 0)
		{
			return false;
		}

		ArrayType *array = DatumGetArrayTypeP(arrayConst->constvalue);

		/* get the necessary information from array type to iterate over it */
		Oid elementType = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(elementType,
							 &typlen,
							 &typbyval,
							 &typalign);

		/* Iterate over the righthand array of expression */
		ArrayIterator arrayIterator = array_create_iterator(array, 0, NULL);
		while (array_iterate(arrayIterator, &arrayElement, &isNull))
		{
			if (isNull)
			{
				/*
				 * We can ignore IN (NULL) clauses because a value is never
				 * equal to NULL.
				 */
				continue;
			}

			foundValid = true;

			if (requestedRestrictions)
			{
				Const *constElement = makeConst(elementType, -1,
												arrayConst->constcollid,
												typlen, arrayElement,
												isNull, typbyval);

				/* build partcol = arrayelem operator */
				OpExpr *arrayEqualityOp = SAORestrictionArrayEqualityOp(
					arrayOperatorExpression,
					partitionColumn);
				arrayEqualityOp->args = list_make2(strippedLeftOpExpression,
												   constElement);

				*requestedRestrictions = lappend(*requestedRestrictions, arrayEqualityOp);
			}
			else
			{
				break;
			}
		}

		return foundValid;
	}

	return false;
}


/*
 * AddNewConjuction adds the OpExpr to pending instance list of context
 * as conjunction as partial instance.
 */
static void
AddNewConjuction(ClauseWalkerContext *context, PruningTreeNode *node)
{
	PendingPruningInstance *instance = palloc0(sizeof(PendingPruningInstance));

	instance->instance = context->currentPruningInstance;
	instance->continueAt = node;

	/*
	 * Signal that this instance is not to be used for pruning on
	 * its own. Once the pending instance is processed, it'll be
	 * used.
	 */
	instance->instance->isPartial = true;
	context->pendingInstances = lappend(context->pendingInstances, instance);
}


/*
 * IsValidPartitionKeyRestriction check whether an operator clause is
 * a valid restriction for comparing to a partition column.
 */
static bool
IsValidPartitionKeyRestriction(OpExpr *opClause)
{
	ListCell *btreeInterpretationCell = NULL;
	bool matchedOp = false;

	List *btreeInterpretationList =
		get_op_btree_interpretation(opClause->opno);
	foreach(btreeInterpretationCell, btreeInterpretationList)
	{
		OpBtreeInterpretation *btreeInterpretation =
			(OpBtreeInterpretation *) lfirst(btreeInterpretationCell);

		if (btreeInterpretation->strategy == ROWCOMPARE_NE)
		{
			/* TODO: could add support for this, if we feel like it */
			return false;
		}

		matchedOp = true;
	}

	return matchedOp;
}


/*
 * AddPartitionKeyRestrictionToInstance adds information about a PartitionKey
 * $op Const restriction to the current pruning instance.
 */
static void
AddPartitionKeyRestrictionToInstance(ClauseWalkerContext *context, OpExpr *opClause,
									 Var *partitionColumn, Const *constantClause)
{
	PruningInstance *prune = context->currentPruningInstance;
	ListCell *btreeInterpretationCell = NULL;

	/* only have extra work to do if const isn't same type as partition column */
	if (constantClause->consttype != partitionColumn->vartype)
	{
		/* we want our restriction value in terms of the type of the partition column */
		constantClause = TransformPartitionRestrictionValue(partitionColumn,
															constantClause);
		if (constantClause == NULL)
		{
			/* couldn't coerce value, its invalid restriction */
			return;
		}
	}

	if (constantClause->constisnull)
	{
		/* we cannot do pruning on NULL values */
		return;
	}

	/* at this point, we'd better be able to pass binary Datums to comparison functions */
	Assert(IsBinaryCoercible(constantClause->consttype, partitionColumn->vartype));

	List *btreeInterpretationList = get_op_btree_interpretation(opClause->opno);
	foreach(btreeInterpretationCell, btreeInterpretationList)
	{
		OpBtreeInterpretation *btreeInterpretation =
			(OpBtreeInterpretation *) lfirst(btreeInterpretationCell);

		switch (btreeInterpretation->strategy)
		{
			case BTLessStrategyNumber:
			{
				if (!prune->lessConsts ||
					PerformValueCompare((FunctionCallInfo) &
										context->compareValueFunctionCall,
										constantClause->constvalue,
										prune->lessConsts->constvalue) < 0)
				{
					prune->lessConsts = constantClause;
				}
				break;
			}

			case BTLessEqualStrategyNumber:
			{
				if (!prune->lessEqualConsts ||
					PerformValueCompare((FunctionCallInfo) &
										context->compareValueFunctionCall,
										constantClause->constvalue,
										prune->lessEqualConsts->constvalue) < 0)
				{
					prune->lessEqualConsts = constantClause;
				}
				break;
			}

			case BTEqualStrategyNumber:
			{
				if (!prune->equalConsts)
				{
					prune->equalConsts = constantClause;
				}
				else if (PerformValueCompare((FunctionCallInfo) &
											 context->compareValueFunctionCall,
											 constantClause->constvalue,
											 prune->equalConsts->constvalue) != 0)
				{
					/* key can't be equal to two values */
					prune->evaluatesToFalse = true;
				}
				break;
			}

			case BTGreaterEqualStrategyNumber:
			{
				if (!prune->greaterEqualConsts ||
					PerformValueCompare((FunctionCallInfo) &
										context->compareValueFunctionCall,
										constantClause->constvalue,
										prune->greaterEqualConsts->constvalue) > 0
					)
				{
					prune->greaterEqualConsts = constantClause;
				}
				break;
			}

			case BTGreaterStrategyNumber:
			{
				if (!prune->greaterConsts ||
					PerformValueCompare((FunctionCallInfo) &
										context->compareValueFunctionCall,
										constantClause->constvalue,
										prune->greaterConsts->constvalue) > 0)
				{
					prune->greaterConsts = constantClause;
				}
				break;
			}

			default:
				Assert(false);
		}
	}

	prune->hasValidConstraint = true;
}


/*
 * TransformPartitionRestrictionValue works around how PostgreSQL sometimes
 * chooses to try to wrap our Var in a coercion rather than the Const.
 * To deal with this, we strip coercions from both and manually coerce
 * the Const into the type of our partition column.
 * It is conceivable that in some instances this may not be possible,
 * in those cases we will simply fail to prune partitions based on this clause.
 */
static Const *
TransformPartitionRestrictionValue(Var *partitionColumn, Const *restrictionValue)
{
	Node *transformedValue = coerce_to_target_type(NULL, (Node *) restrictionValue,
												   restrictionValue->consttype,
												   partitionColumn->vartype,
												   partitionColumn->vartypmod,
												   COERCION_ASSIGNMENT,
												   COERCE_IMPLICIT_CAST, -1);

	/* if NULL, no implicit coercion is possible between the types */
	if (transformedValue == NULL)
	{
		return NULL;
	}

	/* if still not a constant, evaluate coercion */
	if (!IsA(transformedValue, Const))
	{
		transformedValue = (Node *) expression_planner((Expr *) transformedValue);
	}

	/* if still not a constant, no immutable coercion matched */
	if (!IsA(transformedValue, Const))
	{
		return NULL;
	}

	return (Const *) transformedValue;
}


/*
 * IsValidHashRestriction checks whether an operator clause is a valid restriction for hashed column.
 */
static bool
IsValidHashRestriction(OpExpr *opClause)
{
	ListCell *btreeInterpretationCell = NULL;

	List *btreeInterpretationList =
		get_op_btree_interpretation(opClause->opno);
	foreach(btreeInterpretationCell, btreeInterpretationList)
	{
		OpBtreeInterpretation *btreeInterpretation =
			(OpBtreeInterpretation *) lfirst(btreeInterpretationCell);

		if (btreeInterpretation->strategy == BTGreaterEqualStrategyNumber)
		{
			return true;
		}
	}

	return false;
}


/*
 * AddHashRestrictionToInstance adds information about a
 * RESERVED_HASHED_COLUMN_ID = Const restriction to the current pruning
 * instance.
 */
static void
AddHashRestrictionToInstance(ClauseWalkerContext *context, OpExpr *opClause,
							 Var *varClause, Const *constantClause)
{
	/* be paranoid */
	Assert(IsBinaryCoercible(constantClause->consttype, INT4OID));
	Assert(IsValidHashRestriction(opClause));

	/*
	 * Ladidadida, dirty hackety hack. We only add such
	 * constraints (in ShardIntervalOpExpressions()) to select a
	 * shard based on its exact boundaries. For efficient binary
	 * search it's better to simply use one representative value
	 * to look up the shard. In practice, this is sufficient for
	 * now.
	 */
	PruningInstance *prune = context->currentPruningInstance;
	Assert(!prune->hashedEqualConsts);
	prune->hashedEqualConsts = constantClause;
	prune->hasValidConstraint = true;
}


/*
 * CopyPartialPruningInstance copies a partial PruningInstance, so it can be
 * completed.
 */
static PruningInstance *
CopyPartialPruningInstance(PruningInstance *sourceInstance)
{
	PruningInstance *newInstance = palloc(sizeof(PruningInstance));

	Assert(sourceInstance->isPartial);

	/*
	 * To make the new PruningInstance useful for pruning, we have to reset it
	 * being partial - if necessary it'll be marked so again by
	 * PrunableExpressionsWalker().
	 */
	*newInstance = *sourceInstance;
	newInstance->addedToPruningInstances = false;
	newInstance->isPartial = false;

	return newInstance;
}


/*
 * ShardArrayToList builds a list of out the array of ShardInterval*.
 */
static List *
ShardArrayToList(ShardInterval **shardArray, int length)
{
	List *shardIntervalList = NIL;

	for (int shardIndex = 0; shardIndex < length; shardIndex++)
	{
		ShardInterval *shardInterval =
			shardArray[shardIndex];
		shardIntervalList = lappend(shardIntervalList, shardInterval);
	}

	return shardIntervalList;
}


/*
 * DeepCopyShardIntervalList copies originalShardIntervalList and the
 * contained ShardIntervals, into a new list.
 */
static List *
DeepCopyShardIntervalList(List *originalShardIntervalList)
{
	List *copiedShardIntervalList = NIL;
	ListCell *shardIntervalCell = NULL;

	foreach(shardIntervalCell, originalShardIntervalList)
	{
		ShardInterval *originalShardInterval =
			(ShardInterval *) lfirst(shardIntervalCell);
		ShardInterval *copiedShardInterval =
			(ShardInterval *) palloc0(sizeof(ShardInterval));

		CopyShardInterval(originalShardInterval, copiedShardInterval);
		copiedShardIntervalList = lappend(copiedShardIntervalList, copiedShardInterval);
	}

	return copiedShardIntervalList;
}


/*
 * PruneOne returns all shards in the table that match a single
 * PruningInstance.
 */
static List *
PruneOne(CitusTableCacheEntry *cacheEntry, ClauseWalkerContext *context,
		 PruningInstance *prune)
{
	ShardInterval *shardInterval = NULL;

	/* Well, if life always were this easy... */
	if (prune->evaluatesToFalse)
	{
		return NIL;
	}

	/*
	 * For an equal constraints, if there's no overlapping shards (always the
	 * case for hash and range partitioning, sometimes for append), can
	 * perform binary search for the right interval. That's usually the
	 * fastest, so try that first.
	 */
	if (prune->equalConsts &&
		!cacheEntry->hasOverlappingShardInterval)
	{
		shardInterval = FindShardInterval(prune->equalConsts->constvalue, cacheEntry);

		/*
		 * If pruned down to nothing, we're done. Otherwise see if other
		 * methods prune down further / to nothing.
		 */
		if (!shardInterval)
		{
			return NIL;
		}
	}

	/*
	 * If the hash value we're looking for is known, we can search for the
	 * interval directly. That's fast and should only ever be the case for a
	 * hash-partitioned table.
	 */
	if (prune->hashedEqualConsts)
	{
		ShardInterval **sortedShardIntervalArray = cacheEntry->sortedShardIntervalArray;

		Assert(context->partitionMethod == DISTRIBUTE_BY_HASH);

		int shardIndex = FindShardIntervalIndex(prune->hashedEqualConsts->constvalue,
												cacheEntry);

		if (shardIndex == INVALID_SHARD_INDEX)
		{
			return NIL;
		}
		else if (shardInterval &&
				 sortedShardIntervalArray[shardIndex]->shardId != shardInterval->shardId)
		{
			/*
			 * equalConst based pruning above yielded a different shard than
			 * pruning based on pre-hashed equality. This is useful in case
			 * of INSERT ... SELECT, where both can occur together (one via
			 * join/colocation, the other via a plain equality restriction).
			 */
			return NIL;
		}
		else
		{
			return list_make1(sortedShardIntervalArray[shardIndex]);
		}
	}

	/*
	 * If previous pruning method yielded a single shard, and the table is not
	 * hash partitioned, attempt range based pruning to exclude it further.
	 *
	 * That's particularly important in particular for subquery pushdown,
	 * where it's very common to have a user specified equality restriction,
	 * and a range based restriction for shard boundaries, added by the
	 * subquery machinery.
	 */
	if (shardInterval)
	{
		if (context->partitionMethod != DISTRIBUTE_BY_HASH &&
			ExhaustivePruneOne(shardInterval, context, prune))
		{
			return NIL;
		}
		else
		{
			/* no chance to prune further, return */
			return list_make1(shardInterval);
		}
	}

	/*
	 * Should never get here for hashing, we've filtered down to either zero
	 * or one shard, and returned.
	 */
	Assert(context->partitionMethod != DISTRIBUTE_BY_HASH);

	/*
	 * Next method: binary search with fuzzy boundaries. Can't trivially do so
	 * if shards have overlapping boundaries.
	 *
	 * TODO: If we kept shard intervals separately sorted by both upper and
	 * lower boundaries, this should be possible?
	 */
	if (!cacheEntry->hasOverlappingShardInterval && (
			prune->greaterConsts || prune->greaterEqualConsts ||
			prune->lessConsts || prune->lessEqualConsts))
	{
		return PruneWithBoundaries(cacheEntry, context, prune);
	}

	/*
	 * Brute force: Check each shard.
	 */
	return ExhaustivePrune(cacheEntry, context, prune);
}


/*
 * PerformCompare invokes comparator with prepared values, check for
 * unexpected NULL returns.
 */
static int
PerformCompare(FunctionCallInfo compareFunctionCall)
{
	Datum result = FunctionCallInvoke(compareFunctionCall);

	if (compareFunctionCall->isnull)
	{
		elog(ERROR, "function %u returned NULL", compareFunctionCall->flinfo->fn_oid);
	}

	return DatumGetInt32(result);
}


/*
 * PerformValueCompare invokes comparator with a/b, and checks for unexpected
 * NULL returns.
 */
static int
PerformValueCompare(FunctionCallInfo compareFunctionCall, Datum a, Datum b)
{
	fcSetArg(compareFunctionCall, 0, a);
	fcSetArg(compareFunctionCall, 1, b);

	return PerformCompare(compareFunctionCall);
}


/*
 * LowerShardBoundary returns the index of the first ShardInterval that's >=
 * (if includeMax) or > partitionColumnValue.
 */
static int
LowerShardBoundary(Datum partitionColumnValue, ShardInterval **shardIntervalCache,
				   int shardCount, FunctionCallInfo compareFunction, bool includeMax)
{
	int lowerBoundIndex = 0;
	int upperBoundIndex = shardCount;

	Assert(shardCount != 0);

	/* setup partitionColumnValue argument once */
	fcSetArg(compareFunction, 0, partitionColumnValue);

	while (lowerBoundIndex < upperBoundIndex)
	{
		int middleIndex = lowerBoundIndex + ((upperBoundIndex - lowerBoundIndex) / 2);

		/* setup minValue as argument */
		fcSetArg(compareFunction, 1, shardIntervalCache[middleIndex]->minValue);

		/* execute cmp(partitionValue, lowerBound) */
		int minValueComparison = PerformCompare(compareFunction);

		/* and evaluate results */
		if (minValueComparison < 0)
		{
			/* value smaller than entire range */
			upperBoundIndex = middleIndex;
			continue;
		}

		/* setup maxValue as argument */
		fcSetArg(compareFunction, 1, shardIntervalCache[middleIndex]->maxValue);

		/* execute cmp(partitionValue, upperBound) */
		int maxValueComparison = PerformCompare(compareFunction);

		if ((maxValueComparison == 0 && !includeMax) ||
			maxValueComparison > 0)
		{
			/* value bigger than entire range */
			lowerBoundIndex = middleIndex + 1;
			continue;
		}

		/* found interval containing partitionValue */
		return middleIndex;
	}

	Assert(lowerBoundIndex == upperBoundIndex);

	/*
	 * If we get here, none of the ShardIntervals exactly contain the value
	 * (we'd have hit the return middleIndex; case otherwise). Figure out
	 * whether there's possibly any interval containing a value that's bigger
	 * than the partition key one.
	 */
	if (lowerBoundIndex == 0)
	{
		/* all intervals are bigger, thus return 0 */
		return 0;
	}
	else if (lowerBoundIndex == shardCount)
	{
		/* partition value is bigger than all partition values */
		return INVALID_SHARD_INDEX;
	}

	/* value falls inbetween intervals */
	return lowerBoundIndex + 1;
}


/*
 * UpperShardBoundary returns the index of the last ShardInterval that's <=
 * (if includeMin) or < partitionColumnValue.
 */
static int
UpperShardBoundary(Datum partitionColumnValue, ShardInterval **shardIntervalCache,
				   int shardCount, FunctionCallInfo compareFunction, bool includeMin)
{
	int lowerBoundIndex = 0;
	int upperBoundIndex = shardCount;

	Assert(shardCount != 0);

	/* setup partitionColumnValue argument once */
	fcSetArg(compareFunction, 0, partitionColumnValue);

	while (lowerBoundIndex < upperBoundIndex)
	{
		int middleIndex = lowerBoundIndex + ((upperBoundIndex - lowerBoundIndex) / 2);

		/* setup minValue as argument */
		fcSetArg(compareFunction, 1, shardIntervalCache[middleIndex]->minValue);

		/* execute cmp(partitionValue, lowerBound) */
		int minValueComparison = PerformCompare(compareFunction);

		/* and evaluate results */
		if ((minValueComparison == 0 && !includeMin) ||
			minValueComparison < 0)
		{
			/* value smaller than entire range */
			upperBoundIndex = middleIndex;
			continue;
		}

		/* setup maxValue as argument */
		fcSetArg(compareFunction, 1, shardIntervalCache[middleIndex]->maxValue);

		/* execute cmp(partitionValue, upperBound) */
		int maxValueComparison = PerformCompare(compareFunction);

		if (maxValueComparison > 0)
		{
			/* value bigger than entire range */
			lowerBoundIndex = middleIndex + 1;
			continue;
		}

		/* found interval containing partitionValue */
		return middleIndex;
	}

	Assert(lowerBoundIndex == upperBoundIndex);

	/*
	 * If we get here, none of the ShardIntervals exactly contain the value
	 * (we'd have hit the return middleIndex; case otherwise). Figure out
	 * whether there's possibly any interval containing a value that's smaller
	 * than the partition key one.
	 */
	if (upperBoundIndex == shardCount)
	{
		/* all intervals are smaller, thus return 0 */
		return shardCount - 1;
	}
	else if (upperBoundIndex == 0)
	{
		/* partition value is smaller than all partition values */
		return INVALID_SHARD_INDEX;
	}

	/* value falls inbetween intervals, return the inverval one smaller as bound */
	return upperBoundIndex - 1;
}


/*
 * PruneWithBoundaries searches for shards that match inequality constraints,
 * using binary search on both the upper and lower boundary, and returns a
 * list of surviving shards.
 */
static List *
PruneWithBoundaries(CitusTableCacheEntry *cacheEntry, ClauseWalkerContext *context,
					PruningInstance *prune)
{
	List *remainingShardList = NIL;
	int shardCount = cacheEntry->shardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray = cacheEntry->sortedShardIntervalArray;
	bool hasLowerBound = false;
	bool hasUpperBound = false;
	Datum lowerBound = 0;
	Datum upperBound = 0;
	bool lowerBoundInclusive = false;
	bool upperBoundInclusive = false;
	int lowerBoundIdx = -1;
	int upperBoundIdx = -1;
	FunctionCallInfo compareFunctionCall = (FunctionCallInfo) &
										   context->compareIntervalFunctionCall;

	if (prune->greaterEqualConsts)
	{
		lowerBound = prune->greaterEqualConsts->constvalue;
		lowerBoundInclusive = true;
		hasLowerBound = true;
	}
	if (prune->greaterConsts)
	{
		/*
		 * Use the more restrictive one, if both greater and greaterEqual
		 * constraints are specified.
		 */
		if (!hasLowerBound ||
			PerformValueCompare(compareFunctionCall,
								prune->greaterConsts->constvalue,
								lowerBound) >= 0)
		{
			lowerBound = prune->greaterConsts->constvalue;
			lowerBoundInclusive = false;
			hasLowerBound = true;
		}
	}
	if (prune->lessEqualConsts)
	{
		upperBound = prune->lessEqualConsts->constvalue;
		upperBoundInclusive = true;
		hasUpperBound = true;
	}
	if (prune->lessConsts)
	{
		/*
		 * Use the more restrictive one, if both less and lessEqual
		 * constraints are specified.
		 */
		if (!hasUpperBound ||
			PerformValueCompare(compareFunctionCall,
								prune->lessConsts->constvalue,
								upperBound) <= 0)
		{
			upperBound = prune->lessConsts->constvalue;
			upperBoundInclusive = false;
			hasUpperBound = true;
		}
	}

	Assert(hasLowerBound || hasUpperBound);

	/* find lower bound */
	if (hasLowerBound)
	{
		lowerBoundIdx = LowerShardBoundary(lowerBound, sortedShardIntervalArray,
										   shardCount, compareFunctionCall,
										   lowerBoundInclusive);
	}
	else
	{
		lowerBoundIdx = 0;
	}

	/* find upper bound */
	if (hasUpperBound)
	{
		upperBoundIdx = UpperShardBoundary(upperBound, sortedShardIntervalArray,
										   shardCount, compareFunctionCall,
										   upperBoundInclusive);
	}
	else
	{
		upperBoundIdx = shardCount - 1;
	}

	if (lowerBoundIdx == INVALID_SHARD_INDEX)
	{
		return NIL;
	}
	else if (upperBoundIdx == INVALID_SHARD_INDEX)
	{
		return NIL;
	}

	/*
	 * Build list of all shards that are in the range of shards (possibly 0).
	 */
	for (int curIdx = lowerBoundIdx; curIdx <= upperBoundIdx; curIdx++)
	{
		remainingShardList = lappend(remainingShardList,
									 sortedShardIntervalArray[curIdx]);
	}

	return remainingShardList;
}


/*
 * ExhaustivePrune returns a list of shards matching PruningInstances
 * constraints, by simply checking them for each individual shard.
 */
static List *
ExhaustivePrune(CitusTableCacheEntry *cacheEntry, ClauseWalkerContext *context,
				PruningInstance *prune)
{
	List *remainingShardList = NIL;
	int shardCount = cacheEntry->shardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray = cacheEntry->sortedShardIntervalArray;

	for (int curIdx = 0; curIdx < shardCount; curIdx++)
	{
		ShardInterval *curInterval = sortedShardIntervalArray[curIdx];

		if (!ExhaustivePruneOne(curInterval, context, prune))
		{
			remainingShardList = lappend(remainingShardList, curInterval);
		}
	}

	return remainingShardList;
}


/*
 * ExhaustivePruneOne returns whether curInterval is pruned away.
 */
static bool
ExhaustivePruneOne(ShardInterval *curInterval,
				   ClauseWalkerContext *context,
				   PruningInstance *prune)
{
	FunctionCallInfo compareFunctionCall = (FunctionCallInfo) &
										   context->compareIntervalFunctionCall;
	Datum compareWith = 0;

	/* NULL boundaries can't be compared to */
	if (!curInterval->minValueExists || !curInterval->maxValueExists)
	{
		return false;
	}

	if (prune->equalConsts)
	{
		compareWith = prune->equalConsts->constvalue;

		if (PerformValueCompare(compareFunctionCall,
								compareWith,
								curInterval->minValue) < 0)
		{
			return true;
		}

		if (PerformValueCompare(compareFunctionCall,
								compareWith,
								curInterval->maxValue) > 0)
		{
			return true;
		}
	}
	if (prune->greaterEqualConsts)
	{
		compareWith = prune->greaterEqualConsts->constvalue;

		if (PerformValueCompare(compareFunctionCall,
								curInterval->maxValue,
								compareWith) < 0)
		{
			return true;
		}
	}
	if (prune->greaterConsts)
	{
		compareWith = prune->greaterConsts->constvalue;

		if (PerformValueCompare(compareFunctionCall,
								curInterval->maxValue,
								compareWith) <= 0)
		{
			return true;
		}
	}
	if (prune->lessEqualConsts)
	{
		compareWith = prune->lessEqualConsts->constvalue;

		if (PerformValueCompare(compareFunctionCall,
								curInterval->minValue,
								compareWith) > 0)
		{
			return true;
		}
	}
	if (prune->lessConsts)
	{
		compareWith = prune->lessConsts->constvalue;

		if (PerformValueCompare(compareFunctionCall,
								curInterval->minValue,
								compareWith) >= 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * Helper for creating a node for pruning tree
 */
static PruningTreeNode *
CreatePruningNode(BoolExprType boolop)
{
	PruningTreeNode *node = palloc0(sizeof(PruningTreeNode));
	node->boolop = boolop;
	node->childBooleanNodes = NIL;
	node->validConstraints = NIL;
	node->hasInvalidConstraints = false;
	return node;
}


/*
 * SAORestrictionArrayEqualityOp creates an equality operator
 * for a single element of a scalar array constraint.
 */
static OpExpr *
SAORestrictionArrayEqualityOp(ScalarArrayOpExpr *arrayOperatorExpression,
							  Var *partitionColumn)
{
	OpExpr *arrayEqualityOp = makeNode(OpExpr);
	arrayEqualityOp->opno = arrayOperatorExpression->opno;
	arrayEqualityOp->opfuncid = arrayOperatorExpression->opfuncid;
	arrayEqualityOp->inputcollid = arrayOperatorExpression->inputcollid;
	arrayEqualityOp->opresulttype = get_func_rettype(
		arrayOperatorExpression->opfuncid);
	arrayEqualityOp->opcollid = partitionColumn->varcollid;
	arrayEqualityOp->location = -1;
	return arrayEqualityOp;
}


/*
 * DebugLogNode is a helper for logging expression nodes.
 */
static void
DebugLogNode(char *fmt, Node *node, List *deparseCtx)
{
	if (node != NULL)
	{
		char *deparsed = deparse_expression(node, deparseCtx, false, false);
		ereport(DEBUG3, (errmsg(fmt, deparsed)));
	}
}


/*
 * DebugLogPruningInstance is a helper for logging purning constraints.
 */
static void
DebugLogPruningInstance(PruningInstance *pruning, List *deparseCtx)
{
	DebugLogNode("constraint value: %s",
				 (Node *) pruning->equalConsts, deparseCtx);
	DebugLogNode("constraint (lt) value: %s", \
				 (Node *) pruning->lessConsts, deparseCtx);
	DebugLogNode("constraint (lteq) value: %s", \
				 (Node *) pruning->lessEqualConsts, deparseCtx);
	DebugLogNode("constraint (gt) value: %s", \
				 (Node *) pruning->greaterConsts, deparseCtx);
	DebugLogNode("constraint (gteq) value: %s",
				 (Node *) pruning->greaterEqualConsts, deparseCtx);
}


/*
 * ConstraintCount returns how many arguments this node is taking.
 */
static int
ConstraintCount(PruningTreeNode *node)
{
	return list_length(node->childBooleanNodes) +
		   list_length(node->validConstraints) +
		   (node->hasInvalidConstraints ? 1 : 0);
}
