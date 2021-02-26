/*-------------------------------------------------------------------------
 *
 * shard_pruning.h
 *   Shard pruning infrastructure.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARD_PRUNING_H_
#define SHARD_PRUNING_H_

#include "distributed/metadata_cache.h"
#include "distributed/pg_version_constants.h"
#include "nodes/primnodes.h"

#define INVALID_SHARD_INDEX -1

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

#if PG_VERSION_NUM >= PG_VERSION_12
typedef union \
{ \
	FunctionCallInfoBaseData fcinfo; \
	/* ensure enough space for nargs args is available */ \
	char fcinfo_data[SizeForFunctionCallInfo(2)]; \
} FunctionCall2InfoData;
#else
typedef FunctionCallInfoData FunctionCall2InfoData;
#endif

/* Function declarations for shard pruning */
extern List * PruneShards(Oid relationId, Index rangeTableId, List *whereClauseList,
						  Const **partitionValueConst);
extern bool ContainsFalseClause(List *whereClauseList);
extern List * get_all_actual_clauses(List *restrictinfo_list);
extern Const * TransformPartitionRestrictionValue(Var *partitionColumn,
												  Const *restrictionValue,
												  bool missingOk);
bool VarConstOpExprClause(OpExpr *opClause, Var **varClause, Const **constantClause);
extern bool ExhaustivePruneOneWithMinMax(PruningInstance *prune,
										 FunctionCallInfo compareFunctionCall,
										 Datum minimumValue, Datum maximumValue);
extern List * BuildPruningInstanceList(Var *column, List *whereClauseList,
									   FunctionCall2InfoData *compareValueFunctionCall);

#endif /* SHARD_PRUNING_H_ */
