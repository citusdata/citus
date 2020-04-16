/*-------------------------------------------------------------------------
 *
 * multi_join_order.h
 *
 * Type and function declarations for determining a left-only join order for a
 * distributed query plan.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_JOIN_ORDER_H
#define MULTI_JOIN_ORDER_H

#include "postgres.h"

#include "nodes/pg_list.h"
#include "nodes/primnodes.h"


/*
 * JoinRuleType determines the type of the join rule that applies between two
 * tables or row sources. The rule types are ordered below according to their
 * costs, with the cheapes rule appearing at the top. Note that changing the
 * order of these enums *will* change the order in which the rules are applied.
 */
typedef enum JoinRuleType
{
	JOIN_RULE_INVALID_FIRST = 0,
	REFERENCE_JOIN = 1,
	LOCAL_PARTITION_JOIN = 2,
	SINGLE_HASH_PARTITION_JOIN = 3,
	SINGLE_RANGE_PARTITION_JOIN = 4,
	DUAL_PARTITION_JOIN = 5,
	CARTESIAN_PRODUCT_REFERENCE_JOIN = 6,
	CARTESIAN_PRODUCT = 7,

	/*
	 * Add new join rule types above this comment. After adding, you must also
	 * update these arrays: RuleEvalFunctionArray, RuleApplyFunctionArray, and
	 * RuleNameArray.
	 */
	JOIN_RULE_LAST
} JoinRuleType;


/*
 * TableEntry represents a table used when determining the join order. A table
 * entry corresponds to an ordinary relation reference (RTE_RELATION) in the
 * query range table list.
 */
typedef struct TableEntry
{
	Oid relationId;
	uint32 rangeTableId;
} TableEntry;


/*
 * JoinOrderNode represents an element in the join order list; and this list
 * keeps the total join order for a distributed query. The first node in this
 * list later becomes the leftmost table in the join tree, and the successive
 * elements in the list are the joining tables in the left-deep tree.
 */
typedef struct JoinOrderNode
{
	TableEntry *tableEntry;     /* this node's relation and range table id */
	JoinRuleType joinRuleType;  /* not relevant for the first table */
	JoinType joinType;          /* not relevant for the first table */

	/*
	 * We keep track of all unique partition columns in the relation to correctly find
	 * join clauses that can be applied locally.
	 */
	List *partitionColumnList;

	char partitionMethod;
	List *joinClauseList;       /* not relevant for the first table */
	TableEntry *anchorTable;
} JoinOrderNode;


/* Config variables managed via guc.c */
extern bool LogMultiJoinOrder;
extern bool EnableSingleHashRepartitioning;


/* Function declaration for determining table join orders */
extern List * JoinExprList(FromExpr *fromExpr);
extern List * JoinOrderList(List *rangeTableEntryList, List *joinClauseList);
extern bool IsApplicableJoinClause(List *leftTableIdList, uint32 rightTableId,
								   Node *joinClause);
extern List * ApplicableJoinClauses(List *leftTableIdList, uint32 rightTableId,
									List *joinClauseList);
extern bool NodeIsEqualsOpExpr(Node *node);
extern bool IsSupportedReferenceJoin(JoinType joinType, bool leftIsReferenceTable,
									 bool rightIsReferenceTable);
extern OpExpr * SinglePartitionJoinClause(List *partitionColumnList,
										  List *applicableJoinClauses);
extern OpExpr * DualPartitionJoinClause(List *applicableJoinClauses);
extern Var * LeftColumnOrNULL(OpExpr *joinClause);
extern Var * RightColumnOrNULL(OpExpr *joinClause);


#endif   /* MULTI_JOIN_ORDER_H */
