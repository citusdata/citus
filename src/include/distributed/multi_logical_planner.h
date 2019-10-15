/*-------------------------------------------------------------------------
 *
 * multi_logical_planner.h
 *	  Type declarations for multi-relational algebra operators, and function
 *	  declarations for building logical plans over distributed relations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_LOGICAL_PLANNER_H
#define MULTI_LOGICAL_PLANNER_H

#include "distributed/citus_nodes.h"
#include "distributed/errormessage.h"
#include "distributed/multi_join_order.h"
#include "distributed/relation_restriction_equivalence.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"


#define SUBQUERY_RANGE_TABLE_ID -1
#define SUBQUERY_RELATION_ID 10000
#define SUBQUERY_PUSHDOWN_RELATION_ID 10001


/*
 * MultiNode represents the base node type for all multi-relational algebra
 * nodes. By creating this base node, we can simulate inheritance and represent
 * derived operator nodes as a MultiNode type. A similar structure to simulate
 * inheritance is also used in PostgreSQL's plan nodes.
 */
typedef struct MultiNode
{
	CitusNode type;

	struct MultiNode *parentNode;

	/* child node(s) are defined in unary and binary nodes */
} MultiNode;


/* Represents unary nodes that have only one child */
typedef struct MultiUnaryNode
{
	MultiNode node;

	struct MultiNode *childNode;
} MultiUnaryNode;


/* Represents binary nodes that have two children */
typedef struct MultiBinaryNode
{
	MultiNode node;

	struct MultiNode *leftChildNode;
	struct MultiNode *rightChildNode;
} MultiBinaryNode;


/*
 * MultiTreeRoot keeps a pointer to the root node in the multi-relational
 * operator tree. This node is always on the top of every logical plan.
 */
typedef struct MultiTreeRoot
{
	MultiUnaryNode unaryNode;
} MultiTreeRoot;


/*
 * MultiTable represents a partitioned table in a logical query plan. Note that
 * this node does not represent a query operator, and differs from the nodes
 * that follow in that sense.
 */
typedef struct MultiTable
{
	MultiUnaryNode unaryNode;
	Oid relationId;
	int rangeTableId;
	Var *partitionColumn;
	Alias *alias;
	Alias *referenceNames;
	Query *subquery; /* this field is only valid for non-relation subquery types */
} MultiTable;


/* Defines the columns to project in multi-relational algebra */
typedef struct MultiProject
{
	MultiUnaryNode unaryNode;
	List *columnList;
} MultiProject;


/*
 * MultiCollect defines the Collect operator in multi-relational algebra. This
 * operator collects data from remote nodes; the collected data are output from
 * the query operator that is beneath this Collect in the logical query tree.
 */
typedef struct MultiCollect
{
	MultiUnaryNode unaryNode;
} MultiCollect;


/*
 * MultiSelect defines the MultiSelect operator in multi-relational algebra.
 * This operator contains select predicates which apply to a selected logical
 * relation.
 */
typedef struct MultiSelect
{
	MultiUnaryNode unaryNode;
	List *selectClauseList;
} MultiSelect;


/*
 * MultiJoin joins the output of two query operators that are beneath it in the
 * query tree. The operator also keeps the join rule that applies between the
 * two operators, and the partition key to use if the join is distributed.
 */
typedef struct MultiJoin
{
	MultiBinaryNode binaryNode;
	List *joinClauseList;
	JoinRuleType joinRuleType;
	JoinType joinType;
} MultiJoin;


/* Defines the (re-)Partition operator in multi-relational algebra */
typedef struct MultiPartition
{
	MultiUnaryNode unaryNode;
	Var *partitionColumn;
	uint32 splitPointTableId;
} MultiPartition;


/* Defines the CartesianProduct operator in multi-relational algebra */
typedef struct MultiCartesianProduct
{
	MultiBinaryNode binaryNode;
} MultiCartesianProduct;


/*
 * MultiExtendedOp defines a set of extended operators that operate on columns
 * in relational algebra. This node allows us to distinguish between operations
 * in the master and worker nodes, and also captures the following:
 *
 * (1) Aggregate functions such as sums or averages;
 * (2) Grouping of attributes; these groupings may also be tied to aggregates;
 * (3) Extended projection expressions including columns, arithmetic and string
 * functions;
 * (4) User's intented display information, such as column display order;
 * (5) Sort clauses on columns, expressions, or aggregates; and
 * (6) Limit count and offset clause.
 */
typedef struct MultiExtendedOp
{
	MultiUnaryNode unaryNode;
	List *targetList;
	List *groupClauseList;
	List *sortClauseList;
	Node *limitCount;
	Node *limitOffset;
	Node *havingQual;
	List *distinctClause;
	bool hasDistinctOn;
	bool hasWindowFuncs;
	List *windowClause;
} MultiExtendedOp;


/* Function declarations for building logical plans */
extern MultiTreeRoot * MultiLogicalPlanCreate(Query *originalQuery, Query *queryTree,
											  PlannerRestrictionContext *
											  plannerRestrictionContext);
extern bool FindNodeCheck(Node *node, bool (*check)(Node *));
extern bool SingleRelationRepartitionSubquery(Query *queryTree);
extern bool TargetListOnPartitionColumn(Query *query, List *targetEntryList);
extern bool FindNodeCheckInRangeTableList(List *rtable, bool (*check)(Node *));
extern bool IsDistributedTableRTE(Node *node);
extern bool QueryContainsDistributedTableRTE(Query *query);
extern bool ContainsReadIntermediateResultFunction(Node *node);
extern MultiNode * ParentNode(MultiNode *multiNode);
extern MultiNode * ChildNode(MultiUnaryNode *multiNode);
extern MultiNode * GrandChildNode(MultiUnaryNode *multiNode);
extern void SetChild(MultiUnaryNode *parent, MultiNode *child);
extern void SetLeftChild(MultiBinaryNode *parent, MultiNode *leftChild);
extern void SetRightChild(MultiBinaryNode *parent, MultiNode *rightChild);
extern bool UnaryOperator(MultiNode *node);
extern bool BinaryOperator(MultiNode *node);
extern List * OutputTableIdList(MultiNode *multiNode);
extern List * FindNodesOfType(MultiNode *node, int type);
extern List * JoinClauseList(List *whereClauseList);
extern bool IsJoinClause(Node *clause);
extern List * SubqueryEntryList(Query *queryTree);
extern DeferredErrorMessage * DeferErrorIfQueryNotSupported(Query *queryTree);
extern bool ExtractRangeTableIndexWalker(Node *node, List **rangeTableIndexList);
extern List * WhereClauseList(FromExpr *fromExpr);
extern List * QualifierList(FromExpr *fromExpr);
extern List * TableEntryList(List *rangeTableList);
extern List * UsedTableEntryList(Query *query);
extern bool ExtractRangeTableRelationWalker(Node *node, List **rangeTableList);
extern bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);
extern List * pull_var_clause_default(Node *node);
extern bool OperatorImplementsEquality(Oid opno);
extern bool FindNodeCheck(Node *node, bool (*check)(Node *));
extern DeferredErrorMessage * DeferErrorIfUnsupportedClause(List *clauseList);
extern MultiProject * MultiProjectNode(List *targetEntryList);
extern MultiExtendedOp * MultiExtendedOpNode(Query *queryTree);
extern DeferredErrorMessage * DeferErrorIfUnsupportedSubqueryRepartition(Query *
																		 subqueryTree);
extern MultiNode * MultiNodeTree(Query *queryTree);


#endif   /* MULTI_LOGICAL_PLANNER_H */
