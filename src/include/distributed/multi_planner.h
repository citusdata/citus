/*-------------------------------------------------------------------------
 *
 * multi_planner.h
 *	  General Citus planner code.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_PLANNER_H
#define MULTI_PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

#include "distributed/citus_nodes.h"


/* values used by jobs and tasks which do not require identifiers */
#define INVALID_JOB_ID 0
#define INVALID_TASK_ID 0


typedef struct RelationRestrictionContext
{
	bool hasDistributedRelation;
	bool hasLocalRelation;
	bool allReferenceTables;
	List *relationRestrictionList;
} RelationRestrictionContext;

typedef struct RelationRestriction
{
	Index index;
	Oid relationId;
	bool distributedRelation;
	RangeTblEntry *rte;
	RelOptInfo *relOptInfo;
	PlannerInfo *plannerInfo;
	List *prunedShardIntervalList;
} RelationRestriction;

typedef struct RelationShard
{
	CitusNode type;
	Oid relationId;
	uint64 shardId;
} RelationShard;


extern PlannedStmt * multi_planner(Query *parse, int cursorOptions,
								   ParamListInfo boundParams);

extern bool HasCitusToplevelNode(PlannedStmt *planStatement);
struct MultiPlan;
extern struct MultiPlan * GetMultiPlan(CustomScan *node);
extern Node * SerializableMultiPlan(struct MultiPlan *multiPlan);
extern struct MultiPlan * DeSerializeMultiPlan(Node *node);
extern void multi_relation_restriction_hook(PlannerInfo *root, RelOptInfo *relOptInfo,
											Index index, RangeTblEntry *rte);
extern bool IsModifyCommand(Query *query);
extern void VerifyMultiPlanValidity(struct MultiPlan *multiPlan);

#endif /* MULTI_PLANNER_H */
