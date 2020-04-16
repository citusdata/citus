/*
 * citus_local_planner.c
 *
 * Planning logic for queries involving citus local tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * We introduced a new table type to citus, citus local tables. Queries
 * involving citus local tables cannot be planned with other citus planners
 * as they even do not know citus tables with distribution method
 * CITUS_LOCAL_TABLE.
 *
 * Hence, if a query includes at least one citus local table in it, we first
 * fall into CreateCitusLocalPlan, and create a distributed plan including
 * the job to be executed on the coordinator node (note that only the
 * coordinator is allowed to have citus local tables for now). Then we replace
 * OID's of citus local tables with their local shards on the query tree and
 * create the distributed plan with this modified query.
 *
 * Replacing those tables in the given query, then we create a Job which
 * executes the given query via executor. Then those queries will be re-
 * evaluated by the other citus planners without any problems as they know
 * how to process queries with Postgres local tables.
 *
 * In that sense, we will behave those tables as local tables accross the
 * distributed planner and executor. But, for example, we would be erroring
 * out for their "local shard relations" if it is not a supported query as
 * we are threating them as Postgres local tables. To prevent this, before
 * deciding to use CitusLocalPlanner, we first check for unsupported cases
 * by threating those as local tables and error out if needed.
 * (see ErrorIfUnsupportedCitusLocalQuery and its usage)
 *
 * Reason behind that we do not directly replace the citus local tables and
 * use existing planner methods is to take necessary locks on shell tables
 * and keeping citus statistics tracked for citus local tables as well.
 */

#include "distributed/citus_local_planner.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_utils.h"

/* make_ands_explicit */
#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/makefuncs.h"
#else
#include "optimizer/clauses.h"
#endif

static void ErrorIfUnsupportedCitusLocalQuery(Query *query,
											  RTEListProperties *rteListProperties);
static bool QueryIsSelectForUpdateShare(Node *node);
static Job * CreateCitusLocalPlanJob(Query *query, List *citusLocalRelationRTEList);
static List * CitusLocalPlanTaskList(Query *query, List *citusLocalRelationRTEList);
static List * ExtractTableRTEListByDistMethod(List *rteList, char distributionMethod);
static List * ActiveShardPlacementsForTableOnGroup(Oid relationId, int32 groupId);
static void UpdateRelationOidsWithLocalShardOids(Query *query,
												 List *citusLocalRelationRTEList);

/*
 * CreateCitusLocalPlan creates the distributed plan to process given query
 * involving citus local tables. For those queries, CreateCitusLocalPlan is
 * the only appropriate planner function.
 */
DistributedPlan *
CreateCitusLocalPlan(Query *query)
{
	ereport(DEBUG2, (errmsg("Creating citus local plan")));

	List *rangeTableList = ExtractRangeTableEntryList(query);

	List *citusLocalRelationRTEList = ExtractTableRTEListByDistMethod(rangeTableList,
																	  CITUS_LOCAL_TABLE);
	List *referenceTableRTEList = ExtractTableRTEListByDistMethod(rangeTableList,
																  DISTRIBUTE_BY_NONE);

	citusLocalRelationRTEList = list_concat(citusLocalRelationRTEList,
											referenceTableRTEList);

	RTEListProperties *rteListProperties = GetRTEListProperties(rangeTableList);

	ErrorIfUnsupportedCitusLocalQuery(query, rteListProperties);

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	distributedPlan->modLevel = RowModifyLevelForQuery(query);

	Oid resultRelationOid = InvalidOid;

	if (IsModifyCommand(query))
	{
		resultRelationOid = ResultRelationOidForQuery(query);
	}

	if (IsCitusTable(resultRelationOid))
	{
		distributedPlan->targetRelationId = resultRelationOid;
	}

	distributedPlan->routerExecutable = true;

	distributedPlan->workerJob = CreateCitusLocalPlanJob(query,
														 citusLocalRelationRTEList);

	return distributedPlan;
}


/*
 * ErrorIfUnsupportedCitusLocalQuery errors out if the given query is an
 * unsupported citus local query.
 */
static void
ErrorIfUnsupportedCitusLocalQuery(Query *query, RTEListProperties *rteListProperties)
{
	bool queryIsModifyCommand = IsModifyCommand(query);

	if (queryIsModifyCommand)
	{
		Oid targetRelation = ResultRelationOidForQuery(query);

		if (IsCitusTable(targetRelation) && IsReferenceTable(targetRelation))
		{
			ereport(ERROR, (errmsg("cannot plan modification queries with local tables "
								   "which modifies reference tables")));
		}
	}

	bool queryIsSelectForUpdateShare = FindNodeCheck((Node *) query, QueryIsSelectForUpdateShare);

	if (queryIsSelectForUpdateShare && rteListProperties->hasReferenceTable)
	{
		ereport(ERROR, (errmsg("cannot plan SELECT FOR UPDATE/SHARE queries with local "
							   "tables and reference tables")));
	}
}


/*
 * QueryIsSelectForUpdateShare returns true if node is a query which marks for
 * modifications.
 */
static bool
QueryIsSelectForUpdateShare(Node *node)
{
	if (!IsA(node, Query))
	{
		return false;
	}

	Query *query = (Query *) node;
	return (query->commandType == CMD_SELECT) && (query->rowMarks != NIL);
}


/*
 * CreateCitusLocalPlanJob replaces the range table entries belonging to citus
 * local relations (i.e reference tables & citus local tables) and creates a
 * Job to execute that query locally via executor.
 */
static Job *
CreateCitusLocalPlanJob(Query *query, List *citusLocalRelationRTEList)
{
	Job *job = CreateJob(query);

	/* modify the query and create the single element task list with that query */
	job->taskList = CitusLocalPlanTaskList(query, citusLocalRelationRTEList);

	return job;
}


/*
 * CitusLocalPlanTaskList returns a single element task list including the
 * task to execute the given query with citus local relations properly.
 * Also modifies the input query to be put to the single task so it includes
 * local shard relations for citus local tables & reference tables.
 */
static List *
CitusLocalPlanTaskList(Query *query, List *citusLocalRelationRTEList)
{
	Assert(citusLocalRelationRTEList != NIL);

	uint64 anchorShardId = INVALID_SHARD_ID;

	List *shardIntervalList = NIL;
	List *taskPlacementList = NIL;

	/* extract shard placements & shardIds for citus local tables in the query */
	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, citusLocalRelationRTEList)
	{
		Oid tableOid = rangeTableEntry->relid;

		if (!IsCitusTable(tableOid))
		{
			continue;
		}

		const CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(tableOid);

		Assert(cacheEntry != NULL &&
			   CitusTableWithoutDistributionKey(cacheEntry->partitionMethod));

		ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[0];
		shardIntervalList = lappend(shardIntervalList, shardInterval);

		uint64 localShardId = shardInterval->shardId;

		/*
		 * Reference tables have placements on the other nodes as well. However,
		 * as citus local tables do not have placements on the other nodes, task
		 * placement list should not cover remote placements of reference tables.
		 *
		 * For SELECT queries, this case could already be handled by local
		 * executor.
		 * However, for modifying queries, local executor would try to execute
		 * the task on remote nodes as well and it would fail.
		 * Hence, here we should only put coordinator placements of reference
		 * tables to the task placement list.
		 */
		List *shardPlacements = ActiveShardPlacementsForTableOnGroup(tableOid,
																	 COORDINATOR_GROUP_ID);

		Assert(list_length(shardPlacements) == 1);

		taskPlacementList = list_concat(taskPlacementList, shardPlacements);

		/*
		 * Save it for now, we will override for modifications below. For
		 * SELECT queries, it doesn't matter which shard we pick as anchor
		 * shard.
		 */
		anchorShardId = localShardId;
	}

	TaskType taskType = TASK_TYPE_INVALID_FIRST;

	if (query->commandType == CMD_SELECT)
	{
		taskType = SELECT_TASK;
	}
	else if (IsModifyCommand(query))
	{
		taskType = MODIFY_TASK;
	}
	else
	{
		Assert(false);
	}

	bool shardsPresent = false;

	Task *task = CreateTask(taskType);

	if (IsModifyCommand(query))
	{
		/* only required for modify commands */
		Oid resultRelationId = ResultRelationOidForQuery(query);

		if (IsCitusTable(resultRelationId))
		{
			task->anchorDistributedTableId = resultRelationId;

			List *anchorShardIntvervalList = LoadShardIntervalList(resultRelationId);

			ShardInterval *anchorShardInterval = (ShardInterval *) linitial(
				anchorShardIntvervalList);

			/* adjust anchorShardId as well */
			anchorShardId = anchorShardInterval->shardId;
		}
	}

	task->anchorShardId = anchorShardId;
	task->taskPlacementList = taskPlacementList;
	task->relationShardList =
		RelationShardListForShardIntervalList(list_make1(shardIntervalList),
											  &shardsPresent);

	/* make the final changes on the query before setting task query */

	/*
	 * Replace citus local tables with their local shards and acquire necessary
	 * locks
	 */
	UpdateRelationOidsWithLocalShardOids(query, citusLocalRelationRTEList);

	/* convert list of expressions into expression tree for further processing */
	FromExpr *joinTree = query->jointree;
	Node *quals = joinTree->quals;
	if (quals != NULL && IsA(quals, List))
	{
		joinTree->quals = (Node *) make_ands_explicit((List *) quals);
	}

	SetTaskQueryIfShouldLazyDeparse(task, query);

	return list_make1(task);
}


/*
 * ExtractTableRTEListByDistMethod extracts and returns citus table by
 * distribution method.
 */
static List *
ExtractTableRTEListByDistMethod(List *rteList, char distributionMethod)
{
	List *rteListByDistMethod = NIL;

	RangeTblEntry *rte = NULL;
	foreach_ptr(rte, rteList)
	{
		if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION)
		{
			continue;
		}

		Oid relationOid = rte->relid;

		if (!IsCitusTable(relationOid))
		{
			continue;
		}

		char partititonMethod = PartitionMethod(relationOid);
		if (partititonMethod == distributionMethod)
		{
			rteListByDistMethod = lappend(rteListByDistMethod, rte);
		}
	}

	return rteListByDistMethod;
}


/*
 * ActiveShardPlacementsForTableOnGroup accepts a relationId and a group and
 * returns a list of ShardPlacement's representing all of the active placements
 * for the table which reside on the group.
 */
static List *
ActiveShardPlacementsForTableOnGroup(Oid relationId, int32 groupId)
{
	List *groupActivePlacementList = NIL;

	List *shardIntervalList = LoadShardIntervalList(relationId);
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		List *activeShardPlacementList = ActiveShardPlacementList(shardId);
		ShardPlacement *shardPlacement = NULL;
		foreach_ptr(shardPlacement, activeShardPlacementList)
		{
			if (shardPlacement->groupId == groupId)
			{
				groupActivePlacementList = lappend(groupActivePlacementList,
												   shardPlacement);
			}
		}
	}

	return groupActivePlacementList;
}


/*
 * UpdateRelationOidsWithLocalShardOids replaces OID fields of the given range
 * table entries with their local shard relation OID's and acquires necessary
 * locks for those local shard relations.
 *
 * Callers of this function are responsible to provide range table entries only
 * for citus tables without distribution keys, i.e reference tables or citus
 * local tables.
 */
static void
UpdateRelationOidsWithLocalShardOids(Query *query, List *citusLocalRelationRTEList)
{
#if PG_VERSION_NUM < PG_VERSION_12

	/*
	 * We cannot infer the required lock mode per range table entries as they
	 * do not have rellockmode field if PostgreSQL version < 12.0, but we can
	 * deduce it from the query itself for all the range table entries.
	 */
	LOCKMODE localShardLockMode = GetQueryLockMode(query);
#endif

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, citusLocalRelationRTEList)
	{
		Oid relationId = rangeTableEntry->relid;
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

		/* given OID should belong to a valid citus table without dist. key */
		Assert(cacheEntry != NULL &&
			   CitusTableWithoutDistributionKey(cacheEntry->partitionMethod));

		/*
		 * It is callers responsibility to pass relations that has single shards,
		 * namely citus local tables or reference tables.
		 */
		Assert(cacheEntry->shardIntervalArrayLength == 1);

		ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[0];
		uint64 localShardId = shardInterval->shardId;

		Oid tableLocalShardOid = GetTableLocalShardOid(relationId, localShardId);

		/* it is callers responsibility to pass relations that has local placements */
		Assert(OidIsValid(tableLocalShardOid));

		/* override the relation id with the shard's relation OID */
		rangeTableEntry->relid = tableLocalShardOid;

#if PG_VERSION_NUM >= PG_VERSION_12

		/*
		 * We can infer the required lock mode from the rte itself if PostgreSQL
		 * version >= 12.0
		 */
		LOCKMODE localShardLockMode = rangeTableEntry->rellockmode;
#endif

		/*
		 * Parser locks relations in addRangeTableEntry(). So we should lock the
		 * modified ones too.
		 */
		LockRelationOid(tableLocalShardOid, localShardLockMode);
	}
}


/*
 * ShouldUseCitusLocalPlanner returns true if the given rteListProperties
 * yields using citus local planner.
 */
bool
ShouldUseCitusLocalPlanner(RTEListProperties *rteListProperties)
{
	/* queries not involving citus tables should never come here */
	Assert(rteListProperties->hasCitusTable);

	if (rteListProperties->hasDistributedTable)
	{
		/* citus local planner doesn't know how to handle distributed tables */
		return false;
	}

	if (!(IsCoordinator() && CoordinatorAddedAsWorkerNode()))
	{
		/*
		 * Local planner is only intended to work locally on the tables that
		 * are on coordinator.
		 */
		return false;
	}

	return rteListProperties->hasCitusLocalTable || rteListProperties->hasLocalTable;
}
