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
 * (see ErrorIfUnsupportedQueryWithCitusLocalTables and its usage)
 *
 * Reason behind that we do not directly replace the citus local tables and
 * use existing planner methods is to take necessary locks on shell tables
 * and keeping citus statistics tracked for citus local tables as well.
 */

#include "distributed/citus_local_planner.h"
#include "distributed/reference_table_utils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
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

static Job * CreateCitusLocalPlanJob(Query *query, List *localRelationRTEList);
static void UpdateRelationOidsWithLocalShardOids(Query *query,
												 List *localRelationRTEList);
static List * CitusLocalPlanTaskList(Query *query, List *localRelationRTEList);

/*
 * CreateCitusLocalPlan creates the distributed plan to process given query
 * involving citus local tables. For those queries, CreateCitusLocalPlan is
 * the only appropriate planner function.
 */
DistributedPlan *
CreateCitusLocalPlan(Query *query)
{
	ereport(DEBUG2, (errmsg("Creating citus local plan")));

	ErrorIfUnsupportedQueryWithCitusLocalTables(query);

	List *rangeTableList = ExtractRangeTableEntryList(query);

	List *citusLocalTableRTEList =
		ExtractTableRTEListByDistMethod(rangeTableList, CITUS_LOCAL_TABLE);
	List *referenceTableRTEList =
		ExtractTableRTEListByDistMethod(rangeTableList, DISTRIBUTE_BY_NONE);

	List *localRelationRTEList = list_concat(citusLocalTableRTEList,
											 referenceTableRTEList);

	Assert(localRelationRTEList != NIL);

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	distributedPlan->modLevel = RowModifyLevelForQuery(query);


	int resultRelationId = IsModifyCommand(query) ? ResultRelationOidForQuery(query) :
						   InvalidOid;

	if (IsCitusTable(resultRelationId))
	{
		distributedPlan->targetRelationId = resultRelationId;
	}

	distributedPlan->routerExecutable = true;

	distributedPlan->workerJob =
		CreateCitusLocalPlanJob(query, localRelationRTEList);

	/* make the final changes on the query */

	/* convert list of expressions into expression tree for further processing */
	FromExpr *joinTree = query->jointree;
	Node *quals = joinTree->quals;
	if (quals != NULL && IsA(quals, List))
	{
		joinTree->quals = (Node *) make_ands_explicit((List *) quals);
	}

	return distributedPlan;
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
UpdateRelationOidsWithLocalShardOids(Query *query, List *localRelationRTEList)
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
	foreach_ptr(rangeTableEntry, localRelationRTEList)
	{
		Oid relationId = rangeTableEntry->relid;
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

		/* given OID should belong to a valid reference table */
		Assert(cacheEntry != NULL &&
			   CitusTableWithoutDistributionKey(cacheEntry->partitionMethod));

		/*
		 * It is callers reponsibility to pass relations that has single shards,
		 * namely citus local tables or reference tables.
		 */
		Assert(cacheEntry->shardIntervalArrayLength == 1);

		ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[0];
		uint64 localShardId = shardInterval->shardId;

		Oid tableLocalShardOid = GetTableLocalShardOid(relationId, localShardId);

		/* it is callers reponsibility to pass relations that has local placements */
		Assert(OidIsValid(tableLocalShardOid));

		/* override the relation id with the shard's relation id */
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
 * CreateCitusLocalPlanJob returns a Job to be executed by the adaptive executor
 * methods for the query involving "citus local table's" local shard relations.
 * Then, as the query wouldn't have citus local tables at that point, that query
 * will be executed by the other planners.
 */
static Job *
CreateCitusLocalPlanJob(Query *query, List *noDistKeyTableRTEList)
{
	Job *job = CreateJob(query);

	job->taskList = CitusLocalPlanTaskList(query, noDistKeyTableRTEList);

	return job;
}

static List *
ActiveShardPlacementsForTableOnGroup(Oid relationId, int32 groupId);

/*
 * CitusLocalPlanTaskList returns a single element task list including the
 * task to execute the given query with citus local table(s) properly.
 */
static List *
CitusLocalPlanTaskList(Query *query, List *localRelationRTEList)
{
	uint64 anchorShardId = INVALID_SHARD_ID;

	List *shardIntervalList = NIL;
	List *taskPlacementList = NIL;

	/* extract shard placements & shardIds for citus local tables in the query */
	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, localRelationRTEList)
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

		List *shardPlacements = ActiveShardPlacementsForTableOnGroup(tableOid, COORDINATOR_GROUP_ID);

		taskPlacementList = list_concat(taskPlacementList, shardPlacements);

		/* save it for now, we will override for modifications below */
		anchorShardId = localShardId;
	}

	/* prevent possible self dead locks */
	taskPlacementList = SortList(taskPlacementList, CompareShardPlacementsByShardId);

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
		/* only required for INSERTs */
		int resultRelationId = ResultRelationOidForQuery(query);

		if (IsCitusTable(resultRelationId))
		{
			task->anchorDistributedTableId = ResultRelationOidForQuery(query);

			List *anchorShardInvervalList = LoadShardIntervalList(
				task->anchorDistributedTableId);

			ShardInterval *anchorShardInterval = (ShardInterval *) linitial(
				anchorShardInvervalList);
			anchorShardId = anchorShardInterval->shardId;
		}
	}

	task->anchorShardId = anchorShardId;
	task->taskPlacementList = taskPlacementList;
	task->relationShardList =
		RelationShardListForShardIntervalList(list_make1(shardIntervalList),
											  &shardsPresent);

	/*
	 * Replace citus local tables with their local shards and acquire necessary
	 * locks
	 */
	UpdateRelationOidsWithLocalShardOids(query, localRelationRTEList);
	SetTaskQueryIfShouldLazyDeparse(task, query);

	return list_make1(task);
}
/*
 * ActiveShardPlacementsForTableOnGroup accepts a relationId and a group and
 * returns a list of ShardPlacement's representing all of the placements for
 * the table which reside on the group.
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
				groupActivePlacementList = lappend(groupActivePlacementList, shardPlacement);
			}
		}
	}

	return groupActivePlacementList;
}


bool
ShouldUseCitusLocalPlanner(RTEListProperties *rteListProperties)
{
	if (!rteListProperties->hasCitusTable)
	{
		/* only postgres local tables should never come here, but be defensive anyway */
		return false;
	}

	if (rteListProperties->hasDistributedTable)
	{
		/* local planner doesn't know how to handle distributed tables */
		return false;
	}

	if (!(IsCoordinator() && CoordinatorAddedAsWorkerNode()))
	{
		/*
		 * Local planner is only intended to work locally on the tables
		 * that are on coordinator.
		 */
		return false;
	}


	/*
	 * As long as
	 */
	return rteListProperties->hasCitusLocalTable || rteListProperties->hasLocalTable;
}


/*
 * ErrorIfUnsupportedQueryWithCitusLocalTables errors out if the given query
 * is an unsupported "citus local table" query.
 */
void
ErrorIfUnsupportedQueryWithCitusLocalTables(Query *query)
{
	bool isModifyCommand = IsModifyCommand(query);

	if (isModifyCommand)
	{
		Oid targetRelation = ResultRelationOidForQuery(query);

		if (IsCitusTable(targetRelation) && IsReferenceTable(targetRelation))
		{
			ereport(ERROR, (errmsg("cannot plan modification queries with local tables "
								   "which modifies reference tables")));
		}
	}
}
