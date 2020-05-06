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
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/shardinterval_utils.h"

/* make_ands_explicit */
#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/makefuncs.h"
#else
#include "optimizer/clauses.h"
#endif

static Job * CreateCitusLocalPlanJob(Query *query, List *noDistKeyTableRTEList);
static List * CitusLocalPlanTaskList(Query *query, List *noDistKeyTableRTEList);

/*
 * CreateCitusLocalPlan creates the distributed plan to process given query
 * involving citus local tables. For those queries, CreateCitusLocalPlan is
 * the only appropriate planner function.
 */
DistributedPlan *
CreateCitusLocalPlan(Query *query, PlannerRestrictionContext *plannerRestrictionContext)
{
	ereport(DEBUG2, (errmsg("Creating citus local plan")));

	List *rangeTableList = ExtractRangeTableEntryList(query);

	List *noDistKeyTableRTEList = ExtractTableRTEListByDistMethod(rangeTableList,
																  CITUS_LOCAL_TABLE);

	if (plannerRestrictionContext->citusLocalPlanRestrictionContext->isLocalReferenceJoin)
	{
		List *referenceTableRTEList = ExtractTableRTEListByDistMethod(rangeTableList,
																	  DISTRIBUTE_BY_NONE);

		noDistKeyTableRTEList = list_concat(noDistKeyTableRTEList,
											referenceTableRTEList);
	}

	Assert(noDistKeyTableRTEList != NIL);

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	distributedPlan->modLevel = RowModifyLevelForQuery(query);
	distributedPlan->workerJob = CreateCitusLocalPlanJob(query,
														 noDistKeyTableRTEList);

	/* make the final changes on the query */

	/*
	 * Replace citus local tables with their local shards and acquire necessary
	 * locks
	 */
	UpdateTablesWithoutDistKeysWithShards(query, noDistKeyTableRTEList);

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


/*
 * CitusLocalPlanTaskList returns a single element task list including the
 * task to execute the given query with citus local table(s) properly.
 */
static List *
CitusLocalPlanTaskList(Query *query, List *noDistKeyTableRTEList)
{
	List *taskPlacementList = NIL;

	/* extract shard placements & shardIds for citus local tables in the query */
	RangeTblEntry *rte = NULL;
	foreach_ptr(rte, noDistKeyTableRTEList)
	{
		Oid tableOid = rte->relid;

		Assert(IsCitusTable(tableOid) && CitusTableWithoutDistributionKey(PartitionMethod(
																			  tableOid)));

		const CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(tableOid);

		Assert(cacheEntry != NULL && CitusTableWithoutDistributionKey(
				   cacheEntry->partitionMethod));

		const ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[0];
		uint64 localShardId = shardInterval->shardId;

		List *shardPlacements = ActiveShardPlacementList(localShardId);

		taskPlacementList = list_concat(taskPlacementList, shardPlacements);
	}

	/* prevent possible self dead locks */
	taskPlacementList = SortList(taskPlacementList, CompareShardPlacementsByShardId);

	/* pick the shard having the lowest shardId as the anchor shard */
	uint64 anchorShardId = ((ShardPlacement *) linitial(taskPlacementList))->shardId;

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

	Task *task = CreateTask(taskType);

	task->anchorShardId = anchorShardId;
	task->taskPlacementList = taskPlacementList;
	SetTaskQueryIfShouldLazyDeparse(task, query);

	return list_make1(task);
}


/*
 * ErrorIfUnsupportedQueryWithCitusLocalTables errors out if the given query
 * is an unsupported "citus local table" query.
 *
 * A query involving citus local table is unsupported if it is:
 *  - an UPDATE/DELETE command involving reference tables or distributed
 *    tables, or
 *  - an INSERT .. SELECT query on a citus local table which selects from
 *    reference tables or distributed tables, or
 *  - a SELECT query involving distributed tables, or
 *  - a non-simple SELECT query involving reference tables
 * or:
 *  - we are not in the coordinator, or
 *  - coordinator has no placements for citus local tables.
 */
void
ErrorIfUnsupportedQueryWithCitusLocalTables(Query *parse,
											RTEListProperties *rteListProperties)
{
	if (!rteListProperties->hasCitusLocalTable)
	{
		return;
	}

	bool hasNoDistKeyTableCoordinatorPlacements = (IsCoordinator() &&
												   CoordinatorAddedAsWorkerNode());

	if (!hasNoDistKeyTableCoordinatorPlacements)
	{
		ereport(ERROR, (errmsg("citus can plan queries involving citus local tables "
							   "only via coordinator")));
	}

	bool isModifyCommand = IsModifyCommand(parse);

	if (isModifyCommand)
	{
		/* modifying queries */

		if (!rteListProperties->hasReferenceTable &&
			!rteListProperties->hasDistributedTable)
		{
			return;
		}

		if (IsUpdateOrDelete(parse))
		{
			/*
			 * If query is an UPDATE / DELETE query involving a citus local
			 * table and a reference table or a distributed table, error out.
			 */
			ereport(ERROR, (errmsg(
								"cannot plan UPDATE/DELETE queries with citus local tables "
								"involving reference tables or distributed tables")));
		}

		bool queryModifiesCitusLocalTable = false;
		Oid resultRelationOid = ResultRelationOidForQuery(parse);

		if (IsCitusTable(resultRelationOid))
		{
			queryModifiesCitusLocalTable = (PartitionMethod(resultRelationOid) ==
											CITUS_LOCAL_TABLE);
		}

		if (CheckInsertSelectQuery(parse) && queryModifiesCitusLocalTable)
		{
			/*
			 * If query is an INSERT .. SELECT query on a citus local table
			 * selecting from a reference table or a distributed table error
			 * out here.
			 */
			ereport(ERROR, (errmsg(
								"cannot plan INSERT .. SELECT queries to citus local tables "
								"selecting from reference tables or distributed tables")));
		}
	}
	else
	{
		/* select queries */

		if (rteListProperties->hasDistributedTable)
		{
			/*
			 * We do not allow even simple select queries with distributed tables
			 * and local tables, hence should do so for citus local tables.
			 */
			ereport(ERROR, (errmsg(
								"cannot plan SELECT queries with citus local tables and "
								"distributed tables")));
		}

		bool queryIsNotSimpleSelect = FindNodeCheck((Node *) parse,
													QueryIsNotSimpleSelect);

		if (rteListProperties->hasReferenceTable && queryIsNotSimpleSelect)
		{
			/*
			 * If query is not a simple select query involving a citus local table
			 * and a reference, error out here. This is because, in that case, we
			 * will not be able to replace reference table with its local shard.
			 */
			ereport(ERROR, (errmsg(
								"cannot plan non-simple SELECT queries with citus local "
								"tables and reference tables or distributed tables")));
		}
	}
}
