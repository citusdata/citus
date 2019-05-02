/*-------------------------------------------------------------------------
 *
 * insert_select_executor.c
 *
 * Executor logic for INSERT..SELECT.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands/multi_copy.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/distributed_planner.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"


static char * GetForceMaxQueryParallelization(void);
static void SetLocalMaxQueryParallelization(char *value);
static void ExecuteSelectIntoRelation(Oid targetRelationId, List *insertTargetList,
									  Query *selectQuery, EState *executorState);
static HTAB * ExecuteSelectIntoColocatedIntermediateResults(Oid targetRelationId,
															List *insertTargetList,
															Query *selectQuery,
															EState *executorState,
															char *
															intermediateResultIdPrefix);
static List * BuildColumnNameListFromTargetList(Oid targetRelationId,
												List *insertTargetList);
static int PartitionColumnIndexFromColumnList(Oid relationId, List *columnNameList);


/*
 * CoordinatorInsertSelectExecScan executes an INSERT INTO distributed_table
 * SELECT .. query by setting up a DestReceiver that copies tuples into the
 * distributed table and then executing the SELECT query using that DestReceiver
 * as the tuple destination.
 */
TupleTableSlot *
CoordinatorInsertSelectExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		EState *executorState = scanState->customScanState.ss.ps.state;
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Query *selectQuery = distributedPlan->insertSelectSubquery;
		List *insertTargetList = distributedPlan->insertTargetList;
		Oid targetRelationId = distributedPlan->targetRelationId;
		char *intermediateResultIdPrefix = distributedPlan->intermediateResultIdPrefix;
		HTAB *shardConnectionsHash = NULL;
		char *valueOfForceMaxQueryParallelization = NULL;

		ereport(DEBUG1, (errmsg("Collecting INSERT ... SELECT results on coordinator")));

		/*
		 * Modifying CTEs before INSERT .. SELECT have to be executed using one connection
		 * per placement. Otherwise, the COPY into the relation would fail. Thus, we enforce
		 * that.
		 *
		 * Save the value of the GUC before updating its value so that we can re-set
		 * after the execution.
		 */
		if (selectQuery->hasModifyingCTE)
		{
			valueOfForceMaxQueryParallelization = GetForceMaxQueryParallelization();

			SetLocalMaxQueryParallelization("on");
		}

		/*
		 * If we are dealing with partitioned table, we also need to lock its
		 * partitions. Here we only lock targetRelation, we acquire necessary
		 * locks on selected tables during execution of those select queries.
		 */
		if (PartitionedTable(targetRelationId))
		{
			LockPartitionRelations(targetRelationId, RowExclusiveLock);
		}

		if (distributedPlan->workerJob != NULL)
		{
			/*
			 * If we also have a workerJob that means there is a second step
			 * to the INSERT...SELECT. This happens when there is a RETURNING
			 * or ON CONFLICT clause which is implemented as a separate
			 * distributed INSERT...SELECT from a set of intermediate results
			 * to the target relation.
			 */
			Job *workerJob = distributedPlan->workerJob;
			ListCell *taskCell = NULL;
			List *taskList = workerJob->taskList;
			List *prunedTaskList = NIL;
			bool hasReturning = distributedPlan->hasReturning;
			bool isModificationQuery = true;

			shardConnectionsHash = ExecuteSelectIntoColocatedIntermediateResults(
				targetRelationId,
				insertTargetList,
				selectQuery,
				executorState,
				intermediateResultIdPrefix);

			/*
			 * We cannot actually execute INSERT...SELECT tasks that read from
			 * intermediate results that weren't created because no rows were
			 * written to them. Prune those tasks out by only including tasks
			 * on shards with connections.
			 */
			foreach(taskCell, taskList)
			{
				Task *task = (Task *) lfirst(taskCell);
				uint64 shardId = task->anchorShardId;
				bool shardModified = false;

				hash_search(shardConnectionsHash, &shardId, HASH_FIND, &shardModified);
				if (shardModified)
				{
					prunedTaskList = lappend(prunedTaskList, task);
				}
			}

			if (prunedTaskList != NIL)
			{
				ExecuteMultipleTasks(scanState, prunedTaskList, isModificationQuery,
									 hasReturning);
			}
		}
		else
		{
			ExecuteSelectIntoRelation(targetRelationId, insertTargetList, selectQuery,
									  executorState);
		}

		/* update the value to the previous value if we've updated it */
		if (valueOfForceMaxQueryParallelization != NULL)
		{
			SetLocalMaxQueryParallelization(valueOfForceMaxQueryParallelization);
		}

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * SetLocalMaxQueryParallelization sets citus.force_max_query_parallelization
 * to the given value.
 */
static void
SetLocalMaxQueryParallelization(char *value)
{
	set_config_option("citus.force_max_query_parallelization", value,
					  (PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_SET, true, 0, false);
}


/*
 * GetForceMaxQueryParallelization returns the current value of
 * citus.force_max_query_parallelization.
 */
static char *
GetForceMaxQueryParallelization()
{
	const char *gucStrValue =
		GetConfigOption("citus.force_max_query_parallelization", true, false);

	if (gucStrValue == NULL)
	{
		return pstrdup("off");
	}

	return pstrdup(gucStrValue);
}



/*
 * ExecuteSelectIntoColocatedIntermediateResults executes the given select query
 * and inserts tuples into a set of intermediate results that are colocated with
 * the target table for further processing of ON CONFLICT or RETURNING. It also
 * returns the hash of connections that were used to insert tuplesinto the target
 * relation.
 */
static HTAB *
ExecuteSelectIntoColocatedIntermediateResults(Oid targetRelationId,
											  List *insertTargetList,
											  Query *selectQuery, EState *executorState,
											  char *intermediateResultIdPrefix)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	int partitionColumnIndex = -1;
	List *columnNameList = NIL;
	bool stopOnFailure = false;
	char partitionMethod = 0;
	CitusCopyDestReceiver *copyDest = NULL;
	Query *queryCopy = NULL;

	partitionMethod = PartitionMethod(targetRelationId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		stopOnFailure = true;
	}

	/* Get column name list and partition column index for the target table */
	columnNameList = BuildColumnNameListFromTargetList(targetRelationId,
													   insertTargetList);
	partitionColumnIndex = PartitionColumnIndexFromColumnList(targetRelationId,
															  columnNameList);

	/* set up a DestReceiver that copies into the intermediate table */
	copyDest = CreateCitusCopyDestReceiver(targetRelationId, columnNameList,
										   partitionColumnIndex, executorState,
										   stopOnFailure, intermediateResultIdPrefix);

	/*
	 * Make a copy of the query, since ExecuteQueryIntoDestReceiver may scribble on it
	 * and we want it to be replanned every time if it is stored in a prepared
	 * statement.
	 */
	queryCopy = copyObject(selectQuery);

	ExecuteQueryIntoDestReceiver(queryCopy, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;

	XactModificationLevel = XACT_MODIFICATION_DATA;

	return copyDest->shardConnectionHash;
}


/*
 * ExecuteSelectIntoRelation executes given SELECT query and inserts the
 * results into the target relation, which is assumed to be a distributed
 * table.
 */
static void
ExecuteSelectIntoRelation(Oid targetRelationId, List *insertTargetList,
						  Query *selectQuery, EState *executorState)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	int partitionColumnIndex = -1;
	List *columnNameList = NIL;
	bool stopOnFailure = false;
	char partitionMethod = 0;
	CitusCopyDestReceiver *copyDest = NULL;
	Query *queryCopy = NULL;

	partitionMethod = PartitionMethod(targetRelationId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		stopOnFailure = true;
	}

	/* Get column name list and partition column index for the target table */
	columnNameList = BuildColumnNameListFromTargetList(targetRelationId,
													   insertTargetList);
	partitionColumnIndex = PartitionColumnIndexFromColumnList(targetRelationId,
															  columnNameList);

	/* set up a DestReceiver that copies into the distributed table */
	copyDest = CreateCitusCopyDestReceiver(targetRelationId, columnNameList,
										   partitionColumnIndex, executorState,
										   stopOnFailure, NULL);

	/*
	 * Make a copy of the query, since ExecuteQueryIntoDestReceiver may scribble on it
	 * and we want it to be replanned every time if it is stored in a prepared
	 * statement.
	 */
	queryCopy = copyObject(selectQuery);

	ExecuteQueryIntoDestReceiver(queryCopy, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * BuildColumnNameListForCopyStatement build the column name list given the insert
 * target list.
 */
static List *
BuildColumnNameListFromTargetList(Oid targetRelationId, List *insertTargetList)
{
	ListCell *insertTargetCell = NULL;
	List *columnNameList = NIL;

	/* build the list of column names for the COPY statement */
	foreach(insertTargetCell, insertTargetList)
	{
		TargetEntry *insertTargetEntry = (TargetEntry *) lfirst(insertTargetCell);

		columnNameList = lappend(columnNameList, insertTargetEntry->resname);
	}

	return columnNameList;
}


/*
 * PartitionColumnIndexFromColumnList returns the index of partition column from given
 * column name list and relation ID. If given list doesn't contain the partition
 * column, it returns -1.
 */
static int
PartitionColumnIndexFromColumnList(Oid relationId, List *columnNameList)
{
	ListCell *columnNameCell = NULL;
	Var *partitionColumn = PartitionColumn(relationId, 0);
	int partitionColumnIndex = 0;

	foreach(columnNameCell, columnNameList)
	{
		char *columnName = (char *) lfirst(columnNameCell);

		AttrNumber attrNumber = get_attnum(relationId, columnName);

		/* check whether this is the partition column */
		if (partitionColumn != NULL && attrNumber == partitionColumn->varattno)
		{
			return partitionColumnIndex;
		}

		partitionColumnIndex++;
	}

	return -1;
}
