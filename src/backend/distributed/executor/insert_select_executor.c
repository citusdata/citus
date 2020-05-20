/*-------------------------------------------------------------------------
 *
 * insert_select_executor.c
 *
 * Executor logic for INSERT..SELECT.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/adaptive_executor.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/intermediate_results.h"
#include "distributed/local_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_planner.h"
#include "distributed/local_executor.h"
#include "distributed/distributed_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Config variables managed via guc.c */
bool EnableRepartitionedInsertSelect = true;

/* depth of current insert/select executor. */
static int insertSelectExecutorLevel = 0;


static TupleTableSlot * CoordinatorInsertSelectExecScanInternal(CustomScanState *node);
static Query * WrapSubquery(Query *subquery);
static List * TwoPhaseInsertSelectTaskList(Oid targetRelationId, Query *insertSelectQuery,
										   char *resultIdPrefix);
static void ExecutePlanIntoRelation(Oid targetRelationId, List *insertTargetList,
									PlannedStmt *selectPlan, EState *executorState);
static HTAB * ExecutePlanIntoColocatedIntermediateResults(Oid targetRelationId,
														  List *insertTargetList,
														  PlannedStmt *selectPlan,
														  EState *executorState,
														  char *intermediateResultIdPrefix);
static List * BuildColumnNameListFromTargetList(Oid targetRelationId,
												List *insertTargetList);
static int PartitionColumnIndexFromColumnList(Oid relationId, List *columnNameList);
static List * AddInsertSelectCasts(List *insertTargetList, List *selectTargetList,
								   Oid targetRelationId);
static List * RedistributedInsertSelectTaskList(Query *insertSelectQuery,
												CitusTableCacheEntry *targetRelation,
												List **redistributedResults,
												bool useBinaryFormat);
static int PartitionColumnIndex(List *insertTargetList, Var *partitionColumn);
static Expr * CastExpr(Expr *expr, Oid sourceType, Oid targetType, Oid targetCollation,
					   int targetTypeMod);
static void WrapTaskListForProjection(List *taskList, List *projectedTargetEntries);
static void RelableTargetEntryList(List *selectTargetList, List *insertTargetList);


/*
 * CoordinatorInsertSelectExecScan is a wrapper around
 * CoordinatorInsertSelectExecScanInternal which also properly increments
 * or decrements insertSelectExecutorLevel.
 */
TupleTableSlot *
CoordinatorInsertSelectExecScan(CustomScanState *node)
{
	TupleTableSlot *result = NULL;
	insertSelectExecutorLevel++;

	PG_TRY();
	{
		result = CoordinatorInsertSelectExecScanInternal(node);
	}
	PG_CATCH();
	{
		insertSelectExecutorLevel--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	insertSelectExecutorLevel--;
	return result;
}


/*
 * CoordinatorInsertSelectExecScan executes an INSERT INTO distributed_table
 * SELECT .. query by setting up a DestReceiver that copies tuples into the
 * distributed table and then executing the SELECT query using that DestReceiver
 * as the tuple destination.
 */
static TupleTableSlot *
CoordinatorInsertSelectExecScanInternal(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (!scanState->finishedRemoteScan)
	{
		EState *executorState = ScanStateGetExecutorState(scanState);
		ParamListInfo paramListInfo = executorState->es_param_list_info;
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Query *insertSelectQuery = copyObject(distributedPlan->insertSelectQuery);
		List *insertTargetList = insertSelectQuery->targetList;
		RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
		RangeTblEntry *insertRte = ExtractResultRelationRTE(insertSelectQuery);
		Oid targetRelationId = insertRte->relid;
		char *intermediateResultIdPrefix = distributedPlan->intermediateResultIdPrefix;
		bool hasReturning = distributedPlan->expectResults;
		HTAB *shardStateHash = NULL;

		/* select query to execute */
		Query *selectQuery = BuildSelectForInsertSelect(insertSelectQuery);

		selectRte->subquery = selectQuery;
		ReorderInsertSelectTargetLists(insertSelectQuery, insertRte, selectRte);

		/*
		 * Cast types of insert target list and select projection list to
		 * match the column types of the target relation.
		 */
		selectQuery->targetList =
			AddInsertSelectCasts(insertSelectQuery->targetList,
								 selectQuery->targetList,
								 targetRelationId);

		/*
		 * Later we might need to call WrapTaskListForProjection(), which requires
		 * that select target list has unique names, otherwise the outer query
		 * cannot select columns unambiguously. So we relabel select columns to
		 * match target columns.
		 */
		RelableTargetEntryList(selectQuery->targetList, insertTargetList);

		/*
		 * Make a copy of the query, since pg_plan_query may scribble on it and we
		 * want it to be replanned every time if it is stored in a prepared
		 * statement.
		 */
		selectQuery = copyObject(selectQuery);

		/* plan the subquery, this may be another distributed query */
		int cursorOptions = CURSOR_OPT_PARALLEL_OK;
		PlannedStmt *selectPlan = pg_plan_query(selectQuery, cursorOptions,
												paramListInfo);

		/*
		 * If we are dealing with partitioned table, we also need to lock its
		 * partitions. Here we only lock targetRelation, we acquire necessary
		 * locks on selected tables during execution of those select queries.
		 */
		if (PartitionedTable(targetRelationId))
		{
			LockPartitionRelations(targetRelationId, RowExclusiveLock);
		}

		if (IsRedistributablePlan(selectPlan->planTree) &&
			IsSupportedRedistributionTarget(targetRelationId))
		{
			ereport(DEBUG1, (errmsg("performing repartitioned INSERT ... SELECT")));

			DistributedPlan *distSelectPlan =
				GetDistributedPlan((CustomScan *) selectPlan->planTree);
			Job *distSelectJob = distSelectPlan->workerJob;
			List *distSelectTaskList = distSelectJob->taskList;
			bool randomAccess = true;
			bool interTransactions = false;
			bool binaryFormat =
				CanUseBinaryCopyFormatForTargetList(selectQuery->targetList);

			ExecuteSubPlans(distSelectPlan);

			/*
			 * We have a separate directory for each transaction, so choosing
			 * the same result prefix won't cause filename conflicts. Results
			 * directory name also includes node id and database id, so we don't
			 * need to include them in the filename. We include job id here for
			 * the case "INSERT/SELECTs" are executed recursively.
			 */
			StringInfo distResultPrefixString = makeStringInfo();
			appendStringInfo(distResultPrefixString,
							 "repartitioned_results_" UINT64_FORMAT,
							 distSelectJob->jobId);
			char *distResultPrefix = distResultPrefixString->data;

			CitusTableCacheEntry *cachedTargetRelation =
				GetCitusTableCacheEntry(targetRelationId);
			CitusTableCacheEntry *targetRelation = palloc(sizeof(CitusTableCacheEntry));
			*targetRelation = *cachedTargetRelation;

#if PG_USE_ASSERT_CHECKING

			/*
			 * These fields aren't used in the code which follows,
			 * therefore in assert builds NULL these fields to
			 * segfault if they were to be used.
			 */
			targetRelation->partitionKeySTring = NULL;
			targetRelation->shardIntervalCompareFunction = NULL;
			targetRelation->hashFunction = NULL;
			targetRelation->arrayOfPlacementArrayLengths = NULL;
			targetRelation->arrayOfPlacementArrays = NULL;
			targetRelation->referencedRelationViaForeignKey = NULL;
			targetRelation->referencingRelationsViaForeignKey = NULL;
#endif
			targetRelation->partitionColumn = copyObject(
				cachedTargetRelation->partitionColumn);
			targetRelation->sortedShardIntervalArray =
				palloc(targetRelation->shardIntervalArrayLength * sizeof(ShardInterval));
			for (int shardIndex = 0; shardIndex <
				 targetRelation->shardIntervalArrayLength; shardIndex++)
			{
				targetRelation->sortedShardIntervalArray[shardIndex] =
					CopyShardInterval(
						cachedTargetRelation->sortedShardIntervalArray[shardIndex]);
			}

			int partitionColumnIndex =
				PartitionColumnIndex(insertTargetList, targetRelation->partitionColumn);
			if (partitionColumnIndex == -1)
			{
				char *relationName = get_rel_name(targetRelationId);
				Oid schemaOid = get_rel_namespace(targetRelationId);
				char *schemaName = get_namespace_name(schemaOid);

				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg(
									"the partition column of table %s should have a value",
									quote_qualified_identifier(schemaName,
															   relationName))));
			}

			TargetEntry *selectPartitionTE = list_nth(selectQuery->targetList,
													  partitionColumnIndex);
			const char *partitionColumnName = selectPartitionTE->resname ?
											  selectPartitionTE->resname : "(none)";

			ereport(DEBUG2, (errmsg(
								 "partitioning SELECT query by column index %d with name %s",
								 partitionColumnIndex, quote_literal_cstr(
									 partitionColumnName))));

			/*
			 * ExpandWorkerTargetEntry() can add additional columns to the worker
			 * query. Modify the task queries to only select columns we need.
			 */
			int requiredColumnCount = list_length(insertTargetList);
			List *jobTargetList = distSelectJob->jobQuery->targetList;
			if (list_length(jobTargetList) > requiredColumnCount)
			{
				List *projectedTargetEntries = ListTake(jobTargetList,
														requiredColumnCount);
				WrapTaskListForProjection(distSelectTaskList, projectedTargetEntries);
			}

			List **redistributedResults = RedistributeTaskListResults(distResultPrefix,
																	  distSelectTaskList,
																	  partitionColumnIndex,
																	  targetRelation,
																	  binaryFormat);

			/*
			 * At this point select query has been executed on workers and results
			 * have been fetched in such a way that they are colocated with corresponding
			 * target shard. Create and execute a list of tasks of form
			 * INSERT INTO ... SELECT * FROM read_intermediate_results(...);
			 */
			List *taskList = RedistributedInsertSelectTaskList(insertSelectQuery,
															   targetRelation,
															   redistributedResults,
															   binaryFormat);

			scanState->tuplestorestate =
				tuplestore_begin_heap(randomAccess, interTransactions, work_mem);
			TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
			uint64 rowsInserted = ExecuteTaskListIntoTupleStore(ROW_MODIFY_COMMUTATIVE,
																taskList, tupleDescriptor,
																scanState->tuplestorestate,
																hasReturning);

			executorState->es_processed = rowsInserted;
		}
		else if (insertSelectQuery->onConflict || hasReturning)
		{
			ereport(DEBUG1, (errmsg(
								 "Collecting INSERT ... SELECT results on coordinator")));

			/*
			 * If we also have a workerJob that means there is a second step
			 * to the INSERT...SELECT. This happens when there is a RETURNING
			 * or ON CONFLICT clause which is implemented as a separate
			 * distributed INSERT...SELECT from a set of intermediate results
			 * to the target relation.
			 */
			List *prunedTaskList = NIL;

			shardStateHash = ExecutePlanIntoColocatedIntermediateResults(
				targetRelationId,
				insertTargetList,
				selectPlan,
				executorState,
				intermediateResultIdPrefix);

			/* generate tasks for the INSERT..SELECT phase */
			List *taskList = TwoPhaseInsertSelectTaskList(targetRelationId,
														  insertSelectQuery,
														  intermediateResultIdPrefix);

			/*
			 * We cannot actually execute INSERT...SELECT tasks that read from
			 * intermediate results that weren't created because no rows were
			 * written to them. Prune those tasks out by only including tasks
			 * on shards with connections.
			 */
			Task *task = NULL;
			foreach_ptr(task, taskList)
			{
				uint64 shardId = task->anchorShardId;
				bool shardModified = false;

				hash_search(shardStateHash, &shardId, HASH_FIND, &shardModified);
				if (shardModified)
				{
					prunedTaskList = lappend(prunedTaskList, task);
				}
			}

			if (prunedTaskList != NIL)
			{
				bool randomAccess = true;
				bool interTransactions = false;

				Assert(scanState->tuplestorestate == NULL);
				scanState->tuplestorestate =
					tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

				TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
				ExecuteTaskListIntoTupleStore(ROW_MODIFY_COMMUTATIVE, prunedTaskList,
											  tupleDescriptor, scanState->tuplestorestate,
											  hasReturning);

				if (SortReturning && hasReturning)
				{
					SortTupleStore(scanState);
				}
			}
		}
		else
		{
			ereport(DEBUG1, (errmsg(
								 "Collecting INSERT ... SELECT results on coordinator")));

			ExecutePlanIntoRelation(targetRelationId, insertTargetList, selectPlan,
									executorState);
		}

		scanState->finishedRemoteScan = true;
	}

	TupleTableSlot *resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * BuildSelectForInsertSelect extracts the SELECT part from an INSERT...SELECT query.
 * If the INSERT...SELECT has CTEs then these are added to the resulting SELECT instead.
 */
Query *
BuildSelectForInsertSelect(Query *insertSelectQuery)
{
	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
	Query *selectQuery = selectRte->subquery;

	/*
	 * Wrap the SELECT as a subquery if the INSERT...SELECT has CTEs or the SELECT
	 * has top-level set operations.
	 *
	 * We could simply wrap all queries, but that might create a subquery that is
	 * not supported by the logical planner. Since the logical planner also does
	 * not support CTEs and top-level set operations, we can wrap queries containing
	 * those without breaking anything.
	 */
	if (list_length(insertSelectQuery->cteList) > 0)
	{
		selectQuery = WrapSubquery(selectRte->subquery);

		/* copy CTEs from the INSERT ... SELECT statement into outer SELECT */
		selectQuery->cteList = copyObject(insertSelectQuery->cteList);
		selectQuery->hasModifyingCTE = insertSelectQuery->hasModifyingCTE;
	}
	else if (selectQuery->setOperations != NULL)
	{
		/* top-level set operations confuse the ReorderInsertSelectTargetLists logic */
		selectQuery = WrapSubquery(selectRte->subquery);
	}

	return selectQuery;
}


/*
 * WrapSubquery wraps the given query as a subquery in a newly constructed
 * "SELECT * FROM (...subquery...) citus_insert_select_subquery" query.
 */
static Query *
WrapSubquery(Query *subquery)
{
	ParseState *pstate = make_parsestate(NULL);
	List *newTargetList = NIL;

	Query *outerQuery = makeNode(Query);
	outerQuery->commandType = CMD_SELECT;

	/* create range table entries */
	Alias *selectAlias = makeAlias("citus_insert_select_subquery", NIL);
	RangeTblEntry *newRangeTableEntry = addRangeTableEntryForSubquery(pstate, subquery,
																	  selectAlias, false,
																	  true);
	outerQuery->rtable = list_make1(newRangeTableEntry);

	/* set the FROM expression to the subquery */
	RangeTblRef *newRangeTableRef = makeNode(RangeTblRef);
	newRangeTableRef->rtindex = 1;
	outerQuery->jointree = makeFromExpr(list_make1(newRangeTableRef), NULL);

	/* create a target list that matches the SELECT */
	TargetEntry *selectTargetEntry = NULL;
	foreach_ptr(selectTargetEntry, subquery->targetList)
	{
		/* exactly 1 entry in FROM */
		int indexInRangeTable = 1;

		if (selectTargetEntry->resjunk)
		{
			continue;
		}

		Var *newSelectVar = makeVar(indexInRangeTable, selectTargetEntry->resno,
									exprType((Node *) selectTargetEntry->expr),
									exprTypmod((Node *) selectTargetEntry->expr),
									exprCollation((Node *) selectTargetEntry->expr), 0);

		TargetEntry *newSelectTargetEntry = makeTargetEntry((Expr *) newSelectVar,
															selectTargetEntry->resno,
															selectTargetEntry->resname,
															selectTargetEntry->resjunk);

		newTargetList = lappend(newTargetList, newSelectTargetEntry);
	}

	outerQuery->targetList = newTargetList;

	return outerQuery;
}


/*
 * TwoPhaseInsertSelectTaskList generates a list of tasks for a query that
 * inserts into a target relation and selects from a set of co-located
 * intermediate results.
 */
static List *
TwoPhaseInsertSelectTaskList(Oid targetRelationId, Query *insertSelectQuery,
							 char *resultIdPrefix)
{
	List *taskList = NIL;

	/*
	 * Make a copy of the INSERT ... SELECT. We'll repeatedly replace the
	 * subquery of insertResultQuery for different intermediate results and
	 * then deparse it.
	 */
	Query *insertResultQuery = copyObject(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractResultRelationRTE(insertResultQuery);
	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertResultQuery);

	CitusTableCacheEntry *targetCacheEntry = GetCitusTableCacheEntry(targetRelationId);
	int shardCount = targetCacheEntry->shardIntervalArrayLength;
	uint32 taskIdIndex = 1;
	uint64 jobId = INVALID_JOB_ID;

	for (int shardOffset = 0; shardOffset < shardCount; shardOffset++)
	{
		ShardInterval *targetShardInterval =
			targetCacheEntry->sortedShardIntervalArray[shardOffset];
		uint64 shardId = targetShardInterval->shardId;
		List *columnAliasList = NIL;
		StringInfo queryString = makeStringInfo();
		StringInfo resultId = makeStringInfo();

		/* during COPY, the shard ID is appended to the result name */
		appendStringInfo(resultId, "%s_" UINT64_FORMAT, resultIdPrefix, shardId);

		/* generate the query on the intermediate result */
		Query *resultSelectQuery = BuildSubPlanResultQuery(insertSelectQuery->targetList,
														   columnAliasList,
														   resultId->data);

		/* put the intermediate result query in the INSERT..SELECT */
		selectRte->subquery = resultSelectQuery;

		/* setting an alias simplifies deparsing of RETURNING */
		if (insertRte->alias == NULL)
		{
			Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
			insertRte->alias = alias;
		}

		/*
		 * Generate a query string for the query that inserts into a shard and reads
		 * from an intermediate result.
		 *
		 * Since CTEs have already been converted to intermediate results, they need
		 * to removed from the query. Otherwise, worker queries include both
		 * intermediate results and CTEs in the query.
		 */
		insertResultQuery->cteList = NIL;
		deparse_shard_query(insertResultQuery, targetRelationId, shardId, queryString);
		ereport(DEBUG2, (errmsg("distributed statement: %s", queryString->data)));

		LockShardDistributionMetadata(shardId, ShareLock);
		List *insertShardPlacementList = ActiveShardPlacementList(shardId);

		RelationShard *relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = targetShardInterval->relationId;
		relationShard->shardId = targetShardInterval->shardId;

		Task *modifyTask = CreateBasicTask(jobId, taskIdIndex, MODIFY_TASK,
										   queryString->data);
		modifyTask->dependentTaskList = NIL;
		modifyTask->anchorShardId = shardId;
		modifyTask->taskPlacementList = insertShardPlacementList;
		modifyTask->relationShardList = list_make1(relationShard);
		modifyTask->replicationModel = targetCacheEntry->replicationModel;

		taskList = lappend(taskList, modifyTask);

		taskIdIndex++;
	}

	return taskList;
}


/*
 * ExecutePlanIntoColocatedIntermediateResults executes the given PlannedStmt
 * and inserts tuples into a set of intermediate results that are colocated with
 * the target table for further processing of ON CONFLICT or RETURNING. It also
 * returns the hash of shard states that were used to insert tuplesinto the target
 * relation.
 */
static HTAB *
ExecutePlanIntoColocatedIntermediateResults(Oid targetRelationId,
											List *insertTargetList,
											PlannedStmt *selectPlan,
											EState *executorState,
											char *intermediateResultIdPrefix)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	bool stopOnFailure = false;

	char partitionMethod = PartitionMethod(targetRelationId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		stopOnFailure = true;
	}

	/* Get column name list and partition column index for the target table */
	List *columnNameList = BuildColumnNameListFromTargetList(targetRelationId,
															 insertTargetList);
	int partitionColumnIndex = PartitionColumnIndexFromColumnList(targetRelationId,
																  columnNameList);

	/* set up a DestReceiver that copies into the intermediate table */
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState,
																  stopOnFailure,
																  intermediateResultIdPrefix);

	ExecutePlanIntoDestReceiver(selectPlan, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;

	XactModificationLevel = XACT_MODIFICATION_DATA;

	return copyDest->shardStateHash;
}


/*
 * ExecutePlanIntoRelation executes the given plan and inserts the
 * results into the target relation, which is assumed to be a distributed
 * table.
 */
static void
ExecutePlanIntoRelation(Oid targetRelationId, List *insertTargetList,
						PlannedStmt *selectPlan, EState *executorState)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	bool stopOnFailure = false;

	char partitionMethod = PartitionMethod(targetRelationId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		stopOnFailure = true;
	}

	/* Get column name list and partition column index for the target table */
	List *columnNameList = BuildColumnNameListFromTargetList(targetRelationId,
															 insertTargetList);
	int partitionColumnIndex = PartitionColumnIndexFromColumnList(targetRelationId,
																  columnNameList);

	/* set up a DestReceiver that copies into the distributed table */
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState,
																  stopOnFailure, NULL);

	ExecutePlanIntoDestReceiver(selectPlan, paramListInfo, (DestReceiver *) copyDest);

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
	List *columnNameList = NIL;

	/* build the list of column names for the COPY statement */
	TargetEntry *insertTargetEntry = NULL;
	foreach_ptr(insertTargetEntry, insertTargetList)
	{
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
	Var *partitionColumn = PartitionColumn(relationId, 0);
	int partitionColumnIndex = 0;

	const char *columnName = NULL;
	foreach_ptr(columnName, columnNameList)
	{
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


/* ExecutingInsertSelect returns true if we are executing an INSERT ...SELECT query */
bool
ExecutingInsertSelect(void)
{
	return insertSelectExecutorLevel > 0;
}


/*
 * AddInsertSelectCasts makes sure that the types in columns in the given
 * target lists have the same type as the columns of the given relation.
 * It might add casts to ensure that.
 *
 * It returns the updated selectTargetList.
 */
static List *
AddInsertSelectCasts(List *insertTargetList, List *selectTargetList,
					 Oid targetRelationId)
{
	ListCell *insertEntryCell = NULL;
	ListCell *selectEntryCell = NULL;
	List *projectedEntries = NIL;
	List *nonProjectedEntries = NIL;

	/*
	 * ReorderInsertSelectTargetLists() makes sure that first few columns of
	 * the SELECT query match the insert targets. It might contain additional
	 * items for GROUP BY, etc.
	 */
	Assert(list_length(insertTargetList) <= list_length(selectTargetList));

	Relation distributedRelation = heap_open(targetRelationId, RowExclusiveLock);
	TupleDesc destTupleDescriptor = RelationGetDescr(distributedRelation);

	int targetEntryIndex = 0;
	forboth(insertEntryCell, insertTargetList, selectEntryCell, selectTargetList)
	{
		TargetEntry *insertEntry = (TargetEntry *) lfirst(insertEntryCell);
		TargetEntry *selectEntry = (TargetEntry *) lfirst(selectEntryCell);
		Var *insertColumn = (Var *) insertEntry->expr;
		Form_pg_attribute attr = TupleDescAttr(destTupleDescriptor,
											   insertEntry->resno - 1);

		Oid sourceType = insertColumn->vartype;
		Oid targetType = attr->atttypid;
		if (sourceType != targetType)
		{
			insertEntry->expr = CastExpr((Expr *) insertColumn, sourceType, targetType,
										 attr->attcollation, attr->atttypmod);

			/*
			 * We cannot modify the selectEntry in-place, because ORDER BY or
			 * GROUP BY clauses might be pointing to it with comparison types
			 * of the source type. So instead we keep the original one as a
			 * non-projected entry, so GROUP BY and ORDER BY are happy, and
			 * create a duplicated projected entry with the coerced expression.
			 */
			TargetEntry *coercedEntry = copyObject(selectEntry);
			coercedEntry->expr = CastExpr((Expr *) selectEntry->expr, sourceType,
										  targetType, attr->attcollation,
										  attr->atttypmod);
			coercedEntry->ressortgroupref = 0;

			/*
			 * The only requirement is that users don't use this name in ORDER BY
			 * or GROUP BY, and it should be unique across the same query.
			 */
			StringInfo resnameString = makeStringInfo();
			appendStringInfo(resnameString, "auto_coerced_by_citus_%d", targetEntryIndex);
			coercedEntry->resname = resnameString->data;

			projectedEntries = lappend(projectedEntries, coercedEntry);

			if (selectEntry->ressortgroupref != 0)
			{
				selectEntry->resjunk = true;

				/*
				 * This entry might still end up in the SELECT output list, so
				 * rename it to avoid ambiguity.
				 *
				 * See https://github.com/citusdata/citus/pull/3470.
				 */
				resnameString = makeStringInfo();
				appendStringInfo(resnameString, "discarded_target_item_%d",
								 targetEntryIndex);
				selectEntry->resname = resnameString->data;

				nonProjectedEntries = lappend(nonProjectedEntries, selectEntry);
			}
		}
		else
		{
			projectedEntries = lappend(projectedEntries, selectEntry);
		}

		targetEntryIndex++;
	}

	for (int entryIndex = list_length(insertTargetList);
		 entryIndex < list_length(selectTargetList);
		 entryIndex++)
	{
		nonProjectedEntries = lappend(nonProjectedEntries, list_nth(selectTargetList,
																	entryIndex));
	}

	/* selectEntry->resno must be the ordinal number of the entry */
	selectTargetList = list_concat(projectedEntries, nonProjectedEntries);
	int entryResNo = 1;
	TargetEntry *selectTargetEntry = NULL;
	foreach_ptr(selectTargetEntry, selectTargetList)
	{
		selectTargetEntry->resno = entryResNo++;
	}

	heap_close(distributedRelation, NoLock);

	return selectTargetList;
}


/*
 * CastExpr returns an expression which casts the given expr from sourceType to
 * the given targetType.
 */
static Expr *
CastExpr(Expr *expr, Oid sourceType, Oid targetType, Oid targetCollation,
		 int targetTypeMod)
{
	Oid coercionFuncId = InvalidOid;
	CoercionPathType coercionType = find_coercion_pathway(targetType, sourceType,
														  COERCION_EXPLICIT,
														  &coercionFuncId);

	if (coercionType == COERCION_PATH_FUNC)
	{
		FuncExpr *coerceExpr = makeNode(FuncExpr);
		coerceExpr->funcid = coercionFuncId;
		coerceExpr->args = list_make1(copyObject(expr));
		coerceExpr->funccollid = targetCollation;
		coerceExpr->funcresulttype = targetType;

		return (Expr *) coerceExpr;
	}
	else if (coercionType == COERCION_PATH_RELABELTYPE)
	{
		RelabelType *coerceExpr = makeNode(RelabelType);
		coerceExpr->arg = copyObject(expr);
		coerceExpr->resulttype = targetType;
		coerceExpr->resulttypmod = targetTypeMod;
		coerceExpr->resultcollid = targetCollation;
		coerceExpr->relabelformat = COERCE_IMPLICIT_CAST;
		coerceExpr->location = -1;

		return (Expr *) coerceExpr;
	}
	else if (coercionType == COERCION_PATH_ARRAYCOERCE)
	{
		Oid sourceBaseType = get_base_element_type(sourceType);
		Oid targetBaseType = get_base_element_type(targetType);

		CaseTestExpr *elemExpr = makeNode(CaseTestExpr);
		elemExpr->collation = targetCollation;
		elemExpr->typeId = sourceBaseType;
		elemExpr->typeMod = -1;

		Expr *elemCastExpr = CastExpr((Expr *) elemExpr, sourceBaseType,
									  targetBaseType, targetCollation,
									  targetTypeMod);

		ArrayCoerceExpr *coerceExpr = makeNode(ArrayCoerceExpr);
		coerceExpr->arg = copyObject(expr);
		coerceExpr->elemexpr = elemCastExpr;
		coerceExpr->resultcollid = targetCollation;
		coerceExpr->resulttype = targetType;
		coerceExpr->resulttypmod = targetTypeMod;
		coerceExpr->location = -1;
		coerceExpr->coerceformat = COERCE_IMPLICIT_CAST;

		return (Expr *) coerceExpr;
	}
	else if (coercionType == COERCION_PATH_COERCEVIAIO)
	{
		CoerceViaIO *coerceExpr = makeNode(CoerceViaIO);
		coerceExpr->arg = (Expr *) copyObject(expr);
		coerceExpr->resulttype = targetType;
		coerceExpr->resultcollid = targetCollation;
		coerceExpr->coerceformat = COERCE_IMPLICIT_CAST;
		coerceExpr->location = -1;

		return (Expr *) coerceExpr;
	}
	else
	{
		ereport(ERROR, (errmsg("could not find a conversion path from type %d to %d",
							   sourceType, targetType)));
	}
}


/*
 * IsSupportedRedistributionTarget determines whether re-partitioning into the
 * given target relation is supported.
 */
bool
IsSupportedRedistributionTarget(Oid targetRelationId)
{
	CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(targetRelationId);

	/* only range and hash-distributed tables are currently supported */
	if (tableEntry->partitionMethod != DISTRIBUTE_BY_HASH &&
		tableEntry->partitionMethod != DISTRIBUTE_BY_RANGE)
	{
		return false;
	}

	return true;
}


/*
 * RedistributedInsertSelectTaskList returns a task list to insert given
 * redistributedResults into the given target relation.
 * redistributedResults[shardIndex] is list of cstrings each of which is
 * a result name which should be inserted into
 * targetRelation->sortedShardIntervalArray[shardIndex].
 */
static List *
RedistributedInsertSelectTaskList(Query *insertSelectQuery,
								  CitusTableCacheEntry *targetRelation,
								  List **redistributedResults,
								  bool useBinaryFormat)
{
	List *taskList = NIL;

	/*
	 * Make a copy of the INSERT ... SELECT. We'll repeatedly replace the
	 * subquery of insertResultQuery for different intermediate results and
	 * then deparse it.
	 */
	Query *insertResultQuery = copyObject(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractResultRelationRTE(insertResultQuery);
	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertResultQuery);
	List *selectTargetList = selectRte->subquery->targetList;
	Oid targetRelationId = targetRelation->relationId;

	int shardCount = targetRelation->shardIntervalArrayLength;
	int shardOffset = 0;
	uint32 taskIdIndex = 1;
	uint64 jobId = INVALID_JOB_ID;

	for (shardOffset = 0; shardOffset < shardCount; shardOffset++)
	{
		ShardInterval *targetShardInterval =
			targetRelation->sortedShardIntervalArray[shardOffset];
		List *resultIdList = redistributedResults[targetShardInterval->shardIndex];
		uint64 shardId = targetShardInterval->shardId;
		StringInfo queryString = makeStringInfo();

		/* skip empty tasks */
		if (resultIdList == NIL)
		{
			continue;
		}

		/* sort result ids for consistent test output */
		List *sortedResultIds = SortList(resultIdList, pg_qsort_strcmp);

		/* generate the query on the intermediate result */
		Query *fragmentSetQuery = BuildReadIntermediateResultsArrayQuery(selectTargetList,
																		 NIL,
																		 sortedResultIds,
																		 useBinaryFormat);

		/* put the intermediate result query in the INSERT..SELECT */
		selectRte->subquery = fragmentSetQuery;

		/* setting an alias simplifies deparsing of RETURNING */
		if (insertRte->alias == NULL)
		{
			Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
			insertRte->alias = alias;
		}

		/*
		 * Generate a query string for the query that inserts into a shard and reads
		 * from an intermediate result.
		 *
		 * Since CTEs have already been converted to intermediate results, they need
		 * to removed from the query. Otherwise, worker queries include both
		 * intermediate results and CTEs in the query.
		 */
		insertResultQuery->cteList = NIL;
		deparse_shard_query(insertResultQuery, targetRelationId, shardId, queryString);
		ereport(DEBUG2, (errmsg("distributed statement: %s", queryString->data)));

		LockShardDistributionMetadata(shardId, ShareLock);
		List *insertShardPlacementList = ActiveShardPlacementList(shardId);

		RelationShard *relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = targetShardInterval->relationId;
		relationShard->shardId = targetShardInterval->shardId;

		Task *modifyTask = CreateBasicTask(jobId, taskIdIndex, MODIFY_TASK,
										   queryString->data);
		modifyTask->dependentTaskList = NIL;
		modifyTask->anchorShardId = shardId;
		modifyTask->taskPlacementList = insertShardPlacementList;
		modifyTask->relationShardList = list_make1(relationShard);
		modifyTask->replicationModel = targetRelation->replicationModel;

		taskList = lappend(taskList, modifyTask);

		taskIdIndex++;
	}

	return taskList;
}


/*
 * PartitionColumnIndex finds the index of given partition column in the
 * given target list.
 */
static int
PartitionColumnIndex(List *insertTargetList, Var *partitionColumn)
{
	TargetEntry *insertTargetEntry = NULL;
	int targetEntryIndex = 0;
	foreach_ptr(insertTargetEntry, insertTargetList)
	{
		if (insertTargetEntry->resno == partitionColumn->varattno)
		{
			return targetEntryIndex;
		}

		targetEntryIndex++;
	}

	return -1;
}


/*
 * IsRedistributablePlan returns true if the given plan is a redistrituable plan.
 */
bool
IsRedistributablePlan(Plan *selectPlan)
{
	if (!EnableRepartitionedInsertSelect)
	{
		return false;
	}

	/* don't redistribute if query is not distributed or requires merge on coordinator */
	if (!IsCitusCustomScan(selectPlan))
	{
		return false;
	}

	DistributedPlan *distSelectPlan =
		GetDistributedPlan((CustomScan *) selectPlan);
	Job *distSelectJob = distSelectPlan->workerJob;
	List *distSelectTaskList = distSelectJob->taskList;

	/*
	 * Don't use redistribution if only one task. This is to keep the existing
	 * behaviour for CTEs that the last step is a read_intermediate_result()
	 * call. It doesn't hurt much in other cases too.
	 */
	if (list_length(distSelectTaskList) <= 1)
	{
		return false;
	}

	/* don't use redistribution for repartition joins for now */
	if (distSelectJob->dependentJobList != NIL)
	{
		return false;
	}

	if (distSelectPlan->masterQuery != NULL)
	{
		Query *masterQuery = (Query *) distSelectPlan->masterQuery;

		if (contain_nextval_expression_walker((Node *) masterQuery->targetList, NULL))
		{
			/* nextval needs to be evaluated on the coordinator */
			return false;
		}
	}

	return true;
}


/*
 * WrapTaskListForProjection wraps task query string to only select given
 * projected columns. It modifies the taskList.
 */
static void
WrapTaskListForProjection(List *taskList, List *projectedTargetEntries)
{
	StringInfo projectedColumnsString = makeStringInfo();
	int entryIndex = 0;
	TargetEntry *targetEntry = NULL;
	foreach_ptr(targetEntry, projectedTargetEntries)
	{
		if (entryIndex != 0)
		{
			appendStringInfoChar(projectedColumnsString, ',');
		}

		char *columnName = targetEntry->resname;
		Assert(columnName != NULL);
		appendStringInfoString(projectedColumnsString, quote_identifier(columnName));

		entryIndex++;
	}

	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		StringInfo wrappedQuery = makeStringInfo();
		appendStringInfo(wrappedQuery, "SELECT %s FROM (%s) subquery",
						 projectedColumnsString->data,
						 TaskQueryStringForAllPlacements(task));
		SetTaskQueryString(task, wrappedQuery->data);
	}
}


/*
 * RelableTargetEntryList relabels select target list to have matching names with
 * insert target list.
 */
static void
RelableTargetEntryList(List *selectTargetList, List *insertTargetList)
{
	ListCell *selectTargetCell = NULL;
	ListCell *insertTargetCell = NULL;

	forboth(selectTargetCell, selectTargetList, insertTargetCell, insertTargetList)
	{
		TargetEntry *selectTargetEntry = lfirst(selectTargetCell);
		TargetEntry *insertTargetEntry = lfirst(insertTargetCell);

		selectTargetEntry->resname = insertTargetEntry->resname;
	}
}
