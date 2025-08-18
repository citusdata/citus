/*-------------------------------------------------------------------------
 *
 * deparse_shard_query.c
 *
 * This file contains functions for deparsing shard queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parsetree.h"
#include "storage/lock.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/combine_query_planner.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/query_utils.h"
#include "distributed/recursive_planning.h"
#include "distributed/shard_utils.h"
#include "distributed/stats/stat_tenants.h"
#include "distributed/version_compat.h"


static void UpdateTaskQueryString(Query *query, Task *task);
static RelationShard * FindRelationShard(Oid inputRelationId, List *relationShardList);
static void ConvertRteToSubqueryWithEmptyResult(RangeTblEntry *rte);
static bool ShouldLazyDeparseQuery(Task *task);
static char * DeparseTaskQuery(Task *task, Query *query);


/*
 * RebuildQueryStrings deparses the job query for each task to
 * include execution-time changes such as function evaluation.
 */
void
RebuildQueryStrings(Job *workerJob)
{
	Query *originalQuery = workerJob->jobQuery;
	List *taskList = workerJob->taskList;
	Task *task = NULL;
	bool isSingleTask = list_length(taskList) == 1;

	if (originalQuery->commandType == CMD_INSERT)
	{
		AddInsertAliasIfNeeded(originalQuery);
	}

	foreach_declared_ptr(task, taskList)
	{
		Query *query = originalQuery;

		/*
		 * Copy the query if there are multiple tasks. If there is a single
		 * task, we scribble on the original query to avoid the copying
		 * overhead.
		 */
		if (!isSingleTask)
		{
			query = copyObject(originalQuery);
		}

		if (UpdateOrDeleteOrMergeQuery(query))
		{
			List *relationShardList = task->relationShardList;

			/*
			 * For UPDATE and DELETE queries, we may have subqueries and joins, so
			 * we use relation shard list to update shard names and call
			 * pg_get_query_def() directly.
			 */
			UpdateRelationToShardNames((Node *) query, relationShardList);
		}
		else if (query->commandType == CMD_INSERT && task->modifyWithSubquery)
		{
			/* for INSERT..SELECT, adjust shard names in SELECT part */
			List *relationShardList = task->relationShardList;
			ShardInterval *shardInterval = LoadShardInterval(task->anchorShardId);

			RangeTblEntry *copiedInsertRte = ExtractResultRelationRTEOrError(query);
			RangeTblEntry *copiedSubqueryRte = ExtractSelectRangeTableEntry(query);
			Query *copiedSubquery = copiedSubqueryRte->subquery;

			/* there are no restrictions to add for reference and citus local tables */
			if (IsCitusTableType(shardInterval->relationId, DISTRIBUTED_TABLE))
			{
				AddPartitionKeyNotNullFilterToSelect(copiedSubquery);
			}

			ReorderInsertSelectTargetLists(query, copiedInsertRte, copiedSubqueryRte);

			UpdateRelationToShardNames((Node *) copiedSubquery, relationShardList);
		}

		if (query->commandType == CMD_INSERT)
		{
			RangeTblEntry *modifiedRelationRTE = linitial(originalQuery->rtable);

			/*
			 * We store the modified relaiton ID in the task so we can lazily call
			 * deparse_shard_query when the string is needed
			 */
			task->anchorDistributedTableId = modifiedRelationRTE->relid;

			/*
			 * For multi-row inserts, we modify the VALUES before storing the
			 * query in the task.
			 */
			RangeTblEntry *valuesRTE = ExtractDistributedInsertValuesRTE(query);
			if (valuesRTE != NULL)
			{
				Assert(valuesRTE->rtekind == RTE_VALUES);
				Assert(task->rowValuesLists != NULL);

				valuesRTE->values_lists = task->rowValuesLists;
			}
		}

		bool isQueryObjectOrText = GetTaskQueryType(task) == TASK_QUERY_TEXT ||
								   GetTaskQueryType(task) == TASK_QUERY_OBJECT;
		ereport(DEBUG4, (errmsg("query before rebuilding: %s",
								!isQueryObjectOrText
								? "(null)"
								: TaskQueryString(task))));

		task->partitionKeyValue = workerJob->partitionKeyValue;
		SetJobColocationId(workerJob);
		task->colocationId = workerJob->colocationId;

		UpdateTaskQueryString(query, task);

		/*
		 * If parameters were resolved in the job query, then they are now also
		 * resolved in the query string.
		 */
		task->parametersInQueryStringResolved = workerJob->parametersInJobQueryResolved;

		ereport(DEBUG4, (errmsg("query after rebuilding:  %s",
								TaskQueryString(task))));
	}
}


/*
 * AddInsertAliasIfNeeded adds an alias in UPSERTs and multi-row INSERTs to avoid
 * deparsing issues (e.g. RETURNING might reference the original table name,
 * which has been replaced by a shard name).
 */
void
AddInsertAliasIfNeeded(Query *query)
{
	Assert(query->commandType == CMD_INSERT);

	if (query->onConflict == NULL &&
		ExtractDistributedInsertValuesRTE(query) == NULL)
	{
		/* simple single-row insert does not need an alias */
		return;
	}

	RangeTblEntry *rangeTableEntry = linitial(query->rtable);
	if (rangeTableEntry->alias != NULL)
	{
		/* INSERT already has an alias */
		return;
	}

	Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
	rangeTableEntry->alias = alias;
}


/*
 * UpdateTaskQueryString updates the query string stored within the provided
 * Task. If the Task has row values from a multi-row INSERT, those are injected
 * into the provided query before deparse occurs (the query's full VALUES list
 * will be restored before this function returns).
 */
static void
UpdateTaskQueryString(Query *query, Task *task)
{
	SetTaskQueryIfShouldLazyDeparse(task, query);
}


/*
 * CreateQualsForShardInterval creates the necessary qual conditions over the
 * given attnum and rtindex for the given shard interval.
 */
Node *
CreateQualsForShardInterval(RelationShard *relationShard, int attnum, int rtindex)
{
	uint64 shardId = relationShard->shardId;
	Oid relationId = relationShard->relationId;

	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	Var *partitionColumnVar = cacheEntry->partitionColumn;

	/*
	 * Add constraints for the relation identified by rtindex, specifically on its column at attnum.
	 * Create a Var node representing this column, which will be used to compare against the bounds
	 * from the partition column of shard interval.
	 */

	Var *outerTablePartitionColumnVar = makeVar(
		rtindex, attnum, partitionColumnVar->vartype,
		partitionColumnVar->vartypmod,
		partitionColumnVar->varcollid,
		0);

	bool isFirstShard = IsFirstShard(cacheEntry, shardId);

	/* load the interval for the shard and create constant nodes for the upper/lower bounds */
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Const *constNodeLowerBound = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
										   shardInterval->minValue, false, true);
	Const *constNodeUpperBound = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
										   shardInterval->maxValue, false, true);
	Const *constNodeZero = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
									 Int32GetDatum(0), false, true);

	/* create a function expression node for the hash partition column */
	FuncExpr *hashFunction = makeNode(FuncExpr);
	hashFunction->funcid = cacheEntry->hashFunction->fn_oid;
	hashFunction->args = list_make1(outerTablePartitionColumnVar);
	hashFunction->funcresulttype = get_func_rettype(cacheEntry->hashFunction->fn_oid);
	hashFunction->funcretset = false;

	/* create a function expression for the lower bound of the shard interval */
	Oid resultTypeOid = get_func_rettype(
		cacheEntry->shardIntervalCompareFunction->fn_oid);
	FuncExpr *lowerBoundFuncExpr = makeNode(FuncExpr);
	lowerBoundFuncExpr->funcid = cacheEntry->shardIntervalCompareFunction->fn_oid;
	lowerBoundFuncExpr->args = list_make2((Node *) constNodeLowerBound,
										  (Node *) hashFunction);
	lowerBoundFuncExpr->funcresulttype = resultTypeOid;
	lowerBoundFuncExpr->funcretset = false;

	Oid lessThan = GetSysCacheOid(OPERNAMENSP, Anum_pg_operator_oid, CStringGetDatum("<"),
								  resultTypeOid, resultTypeOid, ObjectIdGetDatum(
									  PG_CATALOG_NAMESPACE));

	/*
	 * Finally, check if the comparison result is less than 0, i.e.,
	 * shardInterval->minValue < hash(partitionColumn)
	 * See SearchCachedShardInterval for the behavior at the boundaries.
	 */
	Expr *lowerBoundExpr = make_opclause(lessThan, BOOLOID, false,
										 (Expr *) lowerBoundFuncExpr,
										 (Expr *) constNodeZero, InvalidOid, InvalidOid);

	/* create a function expression for the upper bound of the shard interval */
	FuncExpr *upperBoundFuncExpr = makeNode(FuncExpr);
	upperBoundFuncExpr->funcid = cacheEntry->shardIntervalCompareFunction->fn_oid;
	upperBoundFuncExpr->args = list_make2((Node *) hashFunction,
										  (Expr *) constNodeUpperBound);
	upperBoundFuncExpr->funcresulttype = resultTypeOid;
	upperBoundFuncExpr->funcretset = false;

	Oid lessThanOrEqualTo = GetSysCacheOid(OPERNAMENSP, Anum_pg_operator_oid,
										   CStringGetDatum("<="),
										   resultTypeOid, resultTypeOid,
										   ObjectIdGetDatum(PG_CATALOG_NAMESPACE));


	/*
	 * Finally, check if the comparison result is less than or equal to 0, i.e.,
	 * hash(partitionColumn) <= shardInterval->maxValue
	 * See SearchCachedShardInterval for the behavior at the boundaries.
	 */
	Expr *upperBoundExpr = make_opclause(lessThanOrEqualTo, BOOLOID, false,
										 (Expr *) upperBoundFuncExpr,
										 (Expr *) constNodeZero, InvalidOid, InvalidOid);


	/* create a node for both upper and lower bound */
	Node *shardIntervalBoundQuals = make_and_qual((Node *) lowerBoundExpr,
												  (Node *) upperBoundExpr);

	/*
	 * Add a null test for the partition column for the first shard.
	 * This is because we need to include the null values in exactly one of the shard queries.
	 * The null test is added as an OR clause to the existing AND clause.
	 */
	if (isFirstShard)
	{
		/* null test for the first shard */
		NullTest *nullTest = makeNode(NullTest);
		nullTest->nulltesttype = IS_NULL;  /* Check for IS NULL */
		nullTest->arg = (Expr *) outerTablePartitionColumnVar;  /* The variable to check */
		nullTest->argisrow = false;
		shardIntervalBoundQuals = (Node *) make_orclause(list_make2(nullTest,
																	shardIntervalBoundQuals));
	}
	return shardIntervalBoundQuals;
}


/*
 * UpdateWhereClauseToPushdownRecurringOuterJoinWalker walks over the query tree and
 * updates the WHERE clause for outer joins satisfying feasibility conditions.
 */
bool
UpdateWhereClauseToPushdownRecurringOuterJoinWalker(Node *node, List *relationShardList)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		UpdateWhereClauseToPushdownRecurringOuterJoin((Query *) node, relationShardList);
		return query_tree_walker((Query *) node,
								 UpdateWhereClauseToPushdownRecurringOuterJoinWalker,
								 relationShardList, QTW_EXAMINE_RTES_BEFORE);
	}

	if (!IsA(node, RangeTblEntry))
	{
		return expression_tree_walker(node,
									  UpdateWhereClauseToPushdownRecurringOuterJoinWalker,
									  relationShardList);
	}

	return false;
}


/*
 * UpdateWhereClauseToPushdownRecurringOuterJoin
 *
 * Inject shard interval predicates into the query WHERE clause for certain
 * outer joins to make the join semantically correct when distributed.
 *
 * Why this is needed:
 *   When an inner side of an OUTER JOIN is a distributed table that has been
 *   routed to a single shard, we cannot simply replace the RTE with the shard
 *   name and rely on implicit pruning: the preserved (outer) side could still
 *   produce rows whose join keys would hash to other shards. To keep results
 *   consistent with the global execution semantics we restrict the preserved
 *   (outer) side to only those partition key values that would route to the
 *   chosen shard (plus NULLs, which are assigned to exactly one shard).
 *
 * What the function does:
 *   1. Iterate over the top-level jointree->fromlist.
 *   2. For each JoinExpr call CanPushdownRecurringOuterJoinExtended() which:
 *        - Verifies shape / join type is eligible.
 *        - Returns:
 *            outerRtIndex : RT index whose column we will constrain,
 *            outerRte / innerRte,
 *            attnum       : attribute number (partition column) on outer side.
 *                           This is compared to partition column of innerRte.
 *   3. Find the RelationShard for the inner distributed table (innerRte->relid)
 *      in relationShardList; skip if absent (no fixed shard chosen).
 *   4. Build the shard qualification with CreateQualsForShardInterval():
 *        (minValue < hash(partcol) AND hash(partcol) <= maxValue)
 *      and, for the first shard only, OR (partcol IS NULL).
 *      The Var refers to (outerRtIndex, attnum) so the restriction applies to
 *      the preserved outer input.
 *   5. AND the new quals into jointree->quals (creating it if NULL).
 *
 * The function does not return anything, it modifies the query in place.
 */
void
UpdateWhereClauseToPushdownRecurringOuterJoin(Query *query, List *relationShardList)
{
	if (query == NULL)
	{
		return;
	}

	FromExpr *fromExpr = query->jointree;
	if (fromExpr == NULL || fromExpr->fromlist == NIL)
	{
		return;
	}

	ListCell *fromExprCell;
	foreach(fromExprCell, fromExpr->fromlist)
	{
		Node *fromItem = (Node *) lfirst(fromExprCell);
		if (!IsA(fromItem, JoinExpr))
		{
			continue;
		}
		JoinExpr *joinExpr = (JoinExpr *) fromItem;

		/*
		 * We will check if we need to add constraints to the WHERE clause.
		 */
		RangeTblEntry *innerRte = NULL;
		RangeTblEntry *outerRte = NULL;
		int outerRtIndex = -1;
		int attnum;
		if (!CanPushdownRecurringOuterJoinExtended(joinExpr, query, &outerRtIndex,
												   &outerRte, &innerRte, &attnum))
		{
			continue;
		}

		if (attnum == InvalidAttrNumber)
		{
			continue;
		}
		ereport(DEBUG5, (errmsg(
							 "Distributed table from the inner part of the outer join: %s.",
							 innerRte->eref->aliasname)));

		RelationShard *relationShard = FindRelationShard(innerRte->relid,
														 relationShardList);

		if (relationShard == NULL || relationShard->shardId == INVALID_SHARD_ID)
		{
			continue;
		}

		Node *shardIntervalBoundQuals = CreateQualsForShardInterval(relationShard, attnum,
																	outerRtIndex);
		if (fromExpr->quals == NULL)
		{
			fromExpr->quals = (Node *) shardIntervalBoundQuals;
		}
		else
		{
			fromExpr->quals = make_and_qual(fromExpr->quals, shardIntervalBoundQuals);
		}
	}
}


/*
 * UpdateRelationToShardNames walks over the query tree and appends shard ids to
 * relations. It uses unique identity value to establish connection between a
 * shard and the range table entry. If the range table id is not given a
 * identity, than the relation is not referenced from the query, no connection
 * could be found between a shard and this relation. Therefore relation is replaced
 * by set of NULL values so that the query would work at worker without any problems.
 *
 */
bool
UpdateRelationToShardNames(Node *node, List *relationShardList)
{
	uint64 shardId = INVALID_SHARD_ID;

	if (node == NULL)
	{
		return false;
	}

	/* want to look at all RTEs, even in subqueries, CTEs and such */
	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, UpdateRelationToShardNames,
								 relationShardList, QTW_EXAMINE_RTES_BEFORE);
	}

	if (!IsA(node, RangeTblEntry))
	{
		return expression_tree_walker(node, UpdateRelationToShardNames,
									  relationShardList);
	}

	RangeTblEntry *newRte = (RangeTblEntry *) node;

	if (newRte->rtekind == RTE_FUNCTION)
	{
		newRte = NULL;
		if (!FindCitusExtradataContainerRTE(node, &newRte))
		{
			/* only update function rtes containing citus_extradata_container */
			return false;
		}
	}
	else if (newRte->rtekind != RTE_RELATION)
	{
		return false;
	}

	if (!IsCitusTable(newRte->relid))
	{
		/* leave local tables as is */
		return false;
	}

	RelationShard *relationShard = FindRelationShard(newRte->relid,
													 relationShardList);

	bool replaceRteWithNullValues = relationShard == NULL ||
									relationShard->shardId == INVALID_SHARD_ID;
	if (replaceRteWithNullValues)
	{
		ConvertRteToSubqueryWithEmptyResult(newRte);
		return false;
	}

	shardId = relationShard->shardId;
	Oid relationId = relationShard->relationId;

	char *relationName = get_rel_name(relationId);
	AppendShardIdToName(&relationName, shardId);

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);

	ModifyRangeTblExtraData(newRte, CITUS_RTE_SHARD, schemaName, relationName, NIL);

	return false;
}


/*
 * FindRelationShard finds the RelationShard for shard relation with
 * given Oid if exists in given relationShardList. Otherwise, returns NULL.
 */
static RelationShard *
FindRelationShard(Oid inputRelationId, List *relationShardList)
{
	RelationShard *relationShard = NULL;

	/*
	 * Search for the restrictions associated with the RTE. There better be
	 * some, otherwise this query wouldn't be eligible as a router query.
	 * FIXME: We should probably use a hashtable here, to do efficient lookup.
	 */
	foreach_declared_ptr(relationShard, relationShardList)
	{
		if (inputRelationId == relationShard->relationId)
		{
			return relationShard;
		}
	}

	return NULL;
}


/*
 * ConvertRteToSubqueryWithEmptyResult converts given relation RTE into
 * subquery RTE that returns no results.
 */
static void
ConvertRteToSubqueryWithEmptyResult(RangeTblEntry *rte)
{
	Relation relation = table_open(rte->relid, NoLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	int columnCount = tupleDescriptor->natts;
	List *targetList = NIL;

	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FormData_pg_attribute *attributeForm = TupleDescAttr(tupleDescriptor,
															 columnIndex);

		if (attributeForm->attisdropped)
		{
			continue;
		}

		StringInfo resname = makeStringInfo();
		Const *constValue = makeNullConst(attributeForm->atttypid,
										  attributeForm->atttypmod,
										  attributeForm->attcollation);

		appendStringInfo(resname, "%s", attributeForm->attname.data);

		TargetEntry *targetEntry = makeNode(TargetEntry);
		targetEntry->expr = (Expr *) constValue;
		targetEntry->resno = columnIndex;
		targetEntry->resname = resname->data;

		targetList = lappend(targetList, targetEntry);
	}

	table_close(relation, NoLock);

	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->quals = makeBoolConst(false, false);

	Query *subquery = makeNode(Query);
	subquery->commandType = CMD_SELECT;
	subquery->querySource = QSRC_ORIGINAL;
	subquery->canSetTag = true;
	subquery->targetList = targetList;
	subquery->jointree = joinTree;

	rte->rtekind = RTE_SUBQUERY;
#if PG_VERSION_NUM >= PG_VERSION_16

	/* no permission checking for this RTE */
	rte->perminfoindex = 0;
#endif
	rte->subquery = subquery;
	rte->alias = copyObject(rte->eref);
}


/*
 * ShouldLazyDeparseQuery returns true if we should lazily deparse the query
 * when adding it to the task. Right now it simply checks if any shards on the
 * local node can be used for the task.
 */
static bool
ShouldLazyDeparseQuery(Task *task)
{
	return TaskAccessesLocalNode(task);
}


/*
 * SetTaskQueryIfShouldLazyDeparse attaches the query to the task so that it can be used during
 * execution. If local execution can possibly take place it sets task->jobQueryReferenceForLazyDeparsing.
 * If not it deparses the query and sets queryStringLazy, to avoid blowing the
 * size of the task unnecesarily.
 */
void
SetTaskQueryIfShouldLazyDeparse(Task *task, Query *query)
{
	if (ShouldLazyDeparseQuery(task))
	{
		task->taskQuery.queryType = TASK_QUERY_OBJECT;
		task->taskQuery.data.jobQueryReferenceForLazyDeparsing = query;
		task->queryCount = 1;
		return;
	}

	SetTaskQueryString(task, AnnotateQuery(DeparseTaskQuery(task, query),
										   task->partitionKeyValue, task->colocationId));
}


/*
 * SetTaskQueryString attaches the query string to the task so that it can be
 * used during execution. It also unsets jobQueryReferenceForLazyDeparsing to be sure
 * these are kept in sync.
 */
void
SetTaskQueryString(Task *task, char *queryString)
{
	if (queryString == NULL)
	{
		task->taskQuery.queryType = TASK_QUERY_NULL;
		task->queryCount = 0;
	}
	else
	{
		task->taskQuery.queryType = TASK_QUERY_TEXT;
		task->taskQuery.data.queryStringLazy = queryString;
		task->queryCount = 1;
	}
}


/*
 * SetTaskQueryStringList sets the queryStringList of the given task.
 */
void
SetTaskQueryStringList(Task *task, List *queryStringList)
{
	Assert(queryStringList != NIL);
	task->taskQuery.queryType = TASK_QUERY_TEXT_LIST;
	task->taskQuery.data.queryStringList = queryStringList;
	task->queryCount = list_length(queryStringList);
}


void
SetTaskQueryPlan(Task *task, Query *query, PlannedStmt *localPlan)
{
	Assert(localPlan != NULL);
	task->taskQuery.queryType = TASK_QUERY_LOCAL_PLAN;
	task->taskQuery.data.localCompiled = (LocalCompilation *) palloc0(
		sizeof(LocalCompilation));
	task->taskQuery.data.localCompiled->query = query;
	task->taskQuery.data.localCompiled->plan = localPlan;
	task->queryCount = 1;
}


PlannedStmt *
TaskQueryLocalPlan(Task *task)
{
	Assert(task->taskQuery.queryType == TASK_QUERY_LOCAL_PLAN);
	return task->taskQuery.data.localCompiled->plan;
}


/*
 * DeparseTaskQuery is a general way of deparsing a query based on a task.
 */
static char *
DeparseTaskQuery(Task *task, Query *query)
{
	StringInfo queryString = makeStringInfo();

	if (query->commandType == CMD_INSERT)
	{
		/*
		 * For INSERT queries we cannot use pg_get_query_def. Mainly because we
		 * cannot run UpdateRelationToShardNames on an INSERT query. This is
		 * because the PG deparsing logic fails when trying to insert into a
		 * RTE_FUNCTION (which is what will happen if you call
		 * UpdateRelationToShardNames).
		 */
		deparse_shard_query(query, task->anchorDistributedTableId, task->anchorShardId,
							queryString);
	}
	else
	{
		pg_get_query_def(query, queryString);
	}

	return queryString->data;
}


/*
 * GetTaskQueryType returns the type of the task query.
 */
int
GetTaskQueryType(Task *task)
{
	return task->taskQuery.queryType;
}


/*
 * TaskQueryStringAtIndex returns query at given index among the possibly
 * multiple queries that a task can have.
 */
char *
TaskQueryStringAtIndex(Task *task, int index)
{
	Assert(index < task->queryCount);

	int taskQueryType = GetTaskQueryType(task);
	if (taskQueryType == TASK_QUERY_TEXT_LIST)
	{
		return list_nth(task->taskQuery.data.queryStringList, index);
	}

	return TaskQueryString(task);
}


/*
 * TaskQueryString generates task query string text if missing.
 *
 * For performance reasons, the queryString is generated lazily. For example
 * for local queries it is usually not needed to generate it, so this way we
 * can skip the expensive deparsing+parsing.
 */
char *
TaskQueryString(Task *task)
{
	int taskQueryType = GetTaskQueryType(task);
	if (taskQueryType == TASK_QUERY_NULL)
	{
		/* if task query type is TASK_QUERY_NULL then the data will be NULL,
		 * this is unexpected state */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("unexpected task query state: task query type is null"),
						errdetail("Please report this to the Citus core team.")));
	}
	else if (taskQueryType == TASK_QUERY_TEXT_LIST)
	{
		return StringJoin(task->taskQuery.data.queryStringList, ';');
	}
	else if (taskQueryType == TASK_QUERY_TEXT)
	{
		return task->taskQuery.data.queryStringLazy;
	}
	else if (taskQueryType == TASK_QUERY_LOCAL_PLAN)
	{
		Query *query = task->taskQuery.data.localCompiled->query;
		Assert(query != NULL);

		/*
		 * Use the query of the local compilation to generate the
		 * query string. For local compiled tasks, the query is retained
		 * for this purpose, which may be EXPLAIN ANALYZing the task, or
		 * command logging. Generating the query string on the fly is
		 * acceptable because the plan of the local compilation is used
		 * for query execution.
		 */
		MemoryContext previousContext = MemoryContextSwitchTo(GetMemoryChunkContext(
																  query));
		UpdateRelationToShardNames((Node *) query, task->relationShardList);
		MemoryContextSwitchTo(previousContext);
		return AnnotateQuery(DeparseTaskQuery(task, query),
							 task->partitionKeyValue, task->colocationId);
	}

	Query *jobQueryReferenceForLazyDeparsing =
		task->taskQuery.data.jobQueryReferenceForLazyDeparsing;

	/*
	 *	At this point task query type should be TASK_QUERY_OBJECT.
	 */
	Assert(task->taskQuery.queryType == TASK_QUERY_OBJECT &&
		   jobQueryReferenceForLazyDeparsing != NULL);


	/*
	 * Switch to the memory context of task->jobQueryReferenceForLazyDeparsing before generating the query
	 * string. This way the query string is not freed in between multiple
	 * executions of a prepared statement. Except when UpdateTaskQueryString is
	 * used to set task->jobQueryReferenceForLazyDeparsing, in that case it is freed but it will be set to
	 * NULL on the next execution of the query because UpdateTaskQueryString
	 * does that.
	 */
	MemoryContext previousContext = MemoryContextSwitchTo(GetMemoryChunkContext(
															  jobQueryReferenceForLazyDeparsing));
	char *queryString = DeparseTaskQuery(task, jobQueryReferenceForLazyDeparsing);
	MemoryContextSwitchTo(previousContext);
	SetTaskQueryString(task, queryString);
	return task->taskQuery.data.queryStringLazy;
}
