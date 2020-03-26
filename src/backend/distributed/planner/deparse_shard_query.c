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
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/shard_utils.h"
#include "distributed/version_compat.h"
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


static void UpdateTaskQueryString(Query *query, Oid distributedTableId,
								  RangeTblEntry *valuesRTE, Task *task);
static void ConvertRteToSubqueryWithEmptyResult(RangeTblEntry *rte);
static bool ShouldLazyDeparseQuery(Task *task);
static char * DeparseTaskQuery(Task *task, Query *query);
static bool IsEachPlacementQueryStringDifferent(Task* task);


/*
 * RebuildQueryStrings deparses the job query for each task to
 * include execution-time changes such as function evaluation.
 */
void
RebuildQueryStrings(Job *workerJob)
{
	Query *originalQuery = workerJob->jobQuery;
	List *taskList = workerJob->taskList;
	Oid relationId = ((RangeTblEntry *) linitial(originalQuery->rtable))->relid;
	RangeTblEntry *valuesRTE = ExtractDistributedInsertValuesRTE(originalQuery);

	Task *task = NULL;

	foreach_ptr(task, taskList)
	{
		Query *query = originalQuery;

		if (UpdateOrDeleteQuery(query) && list_length(taskList) > 1)
		{
			query = copyObject(originalQuery);
		}
		else if (query->commandType == CMD_INSERT && task->modifyWithSubquery)
		{
			/* for INSERT..SELECT, adjust shard names in SELECT part */
			List *relationShardList = task->relationShardList;
			ShardInterval *shardInterval = LoadShardInterval(task->anchorShardId);

			query = copyObject(originalQuery);

			RangeTblEntry *copiedInsertRte = ExtractResultRelationRTE(query);
			RangeTblEntry *copiedSubqueryRte = ExtractSelectRangeTableEntry(query);
			Query *copiedSubquery = copiedSubqueryRte->subquery;

			/* there are no restrictions to add for reference tables */
			char partitionMethod = PartitionMethod(shardInterval->relationId);
			if (partitionMethod != DISTRIBUTE_BY_NONE)
			{
				AddShardIntervalRestrictionToSelect(copiedSubquery, shardInterval);
			}

			ReorderInsertSelectTargetLists(query, copiedInsertRte, copiedSubqueryRte);

			/* setting an alias simplifies deparsing of RETURNING */
			if (copiedInsertRte->alias == NULL)
			{
				Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
				copiedInsertRte->alias = alias;
			}

			UpdateRelationToShardNames((Node *) copiedSubquery, relationShardList);
		}
		else if (query->commandType == CMD_INSERT && (query->onConflict != NULL ||
													  valuesRTE != NULL))
		{
			/*
			 * Always an alias in UPSERTs and multi-row INSERTs to avoid
			 * deparsing issues (e.g. RETURNING might reference the original
			 * table name, which has been replaced by a shard name).
			 */
			RangeTblEntry *rangeTableEntry = linitial(query->rtable);
			if (rangeTableEntry->alias == NULL)
			{
				Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
				rangeTableEntry->alias = alias;
			}
		}

		ereport(DEBUG4, (errmsg("query before rebuilding: %s",
								task->queryForLocalExecution == NULL &&
								task->queryStringLazy == NULL
								? "(null)"
								: ApplyLogRedaction(TaskQueryString(task)))));

		UpdateTaskQueryString(query, relationId, valuesRTE, task);

		/*
		 * If parameters were resolved in the job query, then they are now also
		 * resolved in the query string.
		 */
		task->parametersInQueryStringResolved = workerJob->parametersInJobQueryResolved;

		ereport(DEBUG4, (errmsg("query after rebuilding:  %s",
								ApplyLogRedaction(TaskQueryString(task)))));
	}
}


/*
 * UpdateTaskQueryString updates the query string stored within the provided
 * Task. If the Task has row values from a multi-row INSERT, those are injected
 * into the provided query (using the provided valuesRTE, which must belong to
 * the query) before deparse occurs (the query's full VALUES list will be
 * restored before this function returns).
 */
static void
UpdateTaskQueryString(Query *query, Oid distributedTableId, RangeTblEntry *valuesRTE,
					  Task *task)
{
	List *oldValuesLists = NIL;

	if (valuesRTE != NULL)
	{
		Assert(valuesRTE->rtekind == RTE_VALUES);
		Assert(task->rowValuesLists != NULL);

		oldValuesLists = valuesRTE->values_lists;
		valuesRTE->values_lists = task->rowValuesLists;
	}

	if (query->commandType != CMD_INSERT)
	{
		/*
		 * For UPDATE and DELETE queries, we may have subqueries and joins, so
		 * we use relation shard list to update shard names and call
		 * pg_get_query_def() directly.
		 */
		List *relationShardList = task->relationShardList;
		UpdateRelationToShardNames((Node *) query, relationShardList);
	}
	else if (ShouldLazyDeparseQuery(task))
	{
		/*
		 * not all insert queries are copied before calling this
		 * function, so we do it here
		 */
		query = copyObject(query);
	}

	if (query->commandType == CMD_INSERT)
	{
		/*
		 * We store this in the task so we can lazily call
		 * deparse_shard_query when the string is needed
		 */
		task->anchorDistributedTableId = distributedTableId;
	}

	SetTaskQuery(task, query);

	if (valuesRTE != NULL)
	{
		valuesRTE->values_lists = oldValuesLists;
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
	Oid relationId = InvalidOid;
	ListCell *relationShardCell = NULL;
	RelationShard *relationShard = NULL;

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

	if (newRte->rtekind != RTE_RELATION)
	{
		return false;
	}

	/*
	 * Search for the restrictions associated with the RTE. There better be
	 * some, otherwise this query wouldn't be eligible as a router query.
	 *
	 * FIXME: We should probably use a hashtable here, to do efficient
	 * lookup.
	 */
	foreach(relationShardCell, relationShardList)
	{
		relationShard = (RelationShard *) lfirst(relationShardCell);

		if (newRte->relid == relationShard->relationId)
		{
			break;
		}

		relationShard = NULL;
	}

	bool replaceRteWithNullValues = relationShard == NULL ||
									relationShard->shardId == INVALID_SHARD_ID;
	if (replaceRteWithNullValues)
	{
		ConvertRteToSubqueryWithEmptyResult(newRte);
		return false;
	}

	shardId = relationShard->shardId;
	relationId = relationShard->relationId;

	char *relationName = get_rel_name(relationId);
	AppendShardIdToName(&relationName, shardId);

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);

	ModifyRangeTblExtraData(newRte, CITUS_RTE_SHARD, schemaName, relationName, NIL);

	return false;
}


/*
 * UpdateRelationsToLocalShardTables walks over the query tree and appends shard ids to
 * relations. The caller is responsible for ensuring that the resulting Query can
 * be executed locally.
 */
bool
UpdateRelationsToLocalShardTables(Node *node, List *relationShardList)
{
	if (node == NULL)
	{
		return false;
	}

	/* want to look at all RTEs, even in subqueries, CTEs and such */
	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, UpdateRelationsToLocalShardTables,
								 relationShardList, QTW_EXAMINE_RTES_BEFORE);
	}

	if (!IsA(node, RangeTblEntry))
	{
		return expression_tree_walker(node, UpdateRelationsToLocalShardTables,
									  relationShardList);
	}

	RangeTblEntry *newRte = (RangeTblEntry *) node;

	if (newRte->rtekind != RTE_RELATION)
	{
		return false;
	}

	/*
	 * Search for the restrictions associated with the RTE. There better be
	 * some, otherwise this query wouldn't be eligible as a router query.
	 *
	 * FIXME: We should probably use a hashtable here, to do efficient
	 * lookup.
	 */
	ListCell *relationShardCell = NULL;
	RelationShard *relationShard = NULL;

	foreach(relationShardCell, relationShardList)
	{
		relationShard = (RelationShard *) lfirst(relationShardCell);

		if (newRte->relid == relationShard->relationId)
		{
			break;
		}

		relationShard = NULL;
	}

	/* the function should only be called with local shards */
	if (relationShard == NULL)
	{
		return true;
	}

	Oid shardOid = GetShardLocalTableOid(relationShard->relationId,
										 relationShard->shardId);

	newRte->relid = shardOid;

	return false;
}


/*
 * ConvertRteToSubqueryWithEmptyResult converts given relation RTE into
 * subquery RTE that returns no results.
 */
static void
ConvertRteToSubqueryWithEmptyResult(RangeTblEntry *rte)
{
	Relation relation = heap_open(rte->relid, NoLock);
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

	heap_close(relation, NoLock);

	FromExpr *joinTree = makeNode(FromExpr);
	joinTree->quals = makeBoolConst(false, false);

	Query *subquery = makeNode(Query);
	subquery->commandType = CMD_SELECT;
	subquery->querySource = QSRC_ORIGINAL;
	subquery->canSetTag = true;
	subquery->targetList = targetList;
	subquery->jointree = joinTree;

	rte->rtekind = RTE_SUBQUERY;
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
 * SetTaskQuery attaches the query to the task so that it can be used during
 * execution. If local execution can possibly take place it sets task->queryForLocalExecution.
 * If not it deparses the query and sets queryStringLazy, to avoid blowing the
 * size of the task unnecesarily.
 */
void
SetTaskQuery(Task *task, Query *query)
{
	if (ShouldLazyDeparseQuery(task))
	{
		task->queryForLocalExecution = query;
		task->queryStringLazy = NULL;
		return;
	}
	SetTaskQueryString(task, DeparseTaskQuery(task,query));
}


/*
 * SetTaskQueryString attaches the query string to the task so that it can be
 * used during execution. It also unsets queryForLocalExecution to be sure
 * these are kept in sync.
 */
void
SetTaskQueryString(Task *task, char *queryString)
{
	task->queryForLocalExecution = NULL;
	task->queryStringLazy = queryString;
}

/*
 * SetTaskPerPlacementQueryStrings set the perPlacementQueryString for the given task.
 */
void SetTaskPerPlacementQueryStrings(Task *task, List* perPlacementQueryStringList) {
	Assert(perPlacementQueryStringList != NIL);
	task->perPlacementQueryStrings = perPlacementQueryStringList;
	SetTaskQueryString(task, NULL);
}


/*
 * SetTaskQueryStringList sets the queryStringList of the given task.
 */
void
SetTaskQueryStringList(Task *task, List *queryStringList)
{
	task->queryStringList = queryStringList;
	SetTaskQueryString(task, StringJoin(queryStringList, ';'));
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
 * TaskQueryString generates task->queryStringLazy if missing.
 *
 * For performance reasons, the queryString is generated lazily. For example
 * for local queries it is usually not needed to generate it, so this way we
 * can skip the expensive deparsing+parsing.
 */
char *
TaskQueryString(Task *task)
{
	if (task->queryStringLazy != NULL)
	{
		return task->queryStringLazy;
	}
	Assert(task->queryForLocalExecution != NULL);


	/*
	 * Switch to the memory context of task->queryForLocalExecution before generating the query
	 * string. This way the query string is not freed in between multiple
	 * executions of a prepared statement. Except when UpdateTaskQueryString is
	 * used to set task->queryForLocalExecution, in that case it is freed but it will be set to
	 * NULL on the next execution of the query because UpdateTaskQueryString
	 * does that.
	 */
	MemoryContext previousContext = MemoryContextSwitchTo(GetMemoryChunkContext(
															  task->queryForLocalExecution));
	task->queryStringLazy = DeparseTaskQuery(task, task->queryForLocalExecution);
	MemoryContextSwitchTo(previousContext);
	return task->queryStringLazy;
}

/*
 * IsEachPlacementQueryStringDifferent returns true if each placement
 * has a different query string.
 */
static bool IsEachPlacementQueryStringDifferent(Task* task) {
	return list_length(task->perPlacementQueryStrings) > 0;
}