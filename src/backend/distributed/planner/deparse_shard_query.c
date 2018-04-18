/*-------------------------------------------------------------------------
 *
 * deparse_shard_query.c
 *
 * This file contains functions for deparsing shard queries.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
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
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
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


/*
 * RebuildQueryStrings deparses the job query for each task to
 * include execution-time changes such as function evaluation.
 */
void
RebuildQueryStrings(Query *originalQuery, List *taskList)
{
	ListCell *taskCell = NULL;
	Oid relationId = ((RangeTblEntry *) linitial(originalQuery->rtable))->relid;
	RangeTblEntry *valuesRTE = ExtractDistributedInsertValuesRTE(originalQuery);

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		Query *query = originalQuery;

		if (UpdateOrDeleteQuery(query) && list_length(taskList))
		{
			query = copyObject(originalQuery);
		}
		else if (query->commandType == CMD_INSERT && task->modifyWithMultipleTableQuery)
		{
			/* for INSERT..SELECT, adjust shard names in SELECT part */
			RangeTblEntry *copiedInsertRte = NULL;
			RangeTblEntry *copiedSubqueryRte = NULL;
			Query *copiedSubquery = NULL;
			List *relationShardList = task->relationShardList;
			ShardInterval *shardInterval = LoadShardInterval(task->anchorShardId);
			char partitionMethod = 0;

			query = copyObject(originalQuery);

			copiedInsertRte = ExtractInsertRangeTableEntry(query);
			copiedSubqueryRte = ExtractSelectRangeTableEntry(query);
			copiedSubquery = copiedSubqueryRte->subquery;

			/* there are no restrictions to add for reference tables */
			partitionMethod = PartitionMethod(shardInterval->relationId);
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
		else if (task->upsertQuery || valuesRTE != NULL)
		{
			RangeTblEntry *rangeTableEntry = NULL;

			/*
			 * Always an alias in UPSERTs and multi-row INSERTs to avoid
			 * deparsing issues (e.g. RETURNING might reference the original
			 * table name, which has been replaced by a shard name).
			 */
			rangeTableEntry = linitial(query->rtable);
			if (rangeTableEntry->alias == NULL)
			{
				Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
				rangeTableEntry->alias = alias;
			}
		}

		ereport(DEBUG4, (errmsg("query before rebuilding: %s", task->queryString)));

		UpdateTaskQueryString(query, relationId, valuesRTE, task);

		ereport(DEBUG4, (errmsg("query after rebuilding:  %s", task->queryString)));
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
	StringInfo queryString = makeStringInfo();
	List *oldValuesLists = NIL;

	if (valuesRTE != NULL)
	{
		Assert(valuesRTE->rtekind == RTE_VALUES);
		Assert(task->rowValuesLists != NULL);

		oldValuesLists = valuesRTE->values_lists;
		valuesRTE->values_lists = task->rowValuesLists;
	}

	/*
	 * For INSERT queries, we only have one relation to update, so we can
	 * use deparse_shard_query(). For UPDATE and DELETE queries, we may have
	 * subqueries and joins, so we use relation shard list to update shard
	 * names and call pg_get_query_def() directly.
	 */
	if (query->commandType == CMD_INSERT)
	{
		deparse_shard_query(query, distributedTableId, task->anchorShardId, queryString);
	}
	else
	{
		List *relationShardList = task->relationShardList;
		UpdateRelationToShardNames((Node *) query, relationShardList);

		pg_get_query_def(query, queryString);
	}

	if (valuesRTE != NULL)
	{
		valuesRTE->values_lists = oldValuesLists;
	}

	task->queryString = queryString->data;
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
	RangeTblEntry *newRte = NULL;
	uint64 shardId = INVALID_SHARD_ID;
	Oid relationId = InvalidOid;
	Oid schemaId = InvalidOid;
	char *relationName = NULL;
	char *schemaName = NULL;
	bool replaceRteWithNullValues = false;
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
								 relationShardList, QTW_EXAMINE_RTES);
	}

	if (!IsA(node, RangeTblEntry))
	{
		return expression_tree_walker(node, UpdateRelationToShardNames,
									  relationShardList);
	}

	newRte = (RangeTblEntry *) node;

	if (newRte->rtekind != RTE_RELATION)
	{
		return false;
	}

	/*
	 * Search for the restrictions associated with the RTE. There better be
	 * some, otherwise this query wouldn't be elegible as a router query.
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

	replaceRteWithNullValues = relationShard == NULL ||
							   relationShard->shardId == INVALID_SHARD_ID;
	if (replaceRteWithNullValues)
	{
		ConvertRteToSubqueryWithEmptyResult(newRte);
		return false;
	}

	shardId = relationShard->shardId;
	relationId = relationShard->relationId;

	relationName = get_rel_name(relationId);
	AppendShardIdToName(&relationName, shardId);

	schemaId = get_rel_namespace(relationId);
	schemaName = get_namespace_name(schemaId);

	ModifyRangeTblExtraData(newRte, CITUS_RTE_SHARD, schemaName, relationName, NIL);

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
	int columnIndex = 0;
	Query *subquery = NULL;
	List *targetList = NIL;
	FromExpr *joinTree = NULL;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FormData_pg_attribute *attributeForm = TupleDescAttr(tupleDescriptor,
															 columnIndex);
		TargetEntry *targetEntry = NULL;
		StringInfo resname = NULL;
		Const *constValue = NULL;

		if (attributeForm->attisdropped)
		{
			continue;
		}

		resname = makeStringInfo();
		constValue = makeNullConst(attributeForm->atttypid, attributeForm->atttypmod,
								   attributeForm->attcollation);

		appendStringInfo(resname, "%s", attributeForm->attname.data);

		targetEntry = makeNode(TargetEntry);
		targetEntry->expr = (Expr *) constValue;
		targetEntry->resno = columnIndex;
		targetEntry->resname = resname->data;

		targetList = lappend(targetList, targetEntry);
	}

	heap_close(relation, NoLock);

	joinTree = makeNode(FromExpr);
	joinTree->quals = makeBoolConst(false, false);

	subquery = makeNode(Query);
	subquery->commandType = CMD_SELECT;
	subquery->querySource = QSRC_ORIGINAL;
	subquery->canSetTag = true;
	subquery->targetList = targetList;
	subquery->jointree = joinTree;

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->alias = copyObject(rte->eref);
}
