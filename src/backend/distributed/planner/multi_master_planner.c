/*-------------------------------------------------------------------------
 *
 * multi_master_planner.c
 *	  Routines for building create table and select into table statements on the
 *	  master node.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * MasterTargetList uses the given worker target list's expressions, and creates
 * a target target list for the master node. This master target list keeps the
 * temporary table's columns on the master node.
 */
static List *
MasterTargetList(List *workerTargetList)
{
	List *masterTargetList = NIL;
	const Index tableId = 1;
	AttrNumber columnId = 1;

	ListCell *workerTargetCell = NULL;
	foreach(workerTargetCell, workerTargetList)
	{
		TargetEntry *workerTargetEntry = (TargetEntry *) lfirst(workerTargetCell);
		TargetEntry *masterTargetEntry = copyObject(workerTargetEntry);

		Var *masterColumn = makeVarFromTargetEntry(tableId, workerTargetEntry);
		masterColumn->varattno = columnId;
		masterColumn->varoattno = columnId;
		columnId++;

		/*
		 * The master target entry has two pieces to it. The first piece is the
		 * target entry's expression, which we set to the newly created column.
		 * The second piece is sort and group clauses that we implicitly copy
		 * from the worker target entry. Note that any changes to worker target
		 * entry's sort and group clauses will *break* us here.
		 */
		masterTargetEntry->expr = (Expr *) masterColumn;
		masterTargetList = lappend(masterTargetList, masterTargetEntry);
	}

	return masterTargetList;
}


/*
 * BuildCreateStatement builds the executable create statement for creating a
 * temporary table on the master; and then returns this create statement. This
 * function obtains the needed column type information from the target list.
 */
static CreateStmt *
BuildCreateStatement(char *masterTableName, List *masterTargetList,
					 List *masterColumnNameList)
{
	CreateStmt *createStatement = NULL;
	RangeVar *relation = NULL;
	char *relationName = NULL;
	List *columnTypeList = NIL;
	List *columnDefinitionList = NIL;
	ListCell *masterTargetCell = NULL;

	/* build rangevar object for temporary table */
	relationName = masterTableName;
	relation = makeRangeVar(NULL, relationName, -1);
	relation->relpersistence = RELPERSISTENCE_TEMP;

	/* build the list of column types as cstrings */
	foreach(masterTargetCell, masterTargetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(masterTargetCell);
		Var *column = (Var *) targetEntry->expr;
		Oid columnTypeId = exprType((Node *) column);
		int32 columnTypeMod = exprTypmod((Node *) column);

		char *columnTypeName = format_type_with_typemod(columnTypeId, columnTypeMod);
		columnTypeList = lappend(columnTypeList, columnTypeName);
	}

	/* build the column definition list */
	columnDefinitionList = ColumnDefinitionList(masterColumnNameList, columnTypeList);

	/* build the create statement */
	createStatement = CreateStatement(relation, columnDefinitionList);

	return createStatement;
}


/*
 * BuildAggregatePlan creates and returns an aggregate plan. This aggregate plan
 * builds aggreation and grouping operators (if any) that are to be executed on
 * the master node.
 */
static Agg *
BuildAggregatePlan(Query *masterQuery, Plan *subPlan)
{
	Agg *aggregatePlan = NULL;
	AggStrategy aggregateStrategy = AGG_PLAIN;
	AggClauseCosts aggregateCosts;
	AttrNumber *groupColumnIdArray = NULL;
	List *aggregateTargetList = NIL;
	List *groupColumnList = NIL;
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	Oid *groupColumnOpArray = NULL;
	uint32 groupColumnCount = 0;
	const long rowEstimate = 10;

	/* assert that we need to build an aggregate plan */
	Assert(masterQuery->hasAggs || masterQuery->groupClause);

	aggregateTargetList = masterQuery->targetList;
	count_agg_clauses(NULL, (Node *) aggregateTargetList, &aggregateCosts);

	/*
	 * For upper level plans above the sequential scan, the planner expects the
	 * table id (varno) to be set to OUTER_VAR.
	 */
	columnList = pull_var_clause_default((Node *) aggregateTargetList);
	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		column->varno = OUTER_VAR;
	}

	groupColumnList = masterQuery->groupClause;
	groupColumnCount = list_length(groupColumnList);

	/* if we have grouping, then initialize appropriate information */
	if (groupColumnCount > 0)
	{
		if (!grouping_is_hashable(groupColumnList))
		{
			ereport(ERROR, (errmsg("grouped column list cannot be hashed")));
		}

		/* switch to hashed aggregate strategy to allow grouping */
		aggregateStrategy = AGG_HASHED;

		/* get column indexes that are being grouped */
		groupColumnIdArray = extract_grouping_cols(groupColumnList, subPlan->targetlist);
		groupColumnOpArray = extract_grouping_ops(groupColumnList);
	}

	/* finally create the plan */
#if (PG_VERSION_NUM >= 90500)
	aggregatePlan = make_agg(NULL, aggregateTargetList, NIL, aggregateStrategy,
							 &aggregateCosts, groupColumnCount, groupColumnIdArray,
							 groupColumnOpArray, NIL, rowEstimate, subPlan);
#else
	aggregatePlan = make_agg(NULL, aggregateTargetList, NIL, aggregateStrategy,
							 &aggregateCosts, groupColumnCount, groupColumnIdArray,
							 groupColumnOpArray, rowEstimate, subPlan);
#endif

	return aggregatePlan;
}


/*
 * BuildSelectStatement builds the final select statement to run on the master
 * node, before returning results to the user. The function first builds a scan
 * statement for all results fetched to the master, and layers aggregation, sort
 * and limit plans on top of the scan statement if necessary.
 */
static PlannedStmt *
BuildSelectStatement(Query *masterQuery, char *masterTableName,
					 List *masterTargetList)
{
	PlannedStmt *selectStatement = NULL;
	RangeTblEntry *rangeTableEntry = NULL;
	RangeTblEntry *queryRangeTableEntry = NULL;
	SeqScan *sequentialScan = NULL;
	Agg *aggregationPlan = NULL;
	Plan *topLevelPlan = NULL;

	/* (1) make PlannedStmt and set basic information */
	selectStatement = makeNode(PlannedStmt);
	selectStatement->canSetTag = true;
	selectStatement->relationOids = NIL; /* to be filled in exec_Start */
	selectStatement->commandType = CMD_SELECT;

	/* prepare the range table entry for our temporary table */
	Assert(list_length(masterQuery->rtable) == 1);
	queryRangeTableEntry = (RangeTblEntry *) linitial(masterQuery->rtable);

	rangeTableEntry = copyObject(queryRangeTableEntry);
	rangeTableEntry->rtekind = RTE_RELATION;
	rangeTableEntry->eref = makeAlias(masterTableName, NIL);
	rangeTableEntry->relid = 0; /* to be filled in exec_Start */
	rangeTableEntry->inh = false;
	rangeTableEntry->inFromCl = true;

	/* set the single element range table list */
	selectStatement->rtable = list_make1(rangeTableEntry);

	/* (2) build and initialize sequential scan node */
	sequentialScan = makeNode(SeqScan);
	sequentialScan->scanrelid = 1;  /* always one */

	/* (3) add an aggregation plan if needed */
	if (masterQuery->hasAggs || masterQuery->groupClause)
	{
		sequentialScan->plan.targetlist = masterTargetList;

		aggregationPlan = BuildAggregatePlan(masterQuery, (Plan *) sequentialScan);
		topLevelPlan = (Plan *) aggregationPlan;
	}
	else
	{
		/* otherwise set the final projections on the scan plan directly */
		sequentialScan->plan.targetlist = masterQuery->targetList;
		topLevelPlan = (Plan *) sequentialScan;
	}

	/* (4) add a sorting plan if needed */
	if (masterQuery->sortClause)
	{
		List *sortClauseList = masterQuery->sortClause;
		Sort *sortPlan = make_sort_from_sortclauses(NULL, sortClauseList, topLevelPlan);
		topLevelPlan = (Plan *) sortPlan;
	}

	/* (5) add a limit plan if needed */
	if (masterQuery->limitCount)
	{
		Node *limitCount = masterQuery->limitCount;
		Node *limitOffset = masterQuery->limitOffset;
		int64 offsetEstimate = 0;
		int64 countEstimate = 0;

		Limit *limitPlan = make_limit(topLevelPlan, limitOffset, limitCount,
									  offsetEstimate, countEstimate);
		topLevelPlan = (Plan *) limitPlan;
	}

	/* (6) finally set our top level plan in the plan tree */
	selectStatement->planTree = topLevelPlan;

	return selectStatement;
}


/*
 * ValueToStringList walks over the given list of string value types, converts
 * value types to cstrings, and adds these cstrings into a new list.
 */
static List *
ValueToStringList(List *valueList)
{
	List *stringList = NIL;
	ListCell *valueCell = NULL;

	foreach(valueCell, valueList)
	{
		Value *value = (Value *) lfirst(valueCell);
		char *stringValue = strVal(value);

		stringList = lappend(stringList, stringValue);
	}

	return stringList;
}


/*
 * MasterNodeCreateStatement takes in a multi plan, and constructs a statement
 * to create a temporary table on the master node for final result
 * aggregation.
 */
CreateStmt *
MasterNodeCreateStatement(MultiPlan *multiPlan)
{
	Query *masterQuery = multiPlan->masterQuery;
	Job *workerJob = multiPlan->workerJob;
	List *workerTargetList = workerJob->jobQuery->targetList;
	List *rangeTableList = masterQuery->rtable;
	char *tableName = multiPlan->masterTableName;
	CreateStmt *createStatement = NULL;

	RangeTblEntry *rangeTableEntry = (RangeTblEntry *) linitial(rangeTableList);
	List *columnNameValueList = rangeTableEntry->eref->colnames;
	List *columnNameList = ValueToStringList(columnNameValueList);
	List *targetList = MasterTargetList(workerTargetList);

	createStatement = BuildCreateStatement(tableName, targetList, columnNameList);

	return createStatement;
}


/*
 * MasterNodeSelectPlan takes in a distributed plan, finds the master node query
 * structure in that plan, and builds the final select plan to execute on the
 * master node. Note that this select plan is executed after result files are
 * retrieved from worker nodes and are merged into a temporary table.
 */
PlannedStmt *
MasterNodeSelectPlan(MultiPlan *multiPlan)
{
	Query *masterQuery = multiPlan->masterQuery;
	char *tableName = multiPlan->masterTableName;
	PlannedStmt *masterSelectPlan = NULL;

	Job *workerJob = multiPlan->workerJob;
	List *workerTargetList = workerJob->jobQuery->targetList;
	List *masterTargetList = MasterTargetList(workerTargetList);

	masterSelectPlan = BuildSelectStatement(masterQuery, tableName, masterTargetList);

	return masterSelectPlan;
}


/*
 * MasterNodeCopyStatementList takes in a multi plan, and constructs
 * statements that copy over worker task results to a temporary table on the
 * master node.
 */
List *
MasterNodeCopyStatementList(MultiPlan *multiPlan)
{
	Job *workerJob = multiPlan->workerJob;
	List *workerTaskList = workerJob->taskList;
	char *tableName = multiPlan->masterTableName;
	List *copyStatementList = NIL;

	ListCell *workerTaskCell = NULL;
	foreach(workerTaskCell, workerTaskList)
	{
		Task *workerTask = (Task *) lfirst(workerTaskCell);
		StringInfo jobDirectoryName = JobDirectoryName(workerTask->jobId);
		StringInfo taskFilename = TaskFilename(jobDirectoryName, workerTask->taskId);

		RangeVar *relation = makeRangeVar(NULL, tableName, -1);
		CopyStmt *copyStatement = makeNode(CopyStmt);
		copyStatement->relation = relation;
		copyStatement->is_from = true;
		copyStatement->filename = taskFilename->data;
		if (BinaryMasterCopyFormat)
		{
			DefElem *copyOption = makeDefElem("format", (Node *) makeString("binary"));
			copyStatement->options = list_make1(copyOption);
		}
		else
		{
			copyStatement->options = NIL;
		}

		copyStatementList = lappend(copyStatementList, copyStatement);
	}

	return copyStatementList;
}
