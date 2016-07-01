/*-------------------------------------------------------------------------
 *
 * multi_executor.c
 *
 * Entrypoint into distributed query execution.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_utility.h"
#include "distributed/worker_protocol.h"
#include "executor/execdebug.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"


static void CopyQueryResults(List *masterCopyStmtList);


/*
 * multi_ExecutorStart is a hook called at at the beginning of any execution
 * of any query plan.
 *
 * If a distributed relation is the target of the query, perform some validity
 * checks. If a legal statement, start the distributed execution. After that
 * the to-be-executed query is replaced with the portion executing solely on
 * the master.
 */
void
multi_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;

	if (HasCitusToplevelNode(planStatement))
	{
		MultiPlan *multiPlan = GetMultiPlan(planStatement);
		MultiExecutorType executorType = MULTI_EXECUTOR_INVALID_FIRST;
		Job *workerJob = multiPlan->workerJob;

		ExecCheckRTPerms(planStatement->rtable, true);

		executorType = JobExecutorType(multiPlan);
		if (executorType == MULTI_EXECUTOR_ROUTER)
		{
			Task *task = NULL;
			List *taskList = workerJob->taskList;
			TupleDesc tupleDescriptor = ExecCleanTypeFromTL(
				planStatement->planTree->targetlist, false);
			List *dependendJobList PG_USED_FOR_ASSERTS_ONLY = workerJob->dependedJobList;

			/* router executor can only execute distributed plans with a single task */
			Assert(list_length(taskList) == 1);
			Assert(dependendJobList == NIL);

			task = (Task *) linitial(taskList);

			/* we need to set tupleDesc in executorStart */
			queryDesc->tupDesc = tupleDescriptor;

			/* drop into the router executor */
			RouterExecutorStart(queryDesc, eflags, task);
		}
		else
		{
			PlannedStmt *masterSelectPlan = MasterNodeSelectPlan(multiPlan);
			CreateStmt *masterCreateStmt = MasterNodeCreateStatement(multiPlan);
			List *masterCopyStmtList = MasterNodeCopyStatementList(multiPlan);
			RangeTblEntry *masterRangeTableEntry = NULL;
			StringInfo jobDirectoryName = NULL;

			/*
			 * We create a directory on the master node to keep task execution results.
			 * We also register this directory for automatic cleanup on portal delete.
			 */
			jobDirectoryName = JobDirectoryName(workerJob->jobId);
			CreateDirectory(jobDirectoryName);

			ResourceOwnerEnlargeJobDirectories(CurrentResourceOwner);
			ResourceOwnerRememberJobDirectory(CurrentResourceOwner, workerJob->jobId);

			/* pick distributed executor to use */
			if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
			{
				/* skip distributed query execution for EXPLAIN commands */
			}
			else if (executorType == MULTI_EXECUTOR_REAL_TIME)
			{
				MultiRealTimeExecute(workerJob);
			}
			else if (executorType == MULTI_EXECUTOR_TASK_TRACKER)
			{
				MultiTaskTrackerExecute(workerJob);
			}

			/* then create the result relation */
			ProcessUtility((Node *) masterCreateStmt,
						   "(temp table creation)",
						   PROCESS_UTILITY_QUERY,
						   NULL,
						   None_Receiver,
						   NULL);

			/* make the temporary table visible */
			CommandCounterIncrement();

			if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
			{
				CopyQueryResults(masterCopyStmtList);
			}

			/*
			 * Update the QueryDesc's snapshot so it sees the table. That's not
			 * particularly pretty, but we don't have much of a choice.  One might
			 * think we could unregister the snapshot, push a new active one,
			 * update it, register it, and be happy. That only works if it's only
			 * registered once though...
			 */
			queryDesc->snapshot->curcid = GetCurrentCommandId(false);

			/*
			 * Set the OID of the RTE used in the master select statement to point
			 * to the now created (and filled) temporary table. The target
			 * relation's oid is only known now.
			 */
			masterRangeTableEntry =
				(RangeTblEntry *) linitial(masterSelectPlan->rtable);
			masterRangeTableEntry->relid =
				RelnameGetRelid(masterRangeTableEntry->eref->aliasname);

			/*
			 * Replace to-be-run query with the master select query. As the
			 * planned statement is now replaced we can't call GetMultiPlan() in
			 * the later hooks, so we set a flag marking this as a distributed
			 * statement running on the master. That e.g. allows us to drop the
			 * temp table later.
			 *
			 * We copy the original statement's queryId, to allow
			 * pg_stat_statements and similar extension to associate the
			 * statement with the toplevel statement.
			 */
			masterSelectPlan->queryId = queryDesc->plannedstmt->queryId;
			queryDesc->plannedstmt = masterSelectPlan;

			eflags |= EXEC_FLAG_CITUS_MASTER_SELECT;
		}
	}

	/* if the execution is not done for router executor, drop into standard executor */
	if (queryDesc->estate == NULL ||
		!(queryDesc->estate->es_top_eflags & EXEC_FLAG_CITUS_ROUTER_EXECUTOR))
	{
		standard_ExecutorStart(queryDesc, eflags);
	}
}


/*
 * CopyQueryResults executes the commands that copy query results into a
 * temporary table.
 */
static void
CopyQueryResults(List *masterCopyStmtList)
{
	ListCell *masterCopyStmtCell = NULL;

	/* now copy data from all the remote nodes into temp table */
	foreach(masterCopyStmtCell, masterCopyStmtList)
	{
		Node *masterCopyStmt = (Node *) lfirst(masterCopyStmtCell);

		Assert(IsA(masterCopyStmt, CopyStmt));

		ProcessUtility(masterCopyStmt,
					   "(copy job)",
					   PROCESS_UTILITY_QUERY,
					   NULL,
					   None_Receiver,
					   NULL);
	}

	/* make the copied contents visible */
	CommandCounterIncrement();
}


/* Execute query plan. */
void
multi_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	int eflags = queryDesc->estate->es_top_eflags;

	if (eflags & EXEC_FLAG_CITUS_ROUTER_EXECUTOR)
	{
		Task *task = NULL;
		PlannedStmt *planStatement = queryDesc->plannedstmt;
		MultiPlan *multiPlan = GetMultiPlan(planStatement);
		List *taskList = multiPlan->workerJob->taskList;

		/* router executor can only execute distributed plans with a single task */
		Assert(list_length(taskList) == 1);

		task = (Task *) linitial(taskList);

		/* drop into the router executor */
		RouterExecutorRun(queryDesc, direction, count, task);
	}
	else
	{
		/* drop into the standard executor */
		standard_ExecutorRun(queryDesc, direction, count);
	}
}


/* Perform actions, like e.g. firing triggers, after the query has run. */
void
multi_ExecutorFinish(QueryDesc *queryDesc)
{
	int eflags = queryDesc->estate->es_top_eflags;

	if (eflags & EXEC_FLAG_CITUS_ROUTER_EXECUTOR)
	{
		/* drop into the router executor */
		RouterExecutorFinish(queryDesc);
	}
	else
	{
		/* drop into the standard executor */
		standard_ExecutorFinish(queryDesc);
	}
}


/*
 * multi_ExecutorEnd is a hook called to deallocate resources used during
 * query execution.
 *
 * If the query executed was the portion of a distributed query running on the
 * master, remove the resources that were needed for distributed execution.
 */
void
multi_ExecutorEnd(QueryDesc *queryDesc)
{
	int eflags = queryDesc->estate->es_top_eflags;

	if (eflags & EXEC_FLAG_CITUS_ROUTER_EXECUTOR)
	{
		/* drop into the router executor */
		RouterExecutorEnd(queryDesc);
	}
	else
	{
		/* drop into the standard executor */
		standard_ExecutorEnd(queryDesc);
	}

	/*
	 * Final step of a distributed query is executing the master node select
	 * query. We clean up the temp tables after executing it, if we already created it.
	 */
	if (eflags & EXEC_FLAG_CITUS_MASTER_SELECT)
	{
		PlannedStmt *planStatement = queryDesc->plannedstmt;
		int savedLogMinMessages = 0;
		int savedClientMinMessages = 0;

		RangeTblEntry *rangeTableEntry = linitial(planStatement->rtable);
		Oid masterTableRelid = rangeTableEntry->relid;

		ObjectAddress masterTableObject = { InvalidOid, InvalidOid, 0 };

		masterTableObject.classId = RelationRelationId;
		masterTableObject.objectId = masterTableRelid;
		masterTableObject.objectSubId = 0;

		/*
		 * Temporarily change logging level to avoid DEBUG2 logging output by
		 * performDeletion. This avoids breaking the regression tests which
		 * use DEBUG2 logging.
		 */
		savedLogMinMessages = log_min_messages;
		savedClientMinMessages = client_min_messages;

		log_min_messages = INFO;
		client_min_messages = INFO;

		performDeletion(&masterTableObject, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

		log_min_messages = savedLogMinMessages;
		client_min_messages = savedClientMinMessages;
	}
}
