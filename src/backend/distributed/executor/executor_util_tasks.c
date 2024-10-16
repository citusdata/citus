/*-------------------------------------------------------------------------
 *
 * executor_util_tasks.c
 *
 * Utility functions for dealing with task lists in the executor.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "distributed/executor_util.h"
#include "distributed/listutils.h"
#include "distributed/shardinterval_utils.h"


/*
 *  TaskListModifiesDatabase is a helper function for DistributedExecutionModifiesDatabase and
 *  DistributedPlanModifiesDatabase.
 */
bool
TaskListModifiesDatabase(RowModifyLevel modLevel, List *taskList)
{
	if (modLevel > ROW_MODIFY_READONLY)
	{
		return true;
	}

	/*
	 * If we cannot decide by only checking the row modify level,
	 * we should look closer to the tasks.
	 */
	if (list_length(taskList) < 1)
	{
		/* is this ever possible? */
		return false;
	}

	Task *firstTask = (Task *) linitial(taskList);

	return !ReadOnlyTask(firstTask->taskType);
}


/*
 * TaskListRequiresRollback returns true if the distributed
 * execution should start a CoordinatedTransaction. In other words, if the
 * function returns true, the execution sends BEGIN; to every connection
 * involved in the distributed execution.
 */
bool
TaskListRequiresRollback(List *taskList)
{
	int taskCount = list_length(taskList);

	if (taskCount == 0)
	{
		return false;
	}

	Task *task = (Task *) linitial(taskList);
	if (task->cannotBeExecutedInTransction)
	{
		/* vacuum, create index concurrently etc. */
		return false;
	}

	bool selectForUpdate = task->relationRowLockList != NIL;
	if (selectForUpdate)
	{
		/*
		 * Do not check SelectOpensTransactionBlock, always open transaction block
		 * if SELECT FOR UPDATE is executed inside a distributed transaction.
		 */
		return IsMultiStatementTransaction();
	}

	if (ReadOnlyTask(task->taskType))
	{
		return SelectOpensTransactionBlock &&
			   IsTransactionBlock();
	}

	if (IsMultiStatementTransaction())
	{
		return true;
	}

	if (list_length(taskList) > 1)
	{
		return true;
	}

	if (list_length(task->taskPlacementList) > 1)
	{
		/*
		 * Single DML/DDL tasks with replicated tables (including
		 * reference and non-reference tables) should require
		 * BEGIN/COMMIT/ROLLBACK.
		 */
		return true;
	}

	if (task->queryCount > 1)
	{
		/*
		 * When there are multiple sequential queries in a task
		 * we need to run those as a transaction.
		 */
		return true;
	}

	return false;
}


/*
 * TaskListRequires2PC determines whether the given task list requires 2PC.
 */
bool
TaskListRequires2PC(List *taskList)
{
	if (taskList == NIL)
	{
		return false;
	}

	Task *task = (Task *) linitial(taskList);
	if (ReadOnlyTask(task->taskType))
	{
		/* we do not trigger 2PC for ReadOnly queries */
		return false;
	}

	bool singleTask = list_length(taskList) == 1;
	if (singleTask && list_length(task->taskPlacementList) == 1)
	{
		/* we do not trigger 2PC for modifications that are:
		 *    - single task
		 *    - single placement
		 */
		return false;
	}

	/*
	 * Otherwise, all modifications are done via 2PC. This includes:
	 *    - Multi-shard commands irrespective of the replication factor
	 *    - Single-shard commands that are targeting more than one replica
	 */
	return true;
}


/*
 * TaskListCannotBeExecutedInTransaction returns true if any of the
 * tasks in the input cannot be executed in a transaction. These are
 * tasks like VACUUM or CREATE INDEX CONCURRENTLY etc.
 */
bool
TaskListCannotBeExecutedInTransaction(List *taskList)
{
	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		if (task->cannotBeExecutedInTransction)
		{
			return true;
		}
	}

	return false;
}


/*
 * SelectForUpdateOnReferenceTable returns true if the input task
 * contains a FOR UPDATE clause that locks any reference tables.
 */
bool
SelectForUpdateOnReferenceTable(List *taskList)
{
	if (list_length(taskList) != 1)
	{
		/* we currently do not support SELECT FOR UPDATE on multi task queries */
		return false;
	}

	Task *task = (Task *) linitial(taskList);
	RelationRowLock *relationRowLock = NULL;
	foreach_declared_ptr(relationRowLock, task->relationRowLockList)
	{
		Oid relationId = relationRowLock->relationId;

		if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			return true;
		}
	}

	return false;
}


/*
 * ReadOnlyTask returns true if the input task does a read-only operation
 * on the database.
 */
bool
ReadOnlyTask(TaskType taskType)
{
	switch (taskType)
	{
		case READ_TASK:
		case MAP_OUTPUT_FETCH_TASK:
		case MAP_TASK:
		case MERGE_TASK:
		{
			return true;
		}

		default:
		{
			return false;
		}
	}
}


/*
 * ModifiedTableReplicated iterates on the task list and returns true
 * if any of the tasks' anchor shard is a replicated table. We qualify
 * replicated tables as any reference table or any distributed table with
 * replication factor > 1.
 */
bool
ModifiedTableReplicated(List *taskList)
{
	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		int64 shardId = task->anchorShardId;

		if (shardId == INVALID_SHARD_ID)
		{
			continue;
		}

		if (ReferenceTableShardId(shardId))
		{
			return true;
		}

		Oid relationId = RelationIdForShard(shardId);
		if (!SingleReplicatedTable(relationId))
		{
			return true;
		}
	}

	return false;
}


/*
 * ShouldRunTasksSequentially returns true if each of the individual tasks
 * should be executed one by one. Note that this is different than
 * MultiShardConnectionType == SEQUENTIAL_CONNECTION case. In that case,
 * running the tasks across the nodes in parallel is acceptable and implemented
 * in that way.
 *
 * However, the executions that are qualified here would perform poorly if the
 * tasks across the workers are executed in parallel. We currently qualify only
 * one class of distributed queries here, multi-row INSERTs. If we do not enforce
 * true sequential execution, concurrent multi-row upserts could easily form
 * a distributed deadlock when the upserts touch the same rows.
 */
bool
ShouldRunTasksSequentially(List *taskList)
{
	if (list_length(taskList) < 2)
	{
		/* single task plans are already qualified as sequential by definition */
		return false;
	}

	/* all the tasks are the same, so we only look one */
	Task *initialTask = (Task *) linitial(taskList);
	if (initialTask->rowValuesLists != NIL)
	{
		/* found a multi-row INSERT */
		return true;
	}

	return false;
}
