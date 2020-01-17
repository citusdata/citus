#ifndef ADAPTIVE_EXECUTOR_H
#define ADAPTIVE_EXECUTOR_H

#include "server/funcapi.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"

/* GUC, determining whether Citus opens 1 connection per task */
extern bool ForceMaxQueryParallelization;
extern int MaxAdaptiveExecutorPoolSize;

/* GUC, number of ms to wait between opening connections to the same worker */
extern int ExecutorSlowStartInterval;

extern uint64 ExecuteTaskList(RowModifyLevel modLevel, List *taskList, int
							  targetPoolSize);
extern uint64 ExecuteTaskListOutsideTransaction(RowModifyLevel modLevel, List *taskList,
												int
												targetPoolSize);

/*
 * DistributedExecution represents the execution of a distributed query
 * plan.
 */
typedef struct DistributedExecution
{
	/* the corresponding distributed plan's modLevel */
	RowModifyLevel modLevel;

	/*
	 * tasksToExecute contains all the tasks required to finish the execution, and
	 * it is the union of remoteTaskList and localTaskList. After (if any) local
	 * tasks are executed, remoteTaskList becomes equivalent of tasksToExecute.
	 */
	List *tasksToExecute;
	List *remoteTaskList;
	List *localTaskList;

	/* the corresponding distributed plan has RETURNING */
	bool hasReturning;

	/* Parameters for parameterized plans. Can be NULL. */
	ParamListInfo paramListInfo;

	/* Tuple descriptor and destination for result. Can be NULL. */
	TupleDesc tupleDescriptor;
	Tuplestorestate *tupleStore;


	/* list of workers involved in the execution */
	List *workerList;

	/* list of all connections used for distributed execution */
	List *sessionList;

	/*
	 * Flag to indiciate that the set of connections we are interested
	 * in has changed and waitEventSet needs to be rebuilt.
	 */
	bool connectionSetChanged;

	/*
	 * Flag to indiciate that the set of wait events we are interested
	 * in might have changed and waitEventSet needs to be updated.
	 *
	 * Note that we set this flag whenever we assign a value to waitFlags,
	 * but we don't check that the waitFlags is actually different from the
	 * previous value. So we might have some false positives for this flag,
	 * which is OK, because in this case ModifyWaitEvent() is noop.
	 */
	bool waitFlagsChanged;

	/*
	 * WaitEventSet used for waiting for I/O events.
	 *
	 * This could also be local to RunDistributedExecution(), but in that case
	 * we had to mark it as "volatile" to avoid PG_TRY()/PG_CATCH() issues, and
	 * cast it to non-volatile when doing WaitEventSetFree(). We thought that
	 * would make code a bit harder to read than making this non-local, so we
	 * move it here. See comments for PG_TRY() in postgres/src/include/elog.h
	 * and "man 3 siglongjmp" for more context.
	 */
	WaitEventSet *waitEventSet;

	/*
	 * The number of connections we aim to open per worker.
	 *
	 * If there are no more tasks to assigned, the actual number may be lower.
	 * If there are already more connections, the actual number may be higher.
	 */
	int targetPoolSize;

	/* total number of tasks to execute */
	int totalTaskCount;

	/* number of tasks that still need to be executed */
	int unfinishedTaskCount;

	/*
	 * Flag to indicate whether throwing errors on cancellation is
	 * allowed.
	 */
	bool raiseInterrupts;

	/* transactional properties of the current execution */
	TransactionProperties *transactionProperties;

	/* indicates whether distributed execution has failed */
	bool failed;

	/*
	 * For SELECT commands or INSERT/UPDATE/DELETE commands with RETURNING,
	 * the total number of rows received from the workers. For
	 * INSERT/UPDATE/DELETE commands without RETURNING, the total number of
	 * tuples modified.
	 *
	 * Note that for replicated tables (e.g., reference tables), we only consider
	 * a single replica's rows that are processed.
	 */
	uint64 rowsProcessed;

	/* statistics on distributed execution */
	DistributedExecutionStats *executionStats;

	/*
	 * The following fields are used while receiving results from remote nodes.
	 * We store this information here to avoid re-allocating it every time.
	 *
	 * columnArray field is reset/calculated per row, so might be useless for other
	 * contexts. The benefit of keeping it here is to avoid allocating the array
	 * over and over again.
	 */
	AttInMetadata *attributeInputMetadata;
	char **columnArray;

	/*
	 * jobIdList contains all jobs in the job tree, this is used to
	 * do cleanup for repartition queries.
	 */
	List *jobIdList;
} DistributedExecution;

bool TransactionModifiedDistributedTable(DistributedExecution *execution);

#endif /* ADAPTIVE_EXECUTOR_H */
