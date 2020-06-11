/*-------------------------------------------------------------------------
 *
 * multi_executor.h
 *	  Executor support for Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_EXECUTOR_H
#define MULTI_EXECUTOR_H

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"


/* managed via guc.c */
typedef enum
{
	PARALLEL_CONNECTION = 0,
	SEQUENTIAL_CONNECTION = 1
} MultiShardConnectionTypes;

/*
 * TransactionBlocksUsage indicates whether to use remote transaction
 * blocks according to one of the following policies:
 * - opening a remote transaction is required
 * - opening a remote transaction does not matter, so it is allowed but not required.
 * - opening a remote transaction is disallowed
 */
typedef enum TransactionBlocksUsage
{
	TRANSACTION_BLOCKS_REQUIRED,
	TRANSACTION_BLOCKS_ALLOWED,
	TRANSACTION_BLOCKS_DISALLOWED,
} TransactionBlocksUsage;

/*
 * TransactionProperties reflects how we should execute a task list
 * given previous commands in the transaction and the type of task list.
 */
typedef struct TransactionProperties
{
	/* if true, any failure on the worker causes the execution to end immediately */
	bool errorOnAnyFailure;

	/*
	 * Determines whether transaction blocks on workers are required, disallowed, or
	 * allowed (will use them if already in a coordinated transaction).
	 */
	TransactionBlocksUsage useRemoteTransactionBlocks;

	/* if true, the current execution requires 2PC to be globally enabled */
	bool requires2PC;
} TransactionProperties;


extern int MultiShardConnectionType;
extern bool WritableStandbyCoordinator;
extern bool ForceMaxQueryParallelization;
extern int MaxAdaptiveExecutorPoolSize;
extern int ExecutorSlowStartInterval;
extern bool SortReturning;
extern int ExecutorLevel;


extern void CitusExecutorStart(QueryDesc *queryDesc, int eflags);
extern void CitusExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
							 bool execute_once);
extern void AdaptiveExecutorPreExecutorRun(CitusScanState *scanState);
extern TupleTableSlot * AdaptiveExecutor(CitusScanState *scanState);

/*
 * ExecutionParams contains parameters that are used during the execution.
 * Some of these can be the zero value if it is not needed during the execution.
 */
typedef struct ExecutionParams
{
	/* modLevel is the access level for rows.*/
	RowModifyLevel modLevel;

	/* taskList contains the tasks for the execution.*/
	List *taskList;

	/* tupleDescriptor contains the description for the result tuples.*/
	TupleDesc tupleDescriptor;

	/* tupleStore is where the results will be stored for this execution */
	Tuplestorestate *tupleStore;

	/* expectResults is true if this execution will return some result. */
	bool expectResults;

	/* targetPoolSize is the maximum amount of connections per worker */
	int targetPoolSize;

	/* xactProperties contains properties for transactions, such as if we should use 2pc. */
	TransactionProperties xactProperties;

	/* jobIdList contains all job ids for the execution */
	List *jobIdList;

	/* localExecutionSupported is true if we can use local execution, if it is false
	 * local execution will not be used. */
	bool localExecutionSupported;

	/* isUtilityCommand is true if the current execution is for a utility
	 * command such as a DDL command.*/
	bool isUtilityCommand;
} ExecutionParams;

ExecutionParams * CreateBasicExecutionParams(RowModifyLevel modLevel,
											 List *taskList,
											 int targetPoolSize,
											 bool localExecutionSupported);

extern uint64 ExecuteTaskListExtended(ExecutionParams *executionParams);
extern uint64 ExecuteTaskListIntoTupleStore(RowModifyLevel modLevel, List *taskList,
											TupleDesc tupleDescriptor,
											Tuplestorestate *tupleStore,
											bool expectResults);
extern bool IsCitusCustomState(PlanState *planState);
extern TupleTableSlot * CitusExecScan(CustomScanState *node);
extern TupleTableSlot * ReturnTupleFromTuplestore(CitusScanState *scanState);
extern void LoadTuplesIntoTupleStore(CitusScanState *citusScanState, Job *workerJob);
extern void ReadFileIntoTupleStore(char *fileName, char *copyFormat, TupleDesc
								   tupleDescriptor, Tuplestorestate *tupstore);
extern Query * ParseQueryString(const char *queryString, Oid *paramOids, int numParams);
extern Query * RewriteRawQueryStmt(RawStmt *rawStmt, const char *queryString,
								   Oid *paramOids, int numParams);
extern void ExecuteQueryStringIntoDestReceiver(const char *queryString, ParamListInfo
											   params,
											   DestReceiver *dest);
extern void ExecuteQueryIntoDestReceiver(Query *query, ParamListInfo params,
										 DestReceiver *dest);
extern void ExecutePlanIntoDestReceiver(PlannedStmt *queryPlan, ParamListInfo params,
										DestReceiver *dest);
extern void SetLocalMultiShardModifyModeToSequential(void);
extern void SetLocalForceMaxQueryParallelization(void);
extern void SortTupleStore(CitusScanState *scanState);
extern bool DistributedPlanModifiesDatabase(DistributedPlan *plan);
extern bool ReadOnlyTask(TaskType taskType);
extern void ExtractParametersFromParamList(ParamListInfo paramListInfo,
										   Oid **parameterTypes,
										   const char ***parameterValues, bool
										   useOriginalCustomTypeOids);


#endif /* MULTI_EXECUTOR_H */
