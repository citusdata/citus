/*-------------------------------------------------------------------------
 *
 * utility_hook.h
 *	  Citus utility hook and related functionality.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_UTILITY_H
#define MULTI_UTILITY_H

#include "postgres.h"

#include "tcop/utility.h"
#include "utils/relcache.h"

#include "pg_version_constants.h"

#include "distributed/coordinator_protocol.h"
#include "distributed/function_call_delegation.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"

typedef enum
{
	CREATE_OBJECT_PROPAGATION_DEFERRED = 0,
	CREATE_OBJECT_PROPAGATION_AUTOMATIC = 1,
	CREATE_OBJECT_PROPAGATION_IMMEDIATE = 2
} CreateObjectPropagationOptions;

typedef enum
{
	PROPSETCMD_INVALID = -1,
	PROPSETCMD_NONE, /* do not propagate SET commands */
	PROPSETCMD_LOCAL, /* propagate SET LOCAL commands */
	PROPSETCMD_SESSION, /* propagate SET commands, but not SET LOCAL ones */
	PROPSETCMD_ALL /* propagate all SET commands */
} PropSetCmdBehavior;
extern PropSetCmdBehavior PropagateSetCommands;
extern bool EnableDDLPropagation;
extern int CreateObjectPropagationMode;
extern bool EnableCreateDatabasePropagation;
extern bool EnableCreateTypePropagation;
extern bool EnableCreateRolePropagation;
extern bool EnableAlterRolePropagation;
extern bool EnableAlterRoleSetPropagation;
extern bool EnableAlterDatabaseOwner;
extern int UtilityHookLevel;
extern bool InDelegatedProcedureCall;


/*
 * A DDLJob encapsulates the remote tasks and commands needed to process all or
 * part of a distributed DDL command. It hold the target object's address,
 * the original DDL command string (for MX DDL propagation), and a task list of
 * DDL_TASK-type Tasks to be executed.
 */
typedef struct DDLJob
{
	ObjectAddress targetObjectAddress;      /* target distributed object address */

	/*
	 * Whether to commit and start a new transaction before sending commands
	 * (only applies to CONCURRENTLY commands). This is needed for REINDEX CONCURRENTLY
	 * and CREATE INDEX CONCURRENTLY on local shards, which would otherwise
	 * get blocked waiting for the current transaction to finish.
	 */
	bool startNewTransaction;

	/*
	 * Command to run in MX nodes when metadata is synced
	 * This is usually the initial (coordinator) DDL command string
	 */
	const char *metadataSyncCommand;

	List *taskList;            /* worker DDL tasks to execute */

	/*
	 * Only applicable when any of the tasks cannot be executed in a
	 * transaction block.
	 *
	 * Controls whether to emit a warning within the utility hook in case of a
	 * failure.
	 */
	bool warnForPartialFailure;
} DDLJob;

extern ProcessUtility_hook_type PrevProcessUtility;

extern void citus_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								 bool readOnlyTree,
								 ProcessUtilityContext context, ParamListInfo params,
								 struct QueryEnvironment *queryEnv, DestReceiver *dest,
								 QueryCompletion *completionTag
								 );
extern void ProcessUtilityParseTree(Node *node, const char *queryString,
									ProcessUtilityContext context, ParamListInfo
									params,
									DestReceiver *dest,
									QueryCompletion *completionTag
									);
extern void MarkInvalidateForeignKeyGraph(void);
extern void InvalidateForeignKeyGraphForDDL(void);
extern List * DDLTaskList(Oid relationId, const char *commandString);
extern List * NontransactionalNodeDDLTaskList(TargetWorkerSet targets, List *commands,
											  bool warnForPartialFailure);
extern List * NodeDDLTaskList(TargetWorkerSet targets, List *commands);
extern bool AlterTableInProgress(void);
extern bool DropSchemaOrDBInProgress(void);
extern void UndistributeDisconnectedCitusLocalTables(void);
extern void NotifyUtilityHookConstraintDropped(void);
extern void ResetConstraintDropped(void);
extern void ExecuteDistributedDDLJob(DDLJob *ddlJob);

#endif /* MULTI_UTILITY_H */
