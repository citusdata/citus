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

#include "distributed/pg_version_constants.h"

#include "postgres.h"

#include "utils/relcache.h"
#include "tcop/utility.h"

#include "distributed/coordinator_protocol.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"

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
extern bool EnableDependencyCreation;
extern bool EnableCreateTypePropagation;
extern bool EnableAlterRolePropagation;
extern bool EnableAlterRoleSetPropagation;
extern bool EnableAlterDatabaseOwner;
extern int UtilityHookLevel;


/*
 * A DDLJob encapsulates the remote tasks and commands needed to process all or
 * part of a distributed DDL command. It hold the distributed relation's oid,
 * the original DDL command string (for MX DDL propagation), and a task list of
 * DDL_TASK-type Tasks to be executed.
 */
typedef struct DDLJob
{
	Oid targetRelationId;      /* oid of the target distributed relation */
	bool concurrentIndexCmd;   /* related to a CONCURRENTLY index command? */

	/*
	 * Whether to commit and start a new transaction before sending commands
	 * (only applies to CONCURRENTLY commands). This is needed for REINDEX CONCURRENTLY
	 * and CREATE INDEX CONCURRENTLY on local shards, which would otherwise
	 * get blocked waiting for the current transaction to finish.
	 */
	bool startNewTransaction;

	const char *commandString; /* initial (coordinator) DDL command string */
	List *taskList;            /* worker DDL tasks to execute */
} DDLJob;


extern void multi_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
#if PG_VERSION_NUM >= PG_VERSION_14
								 bool readOnlyTree,
#endif
								 ProcessUtilityContext context, ParamListInfo params,
								 struct QueryEnvironment *queryEnv, DestReceiver *dest,
								 QueryCompletionCompat *completionTag
								 );
extern void ProcessUtilityParseTree(Node *node, const char *queryString,
									ProcessUtilityContext context, ParamListInfo
									params,
									DestReceiver *dest,
									QueryCompletionCompat *completionTag
									);
extern void MarkInvalidateForeignKeyGraph(void);
extern void InvalidateForeignKeyGraphForDDL(void);
extern List * DDLTaskList(Oid relationId, const char *commandString);
extern List * NodeDDLTaskList(TargetWorkerSet targets, List *commands);
extern bool AlterTableInProgress(void);
extern bool DropSchemaOrDBInProgress(void);
extern void UndistributeDisconnectedCitusLocalTables(void);
extern void NotifyUtilityHookConstraintDropped(void);
extern void ResetConstraintDropped(void);
extern void ExecuteDistributedDDLJob(DDLJob *ddlJob);

/* forward declarations for sending custom commands to a distributed table */
extern DDLJob * CreateCustomDDLTaskList(Oid relationId, TableDDLCommand *command);

#endif /* MULTI_UTILITY_H */
