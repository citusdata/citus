/*-------------------------------------------------------------------------
 *
 * remote_commands.h
 *	  Helpers to execute commands on remote nodes, over libpq.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REMOTE_COMMAND_H
#define REMOTE_COMMAND_H

#include "distributed/connection_management.h"
#include "distributed/placement_connection.h"


struct pg_result; /* target of the PGresult typedef */
struct Query;

typedef struct BatchCommand
{
	/* user input fields */
	struct ShardPlacement *placement;
	uint32 connectionFlags;
	void *userData;
	const char *commandString;

	/* user output fields */
	bool failed;
	int64 tuples;

	/* internal fields */
	MultiConnection *connection;
} BatchCommand;


/* GUC, determining whether statements sent to remote nodes are logged */
extern bool LogRemoteCommands;


/* simple helpers */
extern bool IsResponseOK(struct pg_result *result);
extern void ForgetResults(MultiConnection *connection);
extern bool SqlStateMatchesCategory(char *sqlStateString, int category);

/* report errors & warnings */
extern void ReportConnectionError(MultiConnection *connection, int elevel);
extern void ReportResultError(MultiConnection *connection, struct pg_result *result,
							  int elevel);
extern void LogRemoteCommand(MultiConnection *connection, const char *command);

/* wrappers around libpq functions, with command logging support */
extern int SendRemoteCommand(MultiConnection *connection, const char *command);


/* libpq helpers */
extern struct pg_result * ExecuteStatement(MultiConnection *connection, const
										   char *statement);
extern struct pg_result * ExecuteStatementParams(MultiConnection *connection,
												 const char *statement,
												 int paramCount, const Oid *paramTypes,
												 const char *const *paramValues);
extern bool ExecuteCheckStatement(MultiConnection *connection, const char *statement);
extern bool ExecuteCheckStatementParams(MultiConnection *connection,
										const char *statement,
										int paramCount, const Oid *paramTypes,
										const char *const *paramValues);


/* higher level command execution helpers */
extern void ExecuteBatchCommands(List *batchCommandList);
extern int64 ExecuteQueryOnPlacements(struct Query *query, List *shardPlacementList,
									  Oid relationId);
extern void ExecuteDDLOnRelationPlacements(Oid relationId, const char *command);
extern void InvalidateFailedPlacements(List *batchCommandList);


#endif /* REMOTE_COMMAND_H */
