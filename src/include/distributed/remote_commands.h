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

/* errors which ExecuteRemoteCommand might return */
#define QUERY_SEND_FAILED 1
#define RESPONSE_NOT_OKAY 2

struct pg_result; /* target of the PGresult typedef */

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
extern void ExecuteCriticalRemoteCommand(MultiConnection *connection,
										 const char *command);
extern int ExecuteOptionalRemoteCommand(MultiConnection *connection,
										const char *command,
										struct pg_result **result);
extern int SendRemoteCommand(MultiConnection *connection, const char *command);
extern int SendRemoteCommandParams(MultiConnection *connection, const char *command,
								   int parameterCount, const Oid *parameterTypes,
								   const char *const *parameterValues);
extern List * ReadFirstColumnAsText(struct pg_result *queryResult);
extern struct pg_result * GetRemoteCommandResult(MultiConnection *connection,
												 bool raiseInterrupts);


#endif /* REMOTE_COMMAND_H */
