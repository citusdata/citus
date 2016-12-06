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


#endif /* REMOTE_COMMAND_H */
