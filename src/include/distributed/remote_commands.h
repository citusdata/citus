/*-------------------------------------------------------------------------
 *
 * remote_commands.h
 *	  Helpers to execute commands on remote nodes, over libpq.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REMOTE_COMMAND_H
#define REMOTE_COMMAND_H

#include "distributed/connection_management.h"

/* errors which ExecuteRemoteCommand might return */
#define RESPONSE_OKAY 0
#define QUERY_SEND_FAILED 1
#define RESPONSE_NOT_OKAY 2

/* GUC, determining whether statements sent to remote nodes are logged */
extern bool LogRemoteCommands;

/* GUC that determines the number of bytes after which remote COPY is flushed */
extern int RemoteCopyFlushThreshold;


/* simple helpers */
extern bool IsResponseOK(PGresult *result);
extern void ForgetResults(MultiConnection *connection);
extern bool ClearResults(MultiConnection *connection, bool raiseErrors);
extern bool ClearResultsDiscardWarnings(MultiConnection *connection, bool raiseErrors);
extern bool ClearResultsIfReady(MultiConnection *connection);

/* report errors & warnings */
extern void ReportConnectionError(MultiConnection *connection, int elevel);
extern void ReportResultError(MultiConnection *connection, PGresult *result,
							  int elevel);
extern char * pchomp(const char *in);
extern void LogRemoteCommand(MultiConnection *connection, const char *command);

/* wrappers around libpq functions, with command logging support */
extern void ExecuteCriticalRemoteCommandList(MultiConnection *connection,
											 List *commandList);
extern void ExecuteCriticalRemoteCommand(MultiConnection *connection,
										 const char *command);
extern int ExecuteOptionalRemoteCommand(MultiConnection *connection,
										const char *command,
										PGresult **result);
extern int SendRemoteCommand(MultiConnection *connection, const char *command);
extern int SendRemoteCommandParams(MultiConnection *connection, const char *command,
								   int parameterCount, const Oid *parameterTypes,
								   const char *const *parameterValues,
								   bool binaryResults);
extern List * ReadFirstColumnAsText(PGresult *queryResult);
extern PGresult * GetRemoteCommandResult(MultiConnection *connection,
										 bool raiseInterrupts);
extern bool PutRemoteCopyData(MultiConnection *connection, const char *buffer,
							  int nbytes);
extern bool PutRemoteCopyEnd(MultiConnection *connection, const char *errormsg);

/* waiting for multiple command results */
extern void WaitForAllConnections(List *connectionList, bool raiseInterrupts);

extern bool SendCancelationRequest(MultiConnection *connection);

#endif /* REMOTE_COMMAND_H */
