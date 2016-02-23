/*-------------------------------------------------------------------------
 *
 * transaction_manager.c
 *     This file contains functions that comprise a pluggable API for
 *     managing transactions across many worker nodes using 1PC or 2PC.
 *
 * Contributed by Konstantin Knizhnik, Postgres Professional
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/connection_cache.h"
#include "distributed/transaction_manager.h"

static bool BeginTransaction(PGconn *connection);
static bool Prepare1PC(PGconn *connection, char *transactionId);
static bool CommitPrepared1PC(PGconn *connection, char *transactionId);
static bool RollbackPrepared1PC(PGconn *connection, char *transactionId);
static bool RollbackTransaction(PGconn *connection);

static bool Prepare2PC(PGconn *connection, char *transactionId);
static bool CommitPrepared2PC(PGconn *connection, char *transactionId);
static bool RollbackPrepared2PC(PGconn *connection, char *transactionId);

static char * Build2PCCommand(char const *command, char *transactionId);

CitusTransactionManager const CitusTransactionManagerImpl[] =
{
	{ BeginTransaction, Prepare1PC, CommitPrepared1PC,
	  RollbackPrepared1PC, RollbackTransaction },
	{ BeginTransaction, Prepare2PC, CommitPrepared2PC,
	  RollbackPrepared2PC, RollbackTransaction }
};


/*
 * BeginTransaction sends a BEGIN command to start a transaction.
 */
static bool
BeginTransaction(PGconn *connection)
{
	return ExecuteCommand(connection, PGRES_COMMAND_OK, "BEGIN");
}


/*
 * Prepare1PC does nothing since 1PC mode does not have a prepare phase.
 * This function is provided for compatibility with the 2PC API.
 */
static bool
Prepare1PC(PGconn *connection, char *transactionId)
{
	return true;
}


/*
 * Commit1PC sends a COMMIT command to commit a transaction.
 */
static bool
CommitPrepared1PC(PGconn *connection, char *transactionId)
{
	return ExecuteCommand(connection, PGRES_COMMAND_OK, "COMMIT");
}


/*
 * RollbackPrepared1PC sends a ROLLBACK command to roll a transaction
 * back. This function is provided for compatibility with the 2PC API.
 */
static bool
RollbackPrepared1PC(PGconn *connection, char *transactionId)
{
	return ExecuteCommand(connection, PGRES_COMMAND_OK, "ROLLBACK");
}


/*
 * Prepare2PC sends a PREPARE TRANSACTION command to prepare a 2PC.
 */
static bool
Prepare2PC(PGconn *connection, char *transactionId)
{
	return ExecuteCommand(connection, PGRES_COMMAND_OK,
						  Build2PCCommand("PREPARE TRANSACTION", transactionId));
}


/*
 * CommitPrepared2PC sends a COMMIT TRANSACTION command to commit a 2PC.
 */
static bool
CommitPrepared2PC(PGconn *connection, char *transactionId)
{
	return ExecuteCommand(connection, PGRES_COMMAND_OK,
						  Build2PCCommand("COMMIT PREPARED", transactionId));
}


/*
 * RollbackPrepared2PC sends a COMMIT TRANSACTION command to commit a 2PC.
 */
static bool
RollbackPrepared2PC(PGconn *connection, char *transactionId)
{
	return ExecuteCommand(connection, PGRES_COMMAND_OK,
						  Build2PCCommand("ROLLBACK PREPARED", transactionId));
}


/*
 * RollbackTransaction sends a ROLLBACK command to roll a transaction back.
 */
static bool
RollbackTransaction(PGconn *connection)
{
	return ExecuteCommand(connection, PGRES_COMMAND_OK, "ROLLBACK");
}


/*
 * Build2PCCommand builds a command with a unique transaction ID for a two-phase commit.
 */
static char *
Build2PCCommand(char const *command, char *transactionId)
{
	StringInfo commandString = makeStringInfo();

	appendStringInfo(commandString, "%s '%s'", transactionId);

	return commandString->data;
}


/*
 * BuildTransactionId helps users construct a unique transaction id from an
 * application-specific id.
 */
char *
BuildTransactionId(int localId)
{
	StringInfo commandString = makeStringInfo();

	appendStringInfo(commandString, "citus_%d_%u_%d", MyProcPid,
									GetCurrentTransactionId(), localId);

	return commandString->data;
}


/*
 * ExecuteCommand executes a statement on a remote node and checks its result.
 */
bool
ExecuteCommand(PGconn *connection, ExecStatusType expectedResult, char const *command)
{
	bool ret = true;
	PGresult *result = PQexec(connection, command);
	if (PQresultStatus(result) != expectedResult)
	{
		ReportRemoteError(connection, result);
		ret = false;
	}
	PQclear(result);
	return ret;
}
