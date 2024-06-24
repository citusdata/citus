/*-------------------------------------------------------------------------
 *
 * variableset.c
 *    Support for propagation of SET (commands to set variables)
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"

#include "common/string.h"
#include "lib/ilist.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"

/*
 * ShouldPropagateSetCommand determines whether a SET or RESET command should be
 * propagated to the workers.
 *
 * We currently propagate:
 * - SET LOCAL (for allowed settings)
 * - SET TRANSACTION
 * - RESET (for allowed settings)
 * - RESET ALL
 */
bool
ShouldPropagateSetCommand(VariableSetStmt *setStmt)
{
	if (PropagateSetCommands != PROPSETCMD_LOCAL)
	{
		/* SET propagation is disabled */
		return false;
	}

	switch (setStmt->kind)
	{
		case VAR_SET_VALUE:
		case VAR_SET_CURRENT:
		case VAR_SET_DEFAULT:
		{
			/* SET LOCAL on a safe setting */
			return setStmt->is_local && IsSettingSafeToPropagate(setStmt->name);
		}

		case VAR_RESET:
		{
			/* may need to reset prior SET LOCAL */
			return IsSettingSafeToPropagate(setStmt->name);
		}

		case VAR_RESET_ALL:
		{
			/* always propagate RESET ALL since it might affect prior SET LOCALs */
			return true;
		}

		case VAR_SET_MULTI:
		default:
		{
			/* SET TRANSACTION is similar to SET LOCAL */
			return strcmp(setStmt->name, "TRANSACTION") == 0;
		}
	}
}


/*
 * PostprocessVariableSetStmt actually does the work of propagating a provided SET stmt
 * to currently-participating worker nodes and adding the SET command test to a string
 * keeping track of all propagated SET commands since (sub-)xact start.
 */
void
PostprocessVariableSetStmt(VariableSetStmt *setStmt, const char *setStmtString)
{
	dlist_iter iter;
	const bool raiseInterrupts = true;
	List *connectionList = NIL;

	/* at present we only support SET LOCAL and SET TRANSACTION */
	Assert(ShouldPropagateSetCommand(setStmt));

	/* haven't seen any SET stmts so far in this (sub-)xact: initialize StringInfo */
	if (activeSetStmts == NULL)
	{
		/* see comments in PushSubXact on why we allocate this in TopTransactionContext */
		MemoryContext old_context = MemoryContextSwitchTo(TopTransactionContext);
		activeSetStmts = makeStringInfo();
		MemoryContextSwitchTo(old_context);
	}

	/* send text of SET stmt to participating nodes... */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);

		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			continue;
		}

		if (!SendRemoteCommand(connection, setStmtString))
		{
			const bool raiseErrors = true;
			HandleRemoteTransactionConnectionError(connection, raiseErrors);
		}

		connectionList = lappend(connectionList, connection);
	}

	WaitForAllConnections(connectionList, raiseInterrupts);

	/* ... and wait for the results */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		const bool raiseErrors = true;

		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			continue;
		}

		ClearResults(connection, raiseErrors);
	}

	/* SET propagation successful: add to active SET stmt string */
	appendStringInfoString(activeSetStmts, setStmtString);

	/* ensure semicolon on end to allow appending future SET stmts */
	if (!pg_str_endswith(setStmtString, ";"))
	{
		appendStringInfoChar(activeSetStmts, ';');
	}
}
