/*-------------------------------------------------------------------------
 *
 * variableset.c
 *    Support for propagation of SET (commands to set variables)
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "common/string.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "lib/ilist.h"
#include "utils/varlena.h"
#include "distributed/remote_commands.h"


/*
 * Checks whether a SET command modifies a variable which might violate assumptions
 * made by Citus. Because we generally don't want a connection to be destroyed on error,
 * and because we eagerly ensure the stack can be fully allocated (at backend startup),
 * permitting changes to these two variables seems unwise. Also, ban propagating the
 * SET command propagation setting (not for correctness, more to avoid confusion).
 */
bool
SetCommandTargetIsValid(VariableSetStmt *setStmt)
{
	/* if this list grows considerably, switch to bsearch */
	const char *blacklist[] = {
		"citus.propagate_set_commands",
		"client_encoding",
		"exit_on_error",
		"max_stack_depth"
	};
	Index idx = 0;

	for (idx = 0; idx < lengthof(blacklist); idx++)
	{
		if (pg_strcasecmp(blacklist[idx], setStmt->name))
		{
			return true;
		}
	}

	return false;
}


/*
 * ProcessVariableSetStmt actually does the work of propagating a provided SET stmt
 * to currently-participating worker nodes and adding the SET command test to a string
 * keeping track of all propagated SET commands since (sub-)xact start.
 */
void
ProcessVariableSetStmt(VariableSetStmt *setStmt, const char *setStmtString)
{
	dlist_iter iter;
	const bool raiseInterrupts = true;
	List *connectionList = NIL;

	/* at present we only support SET LOCAL */
	AssertArg(setStmt->is_local);

	/* haven't seen any SET stmts so far in this (sub-)xact: initialize StringInfo */
	if (activeSetStmts == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(CurTransactionContext);
		activeSetStmts = makeStringInfo();
		MemoryContextSwitchTo(old_context);
	}

	/* send text of SET stmt to participating nodes... */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = NULL;

		transaction = &connection->remoteTransaction;
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
		RemoteTransaction *transaction = NULL;
		const bool raiseErrors = true;

		transaction = &connection->remoteTransaction;
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
