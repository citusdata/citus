/*-------------------------------------------------------------------------
 *
 * begin.c
 *    Processing of the BEGIN command.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"

#include "nodes/parsenodes.h"

#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/transaction_management.h"


/*
 * SaveBeginCommandProperties stores the transaction properties passed
 * via BEGIN.
 */
void
SaveBeginCommandProperties(TransactionStmt *transactionStmt)
{
	DefElem *item = NULL;

	/*
	 * This loop is similar to the one in standard_ProcessUtility.
	 *
	 * While BEGIN can be quite frequent it will rarely have options set.
	 */
	foreach_declared_ptr(item, transactionStmt->options)
	{
		A_Const *constant = (A_Const *) item->arg;

		if (strcmp(item->defname, "transaction_read_only") == 0)
		{
			if (intVal(&constant->val) == 1)
			{
				BeginXactReadOnly = BeginXactReadOnly_Enabled;
			}
			else
			{
				BeginXactReadOnly = BeginXactReadOnly_Disabled;
			}
		}
		else if (strcmp(item->defname, "transaction_deferrable") == 0)
		{
			if (intVal(&constant->val) == 1)
			{
				BeginXactDeferrable = BeginXactDeferrable_Enabled;
			}
			else
			{
				BeginXactDeferrable = BeginXactDeferrable_Disabled;
			}
		}
	}
}
