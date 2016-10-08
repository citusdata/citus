/*-------------------------------------------------------------------------
 *
 * transaction_management.c
 *
 *   Transaction management for Citus.  Most of the work is delegated to other
 *   subsystems, this files, and especially CoordinatedTransactionCallback,
 *   coordinates the work between them.
 *
 *
 *   The standard pattern to perform work spanning this and remote nodes, is to:
 *
 *   1) Call BeginOrContinueCoordinatedTransaction(). This signals that work
 *      on remote nodes should be done inside explicit transactions. If that's
 *      not desired, e.g. inside router executor, this step should be skipped.
 *
 *   2) Acquire a connection to either the remote node (using
 *      GetNodeConnection() or similar) or one associated with a placement
 *      (using GetPlacementConnection() or similar). Always use the latter
 *      when performing work associated with a placement.  Use the
 *      FOR_DML/FOR_DDL flags if appropriate.
 *
 *   3) Call AdjustRemoteTransactionState() or AdjustRemoteTransactionStates()
 *      on all connections used. The latter should be used if multiple
 *      connections are in use, since it is considerably faster.
 *
 *   4) Perform work on the connection, either using MultiConnection->conn
 *      directly via libpq, or using some of the remote_command.h helpers.
 *
 *   5) Done.  If the local transaction commits/aborts, the remote
 *      transaction(s) are going to be committed/aborted as well.  If a
 *      placement has been modified (DML or DDL flag to
 *      GetPlacementConnnection()) and the remote transaction failed,
 *      placements will be marked as invalid, or the entire transaction will
 *      be aborted, as appropriate.
 *
 *
 *   This subsystem delegates work to several subsystems:
 *   - connection lifecycle management is handled in connection_management.[ch]
 *   - transaction on remote nodes are managed via remote_transaction.[ch]
 *   - per-placement visibility, locking and invalidation resides in
 *     placement_connection.[ch]
 *   - simple and complex commands on other nodes can be executed via
 *     remote_commands.[ch]
 *
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/transaction_management.h"
#include "distributed/placement_connection.h"
#include "utils/hsearch.h"


CoordinatedTransactionState CurrentCoordinatedTransactionState = COORD_TRANS_NONE;

/* GUC, the commit protocol to use for commands affecting more than one connection */
int MultiShardCommitProtocol = COMMIT_PROTOCOL_1PC;

/* state needed to keep track of operations used during a transaction */
XactModificationType XactModificationLevel = XACT_MODIFICATION_NONE;


static bool subXactAbortAttempted = false;


/* transaction management functions */
static void CoordinatedTransactionCallback(XactEvent event, void *arg);
static void CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
											  SubTransactionId parentSubid, void *arg);


/*
 * BeginCoordinatedTransaction begins a coordinated transaction. No
 * pre-existing coordinated transaction may be in progress.
 */
void
BeginCoordinatedTransaction(void)
{
	if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE &&
		CurrentCoordinatedTransactionState != COORD_TRANS_IDLE)
	{
		ereport(ERROR, (errmsg("starting transaction in wrong state")));
	}

	CurrentCoordinatedTransactionState = COORD_TRANS_STARTED;
}


/*
 * BeginOrContinueCoordinatedTransaction starts a coordinated transaction,
 * unless one already is in progress.
 */
void
BeginOrContinueCoordinatedTransaction(void)
{
	if (CurrentCoordinatedTransactionState == COORD_TRANS_STARTED)
	{
		return;
	}

	BeginCoordinatedTransaction();
}


/*
 * InCoordinatedTransaction returns whether a coordinated transaction has been
 * started.
 */
bool
InCoordinatedTransaction(void)
{
	return CurrentCoordinatedTransactionState != COORD_TRANS_NONE &&
		   CurrentCoordinatedTransactionState != COORD_TRANS_IDLE;
}


void
InitializeTransactionManagement(void)
{
	/* hook into transaction machinery */
	RegisterXactCallback(CoordinatedTransactionCallback, NULL);
	RegisterSubXactCallback(CoordinatedSubTransactionCallback, NULL);
}


/*
 * Transaction management callback, handling coordinated transaction, and
 * transaction independent connection management.
 *
 * NB: There should only ever be a single transaction callback in citus, the
 * ordering between the callbacks and thee actions within those callbacks
 * otherwise becomes too undeterministic / hard to reason about.
 */
static void
CoordinatedTransactionCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		{
			if (CurrentCoordinatedTransactionState == COORD_TRANS_PREPARED)
			{
				/* handles both already prepared and open transactions */
				CoordinatedRemoteTransactionsCommit();
			}

			/* close connections etc. */
			if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE)
			{
				ResetPlacementConnectionManagement();
				AtEOXact_Connections(true);
			}

			Assert(!subXactAbortAttempted);
			CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
		}
		break;

		case XACT_EVENT_ABORT:
		{
			/*
			 * FIXME: Add warning for the COORD_TRANS_COMMITTED case. That
			 * can be reached if this backend fails after the
			 * XACT_EVENT_PRE_COMMIT state.
			 */

			/* handles both already prepared and open transactions */
			if (CurrentCoordinatedTransactionState > COORD_TRANS_IDLE)
			{
				CoordinatedRemoteTransactionsAbort();
			}

			/* close connections etc. */
			if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE)
			{
				ResetPlacementConnectionManagement();
				AtEOXact_Connections(false);
			}

			CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			subXactAbortAttempted = false;
		}
		break;

		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
		{ }
		  break;

		case XACT_EVENT_PRE_COMMIT:
		{
			bool using2PC = MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC;

			if (subXactAbortAttempted)
			{
				subXactAbortAttempted = false;

				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot ROLLBACK TO SAVEPOINT in transactions "
									   "which modify distributed tables")));
			}

			/* nothing further to do if there's no managed remote xacts */
			if (CurrentCoordinatedTransactionState == COORD_TRANS_NONE)
			{
				break;
			}

			/*
			 * TODO: It's probably a good idea to force constraints and
			 * such to 'immediate' here. Deferred triggers might try to
			 * send stuff to the remote side, which'd not be good.  Doing
			 * so remotely would also catch a class of errors where
			 * committing fails, which can lead to divergence when not
			 * using 2PC.
			 */

			/*
			 * Check whether the coordinated transaction is in a state we want
			 * to persist, or whether we want to error out.  This handles the
			 * case that iteratively executed commands marked all placements
			 * as invalid.
			 */
			CheckForFailedPlacements(true, using2PC);

			if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
			{
				CoordinatedRemoteTransactionsPrepare();
				CurrentCoordinatedTransactionState = COORD_TRANS_PREPARED;
			}
			else
			{
				/*
				 * Have to commit remote transactions in PRE_COMMIT, to allow
				 * us to mark failed placements as invalid.  Better don't use
				 * this for anything important (i.e. DDL/metadata).
				 */
				CoordinatedRemoteTransactionsCommit();
				CurrentCoordinatedTransactionState = COORD_TRANS_COMMITTED;
			}

			/*
			 * Check again whether shards/placement successfully
			 * committed. This handles failure at COMMIT/PREPARE time.
			 */
			CheckForFailedPlacements(false, using2PC);
		}
		break;

		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		{
			/*
			 * FIXME: do we want to support this? Or error out? Might be
			 * annoying to error out as it could prevent experimentation. If
			 * we error out, we should only do so if a coordinated transaction
			 * has been started, so independent 2PC usage doesn't cause
			 * errors.
			 */
		}
		break;
	}
}


/*
 * Subtransaction callback - currently only used to remember whether a
 * savepoint has been rolled back, as we don't support that.
 */
static void
CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
								  SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		subXactAbortAttempted = true;
	}
}
