/*-------------------------------------------------------------------------
 *
 * transaction_management.c
 *
 *   Transaction management for Citus.  Most of the work is delegated to other
 *   subsystems, this files, and especially CoordinatedTransactionCallback,
 *   coordinates the work between them.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/twophase.h"
#include "access/xact.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/transaction_management.h"
#include "utils/hsearch.h"
#include "utils/guc.h"


CoordinatedTransactionState CurrentCoordinatedTransactionState = COORD_TRANS_NONE;

/* GUC, the commit protocol to use for commands affecting more than one connection */
int MultiShardCommitProtocol = COMMIT_PROTOCOL_1PC;

/* state needed to keep track of operations used during a transaction */
XactModificationType XactModificationLevel = XACT_MODIFICATION_NONE;

/* list of connections that are part of the current coordinated transaction */
dlist_head InProgressTransactions = DLIST_STATIC_INIT(InProgressTransactions);


static bool subXactAbortAttempted = false;

/*
 * Should this coordinated transaction use 2PC? Set by
 * CoordinatedTransactionUse2PC(), e.g. if DDL was issued and
 * MultiShardCommitProtocol was set to 2PC.
 */
static bool CurrentTransactionUse2PC = false;

/* transaction management functions */
static void CoordinatedTransactionCallback(XactEvent event, void *arg);
static void CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
											  SubTransactionId parentSubid, void *arg);

/* remaining functions */
static void AdjustMaxPreparedTransactions(void);


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


/*
 * CoordinatedTransactionUse2PC() signals that the current coordinated
 * transaction should use 2PC to commit.
 */
void
CoordinatedTransactionUse2PC(void)
{
	Assert(InCoordinatedTransaction());

	CurrentTransactionUse2PC = true;
}


void
InitializeTransactionManagement(void)
{
	/* hook into transaction machinery */
	RegisterXactCallback(CoordinatedTransactionCallback, NULL);
	RegisterSubXactCallback(CoordinatedSubTransactionCallback, NULL);

	AdjustMaxPreparedTransactions();
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
			/*
			 * Call other parts of citus that need to integrate into
			 * transaction management. Do so before doing other work, so the
			 * callbacks still can perform work if needed.
			 */
			ResetShardPlacementTransactionState();
			RouterExecutorPostCommit();

			if (CurrentCoordinatedTransactionState == COORD_TRANS_PREPARED)
			{
				/* handles both already prepared and open transactions */
				CoordinatedRemoteTransactionsCommit();
			}

			/* close connections etc. */
			if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE)
			{
				AfterXactConnectionHandling(true);
			}

			Assert(!subXactAbortAttempted);
			CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			dlist_init(&InProgressTransactions);
			CurrentTransactionUse2PC = false;
		}
		break;

		case XACT_EVENT_ABORT:
		{
			/*
			 * FIXME: Add warning for the COORD_TRANS_COMMITTED case. That
			 * can be reached if this backend fails after the
			 * XACT_EVENT_PRE_COMMIT state.
			 */

			/*
			 * Call other parts of citus that need to integrate into
			 * transaction management. Do so before doing other work, so the
			 * callbacks still can perform work if needed.
			 */
			ResetShardPlacementTransactionState();
			RouterExecutorPostCommit();

			/* handles both already prepared and open transactions */
			if (CurrentCoordinatedTransactionState > COORD_TRANS_IDLE)
			{
				CoordinatedRemoteTransactionsAbort();
			}

			/* close connections etc. */
			if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE)
			{
				AfterXactConnectionHandling(false);
			}

			CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			dlist_init(&InProgressTransactions);
			CurrentTransactionUse2PC = false;
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
			if (subXactAbortAttempted)
			{
				subXactAbortAttempted = false;

				if (XactModificationLevel != XACT_MODIFICATION_NONE)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cannot ROLLBACK TO SAVEPOINT in transactions "
										   "which modify distributed tables")));
				}
			}

			/* nothing further to do if there's no managed remote xacts */
			if (CurrentCoordinatedTransactionState == COORD_TRANS_NONE)
			{
				break;
			}


			if (CurrentTransactionUse2PC)
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
			 * Call other parts of citus that need to integrate into
			 * transaction management. Call them *after* committing/preparing
			 * the remote transactions, to allow marking shards as invalid
			 * (e.g. if the remote commit failed).
			 */
			RouterExecutorPreCommitCheck();
		}
		break;

		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		{
			if (CurrentCoordinatedTransactionState > COORD_TRANS_NONE)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot use 2PC in transactions involving "
									   "multiple servers")));
			}
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


/*
 * AdjustMaxPreparedTransactions configures the number of available prepared
 * transaction slots at startup.
 */
static void
AdjustMaxPreparedTransactions(void)
{
	/*
	 * As Citus uses 2PC internally, there always should be some available. As
	 * the default is 0, we increase it to something appropriate
	 * (connections * 2 currently).  If the user explicitly configured 2PC, we
	 * leave the configuration alone - there might have been intent behind the
	 * decision.
	 */
	if (max_prepared_xacts == 0)
	{
		char newvalue[12];

		snprintf(newvalue, sizeof(newvalue), "%d", MaxConnections * 2);

		SetConfigOption("max_prepared_transactions", newvalue, PGC_POSTMASTER,
						PGC_S_OVERRIDE);

		ereport(LOG, (errmsg("number of prepared transactions has not been "
							 "configured, overriding"),
					  errdetail("max_prepared_transactions is now set to %s",
								newvalue)));
	}
}
