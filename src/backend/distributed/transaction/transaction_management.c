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
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/intermediate_results.h"
#include "distributed/local_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/transaction_management.h"
#include "distributed/placement_connection.h"
#include "distributed/subplan_execution.h"
#include "distributed/version_compat.h"
#include "utils/hsearch.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "storage/fd.h"


CoordinatedTransactionState CurrentCoordinatedTransactionState = COORD_TRANS_NONE;

/* GUC, the commit protocol to use for commands affecting more than one connection */
int MultiShardCommitProtocol = COMMIT_PROTOCOL_2PC;
int SingleShardCommitProtocol = COMMIT_PROTOCOL_2PC;
int SavedMultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

/* state needed to keep track of operations used during a transaction */
XactModificationType XactModificationLevel = XACT_MODIFICATION_NONE;

/* list of connections that are part of the current coordinated transaction */
dlist_head InProgressTransactions = DLIST_STATIC_INIT(InProgressTransactions);

/*
 * activeSetStmts keeps track of SET LOCAL statements executed within the current
 * subxact and will be set to NULL when pushing into new subxact or ending top xact.
 */
StringInfo activeSetStmts;

/*
 * Though a list, we treat this as a stack, pushing on subxact contexts whenever
 * e.g. a SAVEPOINT is executed (though this is actually performed by providing
 * PostgreSQL with a sub-xact callback). At present, the context of a subxact
 * includes a subxact identifier as well as any SET LOCAL statements propagated
 * to workers during the sub-transaction.
 */
static List *activeSubXactContexts = NIL;

/* some pre-allocated memory so we don't need to call malloc() during callbacks */
MemoryContext CommitContext = NULL;

/*
 * Should this coordinated transaction use 2PC? Set by
 * CoordinatedTransactionUse2PC(), e.g. if DDL was issued and
 * MultiShardCommitProtocol was set to 2PC.
 */
bool CoordinatedTransactionUses2PC = false;

/* if disabled, distributed statements in a function may run as separate transactions */
bool FunctionOpensTransactionBlock = true;

/* stack depth of UDF calls */
int FunctionCallLevel = 0;


/* transaction management functions */
static void BeginCoordinatedTransaction(void);
static void CoordinatedTransactionCallback(XactEvent event, void *arg);
static void CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
											  SubTransactionId parentSubid, void *arg);

/* remaining functions */
static void AdjustMaxPreparedTransactions(void);
static void PushSubXact(SubTransactionId subId);
static void PopSubXact(SubTransactionId subId);
static void SwallowErrors(void (*func)());


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

	CoordinatedTransactionUses2PC = true;
}


void
InitializeTransactionManagement(void)
{
	/* hook into transaction machinery */
	RegisterXactCallback(CoordinatedTransactionCallback, NULL);
	RegisterSubXactCallback(CoordinatedSubTransactionCallback, NULL);

	AdjustMaxPreparedTransactions();

	/* set aside 8kb of memory for use in CoordinatedTransactionCallback */
	CommitContext = AllocSetContextCreateExtended(TopMemoryContext,
												  "CommitContext",
												  8 * 1024,
												  8 * 1024,
												  8 * 1024);
}


/*
 * BeginCoordinatedTransaction begins a coordinated transaction. No
 * pre-existing coordinated transaction may be in progress./
 */
static void
BeginCoordinatedTransaction(void)
{
	if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE &&
		CurrentCoordinatedTransactionState != COORD_TRANS_IDLE)
	{
		ereport(ERROR, (errmsg("starting transaction in wrong state")));
	}

	CurrentCoordinatedTransactionState = COORD_TRANS_STARTED;

	AssignDistributedTransactionId();
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
			 * ERRORs thrown during XACT_EVENT_COMMIT will cause postgres to abort, at
			 * this point enough work has been done that it's not possible to rollback.
			 *
			 * One possible source of errors is memory allocation failures. To minimize
			 * the chance of those happening we've pre-allocated some memory in the
			 * CommitContext, it has 8kb of memory that we're allowed to use.
			 *
			 * We only do this in the COMMIT callback because:
			 * - Errors thrown in other callbacks (such as PRE_COMMIT) won't cause
			 *   crashes, they will simply cause the ABORT handler to be called.
			 * - The exception is ABORT, errors thrown there could also cause crashes, but
			 *   postgres already creates a TransactionAbortContext which performs this
			 *   trick, so there's no need for us to do it again.
			 */
			MemoryContext previousContext = CurrentMemoryContext;
			MemoryContextSwitchTo(CommitContext);

			/*
			 * Call other parts of citus that need to integrate into
			 * transaction management. Do so before doing other work, so the
			 * callbacks still can perform work if needed.
			 */
			ResetShardPlacementTransactionState();

			if (CurrentCoordinatedTransactionState == COORD_TRANS_PREPARED)
			{
				/* handles both already prepared and open transactions */
				CoordinatedRemoteTransactionsCommit();
			}

			/* close connections etc. */
			if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE)
			{
				ResetPlacementConnectionManagement();
				AfterXactConnectionHandling(true);
			}

			CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			LocalExecutionHappened = false;
			dlist_init(&InProgressTransactions);
			activeSetStmts = NULL;
			CoordinatedTransactionUses2PC = false;

			UnSetDistributedTransactionId();

			/* empty the CommitContext to ensure we're not leaking memory */
			MemoryContextSwitchTo(previousContext);
			MemoryContextReset(CommitContext);
			break;
		}

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
			{
				/*
				 * On Windows it's not possible to delete a file before you've closed all
				 * handles to it (rmdir will return success but not take effect). Since
				 * we're in an ABORT handler it's very likely that not all handles have
				 * been closed; force them closed here before running
				 * RemoveIntermediateResultsDirectory.
				 */
				AtEOXact_Files(false);
				SwallowErrors(RemoveIntermediateResultsDirectory);
			}
			ResetShardPlacementTransactionState();

			/* handles both already prepared and open transactions */
			if (CurrentCoordinatedTransactionState > COORD_TRANS_IDLE)
			{
				CoordinatedRemoteTransactionsAbort();
			}

			/* close connections etc. */
			if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE)
			{
				ResetPlacementConnectionManagement();
				AfterXactConnectionHandling(false);
			}

			CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			LocalExecutionHappened = false;
			dlist_init(&InProgressTransactions);
			activeSetStmts = NULL;
			CoordinatedTransactionUses2PC = false;
			FunctionCallLevel = 0;

			/*
			 * We should reset SubPlanLevel in case a transaction is aborted,
			 * otherwise this variable would stay +ve if the transaction is
			 * aborted in the middle of a CTE/complex subquery execution
			 * which would cause the subsequent queries to error out in
			 * case the copy size is greater than
			 * citus.max_intermediate_result_size
			 */
			SubPlanLevel = 0;
			UnSetDistributedTransactionId();
			UnsetCitusNoticeLevel();
			break;
		}

		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
		{
			break;
		}

		case XACT_EVENT_PREPARE:
		{
			/*
			 * This callback is only relevant for worker queries since
			 * distributed queries cannot be executed with 2PC, see
			 * XACT_EVENT_PRE_PREPARE.
			 *
			 * We should remove the intermediate results before unsetting the
			 * distributed transaction id. That is necessary, otherwise Citus
			 * would try to remove a non-existing folder and leak some of the
			 * existing folders that are associated with distributed transaction
			 * ids on the worker nodes.
			 */
			RemoveIntermediateResultsDirectory();

			UnSetDistributedTransactionId();
			break;
		}

		case XACT_EVENT_PRE_COMMIT:
		{
			/*
			 * If the distributed query involves 2PC, we already removed
			 * the intermediate result directory on XACT_EVENT_PREPARE. However,
			 * if not, we should remove it here on the COMMIT. Since
			 * RemoveIntermediateResultsDirectory() is idempotent, we're safe
			 * to call it here again even if the transaction involves 2PC.
			 */
			RemoveIntermediateResultsDirectory();

			/* nothing further to do if there's no managed remote xacts */
			if (CurrentCoordinatedTransactionState == COORD_TRANS_NONE)
			{
				break;
			}

			/*
			 * TODO: It'd probably be a good idea to force constraints and
			 * such to 'immediate' here. Deferred triggers might try to send
			 * stuff to the remote side, which'd not be good.  Doing so
			 * remotely would also catch a class of errors where committing
			 * fails, which can lead to divergence when not using 2PC.
			 */

			/*
			 * Check whether the coordinated transaction is in a state we want
			 * to persist, or whether we want to error out.  This handles the
			 * case where iteratively executed commands marked all placements
			 * as invalid.
			 */
			MarkFailedShardPlacements();

			if (CoordinatedTransactionUses2PC)
			{
				CoordinatedRemoteTransactionsPrepare();
				CurrentCoordinatedTransactionState = COORD_TRANS_PREPARED;

				/*
				 * Make sure we did not have any failures on connections marked as
				 * critical before committing.
				 */
				CheckRemoteTransactionsHealth();
			}
			else
			{
				CheckRemoteTransactionsHealth();

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
			PostCommitMarkFailedShardPlacements(CoordinatedTransactionUses2PC);
			break;
		}

		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		{
			if (CurrentCoordinatedTransactionState > COORD_TRANS_NONE)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot use 2PC in transactions involving "
									   "multiple servers")));
			}
			break;
		}
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
	switch (event)
	{
		/*
		 * Our subtransaction stack should be consistent with postgres' internal
		 * transaction stack. In case of subxact begin, postgres calls our
		 * callback after it has pushed the transaction into stack, so we have to
		 * do the same even if worker commands fail, so we PushSubXact() first.
		 * In case of subxact commit, callback is called before pushing subxact to
		 * the postgres transaction stack, so we call PopSubXact() after making sure
		 * worker commands didn't fail. Otherwise, Postgres would roll back that
		 * would cause us to call PopSubXact again.
		 */
		case SUBXACT_EVENT_START_SUB:
		{
			PushSubXact(subId);
			if (InCoordinatedTransaction())
			{
				CoordinatedRemoteTransactionsSavepointBegin(subId);
			}
			break;
		}

		case SUBXACT_EVENT_COMMIT_SUB:
		{
			if (InCoordinatedTransaction())
			{
				CoordinatedRemoteTransactionsSavepointRelease(subId);
			}
			PopSubXact(subId);
			break;
		}

		case SUBXACT_EVENT_ABORT_SUB:
		{
			if (InCoordinatedTransaction())
			{
				CoordinatedRemoteTransactionsSavepointRollback(subId);
			}
			PopSubXact(subId);

			UnsetCitusNoticeLevel();
			break;
		}

		case SUBXACT_EVENT_PRE_COMMIT_SUB:
		{
			/* nothing to do */
			break;
		}
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


/* PushSubXact pushes subId to the stack of active sub-transactions. */
static void
PushSubXact(SubTransactionId subId)
{
	MemoryContext old_context = MemoryContextSwitchTo(CurTransactionContext);

	/* save provided subId as well as propagated SET LOCAL stmts */
	SubXactContext *state = palloc(sizeof(SubXactContext));
	state->subId = subId;
	state->setLocalCmds = activeSetStmts;

	/* append to list and reset active set stmts for upcoming sub-xact */
	activeSubXactContexts = lcons(state, activeSubXactContexts);
	activeSetStmts = makeStringInfo();

	MemoryContextSwitchTo(old_context);
}


/* PopSubXact pops subId from the stack of active sub-transactions. */
static void
PopSubXact(SubTransactionId subId)
{
	MemoryContext old_context = MemoryContextSwitchTo(CurTransactionContext);
	SubXactContext *state = linitial(activeSubXactContexts);

	/*
	 * the previous activeSetStmts is already invalid because it's in the now-
	 * aborted subxact (what we're popping), so no need to free before assign-
	 * ing with the setLocalCmds of the popped context
	 */
	Assert(state->subId == subId);
	activeSetStmts = state->setLocalCmds;
	activeSubXactContexts = list_delete_first(activeSubXactContexts);

	MemoryContextSwitchTo(old_context);
}


/* ActiveSubXacts returns list of active sub-transactions in temporal order. */
List *
ActiveSubXacts(void)
{
	ListCell *subXactCell = NULL;
	List *activeSubXactsReversed = NIL;

	/*
	 * activeSubXactContexts is in reversed temporal order, so we reverse it to get it
	 * in temporal order.
	 */
	foreach(subXactCell, activeSubXactContexts)
	{
		SubXactContext *state = lfirst(subXactCell);
		activeSubXactsReversed = lcons_int(state->subId, activeSubXactsReversed);
	}

	return activeSubXactsReversed;
}


/* ActiveSubXactContexts returns the list of active sub-xact context in temporal order. */
List *
ActiveSubXactContexts(void)
{
	ListCell *subXactCell = NULL;
	List *reversedSubXactStates = NIL;

	/*
	 * activeSubXactContexts is in reversed temporal order, so we reverse it to get it
	 * in temporal order.
	 */
	foreach(subXactCell, activeSubXactContexts)
	{
		SubXactContext *state = lfirst(subXactCell);
		reversedSubXactStates = lcons(state, reversedSubXactStates);
	}

	return reversedSubXactStates;
}


/*
 * If an ERROR is thrown while processing a transaction the ABORT handler is called.
 * ERRORS thrown during ABORT are not treated any differently, the ABORT handler is also
 * called during processing of those. If an ERROR was raised the first time through it's
 * unlikely that the second try will succeed; more likely that an ERROR will be thrown
 * again. This loop continues until Postgres notices and PANICs, complaining about a stack
 * overflow.
 *
 * Instead of looping and crashing, SwallowErrors lets us attempt to continue running the
 * ABORT logic. This wouldn't be safe in most other parts of the codebase, in
 * approximately none of the places where we emit ERROR do we first clean up after
 * ourselves! It's fine inside the ABORT handler though; Postgres is going to clean
 * everything up before control passes back to us.
 */
static void
SwallowErrors(void (*func)())
{
	MemoryContext savedContext = CurrentMemoryContext;

	PG_TRY();
	{
		func();
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();

		/* don't try to intercept PANIC or FATAL, let those breeze past us */
		if (edata->elevel != ERROR)
		{
			PG_RE_THROW();
		}

		/* turn the ERROR into a WARNING and emit it */
		edata->elevel = WARNING;
		ThrowErrorData(edata);

		/* leave the error handling system */
		FlushErrorState();
		MemoryContextSwitchTo(savedContext);
	}
	PG_END_TRY();
}


/*
 * IsMultiStatementTransaction determines whether the current statement is
 * part of a bigger multi-statement transaction. This is the case when the
 * statement is wrapped in a transaction block (comes after BEGIN), or it
 * is called from a stored procedure or function.
 */
bool
IsMultiStatementTransaction(void)
{
	if (IsTransactionBlock())
	{
		/* in a BEGIN...END block */
		return true;
	}
	else if (StoredProcedureLevel > 0)
	{
		/* in (a transaction within) a stored procedure */
		return true;
	}
	else if (FunctionCallLevel > 0 && FunctionOpensTransactionBlock)
	{
		/* in a language-handler function call, open a transaction if configured to do so */
		return true;
	}
	else
	{
		return false;
	}
}
