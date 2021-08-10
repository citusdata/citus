/*-------------------------------------------------------------------------
 *
 * transaction_management.c
 *
 *   Transaction management for Citus.  Most of the work is delegated to other
 *   subsystems, this files, and especially CoordinatedTransactionCallback,
 *   coordinates the work between them.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/twophase.h"
#include "access/xact.h"
#include "distributed/backend_data.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/connection_management.h"
#include "distributed/distributed_planner.h"
#include "distributed/hash_helpers.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/locally_reserved_shared_connections.h"
#include "distributed/maintenanced.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/repartition_join_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/placement_connection.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/subplan_execution.h"
#include "distributed/version_compat.h"
#include "distributed/worker_log_messages.h"
#include "utils/hsearch.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "storage/fd.h"


CoordinatedTransactionState CurrentCoordinatedTransactionState = COORD_TRANS_NONE;

/* GUC, the commit protocol to use for commands affecting more than one connection */
int MultiShardCommitProtocol = COMMIT_PROTOCOL_2PC;
int SingleShardCommitProtocol = COMMIT_PROTOCOL_2PC;
int SavedMultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

/*
 * GUC that determines whether a SELECT in a transaction block should also run in
 * a transaction block on the worker even if no writes have occurred yet.
 */
bool SelectOpensTransactionBlock = true;

/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;

/* we've deprecated this flag, keeping here for some time not to break existing users */
bool EnableDeadlockPrevention = true;

/* number of nested stored procedure call levels we are currently in */
int StoredProcedureLevel = 0;

/* number of nested DO block levels we are currently in */
int DoBlockLevel = 0;

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
 * MultiShardCommitProtocol was set to 2PC. But, even if this
 * flag is set, the transaction manager is smart enough to only
 * do 2PC on the remote connections that did a modification.
 *
 * As a variable name ShouldCoordinatedTransactionUse2PC could
 * be improved. We use Use2PCForCoordinatedTransaction() as the
 * public API function, hence couldn't come up with a better name
 * for the underlying variable at the moment.
 */
bool ShouldCoordinatedTransactionUse2PC = false;

/* if disabled, distributed statements in a function may run as separate transactions */
bool FunctionOpensTransactionBlock = true;

/* if true, we should trigger metadata sync on commit */
bool MetadataSyncOnCommit = false;


/* transaction management functions */
static void CoordinatedTransactionCallback(XactEvent event, void *arg);
static void CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
											  SubTransactionId parentSubid, void *arg);

/* remaining functions */
static void ResetShardPlacementTransactionState(void);
static void AdjustMaxPreparedTransactions(void);
static void PushSubXact(SubTransactionId subId);
static void PopSubXact(SubTransactionId subId);
static bool MaybeExecutingUDF(void);
static void ResetGlobalVariables(void);


/*
 * UseCoordinatedTransaction sets up the necessary variables to use
 * a coordinated transaction, unless one is already in progress.
 */
void
UseCoordinatedTransaction(void)
{
	if (CurrentCoordinatedTransactionState == COORD_TRANS_STARTED)
	{
		return;
	}

	if (CurrentCoordinatedTransactionState != COORD_TRANS_NONE &&
		CurrentCoordinatedTransactionState != COORD_TRANS_IDLE)
	{
		ereport(ERROR, (errmsg("starting transaction in wrong state")));
	}

	CurrentCoordinatedTransactionState = COORD_TRANS_STARTED;

	/*
	 * If assign_distributed_transaction_id() has been called, we should reuse
	 * that identifier so distributed deadlock detection works properly.
	 */
	DistributedTransactionId *transactionId = GetCurrentDistributedTransactionId();
	if (transactionId->transactionNumber == 0)
	{
		AssignDistributedTransactionId();
	}
}


/*
 * EnsureDistributedTransactionId makes sure that the current transaction
 * has a distributed transaction id. It is either assigned by a previous
 * call of assign_distributed_transaction_id(), or by starting a coordinated
 * transaction.
 */
void
EnsureDistributedTransactionId(void)
{
	DistributedTransactionId *transactionId = GetCurrentDistributedTransactionId();
	if (transactionId->transactionNumber == 0)
	{
		UseCoordinatedTransaction();
	}
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
 * Use2PCForCoordinatedTransaction() signals that the current coordinated
 * transaction should use 2PC to commit.
 *
 * Note that even if 2PC is enabled, it is only used for connections that make
 * modification (DML or DDL).
 */
void
Use2PCForCoordinatedTransaction(void)
{
	Assert(InCoordinatedTransaction());

	ShouldCoordinatedTransactionUse2PC = true;
}


/*
 * GetCoordinatedTransactionShouldUse2PC is a wrapper function to read the value
 * of CoordinatedTransactionShouldUse2PCFlag.
 */
bool
GetCoordinatedTransactionShouldUse2PC(void)
{
	return ShouldCoordinatedTransactionUse2PC;
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

			/*
			 * Changes to catalog tables are now visible to the metadata sync
			 * daemon, so we can trigger metadata sync if necessary.
			 */
			if (MetadataSyncOnCommit)
			{
				TriggerMetadataSync(MyDatabaseId);
			}

			ResetGlobalVariables();

			/*
			 * Make sure that we give the shared connections back to the shared
			 * pool if any. This operation is a no-op if the reserved connections
			 * are already given away.
			 */
			DeallocateReservedConnections();

			UnSetDistributedTransactionId();

			/* empty the CommitContext to ensure we're not leaking memory */
			MemoryContextSwitchTo(previousContext);
			MemoryContextReset(CommitContext);
			break;
		}

		case XACT_EVENT_ABORT:
		{
			/* stop propagating notices from workers, we know the query is failed */
			DisableWorkerMessagePropagation();

			RemoveIntermediateResultsDirectory();

			ResetShardPlacementTransactionState();

			/* handles both already prepared and open transactions */
			if (CurrentCoordinatedTransactionState > COORD_TRANS_IDLE)
			{
				CoordinatedRemoteTransactionsAbort();
			}

			/*
			 * Close connections etc. Contrary to a successful transaction we reset the placement connection management
			 * irregardless of state of the statemachine as recorded in CurrentCoordinatedTransactionState.
			 * The hashmaps recording the connection management live a memory context higher compared to most of the
			 * data referenced in the hashmap. This causes use after free errors when the contents are retained due to
			 * an error caused before the CurrentCoordinatedTransactionState changed.
			 */
			ResetPlacementConnectionManagement();
			AfterXactConnectionHandling(false);

			ResetGlobalVariables();

			/*
			 * Make sure that we give the shared connections back to the shared
			 * pool if any. This operation is a no-op if the reserved connections
			 * are already given away.
			 */
			DeallocateReservedConnections();

			/*
			 * We reset these mainly for posterity. The only way we would normally
			 * get here with ExecutorLevel or PlannerLevel > 0 is during a fatal
			 * error when the process is about to end.
			 */
			ExecutorLevel = 0;
			PlannerLevel = 0;

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
			break;
		}

		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
		{
			break;
		}

		case XACT_EVENT_PREPARE:
		{
			/* we need to reset SavedExplainPlan before TopTransactionContext is deleted */
			FreeSavedExplainPlan();

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

			if (ShouldCoordinatedTransactionUse2PC)
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
			PostCommitMarkFailedShardPlacements(ShouldCoordinatedTransactionUse2PC);
			break;
		}

		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		{
			if (InCoordinatedTransaction())
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
 * ResetGlobalVariables resets global variables that
 * might be changed during the execution of queries.
 */
static void
ResetGlobalVariables()
{
	CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
	XactModificationLevel = XACT_MODIFICATION_NONE;
	SetLocalExecutionStatus(LOCAL_EXECUTION_OPTIONAL);
	FreeSavedExplainPlan();
	dlist_init(&InProgressTransactions);
	activeSetStmts = NULL;
	ShouldCoordinatedTransactionUse2PC = false;
	TransactionModifiedNodeMetadata = false;
	MetadataSyncOnCommit = false;
	ResetWorkerErrorIndication();
}


/*
 * ResetShardPlacementTransactionState performs cleanup after the end of a
 * transaction.
 */
static void
ResetShardPlacementTransactionState(void)
{
	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_BARE)
	{
		MultiShardCommitProtocol = SavedMultiShardCommitProtocol;
		SavedMultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;
	}
}


/*
 * CoordinatedSubTransactionCallback is the callback used to implement
 * distributed ROLLBACK TO SAVEPOINT.
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
			/*
			 * Stop showing message for now, will re-enable when executing
			 * the next statement.
			 */
			DisableWorkerMessagePropagation();

			/*
			 * Given that we aborted, worker error indications can be ignored.
			 */
			ResetWorkerErrorIndication();

			if (InCoordinatedTransaction())
			{
				CoordinatedRemoteTransactionsSavepointRollback(subId);
			}
			PopSubXact(subId);

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

		SafeSnprintf(newvalue, sizeof(newvalue), "%d", MaxConnections * 2);

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
	/*
	 * We need to allocate these in TopTransactionContext instead of current
	 * subxact's memory context. This is because AtSubCommit_Memory won't
	 * delete the subxact's memory context unless it is empty, and this
	 * can cause in memory leaks. For emptiness it just checks if the memory
	 * has been reset, and we cannot reset the subxact context since other
	 * data can be in the context that are needed by upper commits.
	 *
	 * See https://github.com/citusdata/citus/issues/3999
	 */
	MemoryContext old_context = MemoryContextSwitchTo(TopTransactionContext);

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
	SubXactContext *state = linitial(activeSubXactContexts);

	Assert(state->subId == subId);

	/*
	 * Free activeSetStmts to avoid memory leaks when we create subxacts
	 * for each row, e.g. in exception handling of UDFs.
	 */
	if (activeSetStmts != NULL)
	{
		pfree(activeSetStmts->data);
		pfree(activeSetStmts);
	}

	/*
	 * SET LOCAL commands are local to subxact blocks. When a subxact commits
	 * or rolls back, we should roll back our set of SET LOCAL commands to the
	 * ones we had in the upper commit.
	 */
	activeSetStmts = state->setLocalCmds;

	/*
	 * Free state to avoid memory leaks when we create subxacts for each row,
	 * e.g. in exception handling of UDFs.
	 */
	pfree(state);

	activeSubXactContexts = list_delete_first(activeSubXactContexts);
}


/* ActiveSubXactContexts returns the list of active sub-xact context in temporal order. */
List *
ActiveSubXactContexts(void)
{
	List *reversedSubXactStates = NIL;

	/*
	 * activeSubXactContexts is in reversed temporal order, so we reverse it to get it
	 * in temporal order.
	 */
	SubXactContext *state = NULL;
	foreach_ptr(state, activeSubXactContexts)
	{
		reversedSubXactStates = lcons(state, reversedSubXactStates);
	}

	return reversedSubXactStates;
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
	else if (DoBlockLevel > 0)
	{
		/* in (a transaction within) a do block */
		return true;
	}
	else if (StoredProcedureLevel > 0)
	{
		/* in (a transaction within) a stored procedure */
		return true;
	}
	else if (MaybeExecutingUDF() && FunctionOpensTransactionBlock)
	{
		/* in a language-handler function call, open a transaction if configured to do so */
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * MaybeExecutingUDF returns true if we are possibly executing a function call.
 * We use nested level of executor to check this, so this can return true for
 * CTEs, etc. which also start nested executors.
 *
 * If the planner is being called from the executor, then we may also be in
 * a UDF.
 */
static bool
MaybeExecutingUDF(void)
{
	return ExecutorLevel > 1 || (ExecutorLevel == 1 && PlannerLevel > 0);
}


/*
 * TriggerMetadataSyncOnCommit sets a flag to do metadata sync on commit.
 * This is because new metadata only becomes visible to the metadata sync
 * daemon after commit happens.
 */
void
TriggerMetadataSyncOnCommit(void)
{
	MetadataSyncOnCommit = true;
}
