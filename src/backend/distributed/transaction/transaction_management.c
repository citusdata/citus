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
#include "distributed/function_call_delegation.h"
#include "distributed/hash_helpers.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/locally_reserved_shared_connections.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata/dependency.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_explain.h"
#include "distributed/repartition_join_execution.h"
#include "distributed/replication_origin_session_utils.h"
#include "distributed/transaction_management.h"
#include "distributed/placement_connection.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/shard_cleaner.h"
#include "distributed/subplan_execution.h"
#include "distributed/version_compat.h"
#include "distributed/worker_log_messages.h"
#include "distributed/commands.h"
#include "distributed/metadata_cache.h"
#include "utils/hsearch.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/datum.h"
#include "storage/fd.h"
#include "nodes/print.h"


CoordinatedTransactionState CurrentCoordinatedTransactionState = COORD_TRANS_NONE;

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
 *
 * To be clear, last item of activeSubXactContexts list corresponds to top of
 * stack.
 */
static List *activeSubXactContexts = NIL;

/* some pre-allocated memory so we don't need to call malloc() during callbacks */
MemoryContext CitusXactCallbackContext = NULL;

/*
 * Should this coordinated transaction use 2PC? Set by
 * CoordinatedTransactionUse2PC(), e.g. if any modification
 * is issued and us 2PC. But, even if this flag is set,
 * the transaction manager is smart enough to only
 * do 2PC on the remote connections that did a modification.
 *
 * As a variable name ShouldCoordinatedTransactionUse2PC could
 * be improved. We use Use2PCForCoordinatedTransaction() as the
 * public API function, hence couldn't come up with a better name
 * for the underlying variable at the moment.
 */
bool ShouldCoordinatedTransactionUse2PC = false;

/*
 * Distribution function argument (along with colocationId) when delegated
 * using forceDelegation flag.
 */
AllowedDistributionColumn AllowedDistributionColumnValue;

/* if disabled, distributed statements in a function may run as separate transactions */
bool FunctionOpensTransactionBlock = true;

/* if true, we should trigger node metadata sync on commit */
bool NodeMetadataSyncOnCommit = false;

/*
 * In an explicit BEGIN ...; we keep track of top-level transaction characteristics
 * specified by the user.
 */
BeginXactReadOnlyState BeginXactReadOnly = BeginXactReadOnly_NotSet;
BeginXactDeferrableState BeginXactDeferrable = BeginXactDeferrable_NotSet;


/* transaction management functions */
static void CoordinatedTransactionCallback(XactEvent event, void *arg);
static void CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
											  SubTransactionId parentSubid, void *arg);

/* remaining functions */
static void AdjustMaxPreparedTransactions(void);
static void PushSubXact(SubTransactionId subId);
static void PopSubXact(SubTransactionId subId);
static void ResetGlobalVariables(void);
static bool SwallowErrors(void (*func)(void));
static void ForceAllInProgressConnectionsToClose(void);
static void EnsurePrepareTransactionIsAllowed(void);


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
	/*
	 * If this transaction is also a coordinated
	 * transaction, use 2PC. Otherwise, this
	 * state change does nothing.
	 *
	 * In other words, when this flag is set,
	 * we "should" use 2PC when needed (e.g.,
	 * we are in a coordinated transaction and
	 * the coordinated transaction does a remote
	 * modification).
	 */
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
	CitusXactCallbackContext = AllocSetContextCreateInternal(TopMemoryContext,
															 "CitusXactCallbackContext",
															 8 * 1024,
															 8 * 1024,
															 8 * 1024);
}


/*
 * Transaction management callback, handling coordinated transaction, and
 * transaction independent connection management.
 *
 * NB: There should only ever be a single transaction callback in citus, the
 * ordering between the callbacks and the actions within those callbacks
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
			 * CitusXactCallbackContext, it has 8kb of memory that we're allowed to use.
			 *
			 * We only do this in the COMMIT callback because:
			 * - Errors thrown in other callbacks (such as PRE_COMMIT) won't cause
			 *   crashes, they will simply cause the ABORT handler to be called.
			 * - The exception is ABORT, errors thrown there could also cause crashes, but
			 *   postgres already creates a TransactionAbortContext which performs this
			 *   trick, so there's no need for us to do it again.
			 */
			MemoryContext previousContext =
				MemoryContextSwitchTo(CitusXactCallbackContext);

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
			 * daemon, so we can trigger node metadata sync if necessary.
			 */
			if (NodeMetadataSyncOnCommit)
			{
				TriggerNodeMetadataSync(MyDatabaseId);
			}

			ResetGlobalVariables();
			ResetRelationAccessHash();
			ResetObjectsPropagated();

			/*
			 * Make sure that we give the shared connections back to the shared
			 * pool if any. This operation is a no-op if the reserved connections
			 * are already given away.
			 */
			DeallocateReservedConnections();

			UnSetDistributedTransactionId();

			PlacementMovedUsingLogicalReplicationInTX = false;

			/* empty the CitusXactCallbackContext to ensure we're not leaking memory */
			MemoryContextSwitchTo(previousContext);
			MemoryContextReset(CitusXactCallbackContext);

			/* Set CreateCitusTransactionLevel to 0 since original transaction is about to be
			 * committed.
			 */

			if (GetCitusCreationLevel() > 0)
			{
				/* Check CitusCreationLevel was correctly decremented to 1 */
				Assert(GetCitusCreationLevel() == 1);
				SetCreateCitusTransactionLevel(0);
			}
			break;
		}

		case XACT_EVENT_ABORT:
		{
			/* stop propagating notices from workers, we know the query is failed */
			DisableWorkerMessagePropagation();

			RemoveIntermediateResultsDirectories();

			/* handles both already prepared and open transactions */
			if (CurrentCoordinatedTransactionState > COORD_TRANS_IDLE)
			{
				/*
				 * Since CoordinateRemoteTransactionsAbort may cause an error and it is
				 * not allowed to error out at that point, swallow the error if any.
				 *
				 * Particular error we've observed was CreateWaitEventSet throwing an error
				 * when out of file descriptor.
				 *
				 * If an error is swallowed, connections of all active transactions must
				 * be forced to close at the end of the transaction explicitly.
				 */
				bool errorSwallowed = SwallowErrors(CoordinatedRemoteTransactionsAbort);
				if (errorSwallowed == true)
				{
					ForceAllInProgressConnectionsToClose();
				}
			}

			/*
			 * Close connections etc. Contrary to a successful transaction we reset the
			 * placement connection management irregardless of state of the statemachine
			 * as recorded in CurrentCoordinatedTransactionState.
			 * The hashmaps recording the connection management live a memory context
			 * higher compared to most of the data referenced in the hashmap. This causes
			 * use after free errors when the contents are retained due to an error caused
			 * before the CurrentCoordinatedTransactionState changed.
			 */
			ResetPlacementConnectionManagement();
			AfterXactConnectionHandling(false);

			ResetGlobalVariables();
			ResetRelationAccessHash();
			ResetObjectsPropagated();

			/* Reset any local replication origin session since transaction has been aborted.*/
			ResetReplicationOriginLocalSession();

			/* empty the CitusXactCallbackContext to ensure we're not leaking memory */
			MemoryContextReset(CitusXactCallbackContext);

			/*
			 * Clear MetadataCache table if we're aborting from a CREATE EXTENSION Citus
			 * so that any created OIDs from the table are cleared and invalidated. We
			 * also set CreateCitusTransactionLevel to 0 since that process has been aborted
			 */
			if (GetCitusCreationLevel() > 0)
			{
				/* Checks CitusCreationLevel correctly decremented to 1 */
				Assert(GetCitusCreationLevel() == 1);

				InvalidateMetadataSystemCache();
				SetCreateCitusTransactionLevel(0);
			}

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

			PlacementMovedUsingLogicalReplicationInTX = false;
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
			RemoveIntermediateResultsDirectories();

			UnSetDistributedTransactionId();
			break;
		}

		case XACT_EVENT_PRE_COMMIT:
		{
			/*
			 * If the distributed query involves 2PC, we already removed
			 * the intermediate result directory on XACT_EVENT_PREPARE. However,
			 * if not, we should remove it here on the COMMIT. Since
			 * RemoveIntermediateResultsDirectories() is idempotent, we're safe
			 * to call it here again even if the transaction involves 2PC.
			 */
			RemoveIntermediateResultsDirectories();

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
			 * committed. This handles failure at COMMIT time.
			 */
			ErrorIfPostCommitFailedShardPlacements();
			break;
		}

		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		{
			EnsurePrepareTransactionIsAllowed();
			break;
		}
	}
}


/*
 * ForceAllInProgressConnectionsToClose forces all connections of in progress transactions
 * to close at the end of the transaction.
 */
static void
ForceAllInProgressConnectionsToClose(void)
{
	dlist_iter iter;
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection,
													  transactionNode,
													  iter.cur);

		connection->forceCloseAtTransactionEnd = true;
	}
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
 *
 * If it swallows any error, returns true. Otherwise, returns false.
 */
static bool
SwallowErrors(void (*func)())
{
	MemoryContext savedContext = CurrentMemoryContext;
	volatile bool anyErrorSwallowed = false;

	PG_TRY();
	{
		func();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		/* rethrow as WARNING */
		edata->elevel = WARNING;
		ThrowErrorData(edata);

		anyErrorSwallowed = true;
	}
	PG_END_TRY();

	return anyErrorSwallowed;
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
	NodeMetadataSyncOnCommit = false;
	InTopLevelDelegatedFunctionCall = false;
	InTableTypeConversionFunctionCall = false;
	CurrentOperationId = INVALID_OPERATION_ID;
	BeginXactReadOnly = BeginXactReadOnly_NotSet;
	BeginXactDeferrable = BeginXactDeferrable_NotSet;
	ResetWorkerErrorIndication();
	memset(&AllowedDistributionColumnValue, 0,
		   sizeof(AllowedDistributionColumn));
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
			MemoryContext previousContext =
				MemoryContextSwitchTo(CitusXactCallbackContext);

			PushSubXact(subId);
			if (InCoordinatedTransaction())
			{
				CoordinatedRemoteTransactionsSavepointBegin(subId);
			}

			/*
			 * Push a new hash map for tracking objects propagated in the current
			 * subtransaction.
			 */
			PushObjectsPropagatedHash();

			MemoryContextSwitchTo(previousContext);

			break;
		}

		case SUBXACT_EVENT_COMMIT_SUB:
		{
			MemoryContext previousContext =
				MemoryContextSwitchTo(CitusXactCallbackContext);

			if (InCoordinatedTransaction())
			{
				CoordinatedRemoteTransactionsSavepointRelease(subId);
			}
			PopSubXact(subId);

			/* Set CachedDuringCitusCreation to one level lower to represent citus creation is done */

			if (GetCitusCreationLevel() == GetCurrentTransactionNestLevel())
			{
				SetCreateCitusTransactionLevel(GetCitusCreationLevel() - 1);
			}

			MemoryContextSwitchTo(previousContext);

			break;
		}

		case SUBXACT_EVENT_ABORT_SUB:
		{
			MemoryContext previousContext =
				MemoryContextSwitchTo(CitusXactCallbackContext);

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

			/*
			 * Clear MetadataCache table if we're aborting from a CREATE EXTENSION Citus
			 * so that any created OIDs from the table are cleared and invalidated. We
			 * also set CreateCitusTransactionLevel to 0 since subtransaction has been aborted
			 */
			if (GetCitusCreationLevel() == GetCurrentTransactionNestLevel())
			{
				InvalidateMetadataSystemCache();
				SetCreateCitusTransactionLevel(0);
			}

			/* remove the last hashmap from propagated objects stack */
			PopObjectsPropagatedHash();

			/* Reset any local replication origin session since subtransaction has been aborted.*/
			ResetReplicationOriginLocalSession();
			MemoryContextSwitchTo(previousContext);

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
	/* save provided subId as well as propagated SET LOCAL stmts */
	SubXactContext *state = palloc(sizeof(SubXactContext));
	state->subId = subId;
	state->setLocalCmds = activeSetStmts;

	/* append to list and reset active set stmts for upcoming sub-xact */
	activeSubXactContexts = lappend(activeSubXactContexts, state);
	activeSetStmts = makeStringInfo();
}


/* PopSubXact pops subId from the stack of active sub-transactions. */
static void
PopSubXact(SubTransactionId subId)
{
	SubXactContext *state = llast(activeSubXactContexts);

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

	activeSubXactContexts = list_delete_last(activeSubXactContexts);
}


/* ActiveSubXactContexts returns the list of active sub-xact context in temporal order. */
List *
ActiveSubXactContexts(void)
{
	return activeSubXactContexts;
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
bool
MaybeExecutingUDF(void)
{
	return ExecutorLevel > 1 || (ExecutorLevel == 1 && PlannerLevel > 0);
}


/*
 * TriggerNodeMetadataSyncOnCommit sets a flag to do node metadata sync
 * on commit. This is because new metadata only becomes visible to the
 * metadata sync daemon after commit happens.
 */
void
TriggerNodeMetadataSyncOnCommit(void)
{
	NodeMetadataSyncOnCommit = true;
}


/*
 * Function raises an exception, if the current backend started a coordinated
 * transaction and got a PREPARE event to become a participant in a 2PC
 * transaction coordinated by another node.
 */
static void
EnsurePrepareTransactionIsAllowed(void)
{
	if (!InCoordinatedTransaction())
	{
		/* If the backend has not started a coordinated transaction. */
		return;
	}

	if (IsCitusInternalBackend())
	{
		/*
		 * If this is a Citus-initiated backend.
		 */
		return;
	}

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot use 2PC in transactions involving "
						   "multiple servers")));
}
