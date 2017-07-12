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
#include "distributed/metadata_cache.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/transaction_management.h"
#include "distributed/placement_connection.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/guc.h"
#include "port/atomics.h"


CoordinatedTransactionState CurrentCoordinatedTransactionState = COORD_TRANS_NONE;

/* GUC, the commit protocol to use for commands affecting more than one connection */
int MultiShardCommitProtocol = COMMIT_PROTOCOL_1PC;
int SavedMultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

/* state needed to keep track of operations used during a transaction */
XactModificationType XactModificationLevel = XACT_MODIFICATION_NONE;

/* list of connections that are part of the current coordinated transaction */
dlist_head InProgressTransactions = DLIST_STATIC_INIT(InProgressTransactions);

/*
 * Should this coordinated transaction use 2PC? Set by
 * CoordinatedTransactionUse2PC(), e.g. if DDL was issued and
 * MultiShardCommitProtocol was set to 2PC.
 */
bool CoordinatedTransactionUses2PC = false;


static bool subXactAbortAttempted = false;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* transaction management functions */
static void CoordinatedTransactionCallback(XactEvent event, void *arg);
static void CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
											  SubTransactionId parentSubid, void *arg);

/* remaining functions */
static void AdjustMaxPreparedTransactions(void);

TmgmtShmemControlData *TmgmtShmemControl = NULL;
TmgmtBackendData *MyTmgmtBackendData = NULL;

PG_FUNCTION_INFO_V1(assign_distributed_transaction_id);

Datum
assign_distributed_transaction_id(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Assert(MyTmgmtBackendData);

	/* FIXME: spinlock? */
	MyTmgmtBackendData->transactionId.nodeId = PG_GETARG_INT64(0);
	MyTmgmtBackendData->transactionId.transactionId = PG_GETARG_INT64(1);
	MyTmgmtBackendData->transactionId.timestamp = PG_GETARG_TIMESTAMPTZ(2);
	MyTmgmtBackendData->deadlockKilled = false;

	PG_RETURN_VOID();
}


static void
UnsetDistributedTransactionId(void)
{
	if (MyTmgmtBackendData)
	{
		/* FIXME: spinlock? */
		MyTmgmtBackendData->transactionId.nodeId = 0;
		MyTmgmtBackendData->transactionId.transactionId = 0;
		MyTmgmtBackendData->transactionId.timestamp = 0;
		MyTmgmtBackendData->deadlockKilled = false;
	}
}


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

	Assert(MyTmgmtBackendData);

	/* FIXME: Spinlock? Consistency is nice ;) */
	/* FIXME: determine proper local node id for MX etc */
	MyTmgmtBackendData->transactionId.nodeId = 0;
	MyTmgmtBackendData->transactionId.transactionId =
		pg_atomic_fetch_add_u64(&TmgmtShmemControl->nextTransactionId, 1);
	MyTmgmtBackendData->transactionId.timestamp = GetCurrentTimestamp();

	elog(DEBUG3, "assigning xact: (%lu, %lu, %lu)",
		 MyTmgmtBackendData->transactionId.nodeId,
		 MyTmgmtBackendData->transactionId.transactionId,
		 MyTmgmtBackendData->transactionId.timestamp);
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

	CoordinatedTransactionUses2PC = true;
}


static size_t
TmgmtShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(TmgmtShmemControlData));
	size = add_size(size, mul_size(sizeof(TmgmtBackendData), MaxBackends));

	return size;
}


static void
TmgmtShmemInit(void)
{
	bool alreadyInitialized = false;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	TmgmtShmemControl =
		(TmgmtShmemControlData *) ShmemInitStruct("Transaction Management",
												  TmgmtShmemSize(),
												  &alreadyInitialized);
	if (!alreadyInitialized)
	{
		/* initialize lwlock */
		LWLockTranche *tranche = &TmgmtShmemControl->lockTranche;

		/* start by zeroing out all the memory */
		memset(TmgmtShmemControl, 0, TmgmtShmemSize());

		TmgmtShmemControl->numSessions = MaxBackends;

		/* initialize lock */
		TmgmtShmemControl->trancheId = LWLockNewTrancheId();
		tranche->array_base = &TmgmtShmemControl->lock;
		tranche->array_stride = sizeof(LWLock);
		tranche->name = "Distributed Transaction Management";
		LWLockRegisterTranche(TmgmtShmemControl->trancheId, tranche);
		LWLockInitialize(&TmgmtShmemControl->lock,
						 TmgmtShmemControl->trancheId);

		pg_atomic_init_u64(&TmgmtShmemControl->nextTransactionId, 7);
	}
	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


void
InitializeTransactionManagement(void)
{
	/* hook into transaction machinery */
	RegisterXactCallback(CoordinatedTransactionCallback, NULL);
	RegisterSubXactCallback(CoordinatedSubTransactionCallback, NULL);

	AdjustMaxPreparedTransactions();

	/* allocate shared memory */
	RequestAddinShmemSpace(TmgmtShmemSize());

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = TmgmtShmemInit;
}


void
InitializeTransactionManagementBackend(void)
{
	/* Fill this backend's lock information */
	LWLockAcquire(&TmgmtShmemControl->lock, LW_EXCLUSIVE);
	MyTmgmtBackendData = &TmgmtShmemControl->sessions[MyProc->pgprocno];
	MyTmgmtBackendData->databaseId = MyDatabaseId;

	/* FIXME: get id usable for MX, where multiple nodes can start distributed transactions */
	MyTmgmtBackendData->transactionId.nodeId = 0;
	LWLockRelease(&TmgmtShmemControl->lock);
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

			Assert(!subXactAbortAttempted);
			CurrentCoordinatedTransactionState = COORD_TRANS_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			dlist_init(&InProgressTransactions);
			CoordinatedTransactionUses2PC = false;

			UnsetDistributedTransactionId();
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
			dlist_init(&InProgressTransactions);
			CoordinatedTransactionUses2PC = false;
			subXactAbortAttempted = false;

			UnsetDistributedTransactionId();
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
			PostCommitMarkFailedShardPlacements(CoordinatedTransactionUses2PC);
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
