/*-------------------------------------------------------------------------
 *
 * transaction_recovery.c
 *
 * Routines for recovering two-phase commits started by this node if a
 * failure occurs between prepare and commit/abort.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include <sys/stat.h>
#include <unistd.h>

#if PG_VERSION_NUM >= 120000
#include "access/genam.h"
#endif
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_transaction.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(recover_prepared_transactions);


/* Local functions forward declarations */
static int RecoverWorkerTransactions(WorkerNode *workerNode);
static List * PendingWorkerTransactionList(MultiConnection *connection);
static bool IsTransactionInProgress(HTAB *activeTransactionNumberSet,
									char *preparedTransactionName);
static bool RecoverPreparedTransactionOnWorker(MultiConnection *connection,
											   char *transactionName, bool shouldCommit);


/*
 * recover_prepared_transactions recovers any pending prepared
 * transactions started by this node on other nodes.
 */
Datum
recover_prepared_transactions(PG_FUNCTION_ARGS)
{
	int recoveredTransactionCount = 0;

	CheckCitusVersion(ERROR);

	recoveredTransactionCount = RecoverTwoPhaseCommits();

	PG_RETURN_INT32(recoveredTransactionCount);
}


/*
 * LogTransactionRecord registers the fact that a transaction has been
 * prepared on a worker. The presence of this record indicates that the
 * prepared transaction should be committed.
 */
void
LogTransactionRecord(int32 groupId, char *transactionName)
{
	Relation pgDistTransaction = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_transaction];
	bool isNulls[Natts_pg_dist_transaction];

	/* form new transaction tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_transaction_groupid - 1] = Int32GetDatum(groupId);
	values[Anum_pg_dist_transaction_gid - 1] = CStringGetTextDatum(transactionName);

	/* open transaction relation and insert new tuple */
	pgDistTransaction = heap_open(DistTransactionRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistTransaction);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistTransaction, heapTuple);

	CommandCounterIncrement();

	/* close relation and invalidate previous cache entry */
	heap_close(pgDistTransaction, NoLock);
}


/*
 * RecoverTwoPhaseCommits recovers any pending prepared
 * transactions started by this node on other nodes.
 */
int
RecoverTwoPhaseCommits(void)
{
	List *workerList = NIL;
	ListCell *workerNodeCell = NULL;
	int recoveredTransactionCount = 0;

	workerList = ActivePrimaryNodeList(NoLock);

	foreach(workerNodeCell, workerList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		recoveredTransactionCount += RecoverWorkerTransactions(workerNode);
	}

	return recoveredTransactionCount;
}


/*
 * RecoverWorkerTransactions recovers any pending prepared transactions
 * started by this node on the specified worker.
 */
static int
RecoverWorkerTransactions(WorkerNode *workerNode)
{
	int recoveredTransactionCount = 0;

	int32 groupId = workerNode->groupId;
	char *nodeName = workerNode->workerName;
	int nodePort = workerNode->workerPort;

	List *activeTransactionNumberList = NIL;
	HTAB *activeTransactionNumberSet = NULL;

	List *pendingTransactionList = NIL;
	HTAB *pendingTransactionSet = NULL;
	List *recheckTransactionList = NIL;
	HTAB *recheckTransactionSet = NULL;

	Relation pgDistTransaction = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;

	HASH_SEQ_STATUS status;

	MemoryContext localContext = NULL;
	MemoryContext oldContext = NULL;
	bool recoveryFailed = false;

	int connectionFlags = 0;
	MultiConnection *connection = GetNodeConnection(connectionFlags, nodeName, nodePort);
	if (connection->pgConn == NULL || PQstatus(connection->pgConn) != CONNECTION_OK)
	{
		ereport(WARNING, (errmsg("transaction recovery cannot connect to %s:%d",
								 nodeName, nodePort)));

		return 0;
	}

	localContext = AllocSetContextCreateExtended(CurrentMemoryContext,
												 "RecoverWorkerTransactions",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(localContext);

	/* take table lock first to avoid running concurrently */
	pgDistTransaction = heap_open(DistTransactionRelationId(), ShareUpdateExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistTransaction);

	/*
	 * We're going to check the list of prepared transactions on the worker,
	 * but some of those prepared transactions might belong to ongoing
	 * distributed transactions.
	 *
	 * We could avoid this by temporarily blocking new prepared transactions
	 * from being created by taking an ExlusiveLock on pg_dist_transaction.
	 * However, this hurts write performance, so instead we avoid blocking
	 * by consulting the list of active distributed transactions, and follow
	 * a carefully chosen order to avoid race conditions:
	 *
	 * 1) P = prepared transactions on worker
	 * 2) A = active distributed transactions
	 * 3) T = pg_dist_transaction snapshot
	 * 4) Q = prepared transactions on worker
	 *
	 * By observing A after P, we get a conclusive answer to which distributed
	 * transactions we observed in P are still in progress. It is safe to recover
	 * the transactions in P - A based on the presence or absence of a record
	 * in T.
	 *
	 * We also remove records in T if there is no prepared transaction, which
	 * we assume means the transaction committed. However, a transaction could
	 * have left prepared transactions and committed between steps 1 and 2.
	 * In that case, we would incorrectly remove the records, while the
	 * prepared transaction is still in place.
	 *
	 * We therefore observe the set of prepared transactions one more time in
	 * step 4. The aforementioned transactions would show up in Q, but not in
	 * P. We can skip those transactions and recover them later.
	 */

	/* find stale prepared transactions on the remote node */
	pendingTransactionList = PendingWorkerTransactionList(connection);
	pendingTransactionSet = ListToHashSet(pendingTransactionList, NAMEDATALEN, true);

	/* find in-progress distributed transactions */
	activeTransactionNumberList = ActiveDistributedTransactionNumbers();
	activeTransactionNumberSet = ListToHashSet(activeTransactionNumberList,
											   sizeof(uint64), false);

	/* scan through all recovery records of the current worker */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_transaction_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(groupId));

	/* get a snapshot of pg_dist_transaction */
	scanDescriptor = systable_beginscan(pgDistTransaction,
										DistTransactionGroupIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	/* find stale prepared transactions on the remote node */
	recheckTransactionList = PendingWorkerTransactionList(connection);
	recheckTransactionSet = ListToHashSet(recheckTransactionList, NAMEDATALEN, true);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		bool isNull = false;
		bool isTransactionInProgress = false;
		bool foundPreparedTransactionBeforeCommit = false;
		bool foundPreparedTransactionAfterCommit = false;

		Datum transactionNameDatum = heap_getattr(heapTuple,
												  Anum_pg_dist_transaction_gid,
												  tupleDescriptor, &isNull);
		char *transactionName = TextDatumGetCString(transactionNameDatum);

		isTransactionInProgress = IsTransactionInProgress(activeTransactionNumberSet,
														  transactionName);
		if (isTransactionInProgress)
		{
			/*
			 * Do not touch in progress transactions as we might mistakenly
			 * commit a transaction that is actually in the process of
			 * aborting or vice-versa.
			 */
			continue;
		}

		/*
		 * Remove the transaction from the pending list such that only transactions
		 * that need to be aborted remain at the end.
		 */
		hash_search(pendingTransactionSet, transactionName, HASH_REMOVE,
					&foundPreparedTransactionBeforeCommit);

		hash_search(recheckTransactionSet, transactionName, HASH_FIND,
					&foundPreparedTransactionAfterCommit);

		if (foundPreparedTransactionBeforeCommit && foundPreparedTransactionAfterCommit)
		{
			/*
			 * The transaction was committed, but the prepared transaction still exists
			 * on the worker. Try committing it.
			 *
			 * We double check that the recovery record exists both before and after
			 * checking ActiveDistributedTransactionNumbers(), since we may have
			 * observed a prepared transaction that was committed immediately after.
			 */
			bool shouldCommit = true;
			bool commitSucceeded = RecoverPreparedTransactionOnWorker(connection,
																	  transactionName,
																	  shouldCommit);
			if (!commitSucceeded)
			{
				/*
				 * Failed to commit on the current worker. Stop without throwing
				 * an error to allow recover_prepared_transactions to continue with
				 * other workers.
				 */
				recoveryFailed = true;
				break;
			}

			recoveredTransactionCount++;

			/*
			 * We successfully committed the prepared transaction, safe to delete
			 * the recovery record.
			 */
		}
		else if (foundPreparedTransactionAfterCommit)
		{
			/*
			 * We found a committed pg_dist_transaction record that initially did
			 * not have a prepared transaction, but did when we checked again.
			 *
			 * If a transaction started and committed just after we observed the
			 * set of prepared transactions, and just before we called
			 * ActiveDistributedTransactionNumbers, then we would see  a recovery
			 * record without a prepared transaction in pendingTransactionSet,
			 * but there may be prepared transactions that failed to commit.
			 * We should not delete the records for those prepared transactions,
			 * since we would otherwise roll back them on the next call to
			 * recover_prepared_transactions.
			 *
			 * In addition, if the transaction started after the call to
			 * ActiveDistributedTransactionNumbers and finished just before our
			 * pg_dist_transaction snapshot, then it may still be in the process
			 * of comitting the prepared transactions in the post-commit callback
			 * and we should not touch the prepared transactions.
			 *
			 * To handle these cases, we just leave the records and prepared
			 * transactions for the next call to recover_prepared_transactions
			 * and skip them here.
			 */

			continue;
		}
		else
		{
			/*
			 * We found a recovery record without any prepared transaction. It
			 * must have already been committed, so it's safe to delete the
			 * recovery record.
			 *
			 * Transactions that started after we observed pendingTransactionSet,
			 * but successfully committed their prepared transactions before
			 * ActiveDistributedTransactionNumbers are indistinguishable from
			 * transactions that committed at an earlier time, in which case it's
			 * safe delete the recovery record as well.
			 */
		}

		simple_heap_delete(pgDistTransaction, &heapTuple->t_self);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistTransaction, NoLock);

	if (!recoveryFailed)
	{
		char *pendingTransactionName = NULL;
		bool abortSucceeded = true;

		/*
		 * All remaining prepared transactions that are not part of an in-progress
		 * distributed transaction should be aborted since we did not find a recovery
		 * record, which implies the disributed transaction aborted.
		 */
		hash_seq_init(&status, pendingTransactionSet);

		while ((pendingTransactionName = hash_seq_search(&status)) != NULL)
		{
			bool isTransactionInProgress = false;
			bool shouldCommit = false;

			isTransactionInProgress = IsTransactionInProgress(activeTransactionNumberSet,
															  pendingTransactionName);
			if (isTransactionInProgress)
			{
				continue;
			}

			shouldCommit = false;
			abortSucceeded = RecoverPreparedTransactionOnWorker(connection,
																pendingTransactionName,
																shouldCommit);
			if (!abortSucceeded)
			{
				hash_seq_term(&status);
				break;
			}

			recoveredTransactionCount++;
		}
	}

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(localContext);

	return recoveredTransactionCount;
}


/*
 * PendingWorkerTransactionList returns a list of pending prepared
 * transactions on a remote node that were started by this node.
 */
static List *
PendingWorkerTransactionList(MultiConnection *connection)
{
	StringInfo command = makeStringInfo();
	bool raiseInterrupts = true;
	int querySent = 0;
	PGresult *result = NULL;
	int rowCount = 0;
	int rowIndex = 0;
	List *transactionNames = NIL;
	int coordinatorId = GetLocalGroupId();

	appendStringInfo(command, "SELECT gid FROM pg_prepared_xacts "
							  "WHERE gid LIKE 'citus\\_%d\\_%%'",
					 coordinatorId);

	querySent = SendRemoteCommand(connection, command->data);
	if (querySent == 0)
	{
		ReportConnectionError(connection, ERROR);
	}

	result = GetRemoteCommandResult(connection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, ERROR);
	}

	rowCount = PQntuples(result);

	for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		const int columnIndex = 0;
		char *transactionName = PQgetvalue(result, rowIndex, columnIndex);

		transactionNames = lappend(transactionNames, pstrdup(transactionName));
	}

	PQclear(result);
	ForgetResults(connection);

	return transactionNames;
}


/*
 * IsTransactionInProgress returns whether the distributed transaction to which
 * preparedTransactionName belongs is still in progress, or false if the
 * transaction name cannot be parsed. This can happen when the user manually
 * inserts into pg_dist_transaction.
 */
static bool
IsTransactionInProgress(HTAB *activeTransactionNumberSet, char *preparedTransactionName)
{
	int32 groupId = 0;
	int procId = 0;
	uint32 connectionNumber = 0;
	uint64 transactionNumber = 0;
	bool isValidName = false;
	bool isTransactionInProgress = false;

	isValidName = ParsePreparedTransactionName(preparedTransactionName, &groupId, &procId,
											   &transactionNumber, &connectionNumber);
	if (isValidName)
	{
		hash_search(activeTransactionNumberSet, &transactionNumber, HASH_FIND,
					&isTransactionInProgress);
	}

	return isTransactionInProgress;
}


/*
 * RecoverPreparedTransactionOnWorker recovers a single prepared transaction over
 * the given connection. If shouldCommit is true we send
 */
static bool
RecoverPreparedTransactionOnWorker(MultiConnection *connection, char *transactionName,
								   bool shouldCommit)
{
	StringInfo command = makeStringInfo();
	PGresult *result = NULL;
	int executeCommand = 0;
	bool raiseInterrupts = false;

	if (shouldCommit)
	{
		/* should have committed this prepared transaction */
		appendStringInfo(command, "COMMIT PREPARED '%s'", transactionName);
	}
	else
	{
		/* should have aborted this prepared transaction */
		appendStringInfo(command, "ROLLBACK PREPARED '%s'", transactionName);
	}

	executeCommand = ExecuteOptionalRemoteCommand(connection, command->data, &result);
	if (executeCommand == QUERY_SEND_FAILED)
	{
		return false;
	}
	if (executeCommand == RESPONSE_NOT_OKAY)
	{
		return false;
	}

	PQclear(result);
	ClearResults(connection, raiseInterrupts);

	ereport(LOG, (errmsg("recovered a prepared transaction on %s:%d",
						 connection->hostname, connection->port),
				  errcontext("%s", command->data)));

	return true;
}
