/*-------------------------------------------------------------------------
 *
 * transaction_recovery.c
 *
 * Routines for recovering two-phase commits started by this node if a
 * failure occurs between prepare and commit/abort.
 *
 * Copyright (c) 2016, Citus Data, Inc.
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

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_transaction.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
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
static int RecoverPreparedTransactions(void);
static int RecoverWorkerTransactions(WorkerNode *workerNode);
static List * NameListDifference(List *nameList, List *subtractList);
static int CompareNames(const void *leftPointer, const void *rightPointer);
static bool FindMatchingName(char **nameArray, int nameCount, char *needle,
							 int *matchIndex);
static List * PendingWorkerTransactionList(MultiConnection *connection);
static List * UnconfirmedWorkerTransactionsList(int groupId);
static void DeleteTransactionRecord(int32 groupId, char *transactionName);


/*
 * recover_prepared_transactions recovers any pending prepared
 * transactions started by this node on other nodes.
 */
Datum
recover_prepared_transactions(PG_FUNCTION_ARGS)
{
	int recoveredTransactionCount = 0;

	CheckCitusVersion(ERROR);

	recoveredTransactionCount = RecoverPreparedTransactions();

	PG_RETURN_INT32(recoveredTransactionCount);
}


/*
 * LogTransactionRecord registers the fact that a transaction has been
 * prepared on a worker. The presence of this record indicates that the
 * prepared transaction should be committed.
 */
void
LogTransactionRecord(int groupId, char *transactionName)
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

	simple_heap_insert(pgDistTransaction, heapTuple);
	CatalogUpdateIndexes(pgDistTransaction, heapTuple);
	CommandCounterIncrement();

	/* close relation and invalidate previous cache entry */
	heap_close(pgDistTransaction, RowExclusiveLock);
}


/*
 * RecoverPreparedTransactions recovers any pending prepared
 * transactions started by this node on other nodes.
 */
static int
RecoverPreparedTransactions(void)
{
	List *workerList = NIL;
	ListCell *workerNodeCell = NULL;
	int recoveredTransactionCount = 0;

	/*
	 * We block here if metadata transactions are ongoing, since we
	 * mustn't commit/abort their prepared transactions under their
	 * feet. We also prevent concurrent recovery.
	 */
	LockRelationOid(DistTransactionRelationId(), ExclusiveLock);

	workerList = ActiveWorkerNodeList();

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

	int groupId = workerNode->groupId;
	char *nodeName = workerNode->workerName;
	int nodePort = workerNode->workerPort;

	List *pendingTransactionList = NIL;
	ListCell *pendingTransactionCell = NULL;

	List *unconfirmedTransactionList = NIL;
	char **unconfirmedTransactionArray = NULL;
	int unconfirmedTransactionCount = 0;
	int unconfirmedTransactionIndex = 0;

	List *committedTransactionList = NIL;
	ListCell *committedTransactionCell = NULL;

	MemoryContext localContext = NULL;
	MemoryContext oldContext = NULL;

	int connectionFlags = SESSION_LIFESPAN;
	MultiConnection *connection = GetNodeConnection(connectionFlags, nodeName, nodePort);
	if (connection->pgConn == NULL)
	{
		/* cannot recover transactions on this worker right now */
		return 0;
	}

	localContext = AllocSetContextCreate(CurrentMemoryContext,
										 "RecoverWorkerTransactions",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	oldContext = MemoryContextSwitchTo(localContext);

	/* find transactions that were committed, but not yet confirmed */
	unconfirmedTransactionList = UnconfirmedWorkerTransactionsList(groupId);
	unconfirmedTransactionList = SortList(unconfirmedTransactionList, CompareNames);

	/* convert list to an array to use with FindMatchingNames */
	unconfirmedTransactionCount = list_length(unconfirmedTransactionList);
	unconfirmedTransactionArray =
		(char **) PointerArrayFromList(unconfirmedTransactionList);

	/* find stale prepared transactions on the remote node */
	pendingTransactionList = PendingWorkerTransactionList(connection);
	pendingTransactionList = SortList(pendingTransactionList, CompareNames);

	/*
	 * Transactions that have no pending prepared transaction are assumed to
	 * have been committed. Any records in unconfirmedTransactionList that
	 * don't have a transaction in pendingTransactionList can be removed.
	 */
	committedTransactionList = NameListDifference(unconfirmedTransactionList,
												  pendingTransactionList);

	/*
	 * For each pending prepared transaction, check whether there is a transaction
	 * record. If so, commit. If not, the transaction that started the transaction
	 * must have rolled back and thus the prepared transaction should be aborted.
	 */
	foreach(pendingTransactionCell, pendingTransactionList)
	{
		char *transactionName = (char *) lfirst(pendingTransactionCell);
		StringInfo command = makeStringInfo();
		int executeCommand = 0;
		PGresult *result = NULL;

		bool shouldCommit = FindMatchingName(unconfirmedTransactionArray,
											 unconfirmedTransactionCount,
											 transactionName,
											 &unconfirmedTransactionIndex);

		if (shouldCommit)
		{
			/* should have committed this prepared transaction */
			appendStringInfo(command, "COMMIT PREPARED '%s'", transactionName);
		}
		else
		{
			/* no record of this prepared transaction, abort */
			appendStringInfo(command, "ROLLBACK PREPARED '%s'", transactionName);
		}

		executeCommand = ExecuteOptionalRemoteCommand(connection, command->data, &result);
		if (executeCommand == QUERY_SEND_FAILED)
		{
			break;
		}
		if (executeCommand == RESPONSE_NOT_OKAY)
		{
			/* cannot recover this transaction right now */
			continue;
		}

		PQclear(result);
		ForgetResults(connection);

		ereport(NOTICE, (errmsg("recovered a prepared transaction on %s:%d",
								nodeName, nodePort),
						 errcontext("%s", command->data)));

		if (shouldCommit)
		{
			committedTransactionList = lappend(committedTransactionList,
											   transactionName);
		}

		recoveredTransactionCount += 1;
	}

	/* we can remove the transaction records of confirmed transactions */
	foreach(committedTransactionCell, committedTransactionList)
	{
		char *transactionName = (char *) lfirst(committedTransactionCell);

		DeleteTransactionRecord(groupId, transactionName);
	}

	MemoryContextReset(localContext);
	MemoryContextSwitchTo(oldContext);

	return recoveredTransactionCount;
}


/*
 * NameListDifference returns the difference between the bag of
 * names in nameList and subtractList. Both are assumed to be
 * sorted. We cannot use list_difference_ptr here since we need
 * to compare the actual strings.
 */
static List *
NameListDifference(List *nameList, List *subtractList)
{
	List *differenceList = NIL;
	ListCell *nameCell = NULL;

	int subtractIndex = 0;
	int subtractCount = list_length(subtractList);
	char **subtractArray = (char **) PointerArrayFromList(subtractList);

	foreach(nameCell, nameList)
	{
		char *baseName = (char *) lfirst(nameCell);

		bool nameFound = FindMatchingName(subtractArray, subtractCount,
										  baseName, &subtractIndex);

		if (!nameFound)
		{
			/*
			 * baseName is not in subtractArray and thus included
			 * in the difference.
			 */
			differenceList = lappend(differenceList, baseName);
		}
	}

	pfree(subtractArray);

	return differenceList;
}


/*
 * CompareNames compares names using strncmp. Its signature allows it to
 * be used in qsort.
 */
static int
CompareNames(const void *leftPointer, const void *rightPointer)
{
	const char *leftString = *((char **) leftPointer);
	const char *rightString = *((char **) rightPointer);

	int nameCompare = strncmp(leftString, rightString, NAMEDATALEN);

	return nameCompare;
}


/*
 * FindMatchingName searches for name in nameArray, starting at the
 * value pointed to by matchIndex and stopping at the first index of
 * name which is greater or equal to needle. nameArray is assumed
 * to be sorted.
 *
 * The function sets matchIndex to the index of the name and returns
 * true if the name is equal to needle. If matchIndex >= nameCount,
 * then the function always returns false.
 */
static bool
FindMatchingName(char **nameArray, int nameCount, char *needle,
				 int *matchIndex)
{
	bool foundMatchingName = false;
	int searchIndex = *matchIndex;
	int compareResult = -1;

	while (searchIndex < nameCount)
	{
		char *testName = nameArray[searchIndex];
		compareResult = strncmp(needle, testName, NAMEDATALEN);

		if (compareResult <= 0)
		{
			break;
		}

		searchIndex++;
	}

	*matchIndex = searchIndex;

	if (compareResult == 0)
	{
		foundMatchingName = true;
	}

	return foundMatchingName;
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
	int coordinatorId = 0;

	appendStringInfo(command, "SELECT gid FROM pg_prepared_xacts "
							  "WHERE gid LIKE 'citus_%d_%%'",
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
 * UnconfirmedWorkerTransactionList returns a list of unconfirmed transactions
 * for a group of workers from pg_dist_transaction. A transaction is confirmed
 * once we have verified that it does not exist in pg_prepared_xacts on the
 * remote node and the entry in pg_dist_transaction is removed.
 */
static List *
UnconfirmedWorkerTransactionsList(int groupId)
{
	List *transactionNameList = NIL;
	Relation pgDistTransaction = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;

	pgDistTransaction = heap_open(DistTransactionRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_transaction_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(groupId));

	scanDescriptor = systable_beginscan(pgDistTransaction,
										DistTransactionGroupIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistTransaction);
		bool isNull = false;

		Datum transactionNameDatum = heap_getattr(heapTuple,
												  Anum_pg_dist_transaction_gid,
												  tupleDescriptor, &isNull);

		char *transactionName = TextDatumGetCString(transactionNameDatum);
		transactionNameList = lappend(transactionNameList, transactionName);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistTransaction, AccessShareLock);

	return transactionNameList;
}


/*
 * DeleteTransactionRecord opens the pg_dist_transaction system catalog, finds the
 * first (unique) row that corresponds to the given transactionName and worker node,
 * and deletes this row.
 */
static void
DeleteTransactionRecord(int32 groupId, char *transactionName)
{
	Relation pgDistTransaction = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;
	bool heapTupleFound = false;

	pgDistTransaction = heap_open(DistTransactionRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_transaction_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(groupId));

	scanDescriptor = systable_beginscan(pgDistTransaction,
										DistTransactionGroupIndexId(), indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistTransaction);
		bool isNull = false;

		Datum gidDatum = heap_getattr(heapTuple,
									  Anum_pg_dist_transaction_gid,
									  tupleDescriptor, &isNull);

		char *gid = TextDatumGetCString(gidDatum);

		if (strncmp(transactionName, gid, NAMEDATALEN) == 0)
		{
			heapTupleFound = true;
			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* if we couldn't find the transaction record to delete, error out */
	if (!heapTupleFound)
	{
		ereport(ERROR, (errmsg("could not find valid entry for transaction record "
							   "'%s' in group %d",
							   transactionName, groupId)));
	}

	simple_heap_delete(pgDistTransaction, &heapTuple->t_self);
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistTransaction, RowExclusiveLock);
}
