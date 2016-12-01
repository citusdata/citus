/*-------------------------------------------------------------------------
 *
 * multi_copy.c
 *     This file contains implementation of COPY utility for distributed
 *     tables.
 *
 * The CitusCopyFrom function should be called from the utility hook to process
 * COPY ... FROM commands on distributed tables. CitusCopyFrom parses the input
 * from stdin, a program, or a file, and decides to copy new rows to existing
 * shards or new shards based on the partition method of the distributed table.
 * If copy is run a worker node, CitusCopyFrom calls CopyFromWorkerNode which
 * parses the master node copy options and handles communication with the master
 * node.
 *
 * It opens a new connection for every shard placement and uses the PQputCopyData
 * function to copy the data. Because PQputCopyData transmits data, asynchronously,
 * the workers will ingest data at least partially in parallel.
 *
 * For hash-partitioned tables, if it fails to connect to a worker, the master
 * marks the placement for which it was trying to open a connection as inactive,
 * similar to the way DML statements are handled. If a failure occurs after
 * connecting, the transaction is rolled back on all the workers. Note that,
 * in the case of append-partitioned tables, if a fail occurs, immediately
 * metadata changes are rolled back on the master node, but shard placements
 * are left on the worker nodes.
 *
 * By default, COPY uses normal transactions on the workers. In the case of
 * hash or range-partitioned tables, this can cause a problem when some of the
 * transactions fail to commit while others have succeeded. To ensure no data
 * is lost, COPY can use two-phase commit, by increasing max_prepared_transactions
 * on the worker and setting citus.multi_shard_commit_protocol to '2pc'. The default
 * is '1pc'. This is not a problem for append-partitioned tables because new
 * shards are created and in the case of failure, metadata changes are rolled
 * back on the master node.
 *
 * Parsing options are processed and enforced on the node where copy command
 * is run, while constraints are enforced on the worker. In either case,
 * failure causes the whole COPY to roll back.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * With contributions from Postgres Professional.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "plpgsql.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <string.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/nbtree.h"
#include "access/sdir.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "commands/extension.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_protocol.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/memutils.h"


/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

/* use a global connection to the master node in order to skip passing it around */
static PGconn *masterConnection = NULL;


/* Local functions forward declarations */
static void CopyFromWorkerNode(CopyStmt *copyStatement, char *completionTag);
static void CopyToExistingShards(CopyStmt *copyStatement, char *completionTag);
static void CopyToNewShards(CopyStmt *copyStatement, char *completionTag, Oid relationId);
static char MasterPartitionMethod(RangeVar *relation);
static void RemoveMasterOptions(CopyStmt *copyStatement);
static void OpenCopyTransactions(CopyStmt *copyStatement,
								 ShardConnections *shardConnections, bool stopOnFailure,
								 bool useBinaryCopyFormat);
static bool CanUseBinaryCopyFormat(TupleDesc tupleDescription,
								   CopyOutState rowOutputState);
static List * MasterShardPlacementList(uint64 shardId);
static List * RemoteFinalizedShardPlacementList(uint64 shardId);
static void SendCopyBinaryHeaders(CopyOutState copyOutState, List *connectionList);
static void SendCopyBinaryFooters(CopyOutState copyOutState, List *connectionList);
static StringInfo ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId,
										 bool useBinaryCopyFormat);
static void SendCopyDataToAll(StringInfo dataBuffer, List *connectionList);
static void SendCopyDataToPlacement(StringInfo dataBuffer, PGconn *connection,
									int64 shardId);
static void EndRemoteCopy(List *connectionList, bool stopOnFailure);
static void ReportCopyError(PGconn *connection, PGresult *result);
static uint32 AvailableColumnCount(TupleDesc tupleDescriptor);
static void StartCopyToNewShard(ShardConnections *shardConnections,
								CopyStmt *copyStatement, bool useBinaryCopyFormat);
static int64 MasterCreateEmptyShard(char *relationName);
static int64 CreateEmptyShard(char *relationName);
static int64 RemoteCreateEmptyShard(char *relationName);
static void FinalizeCopyToNewShard(ShardConnections *shardConnections);
static void MasterUpdateShardStatistics(uint64 shardId);
static void RemoteUpdateShardStatistics(uint64 shardId);

/* Private functions copied and adapted from copy.c in PostgreSQL */
static void CopySendData(CopyOutState outputState, const void *databuf, int datasize);
static void CopySendString(CopyOutState outputState, const char *str);
static void CopySendChar(CopyOutState outputState, char c);
static void CopySendInt32(CopyOutState outputState, int32 val);
static void CopySendInt16(CopyOutState outputState, int16 val);
static void CopyAttributeOutText(CopyOutState outputState, char *string);
static inline void CopyFlushOutput(CopyOutState outputState, char *start, char *pointer);


/*
 * CitusCopyFrom implements the COPY table_name FROM. It dispacthes the copy
 * statement to related subfunctions based on where the copy command is run
 * and the partition method of the distributed table.
 */
void
CitusCopyFrom(CopyStmt *copyStatement, char *completionTag)
{
	bool isCopyFromWorker = false;

	/* disallow COPY to/from file or program except for superusers */
	if (copyStatement->filename != NULL && !superuser())
	{
		if (copyStatement->is_program)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
	}

	isCopyFromWorker = IsCopyFromWorker(copyStatement);
	if (isCopyFromWorker)
	{
		CopyFromWorkerNode(copyStatement, completionTag);
	}
	else
	{
		Oid relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
		char partitionMethod = PartitionMethod(relationId);

		if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod ==
			DISTRIBUTE_BY_RANGE)
		{
			CopyToExistingShards(copyStatement, completionTag);
		}
		else if (partitionMethod == DISTRIBUTE_BY_APPEND)
		{
			CopyToNewShards(copyStatement, completionTag, relationId);
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("unsupported partition method")));
		}
	}

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * IsCopyFromWorker checks if the given copy statement has the master host option.
 */
bool
IsCopyFromWorker(CopyStmt *copyStatement)
{
	ListCell *optionCell = NULL;
	foreach(optionCell, copyStatement->options)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);
		if (strncmp(defel->defname, "master_host", NAMEDATALEN) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * CopyFromWorkerNode implements the COPY table_name FROM ... from worker nodes
 * for append-partitioned tables.
 */
static void
CopyFromWorkerNode(CopyStmt *copyStatement, char *completionTag)
{
	NodeAddress *masterNodeAddress = MasterNodeAddress(copyStatement);
	char *nodeName = masterNodeAddress->nodeName;
	int32 nodePort = masterNodeAddress->nodePort;
	char *nodeUser = CurrentUserName();

	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("distributed copy operations must not appear in "
							   "transaction blocks containing other distributed "
							   "modifications")));
	}

	masterConnection = ConnectToNode(nodeName, nodePort, nodeUser);

	PG_TRY();
	{
		PGresult *queryResult = NULL;
		Oid relationId = InvalidOid;
		char partitionMethod = 0;

		/* strip schema name for local reference */
		char *schemaName = copyStatement->relation->schemaname;
		copyStatement->relation->schemaname = NULL;

		relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);

		/* put schema name back */
		copyStatement->relation->schemaname = schemaName;

		partitionMethod = MasterPartitionMethod(copyStatement->relation);
		if (partitionMethod != DISTRIBUTE_BY_APPEND)
		{
			ereport(ERROR, (errmsg("copy from worker nodes is only supported "
								   "for append-partitioned tables")));
		}

		/* run all metadata commands in a transaction */
		queryResult = PQexec(masterConnection, "BEGIN");
		if (PQresultStatus(queryResult) != PGRES_COMMAND_OK)
		{
			ereport(ERROR, (errmsg("could not start to update master node metadata")));
		}

		PQclear(queryResult);

		/*
		 * Remove master node options from the copy statement because they are not
		 * recognized by PostgreSQL machinery.
		 */
		RemoveMasterOptions(copyStatement);

		CopyToNewShards(copyStatement, completionTag, relationId);

		/* commit metadata transactions */
		queryResult = PQexec(masterConnection, "COMMIT");
		if (PQresultStatus(queryResult) != PGRES_COMMAND_OK)
		{
			ereport(ERROR, (errmsg("could not commit master node metadata changes")));
		}

		PQclear(queryResult);

		/* close the connection */
		CloseConnectionByPGconn(masterConnection);
		masterConnection = NULL;
	}
	PG_CATCH();
	{
		/* close the connection */
		CloseConnectionByPGconn(masterConnection);
		masterConnection = NULL;

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * CopyToExistingShards implements the COPY table_name FROM ... for hash or
 * range-partitioned tables where there are already shards into which to copy
 * rows.
 */
static void
CopyToExistingShards(CopyStmt *copyStatement, char *completionTag)
{
	Oid tableId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
	char *relationName = get_rel_name(tableId);
	Relation distributedRelation = NULL;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	FmgrInfo *hashFunction = NULL;
	FmgrInfo *compareFunction = NULL;
	bool hasUniformHashDistribution = false;
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(tableId);
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	int shardCount = 0;
	List *shardIntervalList = NULL;
	ShardInterval **shardIntervalCache = NULL;
	bool useBinarySearch = false;

	HTAB *copyConnectionHash = NULL;
	ShardConnections *shardConnections = NULL;
	List *connectionList = NIL;

	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;

	CopyState copyState = NULL;
	CopyOutState copyOutState = NULL;
	FmgrInfo *columnOutputFunctions = NULL;
	uint64 processedRowCount = 0;

	Var *partitionColumn = PartitionColumn(tableId, 0);
	char partitionMethod = PartitionMethod(tableId);

	/* get hash function for partition column */
	hashFunction = cacheEntry->hashFunction;

	/* get compare function for shard intervals */
	compareFunction = cacheEntry->shardIntervalCompareFunction;

	/* allocate column values and nulls arrays */
	distributedRelation = heap_open(tableId, RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(distributedRelation);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	/* load the list of shards and verify that we have shards to copy into */
	shardIntervalList = LoadShardIntervalList(tableId);
	if (shardIntervalList == NIL)
	{
		if (partitionMethod == DISTRIBUTE_BY_HASH)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not find any shards into which to copy"),
							errdetail("No shards exist for distributed table \"%s\".",
									  relationName),
							errhint("Run master_create_worker_shards to create shards "
									"and try again.")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not find any shards into which to copy"),
							errdetail("No shards exist for distributed table \"%s\".",
									  relationName)));
		}
	}

	/* error if any shard missing min/max values */
	if (cacheEntry->hasUninitializedShardInterval)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not start copy"),
						errdetail("Distributed relation \"%s\" has shards "
								  "with missing shardminvalue/shardmaxvalue.",
								  relationName)));
	}

	/* prevent concurrent placement changes and non-commutative DML statements */
	LockShardListMetadata(shardIntervalList, ShareLock);
	LockShardListResources(shardIntervalList, ShareLock);

	/* initialize the shard interval cache */
	shardCount = cacheEntry->shardIntervalArrayLength;
	shardIntervalCache = cacheEntry->sortedShardIntervalArray;
	hasUniformHashDistribution = cacheEntry->hasUniformHashDistribution;

	/* determine whether to use binary search */
	if (partitionMethod != DISTRIBUTE_BY_HASH || !hasUniformHashDistribution)
	{
		useBinarySearch = true;
	}

	/* initialize copy state to read from COPY data source */
	copyState = BeginCopyFrom(distributedRelation,
							  copyStatement->filename,
							  copyStatement->is_program,
							  copyStatement->attlist,
							  copyStatement->options);

	executorState = CreateExecutorState();
	executorTupleContext = GetPerTupleMemoryContext(executorState);
	executorExpressionContext = GetPerTupleExprContext(executorState);

	copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = CanUseBinaryCopyFormat(tupleDescriptor, copyOutState);
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = executorTupleContext;

	columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, copyOutState->binary);

	/*
	 * Create a mapping of shard id to a connection for each of its placements.
	 * The hash should be initialized before the PG_TRY, since it is used in
	 * PG_CATCH. Otherwise, it may be undefined in the PG_CATCH (see sigsetjmp
	 * documentation).
	 */
	copyConnectionHash = CreateShardConnectionHash(TopTransactionContext);

	/* we use a PG_TRY block to roll back on errors (e.g. in NextCopyFrom) */
	PG_TRY();
	{
		ErrorContextCallback errorCallback;

		/* set up callback to identify error line number */
		errorCallback.callback = CopyFromErrorCallback;
		errorCallback.arg = (void *) copyState;
		errorCallback.previous = error_context_stack;
		error_context_stack = &errorCallback;

		/* ensure transactions have unique names on worker nodes */
		InitializeDistributedTransaction();

		while (true)
		{
			bool nextRowFound = false;
			Datum partitionColumnValue = 0;
			ShardInterval *shardInterval = NULL;
			int64 shardId = 0;
			bool shardConnectionsFound = false;
			MemoryContext oldContext = NULL;

			ResetPerTupleExprContext(executorState);

			oldContext = MemoryContextSwitchTo(executorTupleContext);

			/* parse a row from the input */
			nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
										columnValues, columnNulls, NULL);

			if (!nextRowFound)
			{
				MemoryContextSwitchTo(oldContext);
				break;
			}

			CHECK_FOR_INTERRUPTS();

			/* find the partition column value */

			if (columnNulls[partitionColumn->varattno - 1])
			{
				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg("cannot copy row with NULL value "
									   "in partition column")));
			}

			partitionColumnValue = columnValues[partitionColumn->varattno - 1];

			/* find the shard interval and id for the partition column value */
			shardInterval = FindShardInterval(partitionColumnValue, shardIntervalCache,
											  shardCount, partitionMethod,
											  compareFunction, hashFunction,
											  useBinarySearch);
			if (shardInterval == NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("could not find shard for partition column "
									   "value")));
			}

			shardId = shardInterval->shardId;

			MemoryContextSwitchTo(oldContext);

			/* get existing connections to the shard placements, if any */
			shardConnections = GetShardHashConnections(copyConnectionHash, shardId,
													   &shardConnectionsFound);
			if (!shardConnectionsFound)
			{
				/* open connections and initiate COPY on shard placements */
				OpenCopyTransactions(copyStatement, shardConnections, false,
									 copyOutState->binary);

				/* send copy binary headers to shard placements */
				if (copyOutState->binary)
				{
					SendCopyBinaryHeaders(copyOutState,
										  shardConnections->connectionList);
				}
			}

			/* replicate row to shard placements */
			resetStringInfo(copyOutState->fe_msgbuf);
			AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
							  copyOutState, columnOutputFunctions);
			SendCopyDataToAll(copyOutState->fe_msgbuf, shardConnections->connectionList);

			processedRowCount += 1;
		}

		connectionList = ConnectionList(copyConnectionHash);

		/* send copy binary footers to all shard placements */
		if (copyOutState->binary)
		{
			SendCopyBinaryFooters(copyOutState, connectionList);
		}

		/* all lines have been copied, stop showing line number in errors */
		error_context_stack = errorCallback.previous;

		/* close the COPY input on all shard placements */
		EndRemoteCopy(connectionList, true);

		if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
		{
			PrepareRemoteTransactions(connectionList);
		}

		EndCopyFrom(copyState);
		heap_close(distributedRelation, NoLock);

		/* check for cancellation one last time before committing */
		CHECK_FOR_INTERRUPTS();
	}
	PG_CATCH();
	{
		List *abortConnectionList = NIL;

		/* roll back all transactions */
		abortConnectionList = ConnectionList(copyConnectionHash);
		EndRemoteCopy(abortConnectionList, false);
		AbortRemoteTransactions(abortConnectionList);
		CloseConnections(abortConnectionList);

		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Ready to commit the transaction, this code is below the PG_TRY block because
	 * we do not want any of the transactions rolled back if a failure occurs. Instead,
	 * they should be rolled forward.
	 */
	CommitRemoteTransactions(connectionList, false);
	CloseConnections(connectionList);

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
	}
}


/*
 * CopyToNewShards implements the COPY table_name FROM ... for append-partitioned
 * tables where we create new shards into which to copy rows.
 */
static void
CopyToNewShards(CopyStmt *copyStatement, char *completionTag, Oid relationId)
{
	FmgrInfo *columnOutputFunctions = NULL;

	/* allocate column values and nulls arrays */
	Relation distributedRelation = heap_open(relationId, RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(distributedRelation);
	uint32 columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);

	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	/*
	 * Shard connections should be initialized before the PG_TRY, since it is
	 * used in PG_CATCH. Otherwise, it may be undefined in the PG_CATCH
	 * (see sigsetjmp documentation).
	 */
	ShardConnections *shardConnections =
		(ShardConnections *) palloc0(sizeof(ShardConnections));

	/* initialize copy state to read from COPY data source */
	CopyState copyState = BeginCopyFrom(distributedRelation,
										copyStatement->filename,
										copyStatement->is_program,
										copyStatement->attlist,
										copyStatement->options);

	CopyOutState copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = CanUseBinaryCopyFormat(tupleDescriptor, copyOutState);
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = executorTupleContext;

	columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, copyOutState->binary);

	/* we use a PG_TRY block to close connections on errors (e.g. in NextCopyFrom) */
	PG_TRY();
	{
		uint64 shardMaxSizeInBytes = (int64) ShardMaxSize * 1024L;
		uint64 copiedDataSizeInBytes = 0;
		uint64 processedRowCount = 0;

		/* set up callback to identify error line number */
		ErrorContextCallback errorCallback;

		errorCallback.callback = CopyFromErrorCallback;
		errorCallback.arg = (void *) copyState;
		errorCallback.previous = error_context_stack;

		while (true)
		{
			bool nextRowFound = false;
			MemoryContext oldContext = NULL;
			uint64 messageBufferSize = 0;

			ResetPerTupleExprContext(executorState);

			/* switch to tuple memory context and start showing line number in errors */
			error_context_stack = &errorCallback;
			oldContext = MemoryContextSwitchTo(executorTupleContext);

			/* parse a row from the input */
			nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
										columnValues, columnNulls, NULL);

			if (!nextRowFound)
			{
				/* switch to regular memory context and stop showing line number in errors */
				MemoryContextSwitchTo(oldContext);
				error_context_stack = errorCallback.previous;
				break;
			}

			CHECK_FOR_INTERRUPTS();

			/* switch to regular memory context and stop showing line number in errors */
			MemoryContextSwitchTo(oldContext);
			error_context_stack = errorCallback.previous;

			/*
			 * If copied data size is zero, this means either this is the first
			 * line in the copy or we just filled the previous shard up to its
			 * capacity. Either way, we need to create a new shard and
			 * start copying new rows into it.
			 */
			if (copiedDataSizeInBytes == 0)
			{
				/* create shard and open connections to shard placements */
				StartCopyToNewShard(shardConnections, copyStatement,
									copyOutState->binary);

				/* send copy binary headers to shard placements */
				if (copyOutState->binary)
				{
					SendCopyBinaryHeaders(copyOutState,
										  shardConnections->connectionList);
				}
			}

			/* replicate row to shard placements */
			resetStringInfo(copyOutState->fe_msgbuf);
			AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
							  copyOutState, columnOutputFunctions);
			SendCopyDataToAll(copyOutState->fe_msgbuf, shardConnections->connectionList);

			messageBufferSize = copyOutState->fe_msgbuf->len;
			copiedDataSizeInBytes = copiedDataSizeInBytes + messageBufferSize;

			/*
			 * If we filled up this shard to its capacity, send copy binary footers
			 * to shard placements, commit copy transactions, close connections
			 * and finally update shard statistics.
			 *
			 * */
			if (copiedDataSizeInBytes > shardMaxSizeInBytes)
			{
				if (copyOutState->binary)
				{
					SendCopyBinaryFooters(copyOutState,
										  shardConnections->connectionList);
				}
				FinalizeCopyToNewShard(shardConnections);
				MasterUpdateShardStatistics(shardConnections->shardId);

				copiedDataSizeInBytes = 0;
			}

			processedRowCount += 1;
		}

		/*
		 * For the last shard, send copy binary footers to shard placements,
		 * commit copy transactions, close connections and finally update shard
		 * statistics. If no row is send, there is no shard to finalize the
		 * copy command.
		 */
		if (copiedDataSizeInBytes > 0)
		{
			if (copyOutState->binary)
			{
				SendCopyBinaryFooters(copyOutState,
									  shardConnections->connectionList);
			}
			FinalizeCopyToNewShard(shardConnections);
			MasterUpdateShardStatistics(shardConnections->shardId);
		}

		EndCopyFrom(copyState);
		heap_close(distributedRelation, NoLock);

		/* check for cancellation one last time before returning */
		CHECK_FOR_INTERRUPTS();

		if (completionTag != NULL)
		{
			snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
					 "COPY " UINT64_FORMAT, processedRowCount);
		}
	}
	PG_CATCH();
	{
		/* roll back all transactions */
		EndRemoteCopy(shardConnections->connectionList, false);
		AbortRemoteTransactions(shardConnections->connectionList);
		CloseConnections(shardConnections->connectionList);

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * MasterNodeAddress gets the master node address from copy options and returns
 * it. Note that if the master_port is not provided, we use 5432 as the default
 * port.
 */
NodeAddress *
MasterNodeAddress(CopyStmt *copyStatement)
{
	NodeAddress *masterNodeAddress = (NodeAddress *) palloc0(sizeof(NodeAddress));
	char *nodeName = NULL;

	/* set default port to 5432 */
	int32 nodePort = 5432;

	ListCell *optionCell = NULL;
	foreach(optionCell, copyStatement->options)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);
		if (strncmp(defel->defname, "master_host", NAMEDATALEN) == 0)
		{
			nodeName = defGetString(defel);
		}
		else if (strncmp(defel->defname, "master_port", NAMEDATALEN) == 0)
		{
			nodePort = defGetInt32(defel);
		}
	}

	masterNodeAddress->nodeName = nodeName;
	masterNodeAddress->nodePort = nodePort;

	return masterNodeAddress;
}


/*
 * MasterPartitionMethod gets the partition method of the given relation from
 * the master node and returns it.
 */
static char
MasterPartitionMethod(RangeVar *relation)
{
	char partitionMethod = '\0';
	PGresult *queryResult = NULL;

	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;
	char *qualifiedName = quote_qualified_identifier(schemaName, relationName);

	StringInfo partitionMethodCommand = makeStringInfo();
	appendStringInfo(partitionMethodCommand, PARTITION_METHOD_QUERY, qualifiedName);

	queryResult = PQexec(masterConnection, partitionMethodCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		char *partitionMethodString = PQgetvalue((PGresult *) queryResult, 0, 0);
		if (partitionMethodString == NULL || (*partitionMethodString) == '\0')
		{
			ereport(ERROR, (errmsg("could not find a partition method for the "
								   "table %s", relationName)));
		}

		partitionMethod = partitionMethodString[0];
	}
	else
	{
		WarnRemoteError(masterConnection, queryResult);
		ereport(ERROR, (errmsg("could not get the partition method of the "
							   "distributed table")));
	}

	PQclear(queryResult);

	return partitionMethod;
}


/*
 * RemoveMasterOptions removes master node related copy options from the option
 * list of the copy statement.
 */
static void
RemoveMasterOptions(CopyStmt *copyStatement)
{
	List *newOptionList = NIL;
	ListCell *optionCell = NULL;

	/* walk over the list of all options */
	foreach(optionCell, copyStatement->options)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);

		/* skip master related options */
		if ((strncmp(option->defname, "master_host", NAMEDATALEN) == 0) ||
			(strncmp(option->defname, "master_port", NAMEDATALEN) == 0))
		{
			continue;
		}

		newOptionList = lappend(newOptionList, option);
	}

	copyStatement->options = newOptionList;
}


/*
 * OpenCopyTransactions opens a connection for each placement of a shard and
 * starts a COPY transaction. If a connection cannot be opened, then the shard
 * placement is marked as inactive and the COPY continues with the remaining
 * shard placements.
 */
static void
OpenCopyTransactions(CopyStmt *copyStatement, ShardConnections *shardConnections,
					 bool stopOnFailure, bool useBinaryCopyFormat)
{
	List *finalizedPlacementList = NIL;
	List *failedPlacementList = NIL;
	ListCell *placementCell = NULL;
	ListCell *failedPlacementCell = NULL;
	List *connectionList = NULL;
	int64 shardId = shardConnections->shardId;

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "OpenCopyTransactions",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);

	/* release finalized placement list at the end of this function */
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	finalizedPlacementList = MasterShardPlacementList(shardId);

	MemoryContextSwitchTo(oldContext);

	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("distributed copy operations must not appear in "
							   "transaction blocks containing other distributed "
							   "modifications")));
	}

	foreach(placementCell, finalizedPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		char *nodeName = placement->nodeName;
		int nodePort = placement->nodePort;
		WorkerNode *workerNode = FindWorkerNode(nodeName, nodePort);
		int workerGroupId = 0;
		char *nodeUser = CurrentUserName();
		PGconn *connection = ConnectToNode(nodeName, nodePort, nodeUser);

		TransactionConnection *transactionConnection = NULL;
		StringInfo copyCommand = NULL;
		PGresult *result = NULL;

		/*
		 * When a copy is initiated from a worker, the information about the connected
		 * worker node may not be found if pg_dist_node entries are not synced to this
		 * node. In that case we leave the groupId as 0. Fortunately, it is unused since
		 * COPY from a worker does not initiate a 2PC.
		 */
		if (workerNode != NULL)
		{
			workerGroupId = workerNode->groupId;
		}

		if (connection == NULL)
		{
			if (stopOnFailure)
			{
				ereport(ERROR, (errmsg("could not open connection to %s:%d",
									   nodeName, nodePort)));
			}

			failedPlacementList = lappend(failedPlacementList, placement);
			continue;
		}

		result = PQexec(connection, "BEGIN");
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			WarnRemoteError(connection, result);
			failedPlacementList = lappend(failedPlacementList, placement);

			PQclear(result);
			continue;
		}

		PQclear(result);
		copyCommand = ConstructCopyStatement(copyStatement, shardConnections->shardId,
											 useBinaryCopyFormat);

		result = PQexec(connection, copyCommand->data);
		if (PQresultStatus(result) != PGRES_COPY_IN)
		{
			WarnRemoteError(connection, result);
			failedPlacementList = lappend(failedPlacementList, placement);

			PQclear(result);
			continue;
		}

		PQclear(result);

		transactionConnection = palloc0(sizeof(TransactionConnection));

		transactionConnection->groupId = workerGroupId;
		transactionConnection->connectionId = shardConnections->shardId;
		transactionConnection->transactionState = TRANSACTION_STATE_COPY_STARTED;
		transactionConnection->connection = connection;
		transactionConnection->nodeName = nodeName;
		transactionConnection->nodePort = nodePort;

		connectionList = lappend(connectionList, transactionConnection);
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(finalizedPlacementList))
	{
		ereport(ERROR, (errmsg("could not find any active placements")));
	}

	/*
	 * If stopOnFailure is true, we just error out and code execution should
	 * never reach to this point. This is the case for copy from worker nodes.
	 */
	Assert(!stopOnFailure || list_length(failedPlacementList) == 0);

	/* otherwise, mark failed placements as inactive: they're stale */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);

		UpdateShardPlacementState(failedPlacement->placementId, FILE_INACTIVE);
	}

	shardConnections->connectionList = connectionList;

	MemoryContextReset(localContext);
}


/*
 * CanUseBinaryCopyFormat iterates over columns of the relation given in rowOutputState
 * and looks for a column whose type is array of user-defined type or composite type.
 * If it finds such column, that means we cannot use binary format for COPY, because
 * binary format sends Oid of the types, which are generally not same in master and
 * worker nodes for user-defined types.
 */
static bool
CanUseBinaryCopyFormat(TupleDesc tupleDescription, CopyOutState rowOutputState)
{
	bool useBinaryCopyFormat = true;
	int totalColumnCount = tupleDescription->natts;
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescription->attrs[columnIndex];
		Oid typeId = InvalidOid;
		char typeCategory = '\0';
		bool typePreferred = false;

		if (currentColumn->attisdropped)
		{
			continue;
		}

		typeId = currentColumn->atttypid;
		if (typeId >= FirstNormalObjectId)
		{
			get_type_category_preferred(typeId, &typeCategory, &typePreferred);
			if (typeCategory == TYPCATEGORY_ARRAY ||
				typeCategory == TYPCATEGORY_COMPOSITE)
			{
				useBinaryCopyFormat = false;
				break;
			}
		}
	}

	return useBinaryCopyFormat;
}


/*
 * MasterShardPlacementList dispatches the finalized shard placements call
 * between local or remote master node according to the master connection state.
 */
static List *
MasterShardPlacementList(uint64 shardId)
{
	List *finalizedPlacementList = NIL;
	if (masterConnection == NULL)
	{
		finalizedPlacementList = FinalizedShardPlacementList(shardId);
	}
	else
	{
		finalizedPlacementList = RemoteFinalizedShardPlacementList(shardId);
	}

	return finalizedPlacementList;
}


/*
 * RemoteFinalizedShardPlacementList gets the finalized shard placement list
 * for the given shard id from the remote master node.
 */
static List *
RemoteFinalizedShardPlacementList(uint64 shardId)
{
	List *finalizedPlacementList = NIL;
	PGresult *queryResult = NULL;

	StringInfo shardPlacementsCommand = makeStringInfo();
	appendStringInfo(shardPlacementsCommand, FINALIZED_SHARD_PLACEMENTS_QUERY, shardId);

	queryResult = PQexec(masterConnection, shardPlacementsCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		int rowCount = PQntuples(queryResult);
		int rowIndex = 0;

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			char *nodeName = PQgetvalue(queryResult, rowIndex, 0);

			char *nodePortString = PQgetvalue(queryResult, rowIndex, 1);
			uint32 nodePort = atoi(nodePortString);

			ShardPlacement *shardPlacement =
				(ShardPlacement *) palloc0(sizeof(ShardPlacement));

			shardPlacement->nodeName = nodeName;
			shardPlacement->nodePort = nodePort;

			finalizedPlacementList = lappend(finalizedPlacementList, shardPlacement);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("could not get shard placements from the master node")));
	}

	return finalizedPlacementList;
}


/* Send copy binary headers to given connections */
static void
SendCopyBinaryHeaders(CopyOutState copyOutState, List *connectionList)
{
	resetStringInfo(copyOutState->fe_msgbuf);
	AppendCopyBinaryHeaders(copyOutState);
	SendCopyDataToAll(copyOutState->fe_msgbuf, connectionList);
}


/* Send copy binary footers to given connections */
static void
SendCopyBinaryFooters(CopyOutState copyOutState, List *connectionList)
{
	resetStringInfo(copyOutState->fe_msgbuf);
	AppendCopyBinaryFooters(copyOutState);
	SendCopyDataToAll(copyOutState->fe_msgbuf, connectionList);
}


/*
 * ConstructCopyStatement constructs the text of a COPY statement for a particular
 * shard.
 */
static StringInfo
ConstructCopyStatement(CopyStmt *copyStatement, int64 shardId, bool useBinaryCopyFormat)
{
	StringInfo command = makeStringInfo();

	char *schemaName = copyStatement->relation->schemaname;
	char *relationName = copyStatement->relation->relname;

	char *shardName = pstrdup(relationName);
	char *shardQualifiedName = NULL;
	const char *copyFormat = NULL;

	AppendShardIdToName(&shardName, shardId);

	shardQualifiedName = quote_qualified_identifier(schemaName, shardName);

	if (useBinaryCopyFormat)
	{
		copyFormat = "BINARY";
	}
	else
	{
		copyFormat = "TEXT";
	}
	appendStringInfo(command, "COPY %s FROM STDIN WITH (FORMAT %s)", shardQualifiedName,
					 copyFormat);

	return command;
}


/*
 * SendCopyDataToAll sends copy data to all connections in a list.
 */
static void
SendCopyDataToAll(StringInfo dataBuffer, List *connectionList)
{
	ListCell *connectionCell = NULL;
	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 shardId = transactionConnection->connectionId;

		SendCopyDataToPlacement(dataBuffer, connection, shardId);
	}
}


/*
 * SendCopyDataToPlacement sends serialized COPY data to a specific shard placement
 * over the given connection.
 */
static void
SendCopyDataToPlacement(StringInfo dataBuffer, PGconn *connection, int64 shardId)
{
	int copyResult = PQputCopyData(connection, dataBuffer->data, dataBuffer->len);
	if (copyResult != 1)
	{
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");

		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to COPY to shard %ld on %s:%s",
							   shardId, nodeName, nodePort)));
	}
}


/*
 * EndRemoteCopy ends the COPY input on all connections. If stopOnFailure
 * is true, then EndRemoteCopy reports an error on failure, otherwise it
 * reports a warning or continues.
 */
static void
EndRemoteCopy(List *connectionList, bool stopOnFailure)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 shardId = transactionConnection->connectionId;
		int copyEndResult = 0;
		PGresult *result = NULL;

		if (transactionConnection->transactionState != TRANSACTION_STATE_COPY_STARTED)
		{
			/* a failure occurred after having previously called EndRemoteCopy */
			continue;
		}

		/* end the COPY input */
		copyEndResult = PQputCopyEnd(connection, NULL);
		transactionConnection->transactionState = TRANSACTION_STATE_OPEN;

		if (copyEndResult != 1)
		{
			if (stopOnFailure)
			{
				char *nodeName = ConnectionGetOptionValue(connection, "host");
				char *nodePort = ConnectionGetOptionValue(connection, "port");

				ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
								errmsg("failed to COPY to shard %ld on %s:%s",
									   shardId, nodeName, nodePort)));
			}

			continue;
		}

		/* check whether there were any COPY errors */
		result = PQgetResult(connection);
		if (PQresultStatus(result) != PGRES_COMMAND_OK && stopOnFailure)
		{
			ReportCopyError(connection, result);
		}

		PQclear(result);
	}
}


/*
 * ReportCopyError tries to report a useful error message for the user from
 * the remote COPY error messages.
 */
static void
ReportCopyError(PGconn *connection, PGresult *result)
{
	char *remoteMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);

	if (remoteMessage != NULL)
	{
		/* probably a constraint violation, show remote message and detail */
		char *remoteDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);

		ereport(ERROR, (errmsg("%s", remoteMessage),
						errdetail("%s", remoteDetail)));
	}
	else
	{
		/* probably a connection problem, get the message from the connection */
		char *lastNewlineIndex = NULL;
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");

		remoteMessage = PQerrorMessage(connection);
		lastNewlineIndex = strrchr(remoteMessage, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}

		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to complete COPY on %s:%s", nodeName, nodePort),
						errdetail("%s", remoteMessage)));
	}
}


/*
 * ColumnOutputFunctions walks over a table's columns, and finds each column's
 * type information. The function then resolves each type's output function,
 * and stores and returns these output functions in an array.
 */
FmgrInfo *
ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat)
{
	uint32 columnCount = (uint32) rowDescriptor->natts;
	FmgrInfo *columnOutputFunctions = palloc0(columnCount * sizeof(FmgrInfo));

	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *currentOutputFunction = &columnOutputFunctions[columnIndex];
		Form_pg_attribute currentColumn = rowDescriptor->attrs[columnIndex];
		Oid columnTypeId = currentColumn->atttypid;
		Oid outputFunctionId = InvalidOid;
		bool typeVariableLength = false;

		if (currentColumn->attisdropped)
		{
			/* dropped column, leave the output function NULL */
			continue;
		}
		else if (binaryFormat)
		{
			getTypeBinaryOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}
		else
		{
			getTypeOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}

		fmgr_info(outputFunctionId, currentOutputFunction);
	}

	return columnOutputFunctions;
}


/*
 * AppendCopyRowData serializes one row using the column output functions,
 * and appends the data to the row output state object's message buffer.
 * This function is modeled after the CopyOneRowTo() function in
 * commands/copy.c, but only implements a subset of that functionality.
 * Note that the caller of this function should reset row memory context
 * to not bloat memory usage.
 */
void
AppendCopyRowData(Datum *valueArray, bool *isNullArray, TupleDesc rowDescriptor,
				  CopyOutState rowOutputState, FmgrInfo *columnOutputFunctions)
{
	uint32 totalColumnCount = (uint32) rowDescriptor->natts;
	uint32 availableColumnCount = AvailableColumnCount(rowDescriptor);
	uint32 appendedColumnCount = 0;
	uint32 columnIndex = 0;

	MemoryContext oldContext = MemoryContextSwitchTo(rowOutputState->rowcontext);

	if (rowOutputState->binary)
	{
		CopySendInt16(rowOutputState, availableColumnCount);
	}

	for (columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = rowDescriptor->attrs[columnIndex];
		Datum value = valueArray[columnIndex];
		bool isNull = isNullArray[columnIndex];
		bool lastColumn = false;

		if (currentColumn->attisdropped)
		{
			continue;
		}
		else if (rowOutputState->binary)
		{
			if (!isNull)
			{
				FmgrInfo *outputFunctionPointer = &columnOutputFunctions[columnIndex];
				bytea *outputBytes = SendFunctionCall(outputFunctionPointer, value);

				CopySendInt32(rowOutputState, VARSIZE(outputBytes) - VARHDRSZ);
				CopySendData(rowOutputState, VARDATA(outputBytes),
							 VARSIZE(outputBytes) - VARHDRSZ);
			}
			else
			{
				CopySendInt32(rowOutputState, -1);
			}
		}
		else
		{
			if (!isNull)
			{
				FmgrInfo *outputFunctionPointer = &columnOutputFunctions[columnIndex];
				char *columnText = OutputFunctionCall(outputFunctionPointer, value);

				CopyAttributeOutText(rowOutputState, columnText);
			}
			else
			{
				CopySendString(rowOutputState, rowOutputState->null_print_client);
			}

			lastColumn = ((appendedColumnCount + 1) == availableColumnCount);
			if (!lastColumn)
			{
				CopySendChar(rowOutputState, rowOutputState->delim[0]);
			}
		}

		appendedColumnCount++;
	}

	if (!rowOutputState->binary)
	{
		/* append default line termination string depending on the platform */
#ifndef WIN32
		CopySendChar(rowOutputState, '\n');
#else
		CopySendString(rowOutputState, "\r\n");
#endif
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * AvailableColumnCount returns the number of columns in a tuple descriptor, excluding
 * columns that were dropped.
 */
static uint32
AvailableColumnCount(TupleDesc tupleDescriptor)
{
	uint32 columnCount = 0;
	uint32 columnIndex = 0;

	for (columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescriptor->attrs[columnIndex];

		if (!currentColumn->attisdropped)
		{
			columnCount++;
		}
	}

	return columnCount;
}


/*
 * AppendCopyBinaryHeaders appends binary headers to the copy buffer in
 * headerOutputState.
 */
void
AppendCopyBinaryHeaders(CopyOutState headerOutputState)
{
	const int32 zero = 0;
	MemoryContext oldContext = MemoryContextSwitchTo(headerOutputState->rowcontext);

	/* Signature */
	CopySendData(headerOutputState, BinarySignature, 11);

	/* Flags field (no OIDs) */
	CopySendInt32(headerOutputState, zero);

	/* No header extension */
	CopySendInt32(headerOutputState, zero);

	MemoryContextSwitchTo(oldContext);
}


/*
 * AppendCopyBinaryFooters appends binary footers to the copy buffer in
 * footerOutputState.
 */
void
AppendCopyBinaryFooters(CopyOutState footerOutputState)
{
	int16 negative = -1;
	MemoryContext oldContext = MemoryContextSwitchTo(footerOutputState->rowcontext);

	CopySendInt16(footerOutputState, negative);

	MemoryContextSwitchTo(oldContext);
}


/*
 * StartCopyToNewShard creates a new shard and related shard placements and
 * opens connections to shard placements.
 */
static void
StartCopyToNewShard(ShardConnections *shardConnections, CopyStmt *copyStatement,
					bool useBinaryCopyFormat)
{
	char *relationName = copyStatement->relation->relname;
	char *schemaName = copyStatement->relation->schemaname;
	char *qualifiedName = quote_qualified_identifier(schemaName, relationName);

	int64 shardId = MasterCreateEmptyShard(qualifiedName);

	shardConnections->shardId = shardId;

	list_free_deep(shardConnections->connectionList);
	shardConnections->connectionList = NIL;

	/* connect to shards placements and start transactions */
	OpenCopyTransactions(copyStatement, shardConnections, true, useBinaryCopyFormat);
}


/*
 * MasterCreateEmptyShard dispatches the create empty shard call between local or
 * remote master node according to the master connection state.
 */
static int64
MasterCreateEmptyShard(char *relationName)
{
	int64 shardId = 0;
	if (masterConnection == NULL)
	{
		shardId = CreateEmptyShard(relationName);
	}
	else
	{
		shardId = RemoteCreateEmptyShard(relationName);
	}

	return shardId;
}


/*
 * CreateEmptyShard creates a new shard and related shard placements from the
 * local master node.
 */
static int64
CreateEmptyShard(char *relationName)
{
	int64 shardId = 0;

	text *relationNameText = cstring_to_text(relationName);
	Datum relationNameDatum = PointerGetDatum(relationNameText);
	Datum shardIdDatum = DirectFunctionCall1(master_create_empty_shard,
											 relationNameDatum);
	shardId = DatumGetInt64(shardIdDatum);

	return shardId;
}


/*
 * RemoteCreateEmptyShard creates a new shard and related shard placements from
 * the remote master node.
 */
static int64
RemoteCreateEmptyShard(char *relationName)
{
	int64 shardId = 0;
	PGresult *queryResult = NULL;

	StringInfo createEmptyShardCommand = makeStringInfo();
	appendStringInfo(createEmptyShardCommand, CREATE_EMPTY_SHARD_QUERY, relationName);

	queryResult = PQexec(masterConnection, createEmptyShardCommand->data);
	if (PQresultStatus(queryResult) == PGRES_TUPLES_OK)
	{
		char *shardIdString = PQgetvalue((PGresult *) queryResult, 0, 0);
		char *shardIdStringEnd = NULL;
		shardId = strtoul(shardIdString, &shardIdStringEnd, 0);
	}
	else
	{
		WarnRemoteError(masterConnection, queryResult);
		ereport(ERROR, (errmsg("could not create a new empty shard on the remote node")));
	}

	PQclear(queryResult);

	return shardId;
}


/*
 * FinalizeCopyToNewShard commits copy transaction and closes connections to
 * shard placements.
 */
static void
FinalizeCopyToNewShard(ShardConnections *shardConnections)
{
	/* close the COPY input on all shard placements */
	EndRemoteCopy(shardConnections->connectionList, true);

	/* commit transactions and close connections */
	CommitRemoteTransactions(shardConnections->connectionList, true);
	CloseConnections(shardConnections->connectionList);
}


/*
 * MasterUpdateShardStatistics dispatches the update shard statistics call
 * between local or remote master node according to the master connection state.
 */
static void
MasterUpdateShardStatistics(uint64 shardId)
{
	if (masterConnection == NULL)
	{
		UpdateShardStatistics(shardId);
	}
	else
	{
		RemoteUpdateShardStatistics(shardId);
	}
}


/*
 * RemoteUpdateShardStatistics updates shard statistics on the remote master node.
 */
static void
RemoteUpdateShardStatistics(uint64 shardId)
{
	PGresult *queryResult = NULL;

	StringInfo updateShardStatisticsCommand = makeStringInfo();
	appendStringInfo(updateShardStatisticsCommand, UPDATE_SHARD_STATISTICS_QUERY,
					 shardId);

	queryResult = PQexec(masterConnection, updateShardStatisticsCommand->data);
	if (PQresultStatus(queryResult) != PGRES_TUPLES_OK)
	{
		ereport(ERROR, (errmsg("could not update shard statistics")));
	}

	PQclear(queryResult);
}


/* *INDENT-OFF* */
/* Append data to the copy buffer in outputState */
static void
CopySendData(CopyOutState outputState, const void *databuf, int datasize)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, databuf, datasize);
}


/* Append a striong to the copy buffer in outputState. */
static void
CopySendString(CopyOutState outputState, const char *str)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, str, strlen(str));
}


/* Append a char to the copy buffer in outputState. */
static void
CopySendChar(CopyOutState outputState, char c)
{
	appendStringInfoCharMacro(outputState->fe_msgbuf, c);
}


/* Append an int32 to the copy buffer in outputState. */
static void
CopySendInt32(CopyOutState outputState, int32 val)
{
	uint32 buf = htonl((uint32) val);
	CopySendData(outputState, &buf, sizeof(buf));
}


/* Append an int16 to the copy buffer in outputState. */
static void
CopySendInt16(CopyOutState outputState, int16 val)
{
	uint16 buf = htons((uint16) val);
	CopySendData(outputState, &buf, sizeof(buf));
}


/*
 * Send text representation of one column, with conversion and escaping.
 *
 * NB: This function is based on commands/copy.c and doesn't fully conform to
 * our coding style. The function should be kept in sync with copy.c.
 */
static void
CopyAttributeOutText(CopyOutState cstate, char *string)
{
	char *pointer = NULL;
	char *start = NULL;
	char c = '\0';
	char delimc = cstate->delim[0];

	if (cstate->need_transcoding)
	{
		pointer = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	}
	else
	{
		pointer = string;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "pointer"
	 * can be sent literally, but hasn't yet been.
	 *
	 * As all encodings here are safe, i.e. backend supported ones, we can
	 * skip doing pg_encoding_mblen(), because in valid backend encodings,
	 * extra bytes of a multibyte character never look like ASCII.
	 */
	start = pointer;
	while ((c = *pointer) != '\0')
	{
		if ((unsigned char) c < (unsigned char) 0x20)
		{
			/*
			 * \r and \n must be escaped, the others are traditional. We
			 * prefer to dump these using the C-like notation, rather than
			 * a backslash and the literal character, because it makes the
			 * dump file a bit more proof against Microsoftish data
			 * mangling.
			 */
			switch (c)
			{
				case '\b':
					c = 'b';
					break;
				case '\f':
					c = 'f';
					break;
				case '\n':
					c = 'n';
					break;
				case '\r':
					c = 'r';
					break;
				case '\t':
					c = 't';
					break;
				case '\v':
					c = 'v';
					break;
				default:
					/* If it's the delimiter, must backslash it */
					if (c == delimc)
						break;
					/* All ASCII control chars are length 1 */
					pointer++;
					continue;		/* fall to end of loop */
			}
			/* if we get here, we need to convert the control char */
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			CopySendChar(cstate, c);
			start = ++pointer;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			start = pointer++;	/* we include char in next run */
		}
		else
		{
			pointer++;
		}
	}

	CopyFlushOutput(cstate, start, pointer);
}


/* *INDENT-ON* */
/* Helper function to send pending copy output */
static inline void
CopyFlushOutput(CopyOutState cstate, char *start, char *pointer)
{
	if (pointer > start)
	{
		CopySendData(cstate, start, pointer - start);
	}
}
