/*-------------------------------------------------------------------------
 *
 * partition_intermediate_results.c
 *   Functions for writing partitioned intermediate results.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "distributed/intermediate_results.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/remote_commands.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/typcache.h"


/*
 * PartitionedResultDestReceiver is used for streaming tuples into a set of
 * partitioned result files.
 */
typedef struct PartitionedResultDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* partition file $i is stored at file named $resultIdPrefix_$i. */
	char *resultIdPrefix;

	/* use binary copy or just text copy format? */
	bool binaryCopy;

	/* used for deciding which partition a shard belongs to. */
	CitusTableCacheEntry *shardSearchInfo;

	MemoryContext perTupleContext;

	/* how does stream tuples look like? */
	TupleDesc tupleDescriptor;

	/* which column of streamed tuples to use as partition column? */
	int partitionColumnIndex;

	/* how many partitions do we have? */
	int partitionCount;

	/*
	 * Tuples for partition[i] are sent to partitionDestReceivers[i], which
	 * writes it to a result file.
	 */
	DestReceiver **partitionDestReceivers;
} PartitionedResultDestReceiver;

static Portal StartPortalForQueryExecution(const char *queryString);
static CitusTableCacheEntry * QueryTupleShardSearchInfo(ArrayType *minValuesArray,
														ArrayType *maxValuesArray,
														char partitionMethod,
														Var *partitionColumn);
static PartitionedResultDestReceiver * CreatePartitionedResultDestReceiver(char *resultId,
																		   int
																		   partitionColumnIndex,
																		   int
																		   partitionCount,
																		   TupleDesc
																		   tupleDescriptor,
																		   bool binaryCopy,
																		   CitusTableCacheEntry
																		   *
																		   shardSearchInfo,
																		   MemoryContext
																		   perTupleContext);
static void PartitionedResultDestReceiverStartup(DestReceiver *dest, int operation,
												 TupleDesc inputTupleDescriptor);
static bool PartitionedResultDestReceiverReceive(TupleTableSlot *slot,
												 DestReceiver *dest);
static void PartitionedResultDestReceiverShutdown(DestReceiver *destReceiver);
static void PartitionedResultDestReceiverDestroy(DestReceiver *destReceiver);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_partition_query_result);


/*
 * worker_partition_query_result executes a query and writes the results into a
 * set of local files according to the partition scheme and the partition column.
 */
Datum
worker_partition_query_result(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	text *resultIdPrefixText = PG_GETARG_TEXT_P(0);
	char *resultIdPrefixString = text_to_cstring(resultIdPrefixText);

	/* verify that resultIdPrefix doesn't contain invalid characters */
	QueryResultFileName(resultIdPrefixString);

	text *queryText = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(queryText);

	int partitionColumnIndex = PG_GETARG_INT32(2);
	Oid partitionMethodOid = PG_GETARG_OID(3);

	char partitionMethod = LookupDistributionMethod(partitionMethodOid);
	if (partitionMethod != DISTRIBUTE_BY_HASH && partitionMethod != DISTRIBUTE_BY_RANGE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("only hash and range partitiong schemes are supported")));
	}

	ArrayType *minValuesArray = PG_GETARG_ARRAYTYPE_P(4);
	int32 minValuesCount = ArrayObjectCount(minValuesArray);

	ArrayType *maxValuesArray = PG_GETARG_ARRAYTYPE_P(5);
	int32 maxValuesCount = ArrayObjectCount(maxValuesArray);

	bool binaryCopy = PG_GETARG_BOOL(6);

	CheckCitusVersion(ERROR);

	if (!IsMultiStatementTransaction())
	{
		ereport(ERROR, (errmsg("worker_partition_query_result can only be used in a "
							   "transaction block")));
	}

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results will be stored in a directory that is derived
	 * from the distributed transaction ID.
	 */
	EnsureDistributedTransactionId();

	CreateIntermediateResultsDirectory();

	if (minValuesCount != maxValuesCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg(
					 "min values and max values must have the same number of elements")));
	}

	int partitionCount = minValuesCount;
	if (partitionCount == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("number of partitions cannot be 0")));
	}

	/* start execution early in order to extract the tuple descriptor */
	Portal portal = StartPortalForQueryExecution(queryString);

	/* extract the partition column */
	TupleDesc tupleDescriptor = portal->tupDesc;
	if (tupleDescriptor == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("query must generate a set of rows")));
	}

	if (partitionColumnIndex < 0 || partitionColumnIndex >= tupleDescriptor->natts)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("partition column index must be between 0 and %d",
							   tupleDescriptor->natts - 1)));
	}

	FormData_pg_attribute *partitionColumnAttr = TupleDescAttr(tupleDescriptor,
															   partitionColumnIndex);
	Var *partitionColumn = makeVar(partitionColumnIndex, partitionColumnIndex,
								   partitionColumnAttr->atttypid,
								   partitionColumnAttr->atttypmod,
								   partitionColumnAttr->attcollation, 0);

	/* construct an artificial CitusTableCacheEntry for shard pruning */
	CitusTableCacheEntry *shardSearchInfo =
		QueryTupleShardSearchInfo(minValuesArray, maxValuesArray,
								  partitionMethod, partitionColumn);

	/* prepare the output destination */
	EState *estate = CreateExecutorState();
	MemoryContext tupleContext = GetPerTupleMemoryContext(estate);
	PartitionedResultDestReceiver *dest =
		CreatePartitionedResultDestReceiver(resultIdPrefixString, partitionColumnIndex,
											partitionCount, tupleDescriptor, binaryCopy,
											shardSearchInfo, tupleContext);

	/* execute the query */
	PortalRun(portal, FETCH_ALL, false, true, (DestReceiver *) dest,
			  (DestReceiver *) dest, NULL);

	/* construct the output result */
	TupleDesc returnTupleDesc = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &returnTupleDesc);
	resultInfo->returnMode = SFRM_Materialize;
	resultInfo->setResult = tupleStore;
	resultInfo->setDesc = returnTupleDesc;

	for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		uint64 recordsWritten = 0;
		uint64 bytesWritten = 0;
		Datum values[3];
		bool nulls[3];

		if (dest->partitionDestReceivers[partitionIndex] != NULL)
		{
			FileDestReceiverStats(dest->partitionDestReceivers[partitionIndex],
								  &recordsWritten, &bytesWritten);
		}

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(partitionIndex);
		values[1] = UInt64GetDatum(recordsWritten);
		values[2] = UInt64GetDatum(bytesWritten);

		tuplestore_putvalues(tupleStore, returnTupleDesc, values, nulls);
	}

	tuplestore_donestoring(tupleStore);
	PortalDrop(portal, false);
	FreeExecutorState(estate);

	PG_RETURN_INT64(1);
}


/*
 * StartPortalForQueryExecution creates and starts a portal which can be
 * used for running the given query.
 */
static Portal
StartPortalForQueryExecution(const char *queryString)
{
	Query *query = ParseQueryString(queryString, NULL, 0);

	int cursorOptions = CURSOR_OPT_PARALLEL_OK;
	PlannedStmt *queryPlan = pg_plan_query(query, cursorOptions, NULL);

	Portal portal = CreateNewPortal();

	/* don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;

	PortalDefineQuery(portal, NULL, queryString, "SELECT", list_make1(queryPlan), NULL);
	int eflags = 0;
	PortalStart(portal, NULL, eflags, GetActiveSnapshot());

	return portal;
}


/*
 * QueryTupleShardSearchInfo returns a CitusTableCacheEntry which has enough
 * information so that FindShardInterval() can find the shard corresponding
 * to a tuple.
 */
static CitusTableCacheEntry *
QueryTupleShardSearchInfo(ArrayType *minValuesArray, ArrayType *maxValuesArray,
						  char partitionMethod, Var *partitionColumn)
{
	Datum *minValues = 0;
	Datum *maxValues = 0;
	bool *minValueNulls = 0;
	bool *maxValueNulls = 0;
	int minValuesCount = 0;
	int maxValuesCount = 0;
	Oid intervalTypeId = InvalidOid;
	int32 intervalTypeMod = 0;
	deconstruct_array(minValuesArray, TEXTOID, -1, false, 'i', &minValues,
					  &minValueNulls, &minValuesCount);
	deconstruct_array(maxValuesArray, TEXTOID, -1, false, 'i', &maxValues,
					  &maxValueNulls, &maxValuesCount);
	int partitionCount = minValuesCount;
	Assert(maxValuesCount == partitionCount);

	GetIntervalTypeInfo(partitionMethod, partitionColumn,
						&intervalTypeId, &intervalTypeMod);
	FmgrInfo *shardColumnCompare = GetFunctionInfo(partitionColumn->vartype,
												   BTREE_AM_OID, BTORDER_PROC);
	FmgrInfo *shardIntervalCompare = GetFunctionInfo(intervalTypeId,
													 BTREE_AM_OID, BTORDER_PROC);
	FmgrInfo *hashFunction = NULL;
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		TypeCacheEntry *typeEntry = lookup_type_cache(partitionColumn->vartype,
													  TYPECACHE_HASH_PROC_FINFO);

		hashFunction = palloc0(sizeof(FmgrInfo));
		fmgr_info_copy(hashFunction, &(typeEntry->hash_proc_finfo), CurrentMemoryContext);
	}

	ShardInterval **shardIntervalArray = palloc0(partitionCount *
												 sizeof(ShardInterval *));
	for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		Datum datumArray[Natts_pg_dist_shard] = {
			[Anum_pg_dist_shard_logicalrelid - 1] = InvalidOid,
			[Anum_pg_dist_shard_shardid - 1] = partitionIndex,
			[Anum_pg_dist_shard_shardstorage - 1] = SHARD_STORAGE_VIRTUAL,
			[Anum_pg_dist_shard_shardminvalue - 1] = minValues[partitionIndex],
			[Anum_pg_dist_shard_shardmaxvalue - 1] = maxValues[partitionIndex]
		};
		bool nullsArray[Natts_pg_dist_shard] = {
			[Anum_pg_dist_shard_shardminvalue - 1] = minValueNulls[partitionIndex],
			[Anum_pg_dist_shard_shardmaxvalue - 1] = maxValueNulls[partitionIndex]
		};

		shardIntervalArray[partitionIndex] =
			DeformedDistShardTupleToShardInterval(datumArray, nullsArray,
												  intervalTypeId, intervalTypeMod);
		shardIntervalArray[partitionIndex]->shardIndex = partitionIndex;
	}

	CitusTableCacheEntry *result = palloc0(sizeof(CitusTableCacheEntry));
	result->partitionMethod = partitionMethod;
	result->partitionColumn = partitionColumn;
	result->shardIntervalCompareFunction = shardIntervalCompare;
	result->shardColumnCompareFunction = shardColumnCompare;
	result->hashFunction = hashFunction;
	result->sortedShardIntervalArray =
		SortShardIntervalArray(shardIntervalArray, partitionCount,
							   partitionColumn->varcollid, shardIntervalCompare);
	result->hasUninitializedShardInterval =
		HasUninitializedShardInterval(result->sortedShardIntervalArray, partitionCount);
	result->hasOverlappingShardInterval =
		result->hasUninitializedShardInterval ||
		HasOverlappingShardInterval(result->sortedShardIntervalArray, partitionCount,
									partitionColumn->varcollid, shardIntervalCompare);
	ErrorIfInconsistentShardIntervals(result);

	result->shardIntervalArrayLength = partitionCount;

	return result;
}


/*
 * CreatePartitionedResultDestReceiver sets up a partitioned dest receiver.
 */
static PartitionedResultDestReceiver *
CreatePartitionedResultDestReceiver(char *resultIdPrefix, int partitionColumnIndex,
									int partitionCount, TupleDesc tupleDescriptor,
									bool binaryCopy,
									CitusTableCacheEntry *shardSearchInfo,
									MemoryContext perTupleContext)
{
	PartitionedResultDestReceiver *resultDest =
		palloc0(sizeof(PartitionedResultDestReceiver));

	/* set up the DestReceiver function pointers */
	resultDest->pub.receiveSlot = PartitionedResultDestReceiverReceive;
	resultDest->pub.rStartup = PartitionedResultDestReceiverStartup;
	resultDest->pub.rShutdown = PartitionedResultDestReceiverShutdown;
	resultDest->pub.rDestroy = PartitionedResultDestReceiverDestroy;
	resultDest->pub.mydest = DestCopyOut;

	/* set up output parameters */
	resultDest->resultIdPrefix = resultIdPrefix;
	resultDest->perTupleContext = perTupleContext;
	resultDest->partitionColumnIndex = partitionColumnIndex;
	resultDest->partitionCount = partitionCount;
	resultDest->shardSearchInfo = shardSearchInfo;
	resultDest->tupleDescriptor = tupleDescriptor;
	resultDest->binaryCopy = binaryCopy;
	resultDest->partitionDestReceivers =
		(DestReceiver **) palloc0(partitionCount * sizeof(DestReceiver *));

	return resultDest;
}


/*
 * PartitionedResultDestReceiverStartup implements the rStartup interface of
 * PartitionedResultDestReceiver.
 */
static void
PartitionedResultDestReceiverStartup(DestReceiver *copyDest, int operation,
									 TupleDesc inputTupleDescriptor)
{
	/*
	 * We don't expect this to be called multiple times, but if it happens,
	 * we will just overwrite previous files.
	 */
	PartitionedResultDestReceiver *partitionedDest =
		(PartitionedResultDestReceiver *) copyDest;
	int partitionCount = partitionedDest->partitionCount;
	for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		DestReceiver *partitionDest =
			partitionedDest->partitionDestReceivers[partitionIndex];
		if (partitionDest != NULL)
		{
			partitionDest->rStartup(partitionDest, operation, inputTupleDescriptor);
		}
	}
}


/*
 * PartitionedResultDestReceiverReceive implements the receiveSlot interface of
 * PartitionedResultDestReceiver.
 */
static bool
PartitionedResultDestReceiverReceive(TupleTableSlot *slot, DestReceiver *copyDest)
{
	PartitionedResultDestReceiver *partitionedDest =
		(PartitionedResultDestReceiver *) copyDest;

	slot_getallattrs(slot);

	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	if (columnNulls[partitionedDest->partitionColumnIndex])
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("the partition column value cannot be NULL")));
	}

	Datum partitionColumnValue = columnValues[partitionedDest->partitionColumnIndex];
	ShardInterval *shardInterval = FindShardInterval(partitionColumnValue,
													 partitionedDest->shardSearchInfo);
	if (shardInterval == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find shard for partition column "
							   "value")));
	}

	int partitionIndex = shardInterval->shardIndex;
	DestReceiver *partitionDest = partitionedDest->partitionDestReceivers[partitionIndex];
	if (partitionDest == NULL)
	{
		StringInfo resultId = makeStringInfo();
		appendStringInfo(resultId, "%s_%d", partitionedDest->resultIdPrefix,
						 partitionIndex);
		char *filePath = QueryResultFileName(resultId->data);

		partitionDest = CreateFileDestReceiver(filePath, partitionedDest->perTupleContext,
											   partitionedDest->binaryCopy);
		partitionedDest->partitionDestReceivers[partitionIndex] = partitionDest;
		partitionDest->rStartup(partitionDest, 0, partitionedDest->tupleDescriptor);
	}

	partitionDest->receiveSlot(slot, partitionDest);

	return true;
}


/*
 * PartitionedResultDestReceiverShutdown implements the rShutdown interface of
 * PartitionedResultDestReceiver.
 */
static void
PartitionedResultDestReceiverShutdown(DestReceiver *copyDest)
{
	PartitionedResultDestReceiver *partitionedDest =
		(PartitionedResultDestReceiver *) copyDest;
	int partitionCount = partitionedDest->partitionCount;
	for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		DestReceiver *partitionDest =
			partitionedDest->partitionDestReceivers[partitionIndex];
		if (partitionDest != NULL)
		{
			partitionDest->rShutdown(partitionDest);
		}
	}
}


/*
 * PartitionedResultDestReceiverDestroy implements the rDestroy interface of
 * PartitionedResultDestReceiver.
 */
static void
PartitionedResultDestReceiverDestroy(DestReceiver *copyDest)
{
	PartitionedResultDestReceiver *partitionedDest =
		(PartitionedResultDestReceiver *) copyDest;
	int partitionCount = partitionedDest->partitionCount;
	for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		DestReceiver *partitionDest =
			partitionedDest->partitionDestReceivers[partitionIndex];
		if (partitionDest != NULL)
		{
			/* this call should also free partitionDest, so no need to free it after */
			partitionDest->rDestroy(partitionDest);
		}
	}

	pfree(partitionedDest->partitionDestReceivers);
	pfree(partitionedDest);
}
