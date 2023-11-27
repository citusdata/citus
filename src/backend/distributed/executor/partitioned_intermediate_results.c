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

#include "access/hash.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/typcache.h"

#include "distributed/intermediate_results.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/remote_commands.h"
#include "distributed/tuplestore.h"
#include "distributed/utils/array_type.h"
#include "distributed/utils/function.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"


/*
 * PartitionedResultDestReceiver is used for streaming tuples into a set of
 * partitioned result files.
 */
typedef struct PartitionedResultDestReceiver
{
	/* public DestReceiver interface */
	DestReceiver pub;

	/* on lazy startup we only startup the DestReceiver once they receive a tuple */
	bool lazyStartup;

	/*
	 * Stores the arguments passed to the PartidionedResultDestReceiver's rStarup
	 * function. These arguments are reused when lazyStartup has been set to true.
	 * On the processing of a first tuple for a partitionDestReceiver since rStartup it
	 * will pass the arguments here to the rStartup function of partitionDestReceiver to
	 * prepare it for receiving tuples.
	 *
	 * Even though not used without lazyStartup we just always populate these with the
	 * last invoked arguments for our rStartup.
	 */
	struct
	{
		/*
		 * operation as passed to rStartup, mostly the CmdType of the command being
		 * streamed into this DestReceiver
		 */
		int operation;

		/*
		 * TupleDesc describing the layout of the tuples being streamed into the
		 * DestReceiver.
		 */
		TupleDesc tupleDescriptor;
	} startupArguments;

	/* which column of streamed tuples to use as partition column */
	int partitionColumnIndex;

	/* The number of partitions being partitioned into */
	int partitionCount;

	/* used for deciding which partition a shard belongs to. */
	CitusTableCacheEntry *shardSearchInfo;

	/* Tuples matching shardSearchInfo[i] are sent to partitionDestReceivers[i]. */
	DestReceiver **partitionDestReceivers;

	/* keeping track of which partitionDestReceivers have been started */
	Bitmapset *startedDestReceivers;

	/* whether NULL partition column values are allowed */
	bool allowNullPartitionColumnValues;
} PartitionedResultDestReceiver;

static Portal StartPortalForQueryExecution(const char *queryString);
static void PartitionedResultDestReceiverStartup(DestReceiver *dest, int operation,
												 TupleDesc inputTupleDescriptor);
static bool PartitionedResultDestReceiverReceive(TupleTableSlot *slot,
												 DestReceiver *dest);
static void PartitionedResultDestReceiverShutdown(DestReceiver *dest);
static void PartitionedResultDestReceiverDestroy(DestReceiver *copyDest);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_partition_query_result);


/*
 * worker_partition_query_result executes a query and writes the results into a
 * set of local files according to the partition scheme and the partition column.
 */
Datum
worker_partition_query_result(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

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
	bool allowNullPartitionColumnValues = PG_GETARG_BOOL(7);
	bool generateEmptyResults = PG_GETARG_BOOL(8);

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

	/* create all dest receivers */
	DestReceiver **dests = palloc0(partitionCount * sizeof(DestReceiver *));
	for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
	{
		StringInfo resultId = makeStringInfo();
		appendStringInfo(resultId, "%s_%d", resultIdPrefixString, partitionIndex);
		char *filePath = QueryResultFileName(resultId->data);
		DestReceiver *partitionDest = CreateFileDestReceiver(filePath, tupleContext,
															 binaryCopy);
		dests[partitionIndex] = partitionDest;
	}

	/*
	 * If we are asked to generated empty results, use non-lazy startup.
	 *
	 * The rStartup of the FileDestReceiver will be called for all partitions
	 * and generate empty files, which may still have binary header/footer.
	 */
	const bool lazyStartup = !generateEmptyResults;

	DestReceiver *dest = CreatePartitionedResultDestReceiver(
		partitionColumnIndex,
		partitionCount,
		shardSearchInfo,
		dests,
		lazyStartup,
		allowNullPartitionColumnValues);

	/* execute the query */
	PortalRun(portal, FETCH_ALL, false, true, dest, dest, NULL);

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

		FileDestReceiverStats(dests[partitionIndex], &recordsWritten, &bytesWritten);

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(partitionIndex);
		values[1] = UInt64GetDatum(recordsWritten);
		values[2] = UInt64GetDatum(bytesWritten);

		tuplestore_putvalues(tupleStore, returnTupleDesc, values, nulls);
	}
	PortalDrop(portal, false);
	FreeExecutorState(estate);

	dest->rDestroy(dest);

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
	PlannedStmt *queryPlan = pg_plan_query(query, NULL, cursorOptions, NULL);

	Portal portal = CreateNewPortal();

	/* don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;

	PortalDefineQuery(portal, NULL, queryString, CMDTAG_SELECT,
					  list_make1(queryPlan), NULL);
	int eflags = 0;
	PortalStart(portal, NULL, eflags, GetActiveSnapshot());

	return portal;
}


/*
 * QueryTupleShardSearchInfo returns a CitusTableCacheEntry which has enough
 * information so that FindShardInterval() can find the shard corresponding
 * to a tuple.
 */
CitusTableCacheEntry *
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

		if (!OidIsValid(hashFunction->fn_oid))
		{
			ereport(ERROR, (errmsg("no hash function defined for type %s",
								   format_type_be(partitionColumn->vartype))));
		}
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
DestReceiver *
CreatePartitionedResultDestReceiver(int partitionColumnIndex,
									int partitionCount,
									CitusTableCacheEntry *shardSearchInfo,
									DestReceiver **partitionedDestReceivers,
									bool lazyStartup,
									bool allowNullPartitionColumnValues)
{
	PartitionedResultDestReceiver *resultDest =
		palloc0(sizeof(PartitionedResultDestReceiver));

	/* set up the DestReceiver function pointers */
	resultDest->pub.receiveSlot = PartitionedResultDestReceiverReceive;
	resultDest->pub.rStartup = PartitionedResultDestReceiverStartup;
	resultDest->pub.rShutdown = PartitionedResultDestReceiverShutdown;
	resultDest->pub.rDestroy = PartitionedResultDestReceiverDestroy;
	resultDest->pub.mydest = DestCopyOut;

	/* setup routing parameters */
	resultDest->partitionColumnIndex = partitionColumnIndex;
	resultDest->partitionCount = partitionCount;
	resultDest->shardSearchInfo = shardSearchInfo;
	resultDest->partitionDestReceivers = partitionedDestReceivers;
	resultDest->startedDestReceivers = NULL;
	resultDest->lazyStartup = lazyStartup;
	resultDest->allowNullPartitionColumnValues = allowNullPartitionColumnValues;

	return (DestReceiver *) resultDest;
}


/*
 * PartitionedResultDestReceiverStartup implements the rStartup interface of
 * PartitionedResultDestReceiver.
 */
static void
PartitionedResultDestReceiverStartup(DestReceiver *dest, int operation,
									 TupleDesc inputTupleDescriptor)
{
	PartitionedResultDestReceiver *self = (PartitionedResultDestReceiver *) dest;

	self->startupArguments.tupleDescriptor = CreateTupleDescCopy(inputTupleDescriptor);
	self->startupArguments.operation = operation;

	if (self->lazyStartup)
	{
		/* we are initialized, rest happens when needed*/
		return;
	}

	/* no lazy startup, lets startup our partitionedDestReceivers */
	for (int partitionIndex = 0; partitionIndex < self->partitionCount; partitionIndex++)
	{
		DestReceiver *partitionDest = self->partitionDestReceivers[partitionIndex];
		partitionDest->rStartup(partitionDest, operation, inputTupleDescriptor);
		self->startedDestReceivers = bms_add_member(self->startedDestReceivers,
													partitionIndex);
	}
}


/*
 * PartitionedResultDestReceiverReceive implements the receiveSlot interface of
 * PartitionedResultDestReceiver.
 */
static bool
PartitionedResultDestReceiverReceive(TupleTableSlot *slot, DestReceiver *dest)
{
	PartitionedResultDestReceiver *self = (PartitionedResultDestReceiver *) dest;

	slot_getallattrs(slot);

	Datum *columnValues = slot->tts_values;
	bool *columnNulls = slot->tts_isnull;

	int partitionIndex;

	if (columnNulls[self->partitionColumnIndex])
	{
		if (self->allowNullPartitionColumnValues)
		{
			/*
			 * NULL values go into the first partition for both hash- and range-
			 * partitioning, since that is the only way to guarantee that there is
			 * always a partition for NULL and that it is always the same partition.
			 */
			partitionIndex = 0;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("the partition column value cannot be NULL")));
		}
	}
	else
	{
		Datum partitionColumnValue = columnValues[self->partitionColumnIndex];
		ShardInterval *shardInterval = FindShardInterval(partitionColumnValue,
														 self->shardSearchInfo);
		if (shardInterval == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("could not find shard for partition column "
								   "value")));
		}

		partitionIndex = shardInterval->shardIndex;
	}

	DestReceiver *partitionDest = self->partitionDestReceivers[partitionIndex];

	/* check if this partitionDestReceiver has been started before, start if not */
	if (!bms_is_member(partitionIndex, self->startedDestReceivers))
	{
		partitionDest->rStartup(partitionDest,
								self->startupArguments.operation,
								self->startupArguments.tupleDescriptor);
		self->startedDestReceivers = bms_add_member(self->startedDestReceivers,
													partitionIndex);
	}

	/* forward the tuple to the appropriate dest receiver */
	partitionDest->receiveSlot(slot, partitionDest);

	return true;
}


/*
 * PartitionedResultDestReceiverShutdown implements the rShutdown interface of
 * PartitionedResultDestReceiver by calling rShutdown on all started
 * partitionedDestReceivers.
 */
static void
PartitionedResultDestReceiverShutdown(DestReceiver *dest)
{
	PartitionedResultDestReceiver *self = (PartitionedResultDestReceiver *) dest;

	int i = -1;
	while ((i = bms_next_member(self->startedDestReceivers, i)) >= 0)
	{
		DestReceiver *partitionDest = self->partitionDestReceivers[i];
		partitionDest->rShutdown(partitionDest);
	}

	/* empty the set of started receivers which allows them to be restarted again */
	bms_free(self->startedDestReceivers);
	self->startedDestReceivers = NULL;
}


/*
 * PartitionedResultDestReceiverDestroy implements the rDestroy interface of
 * PartitionedResultDestReceiver.
 */
static void
PartitionedResultDestReceiverDestroy(DestReceiver *dest)
{
	PartitionedResultDestReceiver *self = (PartitionedResultDestReceiver *) dest;

	/* we destroy all dest receivers, irregardless if they have been started or not */
	for (int partitionIndex = 0; partitionIndex < self->partitionCount; partitionIndex++)
	{
		DestReceiver *partitionDest = self->partitionDestReceivers[partitionIndex];
		if (partitionDest != NULL)
		{
			partitionDest->rDestroy(partitionDest);
		}
	}
}
