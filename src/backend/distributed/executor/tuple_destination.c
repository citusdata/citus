#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/tupconvert.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"
#include "distributed/multi_server_executor.h"
#include "distributed/subplan_execution.h"
#include "distributed/tuple_destination.h"


/*
 * TupleStoreTupleDestination is internal representation of a TupleDestination
 * which forwards tuples to a tuple store.
 */
typedef struct TupleStoreTupleDestination
{
	TupleDestination pub;

	/* destination of tuples */
	Tuplestorestate *tupleStore;

	/* how does tuples look like? */
	TupleDesc tupleDesc;
} TupleStoreTupleDestination;

/*
 * RowtypeConversion describes how TupleDestDestReceiver converts the values
 * of a result column whose row type differs from the one that the tuple
 * destination expects. See TupleDestDestReceiverStartup.
 */
typedef struct RowtypeConversion
{
	/* index of the result column */
	int columnIndex;

	/* row type that the tuple destination expects */
	TupleDesc outputRowDesc;

	/* NULL when the row types are physically compatible */
	TupleConversionMap *conversionMap;

	/* element types if the column is an array of rows, InvalidOid otherwise */
	Oid inputElementType;
	Oid outputElementType;

	/* storage properties of the row types, used for arrays */
	int16 elementLength;
	bool elementByValue;
	char elementAlign;
} RowtypeConversion;

/*
 * TupleDestDestReceiver is internal representation of a DestReceiver which
 * forards tuples to a tuple destination.
 */
typedef struct TupleDestDestReceiver
{
	DestReceiver pub;
	TupleDestination *tupleDest;

	/* parameters to pass to tupleDest->putTuple() */
	Task *task;
	int placementIndex;

	/* row type conversions to apply to received tuples, if any */
	List *rowtypeConversionList;

	/* tuple descriptor of the converted tuples */
	TupleDesc outputTupleDesc;

	/* per-tuple memory context for the converted tuples */
	MemoryContext conversionContext;
} TupleDestDestReceiver;


/* forward declarations for local functions */
static void TupleStoreTupleDestPutTuple(TupleDestination *self, Task *task,
										int placementIndex, int queryNumber,
										HeapTuple heapTuple, uint64 tupleLibpqSize);
static TupleDesc TupleStoreTupleDestTupleDescForQuery(TupleDestination *self, int
													  queryNumber);
static void TupleDestNonePutTuple(TupleDestination *self, Task *task,
								  int placementIndex, int queryNumber,
								  HeapTuple heapTuple, uint64 tupleLibpqSize);
static TupleDesc TupleDestNoneTupleDescForQuery(TupleDestination *self, int queryNumber);
static void TupleDestDestReceiverStartup(DestReceiver *copyDest, int operation,
										 TupleDesc inputTupleDesc);
static bool TupleDestDestReceiverReceive(TupleTableSlot *slot,
										 DestReceiver *copyDest);
static void TupleDestDestReceiverShutdown(DestReceiver *destReceiver);
static void TupleDestDestReceiverDestroy(DestReceiver *destReceiver);
static RowtypeConversion * CreateRowtypeConversion(Oid inputType, Oid outputType);
static HeapTuple ConvertRowtypesInSlot(TupleDestDestReceiver *tupleDestReceiver,
									   TupleTableSlot *slot);
static Datum ConvertRowDatum(Datum rowDatum, RowtypeConversion *conversion);
static Datum ConvertRowArrayDatum(Datum arrayDatum, RowtypeConversion *conversion);


/*
 * CreateTupleStoreTupleDestWithStats creates a TupleDestination which forwards
 * tuples to a tupleStore, sharing an externally-provided TupleDestinationStats.
 *
 * Sharing stats lets multiple destinations contribute to a single
 * citus.max_intermediate_result_size budget — the sorted-merge per-task
 * dispatch uses this so the global limit is enforced across all per-task
 * stores combined, not per task. Pass NULL if no stats tracking is needed.
 */
TupleDestination *
CreateTupleStoreTupleDestWithStats(Tuplestorestate *tupleStore,
								   TupleDesc tupleDescriptor,
								   TupleDestinationStats *sharedStats)
{
	TupleStoreTupleDestination *tupleStoreTupleDest = palloc0(
		sizeof(TupleStoreTupleDestination));

	tupleStoreTupleDest->tupleStore = tupleStore;
	tupleStoreTupleDest->tupleDesc = tupleDescriptor;
	tupleStoreTupleDest->pub.putTuple = TupleStoreTupleDestPutTuple;
	tupleStoreTupleDest->pub.tupleDescForQuery =
		TupleStoreTupleDestTupleDescForQuery;
	tupleStoreTupleDest->pub.tupleDestinationStats = sharedStats;

	return (TupleDestination *) tupleStoreTupleDest;
}


/*
 * CreateTupleStoreTupleDest creates a TupleDestination which forwards tuples to
 * a tupleStore.
 */
TupleDestination *
CreateTupleStoreTupleDest(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	TupleDestinationStats *stats =
		(TupleDestinationStats *) palloc0(sizeof(TupleDestinationStats));
	return CreateTupleStoreTupleDestWithStats(tupleStore, tupleDescriptor, stats);
}


/*
 * TupleStoreTupleDestPutTuple implements TupleDestination->putTuple for
 * TupleStoreTupleDestination.
 */
static void
TupleStoreTupleDestPutTuple(TupleDestination *self, Task *task,
							int placementIndex, int queryNumber,
							HeapTuple heapTuple, uint64 tupleLibpqSize)
{
	TupleStoreTupleDestination *tupleDest = (TupleStoreTupleDestination *) self;

	/*
	 * Remote execution sets tupleLibpqSize, however it is 0 for local execution. We prefer
	 * to use tupleLibpqSize for  the remote execution because that reflects the exact data
	 * transfer size over the network. For local execution, we rely on the size of the
	 * tuple.
	 */
	uint64 tupleSize = tupleLibpqSize;
	if (tupleSize == 0)
	{
		tupleSize = heapTuple->t_len;
	}

	/*
	 * Enfoce citus.max_intermediate_result_size for subPlans if
	 * the caller requested.
	 */
	TupleDestinationStats *tupleDestinationStats = self->tupleDestinationStats;
	if (SubPlanLevel > 0 && tupleDestinationStats != NULL)
	{
		tupleDestinationStats->totalIntermediateResultSize += tupleSize;
		EnsureIntermediateSizeLimitNotExceeded(tupleDestinationStats);
	}

	/* do the actual work */
	tuplestore_puttuple(tupleDest->tupleStore, heapTuple);

	/* we record tuples received over network */
	task->totalReceivedTupleData += tupleLibpqSize;
}


/*
 * EnsureIntermediateSizeLimitNotExceeded is a helper function for checking the current
 * state of the tupleDestinationStats and throws error if necessary.
 */
void
EnsureIntermediateSizeLimitNotExceeded(TupleDestinationStats *tupleDestinationStats)
{
	if (!tupleDestinationStats)
	{
		/* unexpected, still prefer defensive approach */
		return;
	}

	/*
	 * We only care about subPlans. Also, if user disabled, no need to
	 * check  further.
	 */
	if (SubPlanLevel == 0 || MaxIntermediateResult < 0)
	{
		return;
	}

	uint64 maxIntermediateResultInBytes = MaxIntermediateResult * 1024L;
	if (tupleDestinationStats->totalIntermediateResultSize < maxIntermediateResultInBytes)
	{
		/*
		 * We have not reached the size limit that the user requested, so
		 * nothing to do for now.
		 */
		return;
	}

	ereport(ERROR, (errmsg("the intermediate result size exceeds "
						   "citus.max_intermediate_result_size (currently %d kB)",
						   MaxIntermediateResult),
					errdetail("Citus restricts the size of intermediate "
							  "results of complex subqueries and CTEs to "
							  "avoid accidentally pulling large result sets "
							  "into once place."),
					errhint("To run the current query, set "
							"citus.max_intermediate_result_size to a higher"
							" value or -1 to disable.")));
}


/*
 * TupleStoreTupleDestTupleDescForQuery implements TupleDestination->TupleDescForQuery
 * for TupleStoreTupleDestination.
 */
static TupleDesc
TupleStoreTupleDestTupleDescForQuery(TupleDestination *self, int queryNumber)
{
	Assert(queryNumber == 0);

	TupleStoreTupleDestination *tupleDest = (TupleStoreTupleDestination *) self;

	return tupleDest->tupleDesc;
}


/*
 * CreateTupleDestNone creates a tuple destination which ignores the tuples.
 */
TupleDestination *
CreateTupleDestNone(void)
{
	TupleDestination *tupleDest = palloc0(
		sizeof(TupleDestination));
	tupleDest->putTuple = TupleDestNonePutTuple;
	tupleDest->tupleDescForQuery = TupleDestNoneTupleDescForQuery;

	return (TupleDestination *) tupleDest;
}


/*
 * TupleDestNonePutTuple implements TupleDestination->putTuple for
 * no-op tuple destination.
 */
static void
TupleDestNonePutTuple(TupleDestination *self, Task *task,
					  int placementIndex, int queryNumber,
					  HeapTuple heapTuple, uint64 tupleLibpqSize)
{
	/* nothing to do */
}


/*
 * TupleDestNoneTupleDescForQuery implements TupleDestination->TupleDescForQuery
 * for no-op tuple destination.
 */
static TupleDesc
TupleDestNoneTupleDescForQuery(TupleDestination *self, int queryNumber)
{
	return NULL;
}


/*
 * CreateTupleDestDestReceiver creates a dest receiver which forwards tuples
 * to a tuple destination.
 */
DestReceiver *
CreateTupleDestDestReceiver(TupleDestination *tupleDest, Task *task, int placementIndex)
{
	TupleDestDestReceiver *destReceiver = palloc0(sizeof(TupleDestDestReceiver));
	destReceiver->pub.rStartup = TupleDestDestReceiverStartup;
	destReceiver->pub.receiveSlot = TupleDestDestReceiverReceive;
	destReceiver->pub.rShutdown = TupleDestDestReceiverShutdown;
	destReceiver->pub.rDestroy = TupleDestDestReceiverDestroy;

	destReceiver->tupleDest = tupleDest;
	destReceiver->task = task;
	destReceiver->placementIndex = placementIndex;

	return (DestReceiver *) destReceiver;
}


/*
 * TupleDestDestReceiverStartup implements DestReceiver->rStartup for
 * TupleDestDestReceiver.
 *
 * Local execution runs the shard query, so a whole-row reference to a table
 * yields rows of the shard's row type instead of the table's. Such rows become
 * unreadable once the shard is moved or dropped, and when the table has
 * dropped columns their layout differs from the table's row type. Remote
 * execution does not have this problem, since it parses the results using the
 * types that the tuple destination expects. Hence, for columns whose row type
 * (or array element row type) differs from the expected one, we convert the
 * rows by column name, like PostgreSQL does for ConvertRowtypeExpr.
 */
static void
TupleDestDestReceiverStartup(DestReceiver *destReceiver, int operation,
							 TupleDesc inputTupleDesc)
{
	TupleDestDestReceiver *tupleDestReceiver = (TupleDestDestReceiver *) destReceiver;
	TupleDestination *tupleDest = tupleDestReceiver->tupleDest;

	TupleDesc outputTupleDesc = tupleDest->tupleDescForQuery(tupleDest, 0);
	if (outputTupleDesc == NULL || outputTupleDesc->natts != inputTupleDesc->natts)
	{
		return;
	}

	List *rowtypeConversionList = NIL;
	for (int columnIndex = 0; columnIndex < inputTupleDesc->natts; columnIndex++)
	{
		Oid inputType = TupleDescAttr(inputTupleDesc, columnIndex)->atttypid;
		Oid outputType = TupleDescAttr(outputTupleDesc, columnIndex)->atttypid;

		RowtypeConversion *conversion = CreateRowtypeConversion(inputType, outputType);
		if (conversion != NULL)
		{
			conversion->columnIndex = columnIndex;
			rowtypeConversionList = lappend(rowtypeConversionList, conversion);
		}
	}

	if (rowtypeConversionList == NIL)
	{
		return;
	}

	tupleDestReceiver->rowtypeConversionList = rowtypeConversionList;
	tupleDestReceiver->outputTupleDesc = outputTupleDesc;
	tupleDestReceiver->conversionContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "TupleDestDestReceiver row type conversion",
							  ALLOCSET_DEFAULT_SIZES);
}


/*
 * CreateRowtypeConversion returns how to convert values of inputType to
 * outputType if both are named row types, or arrays of named row types, and
 * they differ. Otherwise, it returns NULL.
 */
static RowtypeConversion *
CreateRowtypeConversion(Oid inputType, Oid outputType)
{
	if (inputType == outputType)
	{
		return NULL;
	}

	Oid inputElementType = get_element_type(inputType);
	Oid outputElementType = get_element_type(outputType);
	if (OidIsValid(inputElementType) != OidIsValid(outputElementType))
	{
		return NULL;
	}

	Oid inputRowType = OidIsValid(inputElementType) ? inputElementType : inputType;
	Oid outputRowType = OidIsValid(outputElementType) ? outputElementType : outputType;
	if (get_typtype(inputRowType) != TYPTYPE_COMPOSITE ||
		get_typtype(outputRowType) != TYPTYPE_COMPOSITE)
	{
		return NULL;
	}

	TupleDesc inputRowDesc = lookup_rowtype_tupdesc_copy(inputRowType, -1);
	TupleDesc outputRowDesc = lookup_rowtype_tupdesc_copy(outputRowType, -1);

	RowtypeConversion *conversion = palloc0(sizeof(RowtypeConversion));
	conversion->outputRowDesc = outputRowDesc;
	conversion->conversionMap = convert_tuples_by_name(inputRowDesc, outputRowDesc);
	conversion->inputElementType = inputElementType;
	conversion->outputElementType = outputElementType;
	get_typlenbyvalalign(inputRowType, &conversion->elementLength,
						 &conversion->elementByValue, &conversion->elementAlign);

	return conversion;
}


/*
 * TupleDestDestReceiverReceive implements DestReceiver->receiveSlot for
 * TupleDestDestReceiver.
 */
static bool
TupleDestDestReceiverReceive(TupleTableSlot *slot,
							 DestReceiver *destReceiver)
{
	TupleDestDestReceiver *tupleDestReceiver = (TupleDestDestReceiver *) destReceiver;
	TupleDestination *tupleDest = tupleDestReceiver->tupleDest;
	Task *task = tupleDestReceiver->task;
	int placementIndex = tupleDestReceiver->placementIndex;

	/*
	 * DestReceiver doesn't support multiple result sets with different shapes.
	 */
	Assert(task->queryCount == 1);
	int queryNumber = 0;

	HeapTuple heapTuple = NULL;
	bool convertRowtypes = tupleDestReceiver->rowtypeConversionList != NIL;
	if (convertRowtypes)
	{
		MemoryContext oldContext =
			MemoryContextSwitchTo(tupleDestReceiver->conversionContext);

		heapTuple = ConvertRowtypesInSlot(tupleDestReceiver, slot);

		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		heapTuple = ExecFetchSlotHeapTuple(slot, true, NULL);
	}

	uint64 tupleLibpqSize = 0;

	tupleDest->putTuple(tupleDest, task, placementIndex, queryNumber, heapTuple,
						tupleLibpqSize);

	if (convertRowtypes)
	{
		MemoryContextReset(tupleDestReceiver->conversionContext);
	}

	return true;
}


/*
 * ConvertRowtypesInSlot returns a tuple of the tuple destination's tuple
 * descriptor that contains the values in the given slot, with the row types
 * converted as determined in TupleDestDestReceiverStartup.
 */
static HeapTuple
ConvertRowtypesInSlot(TupleDestDestReceiver *tupleDestReceiver, TupleTableSlot *slot)
{
	TupleDesc outputTupleDesc = tupleDestReceiver->outputTupleDesc;
	int columnCount = outputTupleDesc->natts;

	slot_getallattrs(slot);

	Datum *values = palloc(columnCount * sizeof(Datum));
	memcpy_s(values, columnCount * sizeof(Datum), slot->tts_values,
			 columnCount * sizeof(Datum));

	RowtypeConversion *conversion = NULL;
	foreach_declared_ptr(conversion, tupleDestReceiver->rowtypeConversionList)
	{
		int columnIndex = conversion->columnIndex;
		if (slot->tts_isnull[columnIndex])
		{
			continue;
		}

		if (OidIsValid(conversion->outputElementType))
		{
			values[columnIndex] = ConvertRowArrayDatum(values[columnIndex], conversion);
		}
		else
		{
			values[columnIndex] = ConvertRowDatum(values[columnIndex], conversion);
		}
	}

	return heap_form_tuple(outputTupleDesc, values, slot->tts_isnull);
}


/*
 * ConvertRowDatum converts a row to the output row type of the conversion,
 * like ExecEvalConvertRowtype does.
 */
static Datum
ConvertRowDatum(Datum rowDatum, RowtypeConversion *conversion)
{
	HeapTupleHeader rowHeader = DatumGetHeapTupleHeader(rowDatum);

	HeapTupleData rowTuple;
	rowTuple.t_len = HeapTupleHeaderGetDatumLength(rowHeader);
	ItemPointerSetInvalid(&(rowTuple.t_self));
	rowTuple.t_tableOid = InvalidOid;
	rowTuple.t_data = rowHeader;

	if (conversion->conversionMap != NULL)
	{
		HeapTuple convertedTuple = execute_attr_map_tuple(&rowTuple,
														  conversion->conversionMap);
		return HeapTupleGetDatum(convertedTuple);
	}

	/* the layouts match, the copy only gets the output row type in its header */
	return heap_copy_tuple_as_datum(&rowTuple, conversion->outputRowDesc);
}


/*
 * ConvertRowArrayDatum converts each row in an array of rows to the output
 * row type of the conversion, and returns an array of that row type.
 */
static Datum
ConvertRowArrayDatum(Datum arrayDatum, RowtypeConversion *conversion)
{
	ArrayType *inputArray = DatumGetArrayTypeP(arrayDatum);

	Datum *elements = NULL;
	bool *elementNulls = NULL;
	int elementCount = 0;
	deconstruct_array(inputArray, conversion->inputElementType,
					  conversion->elementLength, conversion->elementByValue,
					  conversion->elementAlign, &elements, &elementNulls,
					  &elementCount);

	for (int elementIndex = 0; elementIndex < elementCount; elementIndex++)
	{
		if (!elementNulls[elementIndex])
		{
			elements[elementIndex] = ConvertRowDatum(elements[elementIndex],
													 conversion);
		}
	}

	ArrayType *outputArray = construct_md_array(elements, elementNulls,
												ARR_NDIM(inputArray),
												ARR_DIMS(inputArray),
												ARR_LBOUND(inputArray),
												conversion->outputElementType,
												conversion->elementLength,
												conversion->elementByValue,
												conversion->elementAlign);

	return PointerGetDatum(outputArray);
}


/*
 * TupleDestDestReceiverShutdown implements DestReceiver->rShutdown for
 * TupleDestDestReceiver.
 */
static void
TupleDestDestReceiverShutdown(DestReceiver *destReceiver)
{
	TupleDestDestReceiver *tupleDestReceiver = (TupleDestDestReceiver *) destReceiver;

	if (tupleDestReceiver->conversionContext != NULL)
	{
		MemoryContextDelete(tupleDestReceiver->conversionContext);
	}

	tupleDestReceiver->rowtypeConversionList = NIL;
	tupleDestReceiver->outputTupleDesc = NULL;
	tupleDestReceiver->conversionContext = NULL;
}


/*
 * TupleDestDestReceiverDestroy implements DestReceiver->rDestroy for
 * TupleDestDestReceiver.
 */
static void
TupleDestDestReceiverDestroy(DestReceiver *destReceiver)
{
	/* nothing to do */
}
