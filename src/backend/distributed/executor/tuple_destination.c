#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
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
} TupleDestDestReceiver;


/* forward declarations for local functions */
static void TupleStoreTupleDestPutTuple(TupleDestination *self, Task *task,
										int placementIndex, int queryNumber,
										HeapTuple heapTuple, uint64 tupleLibpqSize);
static void EnsureIntermediateSizeLimitNotExceeded(TupleDestinationStats *
												   tupleDestinationStats);
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


/*
 * CreateTupleStoreTupleDest creates a TupleDestination which forwards tuples to
 * a tupleStore.
 */
TupleDestination *
CreateTupleStoreTupleDest(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	TupleStoreTupleDestination *tupleStoreTupleDest = palloc0(
		sizeof(TupleStoreTupleDestination));

	tupleStoreTupleDest->tupleStore = tupleStore;
	tupleStoreTupleDest->tupleDesc = tupleDescriptor;
	tupleStoreTupleDest->pub.putTuple = TupleStoreTupleDestPutTuple;
	tupleStoreTupleDest->pub.tupleDescForQuery =
		TupleStoreTupleDestTupleDescForQuery;

	TupleDestination *tupleDestination = &tupleStoreTupleDest->pub;
	tupleDestination->tupleDestinationStats =
		(TupleDestinationStats *) palloc0(sizeof(TupleDestinationStats));

	return (TupleDestination *) tupleStoreTupleDest;
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
		tupleSize = HeapTupleHeaderGetDatumLength(heapTuple);
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
static void
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
 */
static void
TupleDestDestReceiverStartup(DestReceiver *destReceiver, int operation,
							 TupleDesc inputTupleDesc)
{
	/* nothing to do */
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

	HeapTuple heapTuple = ExecFetchSlotHeapTuple(slot, true, NULL);

	uint64 tupleLibpqSize = 0;

	tupleDest->putTuple(tupleDest, task, placementIndex, queryNumber, heapTuple,
						tupleLibpqSize);

	return true;
}


/*
 * TupleDestDestReceiverShutdown implements DestReceiver->rShutdown for
 * TupleDestDestReceiver.
 */
static void
TupleDestDestReceiverShutdown(DestReceiver *destReceiver)
{
	/* nothing to do */
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
