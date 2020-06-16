#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <sys/stat.h>
#include <unistd.h>

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
	tuplestore_puttuple(tupleDest->tupleStore, heapTuple);
	task->totalReceivedData += tupleLibpqSize;
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
 * TupleStoreTupleDestPutTuple implements TupleDestination->putTuple for
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
 * TupleStoreTupleDestTupleDescForQuery implements TupleDestination->TupleDescForQuery
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
 * TupleDestDestReceiverStartup implements DestReceiver->receiveSlot for
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

#if PG_VERSION_NUM >= PG_VERSION_12
	HeapTuple heapTuple = ExecFetchSlotHeapTuple(slot, true, NULL);
#else
	HeapTuple heapTuple = ExecFetchSlotTuple(slot);
#endif

	tupleDest->putTuple(tupleDest, task, placementIndex, queryNumber, heapTuple, 0);

	return true;
}


/*
 * TupleDestDestReceiverStartup implements DestReceiver->rShutdown for
 * TupleDestDestReceiver.
 */
static void
TupleDestDestReceiverShutdown(DestReceiver *destReceiver)
{
	/* nothing to do */
}


/*
 * TupleDestDestReceiverStartup implements DestReceiver->rDestroy for
 * TupleDestDestReceiver.
 */
static void
TupleDestDestReceiverDestroy(DestReceiver *destReceiver)
{
	/* nothing to do */
}
