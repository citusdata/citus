/*-------------------------------------------------------------------------
 *
 * sorted_merge.c
 *	  Implements coordinator-side sorted merge of pre-sorted worker results.
 *
 *	  CreatePerTaskDispatchDest() creates per-task tuple stores and returns
 *	  a TupleDestination that routes incoming tuples to the correct store
 *	  based on task->taskId. The only Task field written is
 *	  totalReceivedTupleData (execution-time reporting, reset each execution).
 *
 *	  MergePerTaskStoresIntoFinalStore() performs a k-way merge of the
 *	  per-task stores into a single output tuplestore using a binary heap
 *	  and PostgreSQL's SortSupport infrastructure.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "executor/tuptable.h"
#include "lib/binaryheap.h"
#include "utils/hsearch.h"
#include "utils/sortsupport.h"

#include "distributed/listutils.h"
#include "distributed/sorted_merge.h"
#include "distributed/subplan_execution.h"


/*
 * PerTaskDispatchTupleDest routes tuples to per-task tuple stores
 * based on the task's taskId. This is an execution-local object that
 * is never attached to a reusable Task node.
 */
typedef struct PerTaskDispatchTupleDest
{
	TupleDestination pub;
	Tuplestorestate **perTaskStores;
	int taskCount;
	TupleDesc tupleDesc;
	HTAB *taskIdToIndex;   /* maps uint32 taskId -> int array index */
} PerTaskDispatchTupleDest;


/*
 * TaskIdIndexEntry is a hash table entry mapping taskId to per-task store index.
 */
typedef struct TaskIdIndexEntry
{
	uint32 taskId;    /* hash key */
	int index;        /* index into perTaskStores array */
} TaskIdIndexEntry;


/*
 * MergeContext holds the state needed by the binary heap comparator.
 */
typedef struct MergeContext
{
	TupleTableSlot **slots;
	SortSupportData *sortKeys;
	int nkeys;
} MergeContext;


/* forward declarations */
static void PerTaskDispatchPutTuple(TupleDestination *self, Task *task,
									int placementIndex, int queryNumber,
									HeapTuple heapTuple, uint64 tupleLibpqSize);
static TupleDesc PerTaskDispatchTupleDescForQuery(TupleDestination *self,
												  int queryNumber);
static int MergeHeapComparator(Datum a, Datum b, void *arg);


/*
 * CreatePerTaskDispatchDest creates per-task tuple stores and returns a
 * TupleDestination that routes incoming tuples to the correct store based
 * on task->taskId.
 *
 * The per-task stores and their count are returned via out parameters so
 * the caller can pass them to MergePerTaskStoresIntoFinalStore() later.
 *
 * All memory is allocated in CurrentMemoryContext (expected to be the
 * AdaptiveExecutor local context).
 */
TupleDestination *
CreatePerTaskDispatchDest(List *taskList, TupleDesc tupleDesc,
						  TupleDestinationStats *sharedStats,
						  Tuplestorestate ***perTaskStoresOut,
						  int *perTaskStoreCountOut)
{
	int taskCount = list_length(taskList);
	if (taskCount == 0)
	{
		*perTaskStoresOut = NULL;
		*perTaskStoreCountOut = 0;
		return CreateTupleDestNone();
	}

	/*
	 * Allocate per-task tuple stores. Each store gets work_mem / taskCount,
	 * with a floor of 64 kB. Note: this means the aggregate in-memory budget
	 * for per-task stores can exceed a single work_mem allocation when
	 * taskCount is large (e.g., 128 tasks × 64 kB = 8 MB floor). The final
	 * output tuplestore also gets a full work_mem allocation. This is a
	 * deliberate trade-off: per-task stores spill to disk automatically,
	 * and they are freed before the final tuplestore is consumed. The
	 * temporary memory amplification is bounded and short-lived.
	 */
	Tuplestorestate **perTaskStores = palloc(taskCount * sizeof(Tuplestorestate *));
	int perTaskWorkMem = Max(work_mem / Max(taskCount, 1), 64);

	for (int i = 0; i < taskCount; i++)
	{
		perTaskStores[i] = tuplestore_begin_heap(false, false, perTaskWorkMem);
	}

	/* build taskId -> array index hash table */
	HASHCTL hashInfo;
	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(uint32);
	hashInfo.entrysize = sizeof(TaskIdIndexEntry);
	hashInfo.hcxt = CurrentMemoryContext;
	HTAB *taskIdToIndex = hash_create("PerTaskDispatchHash", taskCount,
									  &hashInfo, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	int index = 0;
	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		bool found = false;
		TaskIdIndexEntry *entry = hash_search(taskIdToIndex, &task->taskId,
											  HASH_ENTER, &found);
		Assert(!found);
		entry->index = index;
		index++;
	}

	/* build the dispatch TupleDestination */
	PerTaskDispatchTupleDest *dispatch = palloc0(sizeof(PerTaskDispatchTupleDest));
	dispatch->pub.putTuple = PerTaskDispatchPutTuple;
	dispatch->pub.tupleDescForQuery = PerTaskDispatchTupleDescForQuery;
	dispatch->pub.tupleDestinationStats = sharedStats;
	dispatch->perTaskStores = perTaskStores;
	dispatch->taskCount = taskCount;
	dispatch->tupleDesc = tupleDesc;
	dispatch->taskIdToIndex = taskIdToIndex;

	*perTaskStoresOut = perTaskStores;
	*perTaskStoreCountOut = taskCount;

	return (TupleDestination *) dispatch;
}


/*
 * PerTaskDispatchPutTuple routes a tuple to the per-task store identified
 * by the task's taskId. Matches the behavior of TupleStoreTupleDestPutTuple
 * for intermediate-result accounting and totalReceivedTupleData tracking.
 */
static void
PerTaskDispatchPutTuple(TupleDestination *self, Task *task,
						int placementIndex, int queryNumber,
						HeapTuple heapTuple, uint64 tupleLibpqSize)
{
	PerTaskDispatchTupleDest *dispatch = (PerTaskDispatchTupleDest *) self;

	/* look up the per-task store index */
	bool found = false;
	TaskIdIndexEntry *entry = hash_search(dispatch->taskIdToIndex, &task->taskId,
										  HASH_FIND, &found);
	Assert(found);
	tuplestore_puttuple(dispatch->perTaskStores[entry->index], heapTuple);

	/* intermediate-result size accounting (matches TupleStoreTupleDestPutTuple) */
	uint64 tupleSize = tupleLibpqSize;
	if (tupleSize == 0)
	{
		tupleSize = heapTuple->t_len;
	}

	TupleDestinationStats *stats = self->tupleDestinationStats;
	if (SubPlanLevel > 0 && stats != NULL)
	{
		stats->totalIntermediateResultSize += tupleSize;
		EnsureIntermediateSizeLimitNotExceeded(stats);
	}

	/* track network transfer size (matches TupleStoreTupleDestPutTuple) */
	task->totalReceivedTupleData += tupleLibpqSize;
}


/*
 * PerTaskDispatchTupleDescForQuery returns the tuple descriptor.
 */
static TupleDesc
PerTaskDispatchTupleDescForQuery(TupleDestination *self, int queryNumber)
{
	Assert(queryNumber == 0);
	PerTaskDispatchTupleDest *dispatch = (PerTaskDispatchTupleDest *) self;
	return dispatch->tupleDesc;
}


/*
 * MergePerTaskStoresIntoFinalStore performs a k-way merge of pre-sorted
 * per-task tuple stores into a single output tuplestore using a binary heap.
 *
 * Each per-task store must contain tuples sorted by the given merge keys.
 * The output tuplestore will contain all tuples in globally sorted order.
 *
 * Uses PostgreSQL's public binaryheap and SortSupport APIs.
 */
void
MergePerTaskStoresIntoFinalStore(Tuplestorestate *finalStore,
								 Tuplestorestate **perTaskStores,
								 int nstores,
								 SortedMergeKey *mergeKeys,
								 int nkeys,
								 TupleDesc tupleDesc)
{
	if (nstores == 0 || nkeys == 0)
	{
		return;
	}

	/* allocate one reusable slot per task store */
	TupleTableSlot **slots = palloc(nstores * sizeof(TupleTableSlot *));
	for (int i = 0; i < nstores; i++)
	{
		slots[i] = MakeSingleTupleTableSlot(tupleDesc, &TTSOpsMinimalTuple);
	}

	/* build SortSupport from serialized merge keys */
	SortSupportData *sortKeys = palloc0(nkeys * sizeof(SortSupportData));
	for (int i = 0; i < nkeys; i++)
	{
		SortSupport sk = &sortKeys[i];
		sk->ssup_cxt = CurrentMemoryContext;
		sk->ssup_collation = mergeKeys[i].collation;
		sk->ssup_nulls_first = mergeKeys[i].nullsFirst;
		sk->ssup_attno = mergeKeys[i].attno;
		PrepareSortSupportFromOrderingOp(mergeKeys[i].sortop, sk);
	}

	/* set up merge context for heap comparisons */
	MergeContext ctx;
	ctx.slots = slots;
	ctx.sortKeys = sortKeys;
	ctx.nkeys = nkeys;

	binaryheap *heap = binaryheap_allocate(nstores, MergeHeapComparator, &ctx);

	/* seed the heap with the first tuple from each non-empty store */
	for (int i = 0; i < nstores; i++)
	{
		tuplestore_rescan(perTaskStores[i]);
		if (tuplestore_gettupleslot(perTaskStores[i], true, false, slots[i]))
		{
			binaryheap_add_unordered(heap, Int32GetDatum(i));
		}
	}
	binaryheap_build(heap);

	/* merge loop: extract min, write to final store, advance winner */
	while (!binaryheap_empty(heap))
	{
		int winner = DatumGetInt32(binaryheap_first(heap));
		tuplestore_puttupleslot(finalStore, slots[winner]);

		if (tuplestore_gettupleslot(perTaskStores[winner], true, false,
									slots[winner]))
		{
			binaryheap_replace_first(heap, Int32GetDatum(winner));
		}
		else
		{
			(void) binaryheap_remove_first(heap);
		}
	}

	/* free merge-local resources */
	binaryheap_free(heap);
	for (int i = 0; i < nstores; i++)
	{
		ExecDropSingleTupleTableSlot(slots[i]);
	}
	pfree(slots);
	pfree(sortKeys);
}


/*
 * MergeHeapComparator compares tuples from two task stores by the merge keys.
 * Returns negative if a < b, positive if a > b, zero if equal.
 * The binary heap is a max-heap, so we negate to get min-heap behavior.
 *
 * This is modeled after heap_compare_slots() in nodeMergeAppend.c.
 */
static int
MergeHeapComparator(Datum a, Datum b, void *arg)
{
	MergeContext *ctx = (MergeContext *) arg;
	int slot1 = DatumGetInt32(a);
	int slot2 = DatumGetInt32(b);
	TupleTableSlot *s1 = ctx->slots[slot1];
	TupleTableSlot *s2 = ctx->slots[slot2];

	for (int i = 0; i < ctx->nkeys; i++)
	{
		SortSupport sortKey = &ctx->sortKeys[i];
		AttrNumber attno = sortKey->ssup_attno;
		bool isNull1, isNull2;

		Datum datum1 = slot_getattr(s1, attno, &isNull1);
		Datum datum2 = slot_getattr(s2, attno, &isNull2);

		int compare = ApplySortComparator(datum1, isNull1,
										  datum2, isNull2,
										  sortKey);
		if (compare != 0)
		{
			/* binaryheap is a max-heap, negate for min-heap behavior */
			return -compare;
		}
	}

	return 0;
}


/*
 * SortedMergeAdapter streams tuples from K pre-sorted per-task stores
 * via a binary heap, returning one globally-sorted tuple per call.
 *
 * This is the streaming replacement for MergePerTaskStoresIntoFinalStore().
 * Instead of copying all tuples into a final tuplestore, the adapter holds
 * the per-task stores and heap alive, producing tuples on demand.
 *
 * Modeled after PostgreSQL's MergeAppend (nodeMergeAppend.c), which uses
 * the same binary-heap-over-sorted-inputs pattern.
 */
struct SortedMergeAdapter
{
	Tuplestorestate **perTaskStores;    /* K per-task stores (owned) */
	int nstores;

	binaryheap *heap;

	MergeContext mergeCtx;              /* embedded — passed to heap as bh_arg */

	TupleDesc tupleDesc;
	bool exhausted;
	bool initialized;
};


/*
 * CreateSortedMergeAdapter builds a streaming merge adapter over K per-task
 * stores. The adapter takes ownership of perTaskStores — the caller must
 * not free them; FreeSortedMergeAdapter() handles cleanup.
 *
 * All memory is allocated in CurrentMemoryContext. The caller must ensure
 * this context outlives the adapter (the AdaptiveExecutor local context
 * already satisfies this — see adaptive_executor.c).
 */
SortedMergeAdapter *
CreateSortedMergeAdapter(Tuplestorestate **perTaskStores,
						 int nstores,
						 SortedMergeKey *mergeKeys,
						 int nkeys,
						 TupleDesc tupleDesc)
{
	SortedMergeAdapter *adapter = palloc0(sizeof(SortedMergeAdapter));
	adapter->perTaskStores = perTaskStores;
	adapter->nstores = nstores;
	adapter->tupleDesc = tupleDesc;

	/* one comparison slot per store — owned via mergeCtx.slots */
	TupleTableSlot **slots = palloc(nstores * sizeof(TupleTableSlot *));
	for (int i = 0; i < nstores; i++)
	{
		slots[i] = MakeSingleTupleTableSlot(tupleDesc, &TTSOpsMinimalTuple);
	}

	/* build SortSupport (same logic as MergePerTaskStoresIntoFinalStore) */
	SortSupportData *sortKeys = palloc0(nkeys * sizeof(SortSupportData));
	for (int i = 0; i < nkeys; i++)
	{
		SortSupport sk = &sortKeys[i];
		sk->ssup_cxt = CurrentMemoryContext;
		sk->ssup_collation = mergeKeys[i].collation;
		sk->ssup_nulls_first = mergeKeys[i].nullsFirst;
		sk->ssup_attno = mergeKeys[i].attno;
		PrepareSortSupportFromOrderingOp(mergeKeys[i].sortop, sk);
	}

	/* set up embedded merge context for heap comparisons */
	adapter->mergeCtx.slots = slots;
	adapter->mergeCtx.sortKeys = sortKeys;
	adapter->mergeCtx.nkeys = nkeys;

	/* allocate heap with embedded context as comparator arg */
	adapter->heap = binaryheap_allocate(nstores, MergeHeapComparator,
										&adapter->mergeCtx);

	return adapter;
}


/*
 * SortedMergeAdapterNext returns the next globally-sorted tuple from the
 * adapter by copying it into the provided scanSlot. Returns true if a tuple
 * was returned, false if all stores are exhausted.
 *
 * The heap uses per-store comparison slots (mergeCtx.slots). After
 * identifying the winner, we ExecCopySlot from the winner's comparison
 * slot into the scan slot. This is a MinimalTuple copy, comparable in
 * cost to the tuplestore_puttupleslot write in the eager merge path.
 *
 * On each call after the first, we advance the previous winner's store
 * and update the heap before selecting the new winner. This matches the
 * MergeAppend pattern in nodeMergeAppend.c.
 */
bool
SortedMergeAdapterNext(SortedMergeAdapter *adapter, TupleTableSlot *scanSlot)
{
	if (adapter->exhausted)
	{
		ExecClearTuple(scanSlot);
		return false;
	}

	if (!adapter->initialized)
	{
		/* first call: seed the heap with the first tuple from each store */
		for (int i = 0; i < adapter->nstores; i++)
		{
			tuplestore_rescan(adapter->perTaskStores[i]);
			if (tuplestore_gettupleslot(adapter->perTaskStores[i], true, false,
										adapter->mergeCtx.slots[i]))
			{
				binaryheap_add_unordered(adapter->heap, Int32GetDatum(i));
			}
		}
		binaryheap_build(adapter->heap);
		adapter->initialized = true;
	}
	else
	{
		/* advance the previous winner and update the heap */
		int prevWinner = DatumGetInt32(binaryheap_first(adapter->heap));
		if (tuplestore_gettupleslot(adapter->perTaskStores[prevWinner], true,
									false, adapter->mergeCtx.slots[prevWinner]))
		{
			binaryheap_replace_first(adapter->heap, Int32GetDatum(prevWinner));
		}
		else
		{
			(void) binaryheap_remove_first(adapter->heap);
		}
	}

	if (binaryheap_empty(adapter->heap))
	{
		adapter->exhausted = true;
		ExecClearTuple(scanSlot);
		return false;
	}

	int winner = DatumGetInt32(binaryheap_first(adapter->heap));
	ExecCopySlot(scanSlot, adapter->mergeCtx.slots[winner]);

	return true;
}


/*
 * SortedMergeAdapterRescan resets the adapter to re-read from the beginning.
 * Called from CitusReScan() for cursor WITH HOLD patterns.
 *
 * Cost is O(K log K) to rebuild the heap, which is negligible for typical
 * shard counts (4-64). Both binaryheap_reset() and tuplestore_rescan()
 * are proven APIs used by PostgreSQL's ExecReScanMergeAppend.
 */
void
SortedMergeAdapterRescan(SortedMergeAdapter *adapter)
{
	binaryheap_reset(adapter->heap);

	for (int i = 0; i < adapter->nstores; i++)
	{
		tuplestore_rescan(adapter->perTaskStores[i]);
		if (tuplestore_gettupleslot(adapter->perTaskStores[i], true, false,
									adapter->mergeCtx.slots[i]))
		{
			binaryheap_add_unordered(adapter->heap, Int32GetDatum(i));
		}
	}
	binaryheap_build(adapter->heap);

	adapter->exhausted = false;
	adapter->initialized = true;
}


/*
 * FreeSortedMergeAdapter releases all adapter resources including
 * per-task stores, comparison slots, sort keys, and the heap.
 * Called from CitusEndScan() for deterministic cleanup.
 */
void
FreeSortedMergeAdapter(SortedMergeAdapter *adapter)
{
	if (adapter == NULL)
	{
		return;
	}

	for (int i = 0; i < adapter->nstores; i++)
	{
		tuplestore_end(adapter->perTaskStores[i]);
		ExecDropSingleTupleTableSlot(adapter->mergeCtx.slots[i]);
	}

	binaryheap_free(adapter->heap);
	pfree(adapter->mergeCtx.slots);
	pfree(adapter->mergeCtx.sortKeys);
	pfree(adapter->perTaskStores);

	/* mergeCtx is embedded in adapter, freed with the adapter itself */
	pfree(adapter);
}
