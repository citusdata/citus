/*-------------------------------------------------------------------------
 *
 * sorted_merge.c
 *	  Implements coordinator-side sorted merge of pre-sorted worker results.
 *
 *	  AssignPerTaskDispatchDests() creates one tuplestore per task and assigns
 *	  task->tupleDest to a TupleStoreTupleDest pointing at that store. The
 *	  executor then routes each worker result tuple directly via the task's
 *	  tupleDest, with no hash-table indirection. All per-task tupleDests
 *	  share a single TupleDestinationStats so citus.max_intermediate_result_size
 *	  is enforced across the aggregate, not per task.
 *
 *	  Because Task nodes can be cached on prepared DistributedPlans, the
 *	  caller (AdaptiveExecutor) is responsible for clearing task->tupleDest
 *	  before and after each execution via ClearPerTaskDispatchDests(); this
 *	  module does not retain pointers to Tasks beyond setup.
 *
 *	  FinalizeSortedMerge() builds a SortedMergeAdapter that performs a
 *	  k-way merge of the per-task stores using a binary heap and
 *	  PostgreSQL's SortSupport infrastructure, streaming one globally-sorted
 *	  tuple per call to the executor.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "executor/tuptable.h"
#include "lib/binaryheap.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "utils/sortsupport.h"

#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/sorted_merge.h"
#include "distributed/subplan_execution.h"


/*
 * MergeContext holds the state needed by the binary heap comparator.
 */
typedef struct MergeContext
{
	TupleTableSlot **slots;
	SortSupportData *sortKeys;
	int nkeys;
} MergeContext;


/*
 * SortedMergeAdapter streams tuples from K pre-sorted per-task stores
 * via a binary heap, returning one globally-sorted tuple per call.
 *
 * Modeled after PostgreSQL's MergeAppend (nodeMergeAppend.c), which uses
 * the same binary-heap-over-sorted-inputs pattern.
 */
struct SortedMergeAdapter
{
	Tuplestorestate **perTaskStores;    /* K per-task stores (not owned in eager mode) */
	int nstores;
	bool ownsStores;                    /* if true, FreeSortedMergeAdapter frees stores */

	binaryheap *heap;

	MergeContext mergeCtx;              /* embedded — passed to heap as bh_arg */

	TupleDesc tupleDesc;
	bool exhausted;
	bool initialized;
};


/* forward declarations */
static int MergeHeapComparator(Datum a, Datum b, void *arg);
static bool SortedMergeAdapterInitialize(SortedMergeAdapter *adapter);
static bool SortedMergeAdapterAdvancePreviousWinner(SortedMergeAdapter *adapter);
static TupleTableSlot * SortedMergeAdapterCurrentWinnerSlot(SortedMergeAdapter *adapter);


/*
 * BuildSortedMergeKeys constructs an array of SortedMergeKey from a sort clause
 * list and its corresponding target list. The resulting keys are used by the
 * executor to set up SortSupport structures for the k-way merge.
 *
 * The attribute numbers in the keys correspond to worker output column positions,
 * which align with the 1-based non-junk ordering of the worker target list.
 */
SortedMergeKey *
BuildSortedMergeKeys(List *sortClauseList, List *targetList, int *nkeys)
{
	*nkeys = list_length(sortClauseList);
	if (*nkeys == 0)
	{
		return NULL;
	}

	SortedMergeKey *keys = palloc(*nkeys * sizeof(SortedMergeKey));

	int i = 0;
	SortGroupClause *sgc = NULL;
	foreach_declared_ptr(sgc, sortClauseList)
	{
		TargetEntry *tle = get_sortgroupclause_tle(sgc, targetList);
		keys[i].attno = tle->resno;
		keys[i].sortop = sgc->sortop;
		keys[i].collation = exprCollation((Node *) tle->expr);
		keys[i].nullsFirst = sgc->nulls_first;
		i++;
	}

	return keys;
}


/*
 * AssignPerTaskDispatchDests creates one tuple store per task and sets
 * task->tupleDest to a TupleStoreTupleDest that writes directly to that
 * store. All per-task destinations share a single TupleDestinationStats
 * (sharedStats) so that citus.max_intermediate_result_size is enforced
 * against the sum of bytes across tasks, not per task.
 *
 * The per-task store array (parallel to taskList iteration order) is
 * returned via *perTaskStoresOut; the merge code consumes it in the same
 * 0..k-1 order. The hot dispatch path is therefore task->tupleDest->putTuple
 * (TupleStoreTupleDestPutTuple), with no hash-table lookup.
 *
 * Caller responsibilities:
 *   - taskList must be the canonical Job->taskList; mutations to
 *     task->tupleDest are visible across cached prepared-plan executions
 *     and must be cleared via ClearPerTaskDispatchDests() at execution
 *     start AND end. AdaptiveExecutor handles this.
 *   - All allocations live in CurrentMemoryContext (the AdaptiveExecutor
 *     local context), and become invalid when that context is freed.
 */
void
AssignPerTaskDispatchDests(List *taskList, TupleDesc tupleDesc,
						   TupleDestinationStats *sharedStats,
						   Tuplestorestate ***perTaskStoresOut,
						   int *perTaskStoreCountOut)
{
	int taskCount = list_length(taskList);
	if (taskCount == 0)
	{
		*perTaskStoresOut = NULL;
		*perTaskStoreCountOut = 0;
		return;
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

	int i = 0;
	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		perTaskStores[i] = tuplestore_begin_heap(false, false, perTaskWorkMem);
		task->tupleDest =
			CreateTupleStoreTupleDestWithStats(perTaskStores[i], tupleDesc,
											   sharedStats);
		i++;
	}

	*perTaskStoresOut = perTaskStores;
	*perTaskStoreCountOut = taskCount;
}


/*
 * ClearPerTaskDispatchDests resets task->tupleDest to NULL for every task
 * in taskList. Used to scrub execution-local pointers off the canonical
 * (cached) task list at the start and end of every AdaptiveExecutor()
 * invocation, so that re-execution of a cached prepared plan never sees
 * a stale pointer into a freed memory context.
 *
 * Safe to call on tasks that already have tupleDest == NULL.
 */
void
ClearPerTaskDispatchDests(List *taskList)
{
	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		task->tupleDest = NULL;
	}
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
 * CreateSortedMergeAdapter builds a streaming merge adapter over K per-task
 * stores. When ownsStores is true, FreeSortedMergeAdapter() will call
 * tuplestore_end() on each per-task store; when false, the caller retains
 * ownership and must free them separately.
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
						 TupleDesc tupleDesc,
						 bool ownsStores)
{
	SortedMergeAdapter *adapter = palloc0(sizeof(SortedMergeAdapter));
	adapter->perTaskStores = perTaskStores;
	adapter->nstores = nstores;
	adapter->ownsStores = ownsStores;
	adapter->tupleDesc = tupleDesc;

	/* one comparison slot per store — owned via mergeCtx.slots */
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
 * SortedMergeAdapterInitialize seeds the heap with the first tuple from each
 * per-task store. Returns true if at least one store had a tuple.
 */
static bool
SortedMergeAdapterInitialize(SortedMergeAdapter *adapter)
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

	return !binaryheap_empty(adapter->heap);
}


/*
 * SortedMergeAdapterAdvancePreviousWinner advances the store whose tuple won
 * the previous call, then updates the heap. Returns true if tuples remain.
 */
static bool
SortedMergeAdapterAdvancePreviousWinner(SortedMergeAdapter *adapter)
{
	Assert(!binaryheap_empty(adapter->heap));

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

	return !binaryheap_empty(adapter->heap);
}


/*
 * SortedMergeAdapterCurrentWinnerSlot returns the current heap winner. The
 * returned slot is owned by the adapter and remains valid until the next
 * adapter call, rescan, or free.
 */
static TupleTableSlot *
SortedMergeAdapterCurrentWinnerSlot(SortedMergeAdapter *adapter)
{
	Assert(!binaryheap_empty(adapter->heap));

	int winner = DatumGetInt32(binaryheap_first(adapter->heap));
	return adapter->mergeCtx.slots[winner];
}


/*
 * SortedMergeAdapterNextSlot returns the next globally-sorted tuple from the
 * adapter. The returned slot is adapter-owned and must be treated as read-only
 * by callers. Returns NULL if all stores are exhausted.
 *
 * The heap uses per-store comparison slots (mergeCtx.slots). After
 * identifying the winner, the slot is returned directly to avoid a per-tuple
 * MinimalTuple copy in the streaming path.
 *
 * On each call after the first, we advance the previous winner's store
 * and update the heap before selecting the new winner. This matches the
 * MergeAppend pattern in nodeMergeAppend.c.
 */
TupleTableSlot *
SortedMergeAdapterNextSlot(SortedMergeAdapter *adapter)
{
	if (adapter->exhausted)
	{
		return NULL;
	}

	bool tuplesRemain = false;
	if (!adapter->initialized)
	{
		tuplesRemain = SortedMergeAdapterInitialize(adapter);
	}
	else
	{
		tuplesRemain = SortedMergeAdapterAdvancePreviousWinner(adapter);
	}

	if (!tuplesRemain)
	{
		adapter->exhausted = true;
		return NULL;
	}

	return SortedMergeAdapterCurrentWinnerSlot(adapter);
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

	adapter->exhausted = binaryheap_empty(adapter->heap);
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
		if (adapter->ownsStores)
		{
			tuplestore_end(adapter->perTaskStores[i]);
		}
		ExecDropSingleTupleTableSlot(adapter->mergeCtx.slots[i]);
	}

	binaryheap_free(adapter->heap);
	pfree(adapter->mergeCtx.slots);
	pfree(adapter->mergeCtx.sortKeys);

	if (adapter->ownsStores)
	{
		pfree(adapter->perTaskStores);
	}

	/* mergeCtx is embedded in adapter, freed with the adapter itself */
	pfree(adapter);
}


/*
 * FinalizeSortedMerge performs the post-execution k-way merge of pre-sorted
 * per-task worker results by attaching a streaming SortedMergeAdapter to
 * scanState->mergeAdapter. The adapter takes ownership of the per-task
 * stores and is freed (along with the stores) by CitusEndScan via
 * FreeSortedMergeAdapter.
 *
 * Returns immediately when perTaskStoreCount == 0 (e.g. no remote tasks
 * executed). The plan-time eligibility gate guarantees workerJob->jobQuery
 * has at least one sort clause when this function is called with stores
 * present; we assert this defensively.
 */
void
FinalizeSortedMerge(CitusScanState *scanState,
					Job *workerJob,
					Tuplestorestate **perTaskStores,
					int perTaskStoreCount,
					TupleDesc tupleDescriptor)
{
	if (perTaskStoreCount == 0)
	{
		return;
	}

	int sortedMergeKeyCount = 0;
	SortedMergeKey *sortedMergeKeys =
		BuildSortedMergeKeys(workerJob->jobQuery->sortClause,
							 workerJob->jobQuery->targetList,
							 &sortedMergeKeyCount);

	/* Plan-time eligibility gate guarantees this; assert defensively. */
	Assert(sortedMergeKeyCount > 0);

	scanState->mergeAdapter = CreateSortedMergeAdapter(perTaskStores,
													   perTaskStoreCount,
													   sortedMergeKeys,
													   sortedMergeKeyCount,
													   tupleDescriptor,
													   true);
}
