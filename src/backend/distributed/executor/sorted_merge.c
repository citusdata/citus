/*-------------------------------------------------------------------------
 *
 * sorted_merge.c
 *	  Implements coordinator-side sorted merge of pre-sorted worker results.
 *
 *	  CreatePerTaskDispatchDests() creates one tuplestore per task and assigns
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
 *	  CreatePerTaskDispatchDests() builds a SortedMergeAdapter that performs a
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


/*
 * CreatePerTaskDispatchDests creates one tuple store per task, sets
 * task->tupleDest to a TupleStoreTupleDest that writes directly to that
 * store, and attaches a SortedMergeAdapter to scanState->mergeAdapter so
 * the executor can read globally-sorted tuples lazily after the workers
 * fill the stores.
 *
 * All per-task destinations share a single TupleDestinationStats so that
 * citus.max_intermediate_result_size is enforced against the sum of bytes
 * across tasks, not per task.
 *
 * The hot dispatch path is task->tupleDest->putTuple
 * (TupleStoreTupleDestPutTuple), with no hash-table lookup. The adapter
 * reads from the same stores in 0..k-1 task order on first fetch.
 *
 * Caller responsibilities:
 *   - The scan's task list is the canonical Job->taskList; mutations to
 *     task->tupleDest are visible across cached prepared-plan executions
 *     and must be cleared via ClearPerTaskDispatchDests() at execution
 *     start AND end. AdaptiveExecutor handles this.
 *   - All allocations (per-task stores, adapter, slots, heap) live in
 *     CurrentMemoryContext (the AdaptiveExecutor local context), and
 *     become invalid when that context is freed. The adapter additionally
 *     owns the stores and ends them via FreeSortedMergeAdapter, called
 *     from CitusEndScan when the scan terminates.
 *
 * Always installs scanState->mergeAdapter, even when the task list is empty
 * (e.g. a plan whose tasks were all pruned to no remote work — see Q7 in
 * multi_orderby_pushdown.sql). The adapter degenerates to a zero-store
 * heap that returns no tuples; SortedMergeExecScan still goes through the
 * normal code path with no NULL checks required.
 */
void
CreatePerTaskDispatchDests(CitusScanState *scanState)
{
	Job *workerJob = scanState->distributedPlan->workerJob;
	List *taskList = workerJob->taskList;
	TupleDesc tupleDesc = ScanStateGetTupleDescriptor(scanState);

	/*
	 * Allocate per-task tuple stores. Each store gets work_mem / taskCount,
	 * with a floor of 64 kB. Note: this means the aggregate in-memory budget
	 * for per-task stores can exceed a single work_mem allocation when
	 * taskCount is large (e.g. 128 tasks × 64 kB = 8 MB floor). This is a
	 * deliberate trade-off: per-task stores spill to disk automatically
	 * once their portion of work_mem is exhausted, and the streaming merge
	 * adapter consumes them tuple-by-tuple, so the working set stays bounded.
	 * The temporary memory amplification is bounded and short-lived.
	 */
	int taskCount = list_length(taskList);
	Tuplestorestate **perTaskStores = palloc(taskCount * sizeof(Tuplestorestate *));
	int perTaskWorkMem = Max(work_mem / Max(taskCount, 1), 64);

	ereport(DEBUG2,
			(errmsg("sorted merge: per-task work_mem %d kB × %d tasks "
					"(aggregate floor %d kB), session work_mem %d kB",
					perTaskWorkMem, taskCount,
					perTaskWorkMem * taskCount, work_mem)));

	TupleDestinationStats *sharedStats = palloc0(sizeof(TupleDestinationStats));

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

	/*
	 * Build the streaming merge adapter now, before execution starts. The
	 * adapter caches the per-task store array and the heap/slot/sort-key
	 * scaffolding; lazy seeding (reading the first tuple from each store)
	 * happens on the first fetch from SortedMergeAdapterNext, after the
	 * workers have filled the stores.
	 *
	 * Build SortSupport directly from the worker query's sort clause —
	 * each SortGroupClause maps onto one SortSupportData entry without
	 * needing an intermediate type. Attribute numbers come from the
	 * worker target list and align with the 1-based non-junk ordering of
	 * the per-task tuplestore output.
	 */
	int nkeys = list_length(workerJob->jobQuery->sortClause);

	/*
	 * The plan-time eligibility gate guarantees we have at least one sort
	 * key when sorted merge is active. Defend against a corrupted plan in
	 * release builds: a zero-key adapter would crash later in
	 * PrepareSortSupportFromOrderingOp / ApplySortComparator.
	 */
	if (nkeys == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("sorted merge: worker query has no sort keys"),
				 errhint("This is an internal Citus invariant violation. "
						 "Disable citus.enable_sorted_merge as a workaround.")));
	}

	SortSupportData *sortKeys = palloc0(nkeys * sizeof(SortSupportData));
	int k = 0;
	SortGroupClause *sgc = NULL;
	foreach_declared_ptr(sgc, workerJob->jobQuery->sortClause)
	{
		TargetEntry *tle =
			get_sortgroupclause_tle(sgc, workerJob->jobQuery->targetList);
		SortSupport sk = &sortKeys[k];
		sk->ssup_cxt = CurrentMemoryContext;
		sk->ssup_collation = exprCollation((Node *) tle->expr);
		sk->ssup_nulls_first = sgc->nulls_first;
		sk->ssup_attno = tle->resno;
		PrepareSortSupportFromOrderingOp(sgc->sortop, sk);
		k++;
	}

	scanState->mergeAdapter = CreateSortedMergeAdapter(perTaskStores,
													   taskCount,
													   sortKeys,
													   nkeys,
													   tupleDesc,
													   true);
}


/*
 * ClearPerTaskDispatchDests resets task->tupleDest to NULL for every task
 * in the scan's plan task list. Used to scrub execution-local pointers off
 * the canonical (cached) task list at the start and end of every
 * AdaptiveExecutor() invocation, so that re-execution of a cached prepared
 * plan never sees a stale pointer into a freed memory context.
 *
 * Safe to call on tasks that already have tupleDest == NULL.
 */
void
ClearPerTaskDispatchDests(CitusScanState *scanState)
{
	List *taskList = scanState->distributedPlan->workerJob->taskList;
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
 * When all sort keys compare equal, ties are broken by store (slot) index so
 * that the relative order of equal-key rows is stable within a single
 * execution.
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

	return (slot1 <= slot2) ? 1 : -1;
}


/*
 * CreateSortedMergeAdapter builds a streaming merge adapter over K per-task
 * stores using a caller-supplied array of fully-initialized SortSupportData
 * keys (one per sort column). When ownsStores is true, FreeSortedMergeAdapter()
 * will call tuplestore_end() on each per-task store; when false, the caller
 * retains ownership and must free them separately.
 *
 * All memory is allocated in CurrentMemoryContext. The caller must ensure
 * this context outlives the adapter (the AdaptiveExecutor local context
 * already satisfies this — see adaptive_executor.c). The sortKeys array is
 * adopted by the adapter and freed alongside it.
 */
SortedMergeAdapter *
CreateSortedMergeAdapter(Tuplestorestate **perTaskStores,
						 int nstores,
						 SortSupportData *sortKeys,
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

	/* set up embedded merge context for heap comparisons */
	adapter->mergeCtx.slots = slots;
	adapter->mergeCtx.sortKeys = sortKeys;
	adapter->mergeCtx.nkeys = nkeys;

	/* allocate heap with embedded context as comparator arg */
	adapter->heap = binaryheap_allocate(nstores, MergeHeapComparator,
										&adapter->mergeCtx);

	if (nstores == 0)
	{
		adapter->initialized = true;
		adapter->exhausted = true;
	}

	return adapter;
}


/*
 * SortedMergeAdapterNext returns the next globally-sorted tuple from the
 * adapter. The returned slot is adapter-owned and must be treated as read-only
 * by callers. Returns NULL if all stores are exhausted.
 *
 * The heap uses per-store comparison slots (mergeCtx.slots). After
 * identifying the winner, the slot is returned directly to avoid a per-tuple
 * MinimalTuple copy in the streaming path.
 *
 * On the first call we seed the heap with the first tuple from each store.
 * On subsequent calls we advance the previous winner's store and update the
 * heap before selecting the new winner. This matches the MergeAppend pattern
 * in nodeMergeAppend.c.
 */
TupleTableSlot *
SortedMergeAdapterNext(SortedMergeAdapter *adapter)
{
	if (adapter->exhausted)
	{
		return NULL;
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
		/* advance the store whose tuple won the previous call */
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
	}

	if (binaryheap_empty(adapter->heap))
	{
		adapter->exhausted = true;
		return NULL;
	}

	/* return the current heap winner's slot (adapter-owned, read-only) */
	int winner = DatumGetInt32(binaryheap_first(adapter->heap));
	return adapter->mergeCtx.slots[winner];
}


/*
 * SortedMergeAdapterRescan resets the adapter to re-read from the beginning.
 * Called from CitusReScan() for cursor WITH HOLD patterns.
 *
 * We clear the heap and mark the adapter as uninitialized. On the next
 * SortedMergeAdapterNext() call, the if-branch will rewind each per-task
 * store, seed the heap with the first tuple from each, and return the
 * global winner. This avoids the off-by-one bug that would occur if we
 * seeded here and set initialized=true: the next Next() call would enter
 * the else-branch and advance the seeded winner before returning it,
 * silently dropping the first globally-minimal tuple after every rescan.
 */
void
SortedMergeAdapterRescan(SortedMergeAdapter *adapter)
{
	ereport(DEBUG2,
			(errmsg("sorted merge: SortedMergeAdapterRescan invoked over %d stores",
					adapter->nstores)));

	binaryheap_reset(adapter->heap);

	/*
	 * Let the next SortedMergeAdapterNext() reseed the heap. For the
	 * degenerate zero-store case there is nothing to seed and the adapter
	 * stays exhausted.
	 */
	adapter->initialized = (adapter->nstores == 0);
	adapter->exhausted = (adapter->nstores == 0);
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
