/*-------------------------------------------------------------------------
 *
 * sorted_merge.h
 *	  Declarations for coordinator-side sorted merge of pre-sorted
 *	  worker results using a binary heap.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef SORTED_MERGE_H
#define SORTED_MERGE_H

#include "access/tupdesc.h"
#include "utils/tuplestore.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/tuple_destination.h"


/* opaque streaming merge adapter — full definition in sorted_merge.c */
typedef struct SortedMergeAdapter SortedMergeAdapter;


/*
 * SortedMergeKey describes one sort key for the coordinator-side
 * k-way merge of pre-sorted worker results. The executor uses these
 * to build SortSupport structures for the merge.
 */
typedef struct SortedMergeKey
{
	AttrNumber attno;       /* 1-based attribute in the worker output */
	Oid sortop;             /* ordering operator OID */
	Oid collation;          /* collation OID */
	bool nullsFirst;        /* NULLS FIRST? */
} SortedMergeKey;


extern SortedMergeKey * BuildSortedMergeKeys(List *sortClauseList,
											 List *targetList, int *nkeys);

extern void AssignPerTaskDispatchDests(List *taskList,
									   TupleDesc tupleDesc,
									   TupleDestinationStats *sharedStats,
									   Tuplestorestate ***perTaskStoresOut,
									   int *perTaskStoreCountOut);
extern void ClearPerTaskDispatchDests(List *taskList);

extern void MergePerTaskStoresIntoFinalStore(Tuplestorestate *finalStore,
											 Tuplestorestate **perTaskStores,
											 int nstores,
											 SortedMergeKey *mergeKeys,
											 int nkeys,
											 TupleDesc tupleDesc);

extern SortedMergeAdapter * CreateSortedMergeAdapter(Tuplestorestate **perTaskStores,
													 int nstores,
													 SortedMergeKey *mergeKeys,
													 int nkeys,
													 TupleDesc tupleDesc,
													 bool ownsStores);
extern bool SortedMergeAdapterNext(SortedMergeAdapter *adapter,
								   TupleTableSlot *scanSlot);
extern void SortedMergeAdapterRescan(SortedMergeAdapter *adapter);
extern void FreeSortedMergeAdapter(SortedMergeAdapter *adapter);

/*
 * FinalizeSortedMerge performs the post-execution k-way merge of pre-sorted
 * per-task worker results. It encapsulates both the streaming-adapter and
 * eager-tuplestore modes behind a single entry point so the executor only
 * needs one call site instead of branching on the merge mode inline.
 *
 * In streaming mode (citus.enable_streaming_sorted_merge = on) this attaches
 * a SortedMergeAdapter to scanState->mergeAdapter; the per-task stores are
 * owned by the adapter. In eager mode it creates scanState->tuplestorestate,
 * merges all tuples into it, and frees the per-task stores.
 *
 * No-op if perTaskStoreCount == 0 (e.g. no remote tasks executed).
 */
extern void FinalizeSortedMerge(CitusScanState *scanState,
								Job *workerJob,
								Tuplestorestate **perTaskStores,
								int perTaskStoreCount,
								TupleDesc tupleDescriptor);

#endif /* SORTED_MERGE_H */
