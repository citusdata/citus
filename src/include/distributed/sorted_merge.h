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

#include "distributed/multi_physical_planner.h"
#include "distributed/tuple_destination.h"


/* opaque streaming merge adapter — full definition in sorted_merge.c */
typedef struct SortedMergeAdapter SortedMergeAdapter;


extern TupleDestination * CreatePerTaskDispatchDest(List *taskList,
													TupleDesc tupleDesc,
													TupleDestinationStats *sharedStats,
													Tuplestorestate ***perTaskStoresOut,
													int *perTaskStoreCountOut);

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

#endif /* SORTED_MERGE_H */
