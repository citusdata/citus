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
#include "utils/sortsupport.h"
#include "utils/tuplestore.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/tuple_destination.h"


/* opaque streaming merge adapter — full definition in sorted_merge.c */
typedef struct SortedMergeAdapter SortedMergeAdapter;


extern void CreatePerTaskDispatchDests(CitusScanState *scanState);
extern void ClearPerTaskDispatchDests(CitusScanState *scanState);

extern SortedMergeAdapter * CreateSortedMergeAdapter(Tuplestorestate **perTaskStores,
													 int nstores,
													 SortSupportData *sortKeys,
													 int nkeys,
													 TupleDesc tupleDesc,
													 bool ownsStores);
extern TupleTableSlot * SortedMergeAdapterNext(SortedMergeAdapter *adapter);
extern void SortedMergeAdapterRescan(SortedMergeAdapter *adapter);
extern void FreeSortedMergeAdapter(SortedMergeAdapter *adapter);

#endif /* SORTED_MERGE_H */
