/*-------------------------------------------------------------------------
 *
 * blackhole_am.c
 *	  blackhole table access method code
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  Copied from https://github.com/michaelpq/pg_plugins/blob/master/blackhole_am/blackhole_am.c
 *
 *
 * NOTES
 *	  This file introduces the table access method blackhole, which can
 *	  be used as a template for other table access methods, and guarantees
 *	  that any data inserted into it gets sent to the void.
 *
 *-------------------------------------------------------------------------
 */

/* *INDENT-OFF* */
#include "postgres.h"

#if PG_VERSION_NUM >= 120000

#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"

PG_FUNCTION_INFO_V1(blackhole_am_handler);

/* Base structures for scans */
typedef struct BlackholeScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* Add more fields here as needed by the AM. */
} BlackholeScanDescData;
typedef struct BlackholeScanDescData *BlackholeScanDesc;

static const TableAmRoutine blackhole_methods;


/* ------------------------------------------------------------------------
 * Slot related callbacks for blackhole AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
blackhole_slot_callbacks(Relation relation)
{
	/*
	 * Here you would most likely want to invent your own set of
	 * slot callbacks for your AM.
	 */
	return &TTSOpsMinimalTuple;
}

/* ------------------------------------------------------------------------
 * Table Scan Callbacks for blackhole AM
 * ------------------------------------------------------------------------
 */

static TableScanDesc
blackhole_scan_begin(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 ParallelTableScanDesc parallel_scan,
					 uint32 flags)
{
	BlackholeScanDesc	scan;

	scan = (BlackholeScanDesc) palloc(sizeof(BlackholeScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	return (TableScanDesc) scan;                                                                                                                                           }

static void
blackhole_scan_end(TableScanDesc sscan)
{
	BlackholeScanDesc scan = (BlackholeScanDesc) sscan;
	pfree(scan);
}

static void
blackhole_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
					  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	/* nothing to do */
}

static bool
blackhole_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
						   TupleTableSlot *slot)
{
	/* nothing to do */
	return false;
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for blackhole AM
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *
blackhole_index_fetch_begin(Relation rel)
{
	return NULL;
}

static void
blackhole_index_fetch_reset(IndexFetchTableData *scan)
{
	/* nothing to do here */
}

static void
blackhole_index_fetch_end(IndexFetchTableData *scan)
{
	/* nothing to do here */
}

static bool
blackhole_index_fetch_tuple(struct IndexFetchTableData *scan,
							ItemPointer tid,
							Snapshot snapshot,
							TupleTableSlot *slot,
							bool *call_again, bool *all_dead)
{
	/* there is no data */
	return 0;
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * blackhole AM.
 * ------------------------------------------------------------------------
 */

static bool
blackhole_fetch_row_version(Relation relation,
							ItemPointer tid,
							Snapshot snapshot,
							TupleTableSlot *slot)
{
	/* nothing to do */
	return false;
}

static void
blackhole_get_latest_tid(TableScanDesc sscan,
						 ItemPointer tid)
{
	/* nothing to do */
}

static bool
blackhole_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static bool
blackhole_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	return false;
}

static TransactionId
blackhole_compute_xid_horizon_for_tuples(Relation rel,
										 ItemPointerData *tids,
										 int nitems)
{
	return InvalidTransactionId;
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for blackhole AM.
 * ----------------------------------------------------------------------------
 */

static void
blackhole_tuple_insert(Relation relation, TupleTableSlot *slot,
					   CommandId cid, int options, BulkInsertState bistate)
{
	/* nothing to do */
}

static void
blackhole_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								   CommandId cid, int options,
								   BulkInsertState bistate,
								   uint32 specToken)
{
	/* nothing to do */
}

static void
blackhole_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
									 uint32 spekToken, bool succeeded)
{
	/* nothing to do */
}

static void
blackhole_multi_insert(Relation relation, TupleTableSlot **slots,
					   int ntuples, CommandId cid, int options,
					   BulkInsertState bistate)
{
	/* nothing to do */
}

static TM_Result
blackhole_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					   Snapshot snapshot, Snapshot crosscheck, bool wait,
					   TM_FailureData *tmfd, bool changingPart)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}


static TM_Result
blackhole_tuple_update(Relation relation, ItemPointer otid,
					   TupleTableSlot *slot, CommandId cid,
					   Snapshot snapshot, Snapshot crosscheck,
					   bool wait, TM_FailureData *tmfd,
					   LockTupleMode *lockmode, bool *update_indexes)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static TM_Result
blackhole_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					 LockWaitPolicy wait_policy, uint8 flags,
					 TM_FailureData *tmfd)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static void
blackhole_finish_bulk_insert(Relation relation, int options)
{
	/* nothing to do */
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for blackhole AM.
 * ------------------------------------------------------------------------
 */

static void
blackhole_relation_set_new_filenode(Relation rel,
									const RelFileNode *newrnode,
									char persistence,
									TransactionId *freezeXid,
									MultiXactId *minmulti)
{
	/* nothing to do */
}

static void
blackhole_relation_nontransactional_truncate(Relation rel)
{
	/* nothing to do */
}

static void
blackhole_copy_data(Relation rel, const RelFileNode *newrnode)
{
	/* there is no data */
}

static void
blackhole_copy_for_cluster(Relation OldTable, Relation NewTable,
						   Relation OldIndex, bool use_sort,
						   TransactionId OldestXmin,
						   TransactionId *xid_cutoff,
						   MultiXactId *multi_cutoff,
						   double *num_tuples,
						   double *tups_vacuumed,
						   double *tups_recently_dead)
{
	/* no data, so nothing to do */
}

static void
blackhole_vacuum(Relation onerel, VacuumParams *params,
				 BufferAccessStrategy bstrategy)
{
	/* no data, so nothing to do */
}

static bool
blackhole_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								  BufferAccessStrategy bstrategy)
{
	/* no data, so no point to analyze next block */
	return false;
}

static bool
blackhole_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								  double *liverows, double *deadrows,
								  TupleTableSlot *slot)
{
	/* no data, so no point to analyze next tuple */
	return false;
}

static double
blackhole_index_build_range_scan(Relation tableRelation,
								 Relation indexRelation,
								 IndexInfo *indexInfo,
								 bool allow_sync,
								 bool anyvisible,
								 bool progress,
								 BlockNumber start_blockno,
								 BlockNumber numblocks,
								 IndexBuildCallback callback,
								 void *callback_state,
								 TableScanDesc scan)
{
	/* no data, so no tuples */
	return 0;
}

static void
blackhole_index_validate_scan(Relation tableRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  Snapshot snapshot,
							  ValidateIndexState *state)
{
	/* nothing to do */
}


/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the blackhole AM
 * ------------------------------------------------------------------------
 */

static uint64
blackhole_relation_size(Relation rel, ForkNumber forkNumber)
{
	/* there is nothing */
	return 0;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
blackhole_relation_needs_toast_table(Relation rel)
{
	/* no data, so no toast table needed */
	return false;
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the blackhole AM
 * ------------------------------------------------------------------------
 */

static void
blackhole_estimate_rel_size(Relation rel, int32 *attr_widths,
							BlockNumber *pages, double *tuples,
							double *allvisfrac)
{
	/* no data available */
	*attr_widths = 0;
	*tuples = 0;
	*allvisfrac = 0;
	*pages = 0;
}


/* ------------------------------------------------------------------------
 * Executor related callbacks for the blackhole AM
 * ------------------------------------------------------------------------
 */

static bool
blackhole_scan_bitmap_next_block(TableScanDesc scan,
								 TBMIterateResult *tbmres)
{
	/* no data, so no point to scan next block */
	return false;
}

static bool
blackhole_scan_bitmap_next_tuple(TableScanDesc scan,
								 TBMIterateResult *tbmres,
								 TupleTableSlot *slot)
{
	/* no data, so no point to scan next tuple */
	return false;
}

static bool
blackhole_scan_sample_next_block(TableScanDesc scan,
								 SampleScanState *scanstate)
{
	/* no data, so no point to scan next block for sampling */
	return false;
}

static bool
blackhole_scan_sample_next_tuple(TableScanDesc scan,
								 SampleScanState *scanstate,
								 TupleTableSlot *slot)
{
	/* no data, so no point to scan next tuple for sampling */
	return false;
}


/* ------------------------------------------------------------------------
 * Definition of the blackhole table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine blackhole_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = blackhole_slot_callbacks,

	.scan_begin = blackhole_scan_begin,
	.scan_end = blackhole_scan_end,
	.scan_rescan = blackhole_scan_rescan,
	.scan_getnextslot = blackhole_scan_getnextslot,

	/* these are common helper functions */
	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = blackhole_index_fetch_begin,
	.index_fetch_reset = blackhole_index_fetch_reset,
	.index_fetch_end = blackhole_index_fetch_end,
	.index_fetch_tuple = blackhole_index_fetch_tuple,

	.tuple_insert = blackhole_tuple_insert,
	.tuple_insert_speculative = blackhole_tuple_insert_speculative,
	.tuple_complete_speculative = blackhole_tuple_complete_speculative,
	.multi_insert = blackhole_multi_insert,
	.tuple_delete = blackhole_tuple_delete,
	.tuple_update = blackhole_tuple_update,
	.tuple_lock = blackhole_tuple_lock,
	.finish_bulk_insert = blackhole_finish_bulk_insert,

	.tuple_fetch_row_version = blackhole_fetch_row_version,
	.tuple_get_latest_tid = blackhole_get_latest_tid,
	.tuple_tid_valid = blackhole_tuple_tid_valid,
	.tuple_satisfies_snapshot = blackhole_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = blackhole_compute_xid_horizon_for_tuples,

	.relation_set_new_filenode = blackhole_relation_set_new_filenode,
	.relation_nontransactional_truncate = blackhole_relation_nontransactional_truncate,
	.relation_copy_data = blackhole_copy_data,
	.relation_copy_for_cluster = blackhole_copy_for_cluster,
	.relation_vacuum = blackhole_vacuum,
	.scan_analyze_next_block = blackhole_scan_analyze_next_block,
	.scan_analyze_next_tuple = blackhole_scan_analyze_next_tuple,
	.index_build_range_scan = blackhole_index_build_range_scan,
	.index_validate_scan = blackhole_index_validate_scan,

	.relation_size = blackhole_relation_size,
	.relation_needs_toast_table = blackhole_relation_needs_toast_table,

	.relation_estimate_size = blackhole_estimate_rel_size,

	.scan_bitmap_next_block = blackhole_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = blackhole_scan_bitmap_next_tuple,
	.scan_sample_next_block = blackhole_scan_sample_next_block,
	.scan_sample_next_tuple = blackhole_scan_sample_next_tuple
};


Datum
blackhole_am_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&blackhole_methods);
}

#endif
