/*-------------------------------------------------------------------------
 *
 * fake_am.c
 *	  fake table access method code
 *
 * Copyright (c) Citus Data, Inc.
 *
 * IDENTIFICATION
 *	  Based on https://github.com/michaelpq/pg_plugins/blob/master/blackhole_am/blackhole_am.c
 *
 *
 * NOTES
 *	  This file introduces the table access method "fake", which delegates
 *    bare minimum functionality for testing to heapam to provide an append
 *    only access method, and doesn't implement rest of the functionality.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"
#include "pg_version_compat.h"


#include "access/amapi.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "storage/smgr.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(fake_am_handler);

static const TableAmRoutine fake_methods;


/* ------------------------------------------------------------------------
 * Slot related callbacks for fake AM
 * ------------------------------------------------------------------------
 */
static const TupleTableSlotOps *
fake_slot_callbacks(Relation relation)
{
	return &TTSOpsBufferHeapTuple;
}


/* ------------------------------------------------------------------------
 * Table Scan Callbacks for fake AM
 * ------------------------------------------------------------------------
 */
static TableScanDesc
fake_scan_begin(Relation relation, Snapshot snapshot,
				int nkeys, ScanKey key,
				ParallelTableScanDesc parallel_scan,
				uint32 flags)
{
	return heap_beginscan(relation, snapshot, nkeys, key, parallel_scan, flags);
}


static void
fake_scan_end(TableScanDesc sscan)
{
	heap_endscan(sscan);
}


static void
fake_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				 bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	heap_rescan(sscan, key, set_params, allow_strat, allow_sync, allow_pagemode);
}


static bool
fake_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
					  TupleTableSlot *slot)
{
	ereport(WARNING, (errmsg("fake_scan_getnextslot")));
	return heap_getnextslot(sscan, direction, slot);
}


/* ------------------------------------------------------------------------
 * Index Scan Callbacks for fake AM
 * ------------------------------------------------------------------------
 */
static IndexFetchTableData *
fake_index_fetch_begin(Relation rel)
{
	elog(ERROR, "fake_index_fetch_begin not implemented");
	return NULL;
}


static void
fake_index_fetch_reset(IndexFetchTableData *scan)
{
	elog(ERROR, "fake_index_fetch_reset not implemented");
}


static void
fake_index_fetch_end(IndexFetchTableData *scan)
{
	elog(ERROR, "fake_index_fetch_end not implemented");
}


static bool
fake_index_fetch_tuple(struct IndexFetchTableData *scan,
					   ItemPointer tid,
					   Snapshot snapshot,
					   TupleTableSlot *slot,
					   bool *call_again, bool *all_dead)
{
	elog(ERROR, "fake_index_fetch_tuple not implemented");
	return false;
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * fake AM.
 * ------------------------------------------------------------------------
 */
static bool
fake_fetch_row_version(Relation relation,
					   ItemPointer tid,
					   Snapshot snapshot,
					   TupleTableSlot *slot)
{
	elog(ERROR, "fake_fetch_row_version not implemented");
	return false;
}


static void
fake_get_latest_tid(TableScanDesc sscan,
					ItemPointer tid)
{
	elog(ERROR, "fake_get_latest_tid not implemented");
}


static bool
fake_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "fake_tuple_tid_valid not implemented");
	return false;
}


static bool
fake_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
							  Snapshot snapshot)
{
	elog(ERROR, "fake_tuple_satisfies_snapshot not implemented");
	return false;
}


#if PG_VERSION_NUM >= PG_VERSION_14
static TransactionId
fake_index_delete_tuples(Relation rel,
						 TM_IndexDeleteOp *delstate)
{
	elog(ERROR, "fake_index_delete_tuples not implemented");
	return InvalidTransactionId;
}


#else
static TransactionId
fake_compute_xid_horizon_for_tuples(Relation rel,
									ItemPointerData *tids,
									int nitems)
{
	elog(ERROR, "fake_compute_xid_horizon_for_tuples not implemented");
	return InvalidTransactionId;
}


#endif


/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for fake AM.
 * ----------------------------------------------------------------------------
 */
static void
fake_tuple_insert(Relation relation, TupleTableSlot *slot,
				  CommandId cid, int options, BulkInsertState bistate)
{
	ereport(WARNING, (errmsg("fake_tuple_insert")));

	/*
	 * Code below this point is from heapam_tuple_insert from
	 * heapam_handler.c
	 */

	bool shouldFree = true;
	HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	/* Perform the insertion, and copy the resulting ItemPointer */
	heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (shouldFree)
	{
		pfree(tuple);
	}
}


static void
fake_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
							  CommandId cid, int options,
							  BulkInsertState bistate,
							  uint32 specToken)
{
	elog(ERROR, "fake_tuple_insert_speculative not implemented");
}


static void
fake_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								uint32 spekToken, bool succeeded)
{
	elog(ERROR, "fake_tuple_complete_speculative not implemented");
}


static void
fake_multi_insert(Relation relation, TupleTableSlot **slots,
				  int ntuples, CommandId cid, int options,
				  BulkInsertState bistate)
{
	ereport(WARNING, (errmsg("fake_multi_insert")));

	heap_multi_insert(relation, slots, ntuples, cid, options, bistate);
}


static TM_Result
fake_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
				  Snapshot snapshot, Snapshot crosscheck, bool wait,
				  TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "fake_tuple_delete not implemented");
}


static TM_Result
fake_tuple_update(Relation relation, ItemPointer otid,
				  TupleTableSlot *slot, CommandId cid,
				  Snapshot snapshot, Snapshot crosscheck,
				  bool wait, TM_FailureData *tmfd,
				  LockTupleMode *lockmode, bool *update_indexes)
{
	elog(ERROR, "fake_tuple_update not implemented");
}


static TM_Result
fake_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				LockWaitPolicy wait_policy, uint8 flags,
				TM_FailureData *tmfd)
{
	elog(ERROR, "fake_tuple_lock not implemented");
}


static void
fake_finish_bulk_insert(Relation relation, int options)
{
	/* nothing to do here */
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for fake AM.
 * ------------------------------------------------------------------------
 */
static void
fake_relation_set_new_filenode(Relation rel,
							   const RelFileNode *newrnode,
							   char persistence,
							   TransactionId *freezeXid,
							   MultiXactId *minmulti)
{
	/*
	 * Code below is copied from heapam_relation_set_new_filenode in
	 * heapam_handler.c.
	 */


	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();

	SMgrRelation srel = RelationCreateStorage(*newrnode, persistence);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	smgrclose(srel);
}


static void
fake_relation_nontransactional_truncate(Relation rel)
{
	elog(ERROR, "fake_relation_nontransactional_truncate not implemented");
}


static void
fake_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "fake_copy_data not implemented");
}


static void
fake_copy_for_cluster(Relation OldTable, Relation NewTable,
					  Relation OldIndex, bool use_sort,
					  TransactionId OldestXmin,
					  TransactionId *xid_cutoff,
					  MultiXactId *multi_cutoff,
					  double *num_tuples,
					  double *tups_vacuumed,
					  double *tups_recently_dead)
{
	elog(ERROR, "fake_copy_for_cluster not implemented");
}


static void
fake_vacuum(Relation onerel, VacuumParams *params,
			BufferAccessStrategy bstrategy)
{
	elog(WARNING, "fake_copy_for_cluster not implemented");
}


static bool
fake_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
							 BufferAccessStrategy bstrategy)
{
	/* we don't support analyze, so return false */
	return false;
}


static bool
fake_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
							 double *liverows, double *deadrows,
							 TupleTableSlot *slot)
{
	elog(ERROR, "fake_scan_analyze_next_tuple not implemented");
}


static double
fake_index_build_range_scan(Relation tableRelation,
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
	elog(ERROR, "fake_index_build_range_scan not implemented");
}


static void
fake_index_validate_scan(Relation tableRelation,
						 Relation indexRelation,
						 IndexInfo *indexInfo,
						 Snapshot snapshot,
						 ValidateIndexState *state)
{
	elog(ERROR, "fake_index_build_range_scan not implemented");
}


/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the fake AM
 * ------------------------------------------------------------------------
 */
static uint64
fake_relation_size(Relation rel, ForkNumber forkNumber)
{
	/*
	 * Code below is copied from heapam_relation_size from
	 * heapam_handler.c.
	 */

	uint64 nblocks = 0;

	/* InvalidForkNumber indicates returning the size for all forks */
	if (forkNumber == InvalidForkNumber)
	{
		for (int i = 0; i < MAX_FORKNUM; i++)
		{
			nblocks += smgrnblocks(RelationGetSmgr(rel), i);
		}
	}
	else
	{
		nblocks = smgrnblocks(RelationGetSmgr(rel), forkNumber);
	}

	return nblocks * BLCKSZ;
}


/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
fake_relation_needs_toast_table(Relation rel)
{
	/* we don't test toastable data with this, so no toast table needed */
	return false;
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the fake AM
 * ------------------------------------------------------------------------
 */
static void
fake_estimate_rel_size(Relation rel, int32 *attr_widths,
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
 * Executor related callbacks for the fake AM
 * ------------------------------------------------------------------------
 */
static bool
fake_scan_bitmap_next_block(TableScanDesc scan,
							TBMIterateResult *tbmres)
{
	elog(ERROR, "fake_scan_bitmap_next_block not implemented");
}


static bool
fake_scan_bitmap_next_tuple(TableScanDesc scan,
							TBMIterateResult *tbmres,
							TupleTableSlot *slot)
{
	elog(ERROR, "fake_scan_bitmap_next_tuple not implemented");
}


static bool
fake_scan_sample_next_block(TableScanDesc scan,
							SampleScanState *scanstate)
{
	elog(ERROR, "fake_scan_sample_next_block not implemented");
}


static bool
fake_scan_sample_next_tuple(TableScanDesc scan,
							SampleScanState *scanstate,
							TupleTableSlot *slot)
{
	elog(ERROR, "fake_scan_sample_next_tuple not implemented");
}


/* ------------------------------------------------------------------------
 * Definition of the fake table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine fake_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = fake_slot_callbacks,

	.scan_begin = fake_scan_begin,
	.scan_end = fake_scan_end,
	.scan_rescan = fake_scan_rescan,
	.scan_getnextslot = fake_scan_getnextslot,

	/* these are common helper functions */
	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = fake_index_fetch_begin,
	.index_fetch_reset = fake_index_fetch_reset,
	.index_fetch_end = fake_index_fetch_end,
	.index_fetch_tuple = fake_index_fetch_tuple,

	.tuple_insert = fake_tuple_insert,
	.tuple_insert_speculative = fake_tuple_insert_speculative,
	.tuple_complete_speculative = fake_tuple_complete_speculative,
	.multi_insert = fake_multi_insert,
	.tuple_delete = fake_tuple_delete,
	.tuple_update = fake_tuple_update,
	.tuple_lock = fake_tuple_lock,
	.finish_bulk_insert = fake_finish_bulk_insert,

	.tuple_fetch_row_version = fake_fetch_row_version,
	.tuple_get_latest_tid = fake_get_latest_tid,
	.tuple_tid_valid = fake_tuple_tid_valid,
	.tuple_satisfies_snapshot = fake_tuple_satisfies_snapshot,
#if PG_VERSION_NUM >= PG_VERSION_14
	.index_delete_tuples = fake_index_delete_tuples,
#else
	.compute_xid_horizon_for_tuples = fake_compute_xid_horizon_for_tuples,
#endif

	.relation_set_new_filenode = fake_relation_set_new_filenode,
	.relation_nontransactional_truncate = fake_relation_nontransactional_truncate,
	.relation_copy_data = fake_copy_data,
	.relation_copy_for_cluster = fake_copy_for_cluster,
	.relation_vacuum = fake_vacuum,
	.scan_analyze_next_block = fake_scan_analyze_next_block,
	.scan_analyze_next_tuple = fake_scan_analyze_next_tuple,
	.index_build_range_scan = fake_index_build_range_scan,
	.index_validate_scan = fake_index_validate_scan,

	.relation_size = fake_relation_size,
	.relation_needs_toast_table = fake_relation_needs_toast_table,

	.relation_estimate_size = fake_estimate_rel_size,

	.scan_bitmap_next_block = fake_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = fake_scan_bitmap_next_tuple,
	.scan_sample_next_block = fake_scan_sample_next_block,
	.scan_sample_next_tuple = fake_scan_sample_next_tuple
};


Datum
fake_am_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&fake_methods);
}
