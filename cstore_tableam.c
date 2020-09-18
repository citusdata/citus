#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "cstore.h"
#include "cstore_tableam.h"

typedef struct CStoreScanDescData
{
	TableScanDescData cs_base;
	TableReadState *cs_readState;
} CStoreScanDescData;

typedef struct CStoreScanDescData *CStoreScanDesc;

static TableWriteState *CStoreWriteState = NULL;
static ExecutorEnd_hook_type PreviousExecutorEndHook = NULL;
static MemoryContext CStoreContext = NULL;

static CStoreOptions *
CStoreTableAMGetOptions(void)
{
	CStoreOptions *cstoreOptions = palloc0(sizeof(CStoreOptions));
	cstoreOptions->compressionType = cstore_compression;
	cstoreOptions->stripeRowCount = cstore_stripe_row_count;
	cstoreOptions->blockRowCount = cstore_block_row_count;
	return cstoreOptions;
}


static MemoryContext
CStoreMemoryContext(void)
{
	if (CStoreContext == NULL)
	{
		CStoreContext = AllocSetContextCreate(TopMemoryContext, "cstore context",
											  ALLOCSET_DEFAULT_SIZES);
	}
	return CStoreContext;
}


static void
cstore_init_write_state(Relation relation)
{
	/*TODO: upgrade lock to serialize writes */

	if (CStoreWriteState != NULL)
	{
		/* TODO: consider whether it's possible for a new write to start */
		/* before an old one is flushed */
		Assert(CStoreWriteState->relation->rd_id == relation->rd_id);
	}

	if (CStoreWriteState == NULL)
	{
		CStoreOptions *cstoreOptions = CStoreTableAMGetOptions();
		TupleDesc tupdesc = RelationGetDescr(relation);

		elog(LOG, "initializing write state for relation %d", relation->rd_id);
		CStoreWriteState = CStoreBeginWrite(relation->rd_id,
											cstoreOptions->compressionType,
											cstoreOptions->stripeRowCount,
											cstoreOptions->blockRowCount,
											tupdesc);

		CStoreWriteState->relation = relation;
	}
}


void
cstore_free_write_state()
{
	if (CStoreWriteState != NULL)
	{
		elog(LOG, "flushing write state for relation %d",
			 CStoreWriteState->relation->rd_id);
		CStoreEndWrite(CStoreWriteState);
		CStoreWriteState = NULL;
	}
}


static const TupleTableSlotOps *
cstore_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}


static TableScanDesc
cstore_beginscan(Relation relation, Snapshot snapshot,
				 int nkeys, ScanKey key,
				 ParallelTableScanDesc parallel_scan,
				 uint32 flags)
{
	Oid relid = relation->rd_id;
	TupleDesc tupdesc = relation->rd_att;
	CStoreOptions *cstoreOptions = NULL;
	TableReadState *readState = NULL;
	CStoreScanDesc scan = palloc(sizeof(CStoreScanDescData));
	List *columnList = NIL;
	MemoryContext oldContext = MemoryContextSwitchTo(CStoreMemoryContext());

	cstoreOptions = CStoreTableAMGetOptions();

	scan->cs_base.rs_rd = relation;
	scan->cs_base.rs_snapshot = snapshot;
	scan->cs_base.rs_nkeys = nkeys;
	scan->cs_base.rs_key = key;
	scan->cs_base.rs_flags = flags;
	scan->cs_base.rs_parallel = parallel_scan;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Index varno = 0;
		AttrNumber varattno = i + 1;
		Oid vartype = tupdesc->attrs[i].atttypid;
		int32 vartypmod = 0;
		Oid varcollid = 0;
		Index varlevelsup = 0;
		Var *var;

		if (tupdesc->attrs[i].attisdropped)
		{
			continue;
		}

		var = makeVar(varno, varattno, vartype, vartypmod,
					  varcollid, varlevelsup);
		columnList = lappend(columnList, var);
	}

	readState = CStoreBeginRead(relid, tupdesc, columnList, NULL);
	readState->relation = relation;

	scan->cs_readState = readState;

	MemoryContextSwitchTo(oldContext);
	return ((TableScanDesc) scan);
}


static void
cstore_endscan(TableScanDesc sscan)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;
	CStoreEndRead(scan->cs_readState);
}


static void
cstore_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
			  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	elog(ERROR, "cstore_rescan not implemented");
}


static bool
cstore_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;
	bool nextRowFound;
	MemoryContext oldContext = MemoryContextSwitchTo(CStoreMemoryContext());

	ExecClearTuple(slot);

	nextRowFound = CStoreReadNextRow(scan->cs_readState, slot->tts_values,
									 slot->tts_isnull);

	MemoryContextSwitchTo(oldContext);

	if (!nextRowFound)
	{
		return false;
	}

	ExecStoreVirtualTuple(slot);
	return true;
}


static Size
cstore_parallelscan_estimate(Relation rel)
{
	elog(ERROR, "cstore_parallelscan_estimate not implemented");
}


static Size
cstore_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "cstore_parallelscan_initialize not implemented");
}


static void
cstore_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "cstore_parallelscan_reinitialize not implemented");
}


static IndexFetchTableData *
cstore_index_fetch_begin(Relation rel)
{
	elog(ERROR, "cstore_index_fetch_begin not implemented");
}


static void
cstore_index_fetch_reset(IndexFetchTableData *scan)
{
	elog(ERROR, "cstore_index_fetch_reset not implemented");
}


static void
cstore_index_fetch_end(IndexFetchTableData *scan)
{
	elog(ERROR, "cstore_index_fetch_end not implemented");
}


static bool
cstore_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *call_again, bool *all_dead)
{
	elog(ERROR, "cstore_index_fetch_tuple not implemented");
}


static bool
cstore_fetch_row_version(Relation relation,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot)
{
	elog(ERROR, "cstore_fetch_row_version not implemented");
}


static void
cstore_get_latest_tid(TableScanDesc sscan,
					  ItemPointer tid)
{
	elog(ERROR, "cstore_get_latest_tid not implemented");
}


static bool
cstore_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "cstore_tuple_tid_valid not implemented");
}


static bool
cstore_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								Snapshot snapshot)
{
	return true;
}


static TransactionId
cstore_compute_xid_horizon_for_tuples(Relation rel,
									  ItemPointerData *tids,
									  int nitems)
{
	elog(ERROR, "cstore_compute_xid_horizon_for_tuples not implemented");
}


static void
cstore_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					int options, BulkInsertState bistate)
{
	HeapTuple heapTuple;
	MemoryContext oldContext = MemoryContextSwitchTo(CStoreMemoryContext());

	cstore_init_write_state(relation);

	heapTuple = ExecCopySlotHeapTuple(slot);
	if (HeapTupleHasExternal(heapTuple))
	{
		/* detoast any toasted attributes */
		HeapTuple newTuple = toast_flatten_tuple(heapTuple,
												 slot->tts_tupleDescriptor);

		ExecForceStoreHeapTuple(newTuple, slot, true);
	}

	slot_getallattrs(slot);

	CStoreWriteRow(CStoreWriteState, slot->tts_values, slot->tts_isnull);
	MemoryContextSwitchTo(oldContext);
}


static void
cstore_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								CommandId cid, int options,
								BulkInsertState bistate, uint32 specToken)
{
	elog(ERROR, "cstore_tuple_insert_speculative not implemented");
}


static void
cstore_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								  uint32 specToken, bool succeeded)
{
	elog(ERROR, "cstore_tuple_complete_speculative not implemented");
}


static void
cstore_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					CommandId cid, int options, BulkInsertState bistate)
{
	MemoryContext oldContext = MemoryContextSwitchTo(CStoreMemoryContext());

	cstore_init_write_state(relation);

	for (int i = 0; i < ntuples; i++)
	{
		TupleTableSlot *tupleSlot = slots[i];
		HeapTuple heapTuple = ExecCopySlotHeapTuple(tupleSlot);

		if (HeapTupleHasExternal(heapTuple))
		{
			/* detoast any toasted attributes */
			HeapTuple newTuple = toast_flatten_tuple(heapTuple,
													 tupleSlot->tts_tupleDescriptor);

			ExecForceStoreHeapTuple(newTuple, tupleSlot, true);
		}

		slot_getallattrs(tupleSlot);

		CStoreWriteRow(CStoreWriteState, tupleSlot->tts_values, tupleSlot->tts_isnull);
	}
	MemoryContextSwitchTo(oldContext);
}


static TM_Result
cstore_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					Snapshot snapshot, Snapshot crosscheck, bool wait,
					TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "cstore_tuple_delete not implemented");
}


static TM_Result
cstore_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd,
					LockTupleMode *lockmode, bool *update_indexes)
{
	elog(ERROR, "cstore_tuple_update not implemented");
}


static TM_Result
cstore_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				  LockWaitPolicy wait_policy, uint8 flags,
				  TM_FailureData *tmfd)
{
	elog(ERROR, "cstore_tuple_lock not implemented");
}


static void
cstore_finish_bulk_insert(Relation relation, int options)
{
	/*TODO: flush relation like for heap? */
	/* free write state or only in ExecutorEnd_hook? */

	/* for COPY */
	cstore_free_write_state();
}


static void
cstore_relation_set_new_filenode(Relation rel,
								 const RelFileNode *newrnode,
								 char persistence,
								 TransactionId *freezeXid,
								 MultiXactId *minmulti)
{
	SMgrRelation srel;

	Assert(persistence == RELPERSISTENCE_PERMANENT);
	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();
	srel = RelationCreateStorage(*newrnode, persistence);
	InitializeCStoreTableFile(rel->rd_id, rel, CStoreTableAMGetOptions());
	smgrclose(srel);
}


static void
cstore_relation_nontransactional_truncate(Relation rel)
{
	elog(ERROR, "cstore_relation_nontransactional_truncate not implemented");
}


static void
cstore_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "cstore_relation_copy_data not implemented");
}


static void
cstore_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								 Relation OldIndex, bool use_sort,
								 TransactionId OldestXmin,
								 TransactionId *xid_cutoff,
								 MultiXactId *multi_cutoff,
								 double *num_tuples,
								 double *tups_vacuumed,
								 double *tups_recently_dead)
{
	elog(ERROR, "cstore_relation_copy_for_cluster not implemented");
}


static bool
cstore_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
							   BufferAccessStrategy bstrategy)
{
	/* TODO */
	return false;
}


static bool
cstore_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
							   double *liverows, double *deadrows,
							   TupleTableSlot *slot)
{
	/* TODO */
	return false;
}


static double
cstore_index_build_range_scan(Relation heapRelation,
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
	elog(ERROR, "cstore_index_build_range_scan not implemented");
}


static void
cstore_index_validate_scan(Relation heapRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   Snapshot snapshot,
						   ValidateIndexState *state)
{
	elog(ERROR, "cstore_index_validate_scan not implemented");
}


static uint64
cstore_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 nblocks = 0;

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(rel);

	/* InvalidForkNumber indicates returning the size for all forks */
	if (forkNumber == InvalidForkNumber)
	{
		for (int i = 0; i < MAX_FORKNUM; i++)
		{
			nblocks += smgrnblocks(rel->rd_smgr, i);
		}
	}
	else
	{
		nblocks = smgrnblocks(rel->rd_smgr, forkNumber);
	}

	return nblocks * BLCKSZ;
}


static bool
cstore_relation_needs_toast_table(Relation rel)
{
	return false;
}


static void
cstore_estimate_rel_size(Relation rel, int32 *attr_widths,
						 BlockNumber *pages, double *tuples,
						 double *allvisfrac)
{
	/* TODO */
	*attr_widths = 12;
	*tuples = 100;
	*pages = 10;
	*allvisfrac = 1.0;
}


static bool
cstore_scan_bitmap_next_block(TableScanDesc scan,
							  TBMIterateResult *tbmres)
{
	elog(ERROR, "cstore_scan_bitmap_next_block not implemented");
}


static bool
cstore_scan_bitmap_next_tuple(TableScanDesc scan,
							  TBMIterateResult *tbmres,
							  TupleTableSlot *slot)
{
	elog(ERROR, "cstore_scan_bitmap_next_tuple not implemented");
}


static bool
cstore_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "cstore_scan_sample_next_block not implemented");
}


static bool
cstore_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
							  TupleTableSlot *slot)
{
	elog(ERROR, "cstore_scan_sample_next_tuple not implemented");
}


static void
CStoreExecutorEnd(QueryDesc *queryDesc)
{
	cstore_free_write_state();
	if (PreviousExecutorEndHook)
	{
		PreviousExecutorEndHook(queryDesc);
	}
	else
	{
		standard_ExecutorEnd(queryDesc);
	}
}


void
cstore_tableam_init()
{
	PreviousExecutorEndHook = ExecutorEnd_hook;
	ExecutorEnd_hook = CStoreExecutorEnd;
}


void
cstore_tableam_finish()
{
	ExecutorEnd_hook = PreviousExecutorEndHook;
}


static const TableAmRoutine cstore_am_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = cstore_slot_callbacks,

	.scan_begin = cstore_beginscan,
	.scan_end = cstore_endscan,
	.scan_rescan = cstore_rescan,
	.scan_getnextslot = cstore_getnextslot,

	.parallelscan_estimate = cstore_parallelscan_estimate,
	.parallelscan_initialize = cstore_parallelscan_initialize,
	.parallelscan_reinitialize = cstore_parallelscan_reinitialize,

	.index_fetch_begin = cstore_index_fetch_begin,
	.index_fetch_reset = cstore_index_fetch_reset,
	.index_fetch_end = cstore_index_fetch_end,
	.index_fetch_tuple = cstore_index_fetch_tuple,

	.tuple_fetch_row_version = cstore_fetch_row_version,
	.tuple_get_latest_tid = cstore_get_latest_tid,
	.tuple_tid_valid = cstore_tuple_tid_valid,
	.tuple_satisfies_snapshot = cstore_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = cstore_compute_xid_horizon_for_tuples,

	.tuple_insert = cstore_tuple_insert,
	.tuple_insert_speculative = cstore_tuple_insert_speculative,
	.tuple_complete_speculative = cstore_tuple_complete_speculative,
	.multi_insert = cstore_multi_insert,
	.tuple_delete = cstore_tuple_delete,
	.tuple_update = cstore_tuple_update,
	.tuple_lock = cstore_tuple_lock,
	.finish_bulk_insert = cstore_finish_bulk_insert,

	.relation_set_new_filenode = cstore_relation_set_new_filenode,
	.relation_nontransactional_truncate = cstore_relation_nontransactional_truncate,
	.relation_copy_data = cstore_relation_copy_data,
	.relation_copy_for_cluster = cstore_relation_copy_for_cluster,
	.relation_vacuum = heap_vacuum_rel,
	.scan_analyze_next_block = cstore_scan_analyze_next_block,
	.scan_analyze_next_tuple = cstore_scan_analyze_next_tuple,
	.index_build_range_scan = cstore_index_build_range_scan,
	.index_validate_scan = cstore_index_validate_scan,

	.relation_size = cstore_relation_size,
	.relation_needs_toast_table = cstore_relation_needs_toast_table,

	.relation_estimate_size = cstore_estimate_rel_size,

	.scan_bitmap_next_block = cstore_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = cstore_scan_bitmap_next_tuple,
	.scan_sample_next_block = cstore_scan_sample_next_block,
	.scan_sample_next_tuple = cstore_scan_sample_next_tuple
};


const TableAmRoutine *
GetCstoreTableAmRoutine(void)
{
	return &cstore_am_methods;
}


PG_FUNCTION_INFO_V1(cstore_tableam_handler);
Datum
cstore_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&cstore_am_methods);
}
