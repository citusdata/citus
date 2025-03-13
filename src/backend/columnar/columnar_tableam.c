#include <math.h>

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "safe_lib.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_trigger.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/plancat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "citus_version.h"
#include "pg_version_compat.h"

#include "columnar/columnar.h"
#include "columnar/columnar_customscan.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_version_compat.h"

#include "distributed/listutils.h"

/*
 * Timing parameters for truncate locking heuristics.
 *
 * These are the same values from src/backend/access/heap/vacuumlazy.c
 */
#define VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL 50       /* ms */
#define VACUUM_TRUNCATE_LOCK_TIMEOUT 4500               /* ms */

/*
 * ColumnarScanDescData is the scan state passed between beginscan(),
 * getnextslot(), rescan(), and endscan() calls.
 */
typedef struct ColumnarScanDescData
{
	TableScanDescData cs_base;
	ColumnarReadState *cs_readState;

	/*
	 * We initialize cs_readState lazily in the first getnextslot() call. We
	 * need the following for initialization. We save them in beginscan().
	 */
	MemoryContext scanContext;
	Bitmapset *attr_needed;
	List *scanQual;
} ColumnarScanDescData;


/*
 * IndexFetchColumnarData is the scan state passed between index_fetch_begin,
 * index_fetch_reset, index_fetch_end, index_fetch_tuple calls.
 */
typedef struct IndexFetchColumnarData
{
	IndexFetchTableData cs_base;
	ColumnarReadState *cs_readState;

	/*
	 * We initialize cs_readState lazily in the first columnar_index_fetch_tuple
	 * call. However, we want to do memory allocations in a sub MemoryContext of
	 * columnar_index_fetch_begin. For this reason, we store scanContext in
	 * columnar_index_fetch_begin.
	 */
	MemoryContext scanContext;
} IndexFetchColumnarData;

static object_access_hook_type PrevObjectAccessHook = NULL;
static ProcessUtility_hook_type PrevProcessUtilityHook = NULL;

/* forward declaration for static functions */
static MemoryContext CreateColumnarScanMemoryContext(void);
static void ColumnarTableDropHook(Oid tgid);
static void ColumnarTriggerCreateHook(Oid tgid);
static void ColumnarTableAMObjectAccessHook(ObjectAccessType access, Oid classId,
											Oid objectId, int subId,
											void *arg);
static RangeVar * ColumnarProcessAlterTable(AlterTableStmt *alterTableStmt,
											List **columnarOptions);
static void ColumnarProcessUtility(PlannedStmt *pstmt,
								   const char *queryString,
								   bool readOnlyTree,
								   ProcessUtilityContext context,
								   ParamListInfo params,
								   struct QueryEnvironment *queryEnv,
								   DestReceiver *dest,
								   QueryCompletion *completionTag);
static bool ConditionalLockRelationWithTimeout(Relation rel, LOCKMODE lockMode,
											   int timeout, int retryInterval);
static List * NeededColumnsList(TupleDesc tupdesc, Bitmapset *attr_needed);
static void LogRelationStats(Relation rel, int elevel);
static void TruncateColumnar(Relation rel, int elevel);
static HeapTuple ColumnarSlotCopyHeapTuple(TupleTableSlot *slot);
static void ColumnarCheckLogicalReplication(Relation rel);
static Datum * detoast_values(TupleDesc tupleDesc, Datum *orig_values, bool *isnull);
static ItemPointerData row_number_to_tid(uint64 rowNumber);
static uint64 tid_to_row_number(ItemPointerData tid);
static void ErrorIfInvalidRowNumber(uint64 rowNumber);
static void ColumnarReportTotalVirtualBlocks(Relation relation, Snapshot snapshot,
											 int progressArrIndex);
static BlockNumber ColumnarGetNumberOfVirtualBlocks(Relation relation, Snapshot snapshot);
static ItemPointerData ColumnarGetHighestItemPointer(Relation relation,
													 Snapshot snapshot);
static double ColumnarReadRowsIntoIndex(TableScanDesc scan,
										Relation indexRelation,
										IndexInfo *indexInfo,
										bool progress,
										IndexBuildCallback indexCallback,
										void *indexCallbackState,
										EState *estate, ExprState *predicate);
static void ColumnarReadMissingRowsIntoIndex(TableScanDesc scan, Relation indexRelation,
											 IndexInfo *indexInfo, EState *estate,
											 ExprState *predicate,
											 ValidateIndexState *state);
static ItemPointerData TupleSortSkipSmallerItemPointers(Tuplesortstate *tupleSort,
														ItemPointer targetItemPointer);

/* functions for CheckCitusColumnarVersion */
static bool CheckAvailableVersionColumnar(int elevel);
static bool CheckInstalledVersionColumnar(int elevel);
static char * AvailableExtensionVersionColumnar(void);
static char * InstalledExtensionVersionColumnar(void);
static bool CitusColumnarHasBeenLoadedInternal(void);
static bool CitusColumnarHasBeenLoaded(void);
static bool CheckCitusColumnarVersion(int elevel);
static bool MajorVersionsCompatibleColumnar(char *leftVersion, char *rightVersion);

/* global variables for CheckCitusColumnarVersion */
static bool extensionLoadedColumnar = false;
static bool EnableVersionChecksColumnar = true;
static bool citusVersionKnownCompatibleColumnar = false;

/* Custom tuple slot ops used for columnar. Initialized in columnar_tableam_init(). */
static TupleTableSlotOps TTSOpsColumnar;

static const TupleTableSlotOps *
columnar_slot_callbacks(Relation relation)
{
	return &TTSOpsColumnar;
}


static TableScanDesc
columnar_beginscan(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc parallel_scan,
				   uint32 flags)
{
	CheckCitusColumnarVersion(ERROR);

	int natts = relation->rd_att->natts;

	/* attr_needed represents 0-indexed attribute numbers */
	Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

	TableScanDesc scandesc = columnar_beginscan_extended(relation, snapshot, nkeys, key,
														 parallel_scan,
														 flags, attr_needed, NULL);

	bms_free(attr_needed);

	return scandesc;
}


TableScanDesc
columnar_beginscan_extended(Relation relation, Snapshot snapshot,
							int nkeys, ScanKey key,
							ParallelTableScanDesc parallel_scan,
							uint32 flags, Bitmapset *attr_needed, List *scanQual)
{
	CheckCitusColumnarVersion(ERROR);
	RelFileNumber relfilenumber = RelationPhysicalIdentifierNumber_compat(
		RelationPhysicalIdentifier_compat(relation));

	/*
	 * A memory context to use for scan-wide data, including the lazily
	 * initialized read state. We assume that beginscan is called in a
	 * context that will last until end of scan.
	 */
	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	ColumnarScanDesc scan = palloc0(sizeof(ColumnarScanDescData));
	scan->cs_base.rs_rd = relation;
	scan->cs_base.rs_snapshot = snapshot;
	scan->cs_base.rs_nkeys = nkeys;
	scan->cs_base.rs_key = key;
	scan->cs_base.rs_flags = flags;
	scan->cs_base.rs_parallel = parallel_scan;

	/*
	 * We will initialize this lazily in first tuple, where we have the actual
	 * tuple descriptor to use for reading. In some cases like ALTER TABLE ...
	 * ALTER COLUMN ... TYPE, the tuple descriptor of relation doesn't match
	 * the storage which we are reading, so we need to use the tuple descriptor
	 * of "slot" in first read.
	 */
	scan->cs_readState = NULL;
	scan->attr_needed = bms_copy(attr_needed);
	scan->scanQual = copyObject(scanQual);
	scan->scanContext = scanContext;

	if (PendingWritesInUpperTransactions(relfilenumber, GetCurrentSubTransactionId()))
	{
		elog(ERROR,
			 "cannot read from table when there is unflushed data in upper transactions");
	}

	MemoryContextSwitchTo(oldContext);

	return ((TableScanDesc) scan);
}


/*
 * CreateColumnarScanMemoryContext creates a memory context to store
 * ColumnarReadStare in it.
 */
static MemoryContext
CreateColumnarScanMemoryContext(void)
{
	return AllocSetContextCreate(CurrentMemoryContext, "Columnar Scan Context",
								 ALLOCSET_DEFAULT_SIZES);
}


/*
 * init_columnar_read_state initializes a column store table read and returns the
 * state.
 */
static ColumnarReadState *
init_columnar_read_state(Relation relation, TupleDesc tupdesc, Bitmapset *attr_needed,
						 List *scanQual, MemoryContext scanContext, Snapshot snapshot,
						 bool randomAccess)
{
	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	List *neededColumnList = NeededColumnsList(tupdesc, attr_needed);
	ColumnarReadState *readState = ColumnarBeginRead(relation, tupdesc, neededColumnList,
													 scanQual, scanContext, snapshot,
													 randomAccess);

	MemoryContextSwitchTo(oldContext);

	return readState;
}


static void
columnar_endscan(TableScanDesc sscan)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;
	if (scan->cs_readState != NULL)
	{
		ColumnarEndRead(scan->cs_readState);
		scan->cs_readState = NULL;
	}

	if (scan->cs_base.rs_flags & SO_TEMP_SNAPSHOT)
	{
		UnregisterSnapshot(scan->cs_base.rs_snapshot);
	}
}


static void
columnar_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	/* XXX: hack to pass in new quals that aren't actually scan keys */
	List *scanQual = (List *) key;

	if (scan->cs_readState != NULL)
	{
		ColumnarRescan(scan->cs_readState, scanQual);
	}
}


static bool
columnar_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	/*
	 * if this is the first row, initialize read state.
	 */
	if (scan->cs_readState == NULL)
	{
		bool randomAccess = false;
		scan->cs_readState =
			init_columnar_read_state(scan->cs_base.rs_rd, slot->tts_tupleDescriptor,
									 scan->attr_needed, scan->scanQual,
									 scan->scanContext, scan->cs_base.rs_snapshot,
									 randomAccess);
	}

	ExecClearTuple(slot);

	uint64 rowNumber;
	bool nextRowFound = ColumnarReadNextRow(scan->cs_readState, slot->tts_values,
											slot->tts_isnull, &rowNumber);

	if (!nextRowFound)
	{
		return false;
	}

	ExecStoreVirtualTuple(slot);

	slot->tts_tid = row_number_to_tid(rowNumber);

	return true;
}


/*
 * row_number_to_tid maps given rowNumber to ItemPointerData.
 */
static ItemPointerData
row_number_to_tid(uint64 rowNumber)
{
	ErrorIfInvalidRowNumber(rowNumber);

	ItemPointerData tid = { 0 };
	ItemPointerSetBlockNumber(&tid, rowNumber / VALID_ITEMPOINTER_OFFSETS);
	ItemPointerSetOffsetNumber(&tid, rowNumber % VALID_ITEMPOINTER_OFFSETS +
							   FirstOffsetNumber);
	return tid;
}


/*
 * tid_to_row_number maps given ItemPointerData to rowNumber.
 */
static uint64
tid_to_row_number(ItemPointerData tid)
{
	uint64 rowNumber = ItemPointerGetBlockNumber(&tid) * VALID_ITEMPOINTER_OFFSETS +
					   ItemPointerGetOffsetNumber(&tid) - FirstOffsetNumber;

	ErrorIfInvalidRowNumber(rowNumber);

	return rowNumber;
}


/*
 * ErrorIfInvalidRowNumber errors out if given rowNumber is invalid.
 */
static void
ErrorIfInvalidRowNumber(uint64 rowNumber)
{
	if (rowNumber == COLUMNAR_INVALID_ROW_NUMBER)
	{
		/* not expected but be on the safe side */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("unexpected row number for columnar table")));
	}
	else if (rowNumber > COLUMNAR_MAX_ROW_NUMBER)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("columnar tables can't have row numbers "
							   "greater than " UINT64_FORMAT,
							   (uint64) COLUMNAR_MAX_ROW_NUMBER),
						errhint("Consider using VACUUM FULL for your table")));
	}
}


static Size
columnar_parallelscan_estimate(Relation rel)
{
	elog(ERROR, "columnar_parallelscan_estimate not implemented");
}


static Size
columnar_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnar_parallelscan_initialize not implemented");
}


static void
columnar_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnar_parallelscan_reinitialize not implemented");
}


static IndexFetchTableData *
columnar_index_fetch_begin(Relation rel)
{
	CheckCitusColumnarVersion(ERROR);

	RelFileNumber relfilenumber = RelationPhysicalIdentifierNumber_compat(
		RelationPhysicalIdentifier_compat(rel));
	if (PendingWritesInUpperTransactions(relfilenumber, GetCurrentSubTransactionId()))
	{
		/* XXX: maybe we can just flush the data and continue */
		elog(ERROR, "cannot read from index when there is unflushed data in "
					"upper transactions");
	}

	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	IndexFetchColumnarData *scan = palloc0(sizeof(IndexFetchColumnarData));
	scan->cs_base.rel = rel;
	scan->cs_readState = NULL;
	scan->scanContext = scanContext;

	MemoryContextSwitchTo(oldContext);

	return &scan->cs_base;
}


static void
columnar_index_fetch_reset(IndexFetchTableData *sscan)
{
	/* no-op */
}


static void
columnar_index_fetch_end(IndexFetchTableData *sscan)
{
	columnar_index_fetch_reset(sscan);

	IndexFetchColumnarData *scan = (IndexFetchColumnarData *) sscan;
	if (scan->cs_readState)
	{
		ColumnarEndRead(scan->cs_readState);
		scan->cs_readState = NULL;
	}
}


static bool
columnar_index_fetch_tuple(struct IndexFetchTableData *sscan,
						   ItemPointer tid,
						   Snapshot snapshot,
						   TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	/* no HOT chains are possible in columnar, directly set it to false */
	*call_again = false;

	/*
	 * Initialize all_dead to false if passed to be non-NULL.
	 *
	 * XXX: For aborted writes, we should set all_dead to true but this would
	 * require implementing columnar_index_delete_tuples for simple deletion
	 * of dead tuples (TM_IndexDeleteOp.bottomup = false).
	 */
	if (all_dead)
	{
		*all_dead = false;
	}

	ExecClearTuple(slot);

	IndexFetchColumnarData *scan = (IndexFetchColumnarData *) sscan;
	Relation columnarRelation = scan->cs_base.rel;

	/* initialize read state for the first row */
	if (scan->cs_readState == NULL)
	{
		/* we need all columns */
		int natts = columnarRelation->rd_att->natts;
		Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

		/* no quals for index scan */
		List *scanQual = NIL;

		bool randomAccess = true;
		scan->cs_readState = init_columnar_read_state(columnarRelation,
													  slot->tts_tupleDescriptor,
													  attr_needed, scanQual,
													  scan->scanContext,
													  snapshot, randomAccess);
	}

	uint64 rowNumber = tid_to_row_number(*tid);
	StripeMetadata *stripeMetadata =
		FindStripeWithMatchingFirstRowNumber(columnarRelation, rowNumber, snapshot);
	if (!stripeMetadata)
	{
		/* it is certain that tuple with rowNumber doesn't exist */
		return false;
	}

	StripeWriteStateEnum stripeWriteState = StripeWriteState(stripeMetadata);
	if (stripeWriteState == STRIPE_WRITE_FLUSHED &&
		!ColumnarReadRowByRowNumber(scan->cs_readState, rowNumber,
									slot->tts_values, slot->tts_isnull))
	{
		/*
		 * FindStripeWithMatchingFirstRowNumber doesn't verify upper row
		 * number boundary of found stripe. For this reason, we didn't
		 * certainly know if given row number belongs to one of the stripes.
		 */
		return false;
	}
	else if (stripeWriteState == STRIPE_WRITE_ABORTED)
	{
		/*
		 * We only expect to see un-flushed stripes when checking against
		 * constraint violation. In that case, indexAM provides dirty
		 * snapshot to index_fetch_tuple callback.
		 */
		Assert(snapshot->snapshot_type == SNAPSHOT_DIRTY);
		return false;
	}
	else if (stripeWriteState == STRIPE_WRITE_IN_PROGRESS)
	{
		if (stripeMetadata->insertedByCurrentXact)
		{
			/*
			 * Stripe write is in progress and its entry is inserted by current
			 * transaction, so obviously it must be written by me. Since caller
			 * might want to use tupleslot datums for some reason, do another
			 * look-up, but this time by first flushing our writes.
			 *
			 * XXX: For index scan, this is the only case that we flush pending
			 * writes of the current backend. If we have taught reader how to
			 * read from WriteStateMap. then we could guarantee that
			 * index_fetch_tuple would never flush pending writes, but this seem
			 * to be too much work for now, but should be doable.
			 */
			ColumnarReadFlushPendingWrites(scan->cs_readState);

			/*
			 * Fill the tupleslot and fall through to return true, it
			 * certainly exists.
			 */
			ColumnarReadRowByRowNumberOrError(scan->cs_readState, rowNumber,
											  slot->tts_values, slot->tts_isnull);
		}
		else
		{
			/* similar to aborted writes, it should be dirty snapshot */
			Assert(snapshot->snapshot_type == SNAPSHOT_DIRTY);

			/*
			 * Stripe that "might" contain the tuple with rowNumber is not
			 * flushed yet. Here we set all attributes of given tupleslot to NULL
			 * before returning true and expect the indexAM callback that called
			 * us --possibly to check against constraint violation-- blocks until
			 * writer transaction commits or aborts, without requiring us to fill
			 * the tupleslot properly.
			 *
			 * XXX: Note that the assumption we made above for the tupleslot
			 * holds for "unique" constraints defined on "btree" indexes.
			 *
			 * For the other constraints that we support, namely:
			 * * exclusion on btree,
			 * * exclusion on hash,
			 * * unique on btree;
			 * we still need to fill tts_values.
			 *
			 * However, for the same reason, we should have already flushed
			 * single tuple stripes when inserting into table for those three
			 * classes of constraints.
			 *
			 * This is annoying, but this also explains why this hack works for
			 * unique constraints on btree indexes, and also explains how we
			 * would never end up with "else" condition otherwise.
			 */
			memset(slot->tts_isnull, true, slot->tts_nvalid * sizeof(bool));
		}
	}
	else
	{
		/*
		 * At this point, we certainly know that stripe is flushed and
		 * ColumnarReadRowByRowNumber successfully filled the tupleslot.
		 */
		Assert(stripeWriteState == STRIPE_WRITE_FLUSHED);
	}

	slot->tts_tableOid = RelationGetRelid(columnarRelation);
	slot->tts_tid = *tid;
	ExecStoreVirtualTuple(slot);

	return true;
}


static bool
columnar_fetch_row_version(Relation relation,
						   ItemPointer tid,
						   Snapshot snapshot,
						   TupleTableSlot *slot)
{
	elog(ERROR, "columnar_fetch_row_version not implemented");
}


static void
columnar_get_latest_tid(TableScanDesc sscan,
						ItemPointer tid)
{
	elog(ERROR, "columnar_get_latest_tid not implemented");
}


static bool
columnar_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "columnar_tuple_tid_valid not implemented");
}


static bool
columnar_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	CheckCitusColumnarVersion(ERROR);

	uint64 rowNumber = tid_to_row_number(slot->tts_tid);
	StripeMetadata *stripeMetadata = FindStripeByRowNumber(rel, rowNumber, snapshot);
	return stripeMetadata != NULL;
}


static TransactionId
columnar_index_delete_tuples(Relation rel,
							 TM_IndexDeleteOp *delstate)
{
	CheckCitusColumnarVersion(ERROR);

	/*
	 * XXX: We didn't bother implementing index_delete_tuple for neither of
	 * simple deletion and bottom-up deletion cases. There is no particular
	 * reason for that, just to keep things simple.
	 *
	 * See the rest of this function to see how we deal with
	 * index_delete_tuples requests made to columnarAM.
	 */

	if (delstate->bottomup)
	{
		/*
		 * Ignore any bottom-up deletion requests.
		 *
		 * Currently only caller in postgres that does bottom-up deletion is
		 * _bt_bottomupdel_pass, which in turn calls _bt_delitems_delete_check.
		 * And this function is okay with ndeltids being set to 0 by tableAM
		 * for bottom-up deletion.
		 */
		delstate->ndeltids = 0;
		return InvalidTransactionId;
	}
	else
	{
		/*
		 * TableAM is not expected to set ndeltids to 0 for simple deletion
		 * case, so here we cannot do the same trick that we do for
		 * bottom-up deletion.
		 * See the assertion around table_index_delete_tuples call in pg
		 * function index_compute_xid_horizon_for_tuples.
		 *
		 * For this reason, to avoid receiving simple deletion requests for
		 * columnar tables (bottomup = false), columnar_index_fetch_tuple
		 * doesn't ever set all_dead to true in order to prevent triggering
		 * simple deletion of index tuples. But let's throw an error to be on
		 * the safe side.
		 */
		elog(ERROR, "columnar_index_delete_tuples not implemented for simple deletion");
	}
}


static void
columnar_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					  int options, BulkInsertState bistate)
{
	CheckCitusColumnarVersion(ERROR);

	/*
	 * columnar_init_write_state allocates the write state in a longer
	 * lasting context, so no need to worry about it.
	 */
	ColumnarWriteState *writeState = columnar_init_write_state(relation,
															   RelationGetDescr(relation),
															   slot->tts_tableOid,
															   GetCurrentSubTransactionId());

	MemoryContext oldContext = MemoryContextSwitchTo(ColumnarWritePerTupleContext(
														 writeState));

	ColumnarCheckLogicalReplication(relation);

	slot_getallattrs(slot);

	Datum *values = detoast_values(slot->tts_tupleDescriptor,
								   slot->tts_values, slot->tts_isnull);

	uint64 writtenRowNumber = ColumnarWriteRow(writeState, values, slot->tts_isnull);
	slot->tts_tid = row_number_to_tid(writtenRowNumber);

	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(ColumnarWritePerTupleContext(writeState));
}


static void
columnar_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								  CommandId cid, int options,
								  BulkInsertState bistate, uint32 specToken)
{
	elog(ERROR, "columnar_tuple_insert_speculative not implemented");
}


static void
columnar_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
									uint32 specToken, bool succeeded)
{
	elog(ERROR, "columnar_tuple_complete_speculative not implemented");
}


static void
columnar_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					  CommandId cid, int options, BulkInsertState bistate)
{
	CheckCitusColumnarVersion(ERROR);

	/*
	 * The callback to .multi_insert is table_multi_insert() and this is only used for the COPY
	 * command, so slot[i]->tts_tableoid will always be equal to relation->id. Thus, we can send
	 * RelationGetRelid(relation) as the tupSlotTableOid
	 */
	ColumnarWriteState *writeState = columnar_init_write_state(relation,
															   RelationGetDescr(relation),
															   RelationGetRelid(relation),
															   GetCurrentSubTransactionId());

	ColumnarCheckLogicalReplication(relation);

	MemoryContext oldContext = MemoryContextSwitchTo(ColumnarWritePerTupleContext(
														 writeState));

	for (int i = 0; i < ntuples; i++)
	{
		TupleTableSlot *tupleSlot = slots[i];

		slot_getallattrs(tupleSlot);

		Datum *values = detoast_values(tupleSlot->tts_tupleDescriptor,
									   tupleSlot->tts_values, tupleSlot->tts_isnull);

		uint64 writtenRowNumber = ColumnarWriteRow(writeState, values,
												   tupleSlot->tts_isnull);
		tupleSlot->tts_tid = row_number_to_tid(writtenRowNumber);

		MemoryContextReset(ColumnarWritePerTupleContext(writeState));
	}

	MemoryContextSwitchTo(oldContext);
}


static TM_Result
columnar_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck, bool wait,
					  TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "columnar_tuple_delete not implemented");
}


static TM_Result
columnar_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	elog(ERROR, "columnar_tuple_update not implemented");
}


static TM_Result
columnar_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					TM_FailureData *tmfd)
{
	elog(ERROR, "columnar_tuple_lock not implemented");
}


static void
columnar_finish_bulk_insert(Relation relation, int options)
{
	/*
	 * Nothing to do here. We keep write states live until transaction end.
	 */
}


static void
columnar_relation_set_new_filelocator(Relation rel,
									  const RelFileLocator *newrlocator,
									  char persistence,
									  TransactionId *freezeXid,
									  MultiXactId *minmulti)
{
	CheckCitusColumnarVersion(ERROR);

	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unlogged columnar tables are not supported")));
	}

	/*
	 * If existing and new relfilenode are different, that means the existing
	 * storage was dropped and we also need to clean up the metadata and
	 * state. If they are equal, this is a new relation object and we don't
	 * need to clean anything.
	 */
	if (RelationPhysicalIdentifierNumber_compat(RelationPhysicalIdentifier_compat(rel)) !=
		RelationPhysicalIdentifierNumberPtr_compat(newrlocator))
	{
		MarkRelfilenumberDropped(RelationPhysicalIdentifierNumber_compat(
									 RelationPhysicalIdentifier_compat(rel)),
								 GetCurrentSubTransactionId());

		DeleteMetadataRows(RelationPhysicalIdentifier_compat(rel));
	}

	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();
	SMgrRelation srel = RelationCreateStorage(*newrlocator, persistence, true);

	ColumnarStorageInit(srel, ColumnarMetadataNewStorageId());
	InitColumnarOptions(rel->rd_id);

	smgrclose(srel);

	/* we will lazily initialize metadata in first stripe reservation */
}


static void
columnar_relation_nontransactional_truncate(Relation rel)
{
	CheckCitusColumnarVersion(ERROR);
	RelFileLocator relfilelocator = RelationPhysicalIdentifier_compat(rel);

	NonTransactionDropWriteState(RelationPhysicalIdentifierNumber_compat(relfilelocator));

	/* Delete old relfilenode metadata */
	DeleteMetadataRows(relfilelocator);

	/*
	 * No need to set new relfilenode, since the table was created in this
	 * transaction and no other transaction can see this relation yet. We
	 * can just truncate the relation.
	 *
	 * This is similar to what is done in heapam_relation_nontransactional_truncate.
	 */
	RelationTruncate(rel, 0);

	uint64 storageId = ColumnarMetadataNewStorageId();
	ColumnarStorageInit(RelationGetSmgr(rel), storageId);
}


static void
columnar_relation_copy_data(Relation rel, const RelFileLocator *newrnode)
{
	elog(ERROR, "columnar_relation_copy_data not implemented");
}


/*
 * columnar_relation_copy_for_cluster is called on VACUUM FULL, at which
 * we should copy data from OldHeap to NewHeap.
 *
 * In general TableAM case this can also be called for the CLUSTER command
 * which is not applicable for columnar since it doesn't support indexes.
 */
static void
columnar_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								   Relation OldIndex, bool use_sort,
								   TransactionId OldestXmin,
								   TransactionId *xid_cutoff,
								   MultiXactId *multi_cutoff,
								   double *num_tuples,
								   double *tups_vacuumed,
								   double *tups_recently_dead)
{
	CheckCitusColumnarVersion(ERROR);

	TupleDesc sourceDesc = RelationGetDescr(OldHeap);
	TupleDesc targetDesc = RelationGetDescr(NewHeap);

	if (OldIndex != NULL || use_sort)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("clustering columnar tables using indexes is "
							   "not supported")));
	}

	/*
	 * copy_table_data in cluster.c assumes tuple descriptors are exactly
	 * the same. Even dropped columns exist and are marked as attisdropped
	 * in the target relation.
	 */
	Assert(sourceDesc->natts == targetDesc->natts);

	/* read settings from old heap, relfilenode will be swapped at the end */
	ColumnarOptions columnarOptions = { 0 };
	ReadColumnarOptions(OldHeap->rd_id, &columnarOptions);

	ColumnarWriteState *writeState = ColumnarBeginWrite(RelationPhysicalIdentifier_compat(
															NewHeap),
														columnarOptions,
														targetDesc);

	/* we need all columns */
	int natts = OldHeap->rd_att->natts;
	Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);

	/* no quals for table rewrite */
	List *scanQual = NIL;

	/* use SnapshotAny when re-writing table as heapAM does */
	Snapshot snapshot = SnapshotAny;

	MemoryContext scanContext = CreateColumnarScanMemoryContext();
	bool randomAccess = false;
	ColumnarReadState *readState = init_columnar_read_state(OldHeap, sourceDesc,
															attr_needed, scanQual,
															scanContext, snapshot,
															randomAccess);

	Datum *values = palloc0(sourceDesc->natts * sizeof(Datum));
	bool *nulls = palloc0(sourceDesc->natts * sizeof(bool));

	*num_tuples = 0;

	/* we don't need to know rowNumber here */
	while (ColumnarReadNextRow(readState, values, nulls, NULL))
	{
		ColumnarWriteRow(writeState, values, nulls);
		(*num_tuples)++;
	}

	*tups_vacuumed = 0;

	ColumnarEndWrite(writeState);
	ColumnarEndRead(readState);
}


/*
 * NeededColumnsList returns a list of AttrNumber's for the columns that
 * are not dropped and specified by attr_needed.
 */
static List *
NeededColumnsList(TupleDesc tupdesc, Bitmapset *attr_needed)
{
	List *columnList = NIL;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		if (tupdesc->attrs[i].attisdropped)
		{
			continue;
		}

		/* attr_needed is 0-indexed but columnList is 1-indexed */
		if (bms_is_member(i, attr_needed))
		{
			AttrNumber varattno = i + 1;
			columnList = lappend_int(columnList, varattno);
		}
	}

	return columnList;
}


/*
 * ColumnarTableTupleCount returns the number of tuples that columnar
 * table with relationId has by using stripe metadata.
 */
static uint64
ColumnarTableTupleCount(Relation relation)
{
	List *stripeList = StripesForRelfilelocator(RelationPhysicalIdentifier_compat(
													relation));
	uint64 tupleCount = 0;

	ListCell *lc = NULL;
	foreach(lc, stripeList)
	{
		StripeMetadata *stripe = lfirst(lc);
		tupleCount += stripe->rowCount;
	}

	return tupleCount;
}


/*
 * columnar_vacuum_rel implements VACUUM without FULL option.
 */
static void
columnar_vacuum_rel(Relation rel, VacuumParams *params,
					BufferAccessStrategy bstrategy)
{
	if (!CheckCitusColumnarVersion(WARNING))
	{
		/*
		 * Skip if the extension catalogs are not up-to-date, but avoid
		 * erroring during auto-vacuum.
		 */
		return;
	}

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  RelationGetRelid(rel));

	/*
	 * If metapage version of relation is older, then we hint users to VACUUM
	 * the relation in ColumnarMetapageCheckVersion. So if needed, upgrade
	 * the metapage before doing anything.
	 */
	bool isUpgrade = true;
	ColumnarStorageUpdateIfNeeded(rel, isUpgrade);

	int elevel = (params->options & VACOPT_VERBOSE) ? INFO : DEBUG2;

	/* this should have been resolved by vacuum.c until now */
	Assert(params->truncate != VACOPTVALUE_UNSPECIFIED);

	LogRelationStats(rel, elevel);

	/*
	 * We don't have updates, deletes, or concurrent updates, so all we
	 * care for now is truncating the unused space at the end of storage.
	 */
	if (params->truncate == VACOPTVALUE_ENABLED)
	{
		TruncateColumnar(rel, elevel);
	}

	BlockNumber new_rel_pages = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);

	/* get the number of indexes */
	List *indexList = RelationGetIndexList(rel);
	int nindexes = list_length(indexList);

#if PG_VERSION_NUM >= PG_VERSION_16
	struct VacuumCutoffs cutoffs;
	vacuum_get_cutoffs(rel, params, &cutoffs);

	Assert(MultiXactIdPrecedesOrEquals(cutoffs.MultiXactCutoff, cutoffs.OldestMxact));
	Assert(TransactionIdPrecedesOrEquals(cutoffs.FreezeLimit, cutoffs.OldestXmin));

	/*
	 * Columnar storage doesn't hold any transaction IDs, so we can always
	 * just advance to the most aggressive value.
	 */
	TransactionId newRelFrozenXid = cutoffs.OldestXmin;
	MultiXactId newRelminMxid = cutoffs.OldestMxact;
	double new_live_tuples = ColumnarTableTupleCount(rel);

	/* all visible pages are always 0 */
	BlockNumber new_rel_allvisible = 0;

	bool frozenxid_updated;
	bool minmulti_updated;

	vac_update_relstats(rel, new_rel_pages, new_live_tuples,
						new_rel_allvisible, nindexes > 0,
						newRelFrozenXid, newRelminMxid,
						&frozenxid_updated, &minmulti_updated, false);
#else
	TransactionId oldestXmin;
	TransactionId freezeLimit;
	MultiXactId multiXactCutoff;

	/* initialize xids */
#if (PG_VERSION_NUM >= PG_VERSION_15) && (PG_VERSION_NUM < PG_VERSION_16)
	MultiXactId oldestMxact;
	vacuum_set_xid_limits(rel,
						  params->freeze_min_age,
						  params->freeze_table_age,
						  params->multixact_freeze_min_age,
						  params->multixact_freeze_table_age,
						  &oldestXmin, &oldestMxact,
						  &freezeLimit, &multiXactCutoff);

	Assert(MultiXactIdPrecedesOrEquals(multiXactCutoff, oldestMxact));
#else
	TransactionId xidFullScanLimit;
	MultiXactId mxactFullScanLimit;
	vacuum_set_xid_limits(rel,
						  params->freeze_min_age,
						  params->freeze_table_age,
						  params->multixact_freeze_min_age,
						  params->multixact_freeze_table_age,
						  &oldestXmin, &freezeLimit, &xidFullScanLimit,
						  &multiXactCutoff, &mxactFullScanLimit);
#endif

	Assert(TransactionIdPrecedesOrEquals(freezeLimit, oldestXmin));

	/*
	 * Columnar storage doesn't hold any transaction IDs, so we can always
	 * just advance to the most aggressive value.
	 */
	TransactionId newRelFrozenXid = oldestXmin;
#if (PG_VERSION_NUM >= PG_VERSION_15) && (PG_VERSION_NUM < PG_VERSION_16)
	MultiXactId newRelminMxid = oldestMxact;
#else
	MultiXactId newRelminMxid = multiXactCutoff;
#endif

	double new_live_tuples = ColumnarTableTupleCount(rel);

	/* all visible pages are always 0 */
	BlockNumber new_rel_allvisible = 0;

#if (PG_VERSION_NUM >= PG_VERSION_15) && (PG_VERSION_NUM < PG_VERSION_16)
	bool frozenxid_updated;
	bool minmulti_updated;

	vac_update_relstats(rel, new_rel_pages, new_live_tuples,
						new_rel_allvisible, nindexes > 0,
						newRelFrozenXid, newRelminMxid,
						&frozenxid_updated, &minmulti_updated, false);
#else
	vac_update_relstats(rel, new_rel_pages, new_live_tuples,
						new_rel_allvisible, nindexes > 0,
						newRelFrozenXid, newRelminMxid, false);
#endif
#endif

	pgstat_report_vacuum(RelationGetRelid(rel),
						 rel->rd_rel->relisshared,
						 Max(new_live_tuples, 0),
						 0);
	pgstat_progress_end_command();
}


/*
 * LogRelationStats logs statistics as the output of the VACUUM VERBOSE.
 */
static void
LogRelationStats(Relation rel, int elevel)
{
	ListCell *stripeMetadataCell = NULL;
	RelFileLocator relfilelocator = RelationPhysicalIdentifier_compat(rel);
	StringInfo infoBuf = makeStringInfo();

	int compressionStats[COMPRESSION_COUNT] = { 0 };
	uint64 totalStripeLength = 0;
	uint64 tupleCount = 0;
	uint64 chunkCount = 0;
	TupleDesc tupdesc = RelationGetDescr(rel);
	uint64 droppedChunksWithData = 0;
	uint64 totalDecompressedLength = 0;

	List *stripeList = StripesForRelfilelocator(relfilelocator);
	int stripeCount = list_length(stripeList);

	foreach(stripeMetadataCell, stripeList)
	{
		StripeMetadata *stripe = lfirst(stripeMetadataCell);
		StripeSkipList *skiplist = ReadStripeSkipList(relfilelocator, stripe->id,
													  RelationGetDescr(rel),
													  stripe->chunkCount,
													  GetTransactionSnapshot());
		for (uint32 column = 0; column < skiplist->columnCount; column++)
		{
			bool attrDropped = tupdesc->attrs[column].attisdropped;
			for (uint32 chunk = 0; chunk < skiplist->chunkCount; chunk++)
			{
				ColumnChunkSkipNode *skipnode =
					&skiplist->chunkSkipNodeArray[column][chunk];

				/* ignore zero length chunks for dropped attributes */
				if (skipnode->valueLength > 0)
				{
					compressionStats[skipnode->valueCompressionType]++;
					chunkCount++;

					if (attrDropped)
					{
						droppedChunksWithData++;
					}
				}

				/*
				 * We don't compress exists buffer, so its compressed & decompressed
				 * lengths are the same.
				 */
				totalDecompressedLength += skipnode->existsLength;
				totalDecompressedLength += skipnode->decompressedValueSize;
			}
		}

		tupleCount += stripe->rowCount;
		totalStripeLength += stripe->dataLength;
	}

	uint64 relPages = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);
	RelationCloseSmgr(rel);

	Datum storageId = DirectFunctionCall1(columnar_relation_storageid,
										  ObjectIdGetDatum(RelationGetRelid(rel)));

	double compressionRate = totalStripeLength ?
							 (double) totalDecompressedLength / totalStripeLength :
							 1.0;

	appendStringInfo(infoBuf, "storage id: %ld\n", DatumGetInt64(storageId));
	appendStringInfo(infoBuf, "total file size: %ld, total data size: %ld\n",
					 relPages * BLCKSZ, totalStripeLength);
	appendStringInfo(infoBuf, "compression rate: %.2fx\n", compressionRate);
	appendStringInfo(infoBuf,
					 "total row count: %ld, stripe count: %d, "
					 "average rows per stripe: %ld\n",
					 tupleCount, stripeCount,
					 stripeCount ? tupleCount / stripeCount : 0);
	appendStringInfo(infoBuf,
					 "chunk count: %ld"
					 ", containing data for dropped columns: %ld",
					 chunkCount, droppedChunksWithData);
	for (int compressionType = 0; compressionType < COMPRESSION_COUNT; compressionType++)
	{
		const char *compressionName = CompressionTypeStr(compressionType);

		/* skip if this compression algorithm has not been compiled */
		if (compressionName == NULL)
		{
			continue;
		}

		/* skip if no chunks use this compression type */
		if (compressionStats[compressionType] == 0)
		{
			continue;
		}

		appendStringInfo(infoBuf,
						 ", %s compressed: %d",
						 compressionName,
						 compressionStats[compressionType]);
	}
	appendStringInfoString(infoBuf, "\n");

	ereport(elevel, (errmsg("statistics for \"%s\":\n%s", RelationGetRelationName(rel),
							infoBuf->data)));
}


/*
 * TruncateColumnar truncates the unused space at the end of main fork for
 * a columnar table. This unused space can be created by aborted transactions.
 *
 * This implementation is based on heap_vacuum_rel in vacuumlazy.c with some
 * changes so it suits columnar store relations.
 */
static void
TruncateColumnar(Relation rel, int elevel)
{
	PGRUsage ru0;

	pg_rusage_init(&ru0);

	/* Report that we are now truncating */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_TRUNCATE);


	/*
	 * We need access exclusive lock on the relation in order to do
	 * truncation. If we can't get it, give up rather than waiting --- we
	 * don't want to block other backends, and we don't want to deadlock
	 * (which is quite possible considering we already hold a lower-grade
	 * lock).
	 *
	 * The decisions for AccessExclusiveLock and conditional lock with
	 * a timeout is based on lazy_truncate_heap in vacuumlazy.c.
	 */
	if (!ConditionalLockRelationWithTimeout(rel, AccessExclusiveLock,
											VACUUM_TRUNCATE_LOCK_TIMEOUT,
											VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL))
	{
		/*
		 * We failed to establish the lock in the specified number of
		 * retries. This means we give up truncating.
		 */
		ereport(elevel,
				(errmsg("\"%s\": stopping truncate due to conflicting lock request",
						RelationGetRelationName(rel))));
		return;
	}

	/*
	 * Due to the AccessExclusive lock there's no danger that
	 * new stripes be added beyond highestPhysicalAddress while
	 * we're truncating.
	 */
	uint64 newDataReservation = Max(GetHighestUsedAddress(
										RelationPhysicalIdentifier_compat(rel)) + 1,
									ColumnarFirstLogicalOffset);

	BlockNumber old_rel_pages = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);

	if (!ColumnarStorageTruncate(rel, newDataReservation))
	{
		UnlockRelation(rel, AccessExclusiveLock);
		return;
	}

	BlockNumber new_rel_pages = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);

	/*
	 * We can release the exclusive lock as soon as we have truncated.
	 * Other backends can't safely access the relation until they have
	 * processed the smgr invalidation that smgrtruncate sent out ... but
	 * that should happen as part of standard invalidation processing once
	 * they acquire lock on the relation.
	 */
	UnlockRelation(rel, AccessExclusiveLock);

	ereport(elevel,
			(errmsg("\"%s\": truncated %u to %u pages",
					RelationGetRelationName(rel),
					old_rel_pages, new_rel_pages),
			 errdetail_internal("%s", pg_rusage_show(&ru0))));
}


/*
 * ConditionalLockRelationWithTimeout tries to acquire a relation lock until
 * it either succeeds or timesout. It doesn't enter wait queue and instead it
 * sleeps between lock tries.
 *
 * This is based on the lock loop in lazy_truncate_heap().
 */
static bool
ConditionalLockRelationWithTimeout(Relation rel, LOCKMODE lockMode, int timeout,
								   int retryInterval)
{
	int lock_retry = 0;

	while (true)
	{
		if (ConditionalLockRelation(rel, lockMode))
		{
			break;
		}

		/*
		 * Check for interrupts while trying to (re-)acquire the lock
		 */
		CHECK_FOR_INTERRUPTS();

		if (++lock_retry > (timeout / retryInterval))
		{
			return false;
		}

		pg_usleep(retryInterval * 1000L);
	}

	return true;
}


static bool
columnar_scan_analyze_next_block(TableScanDesc scan,
#if PG_VERSION_NUM >= PG_VERSION_17
								 ReadStream *stream)
#else
								 BlockNumber blockno,
								 BufferAccessStrategy bstrategy)
#endif
{
	/*
	 * Our access method is not pages based, i.e. tuples are not confined
	 * to pages boundaries. So not much to do here. We return true anyway
	 * so acquire_sample_rows() in analyze.c would call our
	 * columnar_scan_analyze_next_tuple() callback.
	 * In PG17, we return false in case there is no buffer left, since
	 * the outer loop changed in acquire_sample_rows(), and it is
	 * expected for the scan_analyze_next_block function to check whether
	 * there are any blocks left in the block sampler.
	 */
#if PG_VERSION_NUM >= PG_VERSION_17
	Buffer buf = read_stream_next_buffer(stream, NULL);
	if (!BufferIsValid(buf))
	{
		return false;
	}
	ReleaseBuffer(buf);
#endif
	return true;
}


static bool
columnar_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								 double *liverows, double *deadrows,
								 TupleTableSlot *slot)
{
	/*
	 * Currently we don't do anything smart to reduce number of rows returned
	 * for ANALYZE. The TableAM API's ANALYZE functions are designed for page
	 * based access methods where it chooses random pages, and then reads
	 * tuples from those pages.
	 *
	 * We could do something like that here by choosing sample stripes or chunks,
	 * but getting that correct might need quite some work. Since columnar_fdw's
	 * ANALYZE scanned all rows, as a starter we do the same here and scan all
	 * rows.
	 */
	if (columnar_getnextslot(scan, ForwardScanDirection, slot))
	{
		(*liverows)++;
		return true;
	}

	return false;
}


static double
columnar_index_build_range_scan(Relation columnarRelation,
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
	CheckCitusColumnarVersion(ERROR);

	if (start_blockno != 0 || numblocks != InvalidBlockNumber)
	{
		/*
		 * Columnar utility hook already errors out for BRIN indexes on columnar
		 * tables, but be on the safe side.
		 */
		ereport(ERROR, (errmsg("BRIN indexes on columnar tables are not supported")));
	}

	if (scan)
	{
		/*
		 * Parallel scans on columnar tables are already discardad by
		 * ColumnarGetRelationInfoHook but be on the safe side.
		 */
		elog(ERROR, "parallel scans on columnar are not supported");
	}

	/*
	 * In a normal index build, we use SnapshotAny to retrieve all tuples. In
	 * a concurrent build or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 */
	TransactionId OldestXmin = InvalidTransactionId;
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
	{
		/* ignore lazy VACUUM's */
		OldestXmin = GetOldestNonRemovableTransactionId(columnarRelation);
	}

	Snapshot snapshot = { 0 };
	bool snapshotRegisteredByUs = false;

	/*
	 * For serial index build, we begin our own scan. We may also need to
	 * register a snapshot whose lifetime is under our direct control.
	 */
	if (!TransactionIdIsValid(OldestXmin))
	{
		snapshot = RegisterSnapshot(GetTransactionSnapshot());
		snapshotRegisteredByUs = true;
	}
	else
	{
		snapshot = SnapshotAny;
	}

	int nkeys = 0;
	ScanKeyData *scanKey = NULL;
	bool allowAccessStrategy = true;
	scan = table_beginscan_strat(columnarRelation, snapshot, nkeys, scanKey,
								 allowAccessStrategy, allow_sync);

	if (progress)
	{
		ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
										 PROGRESS_SCAN_BLOCKS_TOTAL);
	}

	/*
	 * Set up execution state for predicate, if any.
	 * Note that this is only useful for partial indexes.
	 */
	EState *estate = CreateExecutorState();
	ExprContext *econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = table_slot_create(columnarRelation, NULL);
	ExprState *predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	double reltuples = ColumnarReadRowsIntoIndex(scan, indexRelation, indexInfo,
												 progress, callback, callback_state,
												 estate, predicate);
	table_endscan(scan);

	if (progress)
	{
		/* report the last "virtual" block as "done" */
		ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
										 PROGRESS_SCAN_BLOCKS_DONE);
	}

	if (snapshotRegisteredByUs)
	{
		UnregisterSnapshot(snapshot);
	}

	ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
	FreeExecutorState(estate);
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}


/*
 * ColumnarReportTotalVirtualBlocks reports progress for index build based on
 * number of "virtual" blocks that given relation has.
 * "progressArrIndex" argument determines which entry in st_progress_param
 * array should be updated. In this case, we only expect PROGRESS_SCAN_BLOCKS_TOTAL
 * or PROGRESS_SCAN_BLOCKS_DONE to specify whether we want to report calculated
 * number of blocks as "done" or as "total" number of "virtual" blocks to scan.
 */
static void
ColumnarReportTotalVirtualBlocks(Relation relation, Snapshot snapshot,
								 int progressArrIndex)
{
	/*
	 * Indeed, columnar tables might have gaps between row numbers, e.g
	 * due to aborted transactions etc. Also, ItemPointer BlockNumber's
	 * for columnar tables don't actually correspond to actual disk blocks
	 * as in heapAM. For this reason, we call them as "virtual" blocks. At
	 * the moment, we believe it is better to report our progress based on
	 * this "virtual" block concept instead of doing nothing.
	 */
	Assert(progressArrIndex == PROGRESS_SCAN_BLOCKS_TOTAL ||
		   progressArrIndex == PROGRESS_SCAN_BLOCKS_DONE);
	BlockNumber nvirtualBlocks =
		ColumnarGetNumberOfVirtualBlocks(relation, snapshot);
	pgstat_progress_update_param(progressArrIndex, nvirtualBlocks);
}


/*
 * ColumnarGetNumberOfVirtualBlocks returns total number of "virtual" blocks
 * that given columnar table has based on based on ItemPointer BlockNumber's.
 */
static BlockNumber
ColumnarGetNumberOfVirtualBlocks(Relation relation, Snapshot snapshot)
{
	ItemPointerData highestItemPointer =
		ColumnarGetHighestItemPointer(relation, snapshot);
	if (!ItemPointerIsValid(&highestItemPointer))
	{
		/* table is empty according to our snapshot */
		return 0;
	}

	/*
	 * Since BlockNumber is 0-based, increment it by 1 to find the total
	 * number of "virtual" blocks.
	 */
	return ItemPointerGetBlockNumber(&highestItemPointer) + 1;
}


/*
 * ColumnarGetHighestItemPointer returns ItemPointerData for the tuple with
 * highest tid for given relation.
 * If given relation is empty, then returns invalid item pointer.
 */
static ItemPointerData
ColumnarGetHighestItemPointer(Relation relation, Snapshot snapshot)
{
	StripeMetadata *stripeWithHighestRowNumber =
		FindStripeWithHighestRowNumber(relation, snapshot);
	if (stripeWithHighestRowNumber == NULL ||
		StripeGetHighestRowNumber(stripeWithHighestRowNumber) == 0)
	{
		/* table is empty according to our snapshot */
		ItemPointerData invalidItemPtr;
		ItemPointerSetInvalid(&invalidItemPtr);
		return invalidItemPtr;
	}

	uint64 highestRowNumber = StripeGetHighestRowNumber(stripeWithHighestRowNumber);
	return row_number_to_tid(highestRowNumber);
}


/*
 * ColumnarReadRowsIntoIndex builds indexRelation tuples by reading the
 * actual relation based on given "scan" and returns number of tuples
 * scanned to build the indexRelation.
 */
static double
ColumnarReadRowsIntoIndex(TableScanDesc scan, Relation indexRelation,
						  IndexInfo *indexInfo, bool progress,
						  IndexBuildCallback indexCallback,
						  void *indexCallbackState, EState *estate,
						  ExprState *predicate)
{
	double reltuples = 0;

	BlockNumber lastReportedBlockNumber = InvalidBlockNumber;

	ExprContext *econtext = GetPerTupleExprContext(estate);
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	while (columnar_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();

		BlockNumber currentBlockNumber = ItemPointerGetBlockNumber(&slot->tts_tid);
		if (progress && lastReportedBlockNumber != currentBlockNumber)
		{
			/*
			 * columnar_getnextslot guarantees that returned tuple will
			 * always have a greater ItemPointer than the ones we fetched
			 * before, so we directly use BlockNumber to report our progress.
			 */
			Assert(lastReportedBlockNumber == InvalidBlockNumber ||
				   currentBlockNumber >= lastReportedBlockNumber);
			pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
										 currentBlockNumber);
			lastReportedBlockNumber = currentBlockNumber;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (predicate != NULL && !ExecQual(predicate, econtext))
		{
			/* for partial indexes, discard tuples that don't satisfy the predicate */
			continue;
		}

		Datum indexValues[INDEX_MAX_KEYS];
		bool indexNulls[INDEX_MAX_KEYS];
		FormIndexDatum(indexInfo, slot, estate, indexValues, indexNulls);

		ItemPointerData itemPointerData = slot->tts_tid;

		/* currently, columnar tables can't have dead tuples */
		bool tupleIsAlive = true;
		indexCallback(indexRelation, &itemPointerData, indexValues, indexNulls,
					  tupleIsAlive, indexCallbackState);

		reltuples++;
	}

	return reltuples;
}


static void
columnar_index_validate_scan(Relation columnarRelation,
							 Relation indexRelation,
							 IndexInfo *indexInfo,
							 Snapshot snapshot,
							 ValidateIndexState *
							 validateIndexState)
{
	CheckCitusColumnarVersion(ERROR);

	ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
									 PROGRESS_SCAN_BLOCKS_TOTAL);

	/*
	 * Set up execution state for predicate, if any.
	 * Note that this is only useful for partial indexes.
	 */
	EState *estate = CreateExecutorState();
	ExprContext *econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = table_slot_create(columnarRelation, NULL);
	ExprState *predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	int nkeys = 0;
	ScanKeyData *scanKey = NULL;
	bool allowAccessStrategy = true;
	bool allowSyncScan = false;
	TableScanDesc scan = table_beginscan_strat(columnarRelation, snapshot, nkeys, scanKey,
											   allowAccessStrategy, allowSyncScan);

	ColumnarReadMissingRowsIntoIndex(scan, indexRelation, indexInfo, estate,
									 predicate, validateIndexState);

	table_endscan(scan);

	/* report the last "virtual" block as "done" */
	ColumnarReportTotalVirtualBlocks(columnarRelation, snapshot,
									 PROGRESS_SCAN_BLOCKS_DONE);

	ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
	FreeExecutorState(estate);
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}


/*
 * ColumnarReadMissingRowsIntoIndex inserts the tuples that are not in
 * the index yet by reading the actual relation based on given "scan".
 */
static void
ColumnarReadMissingRowsIntoIndex(TableScanDesc scan, Relation indexRelation,
								 IndexInfo *indexInfo, EState *estate,
								 ExprState *predicate,
								 ValidateIndexState *validateIndexState)
{
	BlockNumber lastReportedBlockNumber = InvalidBlockNumber;

	bool indexTupleSortEmpty = false;
	ItemPointerData indexedItemPointerData;
	ItemPointerSetInvalid(&indexedItemPointerData);

	ExprContext *econtext = GetPerTupleExprContext(estate);
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	while (columnar_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();

		ItemPointer columnarItemPointer = &slot->tts_tid;
		BlockNumber currentBlockNumber = ItemPointerGetBlockNumber(columnarItemPointer);
		if (lastReportedBlockNumber != currentBlockNumber)
		{
			/*
			 * columnar_getnextslot guarantees that returned tuple will
			 * always have a greater ItemPointer than the ones we fetched
			 * before, so we directly use BlockNumber to report our progress.
			 */
			Assert(lastReportedBlockNumber == InvalidBlockNumber ||
				   currentBlockNumber >= lastReportedBlockNumber);
			pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
										 currentBlockNumber);
			lastReportedBlockNumber = currentBlockNumber;
		}

		validateIndexState->htups += 1;

		if (!indexTupleSortEmpty &&
			(!ItemPointerIsValid(&indexedItemPointerData) ||
			 ItemPointerCompare(&indexedItemPointerData, columnarItemPointer) < 0))
		{
			/*
			 * Skip indexed item pointers until we find or pass the current
			 * columnar relation item pointer.
			 */
			indexedItemPointerData =
				TupleSortSkipSmallerItemPointers(validateIndexState->tuplesort,
												 columnarItemPointer);
			indexTupleSortEmpty = !ItemPointerIsValid(&indexedItemPointerData);
		}

		if (!indexTupleSortEmpty &&
			ItemPointerCompare(&indexedItemPointerData, columnarItemPointer) == 0)
		{
			/* tuple is already covered by the index, skip */
			continue;
		}
		Assert(indexTupleSortEmpty ||
			   ItemPointerCompare(&indexedItemPointerData, columnarItemPointer) > 0);

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (predicate != NULL && !ExecQual(predicate, econtext))
		{
			/* for partial indexes, discard tuples that don't satisfy the predicate */
			continue;
		}

		Datum indexValues[INDEX_MAX_KEYS];
		bool indexNulls[INDEX_MAX_KEYS];
		FormIndexDatum(indexInfo, slot, estate, indexValues, indexNulls);

		Relation columnarRelation = scan->rs_rd;
		IndexUniqueCheck indexUniqueCheck =
			indexInfo->ii_Unique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO;
		index_insert(indexRelation, indexValues, indexNulls, columnarItemPointer,
					 columnarRelation, indexUniqueCheck, false, indexInfo);

		validateIndexState->tups_inserted += 1;
	}
}


/*
 * TupleSortSkipSmallerItemPointers iterates given tupleSort until finding an
 * ItemPointer that is greater than or equal to given targetItemPointer and
 * returns that ItemPointer.
 * If such an ItemPointer does not exist, then returns invalid ItemPointer.
 *
 * Note that this function assumes given tupleSort doesn't have any NULL
 * Datum's.
 */
static ItemPointerData
TupleSortSkipSmallerItemPointers(Tuplesortstate *tupleSort, ItemPointer targetItemPointer)
{
	ItemPointerData tsItemPointerData;
	ItemPointerSetInvalid(&tsItemPointerData);

	while (!ItemPointerIsValid(&tsItemPointerData) ||
		   ItemPointerCompare(&tsItemPointerData, targetItemPointer) < 0)
	{
		bool forwardDirection = true;
		Datum *abbrev = NULL;
		Datum tsDatum;
		bool tsDatumIsNull;
		if (!tuplesort_getdatum_compat(tupleSort, forwardDirection, false,
									   &tsDatum, &tsDatumIsNull, abbrev))
		{
			ItemPointerSetInvalid(&tsItemPointerData);
			break;
		}

		Assert(!tsDatumIsNull);
		itemptr_decode(&tsItemPointerData, DatumGetInt64(tsDatum));

#ifndef USE_FLOAT8_BYVAL

		/*
		 * If int8 is pass-by-ref, we need to free Datum memory.
		 * See tuplesort_getdatum function's comment.
		 */
		pfree(DatumGetPointer(tsDatum));
#endif
	}

	return tsItemPointerData;
}


static uint64
columnar_relation_size(Relation rel, ForkNumber forkNumber)
{
	CheckCitusColumnarVersion(ERROR);

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


static bool
columnar_relation_needs_toast_table(Relation rel)
{
	CheckCitusColumnarVersion(ERROR);

	return false;
}


static void
columnar_estimate_rel_size(Relation rel, int32 *attr_widths,
						   BlockNumber *pages, double *tuples,
						   double *allvisfrac)
{
	CheckCitusColumnarVersion(ERROR);
	*pages = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);
	*tuples = ColumnarTableRowCount(rel);

	/*
	 * Append-only, so everything is visible except in-progress or rolled-back
	 * transactions.
	 */
	*allvisfrac = 1.0;

	get_rel_data_width(rel, attr_widths);
}


static bool
columnar_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "columnar_scan_sample_next_block not implemented");
}


static bool
columnar_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								TupleTableSlot *slot)
{
	elog(ERROR, "columnar_scan_sample_next_tuple not implemented");
}


static void
ColumnarXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
		{
			/* nothing to do */
			break;
		}

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		{
			DiscardWriteStateForAllRels(GetCurrentSubTransactionId(), 0);
			break;
		}

		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
		{
			FlushWriteStateForAllRels(GetCurrentSubTransactionId(), 0);
			break;
		}
	}
}


static void
ColumnarSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
						SubTransactionId parentSubid, void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
		case SUBXACT_EVENT_COMMIT_SUB:
		{
			/* nothing to do */
			break;
		}

		case SUBXACT_EVENT_ABORT_SUB:
		{
			DiscardWriteStateForAllRels(mySubid, parentSubid);
			break;
		}

		case SUBXACT_EVENT_PRE_COMMIT_SUB:
		{
			FlushWriteStateForAllRels(mySubid, parentSubid);
			break;
		}
	}
}


void
columnar_tableam_init()
{
	RegisterXactCallback(ColumnarXactCallback, NULL);
	RegisterSubXactCallback(ColumnarSubXactCallback, NULL);

	PrevObjectAccessHook = object_access_hook;
	object_access_hook = ColumnarTableAMObjectAccessHook;

	PrevProcessUtilityHook = ProcessUtility_hook ?
							 ProcessUtility_hook :
							 standard_ProcessUtility;
	ProcessUtility_hook = ColumnarProcessUtility;

	columnar_customscan_init();

	TTSOpsColumnar = TTSOpsVirtual;
	TTSOpsColumnar.copy_heap_tuple = ColumnarSlotCopyHeapTuple;
	DefineCustomBoolVariable(
		"columnar.enable_version_checks",
		gettext_noop("Enables Version Check for Columnar"),
		NULL,
		&EnableVersionChecksColumnar,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
}


/*
 * Get the number of chunks filtered out during the given scan.
 */
int64
ColumnarScanChunkGroupsFiltered(ColumnarScanDesc columnarScanDesc)
{
	ColumnarReadState *readState = columnarScanDesc->cs_readState;

	/* readState is initialized lazily */
	if (readState != NULL)
	{
		return ColumnarReadChunkGroupsFiltered(readState);
	}
	else
	{
		return 0;
	}
}


/*
 * Implementation of TupleTableSlotOps.copy_heap_tuple for TTSOpsColumnar.
 */
static HeapTuple
ColumnarSlotCopyHeapTuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	HeapTuple tuple = heap_form_tuple(slot->tts_tupleDescriptor,
									  slot->tts_values,
									  slot->tts_isnull);

	/* slot->tts_tid is filled in columnar_getnextslot */
	tuple->t_self = slot->tts_tid;

	return tuple;
}


/*
 * ColumnarTableDropHook
 *
 * Clean-up resources for columnar tables.
 */
static void
ColumnarTableDropHook(Oid relid)
{
	/*
	 * Lock relation to prevent it from being dropped and to avoid
	 * race conditions in the next if block.
	 */
	LockRelationOid(relid, AccessShareLock);

	if (IsColumnarTableAmTable(relid))
	{
		CheckCitusColumnarVersion(ERROR);

		/*
		 * Drop metadata. No need to drop storage here since for
		 * tableam tables storage is managed by postgres.
		 */
		Relation rel = table_open(relid, AccessExclusiveLock);
		RelFileLocator relfilelocator = RelationPhysicalIdentifier_compat(rel);

		DeleteMetadataRows(relfilelocator);
		DeleteColumnarTableOptions(rel->rd_id, true);

		MarkRelfilenumberDropped(RelationPhysicalIdentifierNumber_compat(relfilelocator),
								 GetCurrentSubTransactionId());

		/* keep the lock since we did physical changes to the relation */
		table_close(rel, NoLock);
	}
}


/*
 * Reject AFTER ... FOR EACH ROW triggers on columnar tables.
 */
static void
ColumnarTriggerCreateHook(Oid tgid)
{
	/*
	 * Fetch the pg_trigger tuple by the Oid of the trigger
	 */
	ScanKeyData skey[1];
	Relation tgrel = table_open(TriggerRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_trigger_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(tgid));

	SysScanDesc tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true,
											SnapshotSelf, 1, skey);

	HeapTuple tgtup = systable_getnext(tgscan);

	if (!HeapTupleIsValid(tgtup))
	{
		table_close(tgrel, AccessShareLock);
		return;
	}

	Form_pg_trigger tgrec = (Form_pg_trigger) GETSTRUCT(tgtup);

	Oid tgrelid = tgrec->tgrelid;
	int16 tgtype = tgrec->tgtype;

	systable_endscan(tgscan);
	table_close(tgrel, AccessShareLock);

	if (TRIGGER_FOR_ROW(tgtype) && TRIGGER_FOR_AFTER(tgtype) &&
		IsColumnarTableAmTable(tgrelid))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"Foreign keys and AFTER ROW triggers are not supported for columnar tables"),
						errhint("Consider an AFTER STATEMENT trigger instead.")));
	}
}


/*
 * Capture create/drop events and dispatch to the proper action.
 */
static void
ColumnarTableAMObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
								int subId, void *arg)
{
	if (PrevObjectAccessHook)
	{
		PrevObjectAccessHook(access, classId, objectId, subId, arg);
	}

	/* dispatch to the proper action */
	if (access == OAT_DROP && classId == RelationRelationId && !OidIsValid(subId))
	{
		ColumnarTableDropHook(objectId);
	}
	else if (access == OAT_POST_CREATE && classId == TriggerRelationId)
	{
		ColumnarTriggerCreateHook(objectId);
	}
}


/*
 * ColumnarProcessAlterTable - if modifying a columnar table, extract columnar
 * options and return the table's RangeVar.
 */
static RangeVar *
ColumnarProcessAlterTable(AlterTableStmt *alterTableStmt, List **columnarOptions)
{
	RangeVar *columnarRangeVar = NULL;
	Relation rel = relation_openrv_extended(alterTableStmt->relation, AccessShareLock,
											alterTableStmt->missing_ok);

	if (rel == NULL)
	{
		return NULL;
	}

	/* track separately in case of ALTER TABLE ... SET ACCESS METHOD */
	bool srcIsColumnar = rel->rd_tableam == GetColumnarTableAmRoutine();
	bool destIsColumnar = srcIsColumnar;

	ListCell *lc = NULL;
	foreach(lc, alterTableStmt->cmds)
	{
		AlterTableCmd *alterTableCmd = castNode(AlterTableCmd, lfirst(lc));

		if (alterTableCmd->subtype == AT_SetRelOptions ||
			alterTableCmd->subtype == AT_ResetRelOptions)
		{
			List *options = castNode(List, alterTableCmd->def);

			alterTableCmd->def = (Node *) ExtractColumnarRelOptions(
				options, columnarOptions);

			if (destIsColumnar)
			{
				columnarRangeVar = alterTableStmt->relation;
			}
		}
		else if (alterTableCmd->subtype == AT_SetAccessMethod)
		{
			if (columnarRangeVar || *columnarOptions)
			{
				ereport(ERROR, (errmsg(
									"ALTER TABLE cannot alter the access method after altering storage parameters"),
								errhint(
									"Specify SET ACCESS METHOD before storage parameters, or use separate ALTER TABLE commands.")));
			}

			destIsColumnar = (strcmp(alterTableCmd->name ? alterTableCmd->name :
									 default_table_access_method,
									 COLUMNAR_AM_NAME) == 0);

			if (srcIsColumnar && !destIsColumnar)
			{
				DeleteColumnarTableOptions(RelationGetRelid(rel), true);
			}
		}
	}

	relation_close(rel, NoLock);

	return columnarRangeVar;
}


/*
 * Utility hook for columnar tables.
 */
static void
ColumnarProcessUtility(PlannedStmt *pstmt,
					   const char *queryString,
					   bool readOnlyTree,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   struct QueryEnvironment *queryEnv,
					   DestReceiver *dest,
					   QueryCompletion *completionTag)
{
	if (readOnlyTree)
	{
		pstmt = copyObject(pstmt);
	}

	Node *parsetree = pstmt->utilityStmt;

	RangeVar *columnarRangeVar = NULL;
	List *columnarOptions = NIL;

	switch (nodeTag(parsetree))
	{
		case T_IndexStmt:
		{
			IndexStmt *indexStmt = (IndexStmt *) parsetree;

			Relation rel = relation_openrv(indexStmt->relation,
										   indexStmt->concurrent ?
										   ShareUpdateExclusiveLock :
										   ShareLock);

			if (rel->rd_tableam == GetColumnarTableAmRoutine())
			{
				CheckCitusColumnarVersion(ERROR);
				if (!ColumnarSupportsIndexAM(indexStmt->accessMethod))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("unsupported access method for the "
										   "index on columnar table %s",
										   RelationGetRelationName(rel))));
				}
			}

			RelationClose(rel);
			break;
		}

		case T_CreateStmt:
		{
			CreateStmt *createStmt = castNode(CreateStmt, parsetree);
			bool no_op = false;

			if (createStmt->if_not_exists)
			{
				Oid existing_relid;

				/* use same check as transformCreateStmt */
				(void) RangeVarGetAndCheckCreationNamespace(
					createStmt->relation, AccessShareLock, &existing_relid);

				no_op = OidIsValid(existing_relid);
			}

			if (!no_op && createStmt->accessMethod != NULL &&
				!strcmp(createStmt->accessMethod, COLUMNAR_AM_NAME))
			{
				columnarRangeVar = createStmt->relation;
				createStmt->options = ExtractColumnarRelOptions(createStmt->options,
																&columnarOptions);
			}
			break;
		}

		case T_CreateTableAsStmt:
		{
			CreateTableAsStmt *createTableAsStmt = castNode(CreateTableAsStmt, parsetree);
			IntoClause *into = createTableAsStmt->into;
			bool no_op = false;

			if (createTableAsStmt->if_not_exists)
			{
				Oid existing_relid;

				/* use same check as transformCreateStmt */
				(void) RangeVarGetAndCheckCreationNamespace(
					into->rel, AccessShareLock, &existing_relid);

				no_op = OidIsValid(existing_relid);
			}

			if (!no_op && into->accessMethod != NULL &&
				!strcmp(into->accessMethod, COLUMNAR_AM_NAME))
			{
				columnarRangeVar = into->rel;
				into->options = ExtractColumnarRelOptions(into->options,
														  &columnarOptions);
			}
			break;
		}

		case T_AlterTableStmt:
		{
			AlterTableStmt *alterTableStmt = castNode(AlterTableStmt, parsetree);
			columnarRangeVar = ColumnarProcessAlterTable(alterTableStmt,
														 &columnarOptions);
			break;
		}

		default:

			/* FALL THROUGH */
			break;
	}

	if (columnarOptions != NIL && columnarRangeVar == NULL)
	{
		ereport(ERROR,
				(errmsg("columnar storage parameters specified on non-columnar table")));
	}

	if (IsA(parsetree, CreateExtensionStmt))
	{
		CheckCitusColumnarCreateExtensionStmt(parsetree);
	}

	if (IsA(parsetree, AlterExtensionStmt))
	{
		CheckCitusColumnarAlterExtensionStmt(parsetree);
	}

	PrevProcessUtilityHook(pstmt, queryString, false, context,
						   params, queryEnv, dest, completionTag);

	if (columnarOptions != NIL)
	{
		SetColumnarRelOptions(columnarRangeVar, columnarOptions);
	}
}


/*
 * ColumnarSupportsIndexAM returns true if indexAM with given name is
 * supported by columnar tables.
 */
bool
ColumnarSupportsIndexAM(char *indexAMName)
{
	return strncmp(indexAMName, "btree", NAMEDATALEN) == 0 ||
		   strncmp(indexAMName, "hash", NAMEDATALEN) == 0;
}


/*
 * IsColumnarTableAmTable returns true if relation has columnar_tableam
 * access method. This can be called before extension creation.
 */
bool
IsColumnarTableAmTable(Oid relationId)
{
	if (!OidIsValid(relationId))
	{
		return false;
	}

	/*
	 * Lock relation to prevent it from being dropped &
	 * avoid race conditions.
	 */
	Relation rel = relation_open(relationId, AccessShareLock);
	bool result = rel->rd_tableam == GetColumnarTableAmRoutine();
	relation_close(rel, NoLock);

	return result;
}


/*
 * CheckCitusColumnarCreateExtensionStmt determines whether can install
 * citus_columnar per given CREATE extension statment
 */
void
CheckCitusColumnarCreateExtensionStmt(Node *parseTree)
{
	CreateExtensionStmt *createExtensionStmt = castNode(CreateExtensionStmt,
														parseTree);
	if (get_extension_oid("citus_columnar", true) == InvalidOid)
	{
		if (strcmp(createExtensionStmt->extname, "citus_columnar") == 0)
		{
			DefElem *newVersionValue = GetExtensionOption(
				createExtensionStmt->options,
				"new_version");

			/*we are not allowed to install citus_columnar as version 11.1-0 by cx*/
			if (newVersionValue)
			{
				const char *newVersion = defGetString(newVersionValue);
				if (strcmp(newVersion, CITUS_COLUMNAR_INTERNAL_VERSION) == 0)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg(
										"unsupported citus_columnar version 11.1-0")));
				}
			}
		}
	}
}


/*
 * CheckCitusColumnarAlterExtensionStmt determines whether can alter
 * citus_columnar per given ALTER extension statment
 */
void
CheckCitusColumnarAlterExtensionStmt(Node *parseTree)
{
	AlterExtensionStmt *alterExtensionStmt = castNode(AlterExtensionStmt, parseTree);
	if (strcmp(alterExtensionStmt->extname, "citus_columnar") == 0)
	{
		DefElem *newVersionValue = GetExtensionOption(alterExtensionStmt->options,
													  "new_version");

		/*we are not allowed cx to downgrade citus_columnar to 11.1-0*/
		if (newVersionValue)
		{
			const char *newVersion = defGetString(newVersionValue);
			if (strcmp(newVersion, CITUS_COLUMNAR_INTERNAL_VERSION) == 0)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("unsupported citus_columnar version 11.1-0")));
			}
		}
	}
}


static const TableAmRoutine columnar_am_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = columnar_slot_callbacks,

	.scan_begin = columnar_beginscan,
	.scan_end = columnar_endscan,
	.scan_rescan = columnar_rescan,
	.scan_getnextslot = columnar_getnextslot,

	.parallelscan_estimate = columnar_parallelscan_estimate,
	.parallelscan_initialize = columnar_parallelscan_initialize,
	.parallelscan_reinitialize = columnar_parallelscan_reinitialize,

	.index_fetch_begin = columnar_index_fetch_begin,
	.index_fetch_reset = columnar_index_fetch_reset,
	.index_fetch_end = columnar_index_fetch_end,
	.index_fetch_tuple = columnar_index_fetch_tuple,

	.tuple_fetch_row_version = columnar_fetch_row_version,
	.tuple_get_latest_tid = columnar_get_latest_tid,
	.tuple_tid_valid = columnar_tuple_tid_valid,
	.tuple_satisfies_snapshot = columnar_tuple_satisfies_snapshot,
	.index_delete_tuples = columnar_index_delete_tuples,

	.tuple_insert = columnar_tuple_insert,
	.tuple_insert_speculative = columnar_tuple_insert_speculative,
	.tuple_complete_speculative = columnar_tuple_complete_speculative,
	.multi_insert = columnar_multi_insert,
	.tuple_delete = columnar_tuple_delete,
	.tuple_update = columnar_tuple_update,
	.tuple_lock = columnar_tuple_lock,
	.finish_bulk_insert = columnar_finish_bulk_insert,

#if PG_VERSION_NUM >= PG_VERSION_16
	.relation_set_new_filelocator = columnar_relation_set_new_filelocator,
#else
	.relation_set_new_filenode = columnar_relation_set_new_filelocator,
#endif
	.relation_nontransactional_truncate = columnar_relation_nontransactional_truncate,
	.relation_copy_data = columnar_relation_copy_data,
	.relation_copy_for_cluster = columnar_relation_copy_for_cluster,
	.relation_vacuum = columnar_vacuum_rel,
	.scan_analyze_next_block = columnar_scan_analyze_next_block,
	.scan_analyze_next_tuple = columnar_scan_analyze_next_tuple,
	.index_build_range_scan = columnar_index_build_range_scan,
	.index_validate_scan = columnar_index_validate_scan,

	.relation_size = columnar_relation_size,
	.relation_needs_toast_table = columnar_relation_needs_toast_table,

	.relation_estimate_size = columnar_estimate_rel_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = columnar_scan_sample_next_block,
	.scan_sample_next_tuple = columnar_scan_sample_next_tuple
};


const TableAmRoutine *
GetColumnarTableAmRoutine(void)
{
	return &columnar_am_methods;
}


PG_FUNCTION_INFO_V1(columnar_handler);
Datum
columnar_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&columnar_am_methods);
}


/*
 * detoast_values
 *
 * Detoast and decompress all values. If there's no work to do, return
 * original pointer; otherwise return a newly-allocated values array. Should
 * be called in per-tuple context.
 */
static Datum *
detoast_values(TupleDesc tupleDesc, Datum *orig_values, bool *isnull)
{
	int natts = tupleDesc->natts;

	/* copy on write to optimize for case where nothing is toasted */
	Datum *values = orig_values;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		if (!isnull[i] && tupleDesc->attrs[i].attlen == -1 &&
			VARATT_IS_EXTENDED(values[i]))
		{
			/* make a copy */
			if (values == orig_values)
			{
				values = palloc(sizeof(Datum) * natts);

				/*
				 * We use IGNORE-BANNED here since we don't want to limit
				 * size of the buffer that holds the datum array to RSIZE_MAX
				 * unnecessarily.
				 */
				memcpy(values, orig_values, sizeof(Datum) * natts); /* IGNORE-BANNED */
			}

			/* will be freed when per-tuple context is reset */
			struct varlena *new_value = (struct varlena *) DatumGetPointer(values[i]);
			new_value = detoast_attr(new_value);
			values[i] = PointerGetDatum(new_value);
		}
	}

	return values;
}


/*
 * ColumnarCheckLogicalReplication throws an error if the relation is
 * part of any publication. This should be called before any write to
 * a columnar table, because columnar changes are not replicated with
 * logical replication (similar to a row table without a replica
 * identity).
 */
static void
ColumnarCheckLogicalReplication(Relation rel)
{
	bool pubActionInsert = false;

	if (!is_publishable_relation(rel))
	{
		return;
	}

	{
		PublicationDesc pubdesc;

		RelationBuildPublicationDesc(rel, &pubdesc);
		pubActionInsert = pubdesc.pubactions.pubinsert;
	}

	if (pubActionInsert)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"cannot insert into columnar table that is a part of a publication")));
	}
}


/*
 * alter_columnar_table_set()
 *
 * Deprecated in 11.1-1: should issue ALTER TABLE ... SET instead. Function
 * still available, but implemented in PL/pgSQL instead of C.
 *
 * C code is removed -- the symbol may still be required in some
 * upgrade/downgrade paths, but it should not be called.
 */
PG_FUNCTION_INFO_V1(alter_columnar_table_set);
Datum
alter_columnar_table_set(PG_FUNCTION_ARGS)
{
	elog(ERROR, "alter_columnar_table_set is deprecated");
}


/*
 * alter_columnar_table_reset()
 *
 * Deprecated in 11.1-1: should issue ALTER TABLE ... RESET instead. Function
 * still available, but implemented in PL/pgSQL instead of C.
 *
 * C code is removed -- the symbol may still be required in some
 * upgrade/downgrade paths, but it should not be called.
 */
PG_FUNCTION_INFO_V1(alter_columnar_table_reset);
Datum
alter_columnar_table_reset(PG_FUNCTION_ARGS)
{
	elog(ERROR, "alter_columnar_table_reset is deprecated");
}


/*
 * upgrade_columnar_storage - upgrade columnar storage to the current
 * version.
 *
 * DDL:
 *   CREATE OR REPLACE FUNCTION upgrade_columnar_storage(rel regclass)
 *     RETURNS VOID
 *     STRICT
 *     LANGUAGE c AS 'MODULE_PATHNAME', 'upgrade_columnar_storage';
 */
PG_FUNCTION_INFO_V1(upgrade_columnar_storage);
Datum
upgrade_columnar_storage(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	/*
	 * ACCESS EXCLUSIVE LOCK is not required by the low-level routines, so we
	 * can take only an ACCESS SHARE LOCK. But all access to non-current
	 * columnar tables will fail anyway, so it's better to take ACCESS
	 * EXCLUSIVE LOCK now.
	 */
	Relation rel = table_open(relid, AccessExclusiveLock);
	if (!IsColumnarTableAmTable(relid))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	ColumnarStorageUpdateIfNeeded(rel, true);

	table_close(rel, AccessExclusiveLock);
	PG_RETURN_VOID();
}


/*
 * downgrade_columnar_storage - downgrade columnar storage to the
 * current version.
 *
 * DDL:
 *   CREATE OR REPLACE FUNCTION downgrade_columnar_storage(rel regclass)
 *     RETURNS VOID
 *     STRICT
 *     LANGUAGE c AS 'MODULE_PATHNAME', 'downgrade_columnar_storage';
 */
PG_FUNCTION_INFO_V1(downgrade_columnar_storage);
Datum
downgrade_columnar_storage(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	/*
	 * ACCESS EXCLUSIVE LOCK is not required by the low-level routines, so we
	 * can take only an ACCESS SHARE LOCK. But all access to non-current
	 * columnar tables will fail anyway, so it's better to take ACCESS
	 * EXCLUSIVE LOCK now.
	 */
	Relation rel = table_open(relid, AccessExclusiveLock);
	if (!IsColumnarTableAmTable(relid))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	ColumnarStorageUpdateIfNeeded(rel, false);

	table_close(rel, AccessExclusiveLock);
	PG_RETURN_VOID();
}


/*
 * Code to check the Citus Version, helps remove dependency from Citus
 */

/*
 * CitusColumnarHasBeenLoaded returns true if the citus extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false. The result is cached as this is called very frequently.
 */
bool
CitusColumnarHasBeenLoaded(void)
{
	if (!extensionLoadedColumnar || creating_extension)
	{
		/*
		 * Refresh if we have not determined whether the extension has been
		 * loaded yet, or in case of ALTER EXTENSION since we want to treat
		 * Citus as "not loaded" during ALTER EXTENSION citus.
		 */
		bool extensionLoaded = CitusColumnarHasBeenLoadedInternal();
		extensionLoadedColumnar = extensionLoaded;
	}

	return extensionLoadedColumnar;
}


/*
 * CitusColumnarHasBeenLoadedInternal returns true if the citus extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false.
 */
static bool
CitusColumnarHasBeenLoadedInternal(void)
{
	if (IsBinaryUpgrade)
	{
		/* never use Citus logic during pg_upgrade */
		return false;
	}

	Oid citusExtensionOid = get_extension_oid("citus", true);
	if (citusExtensionOid == InvalidOid)
	{
		/* Citus extension does not exist yet */
		return false;
	}

	if (creating_extension && CurrentExtensionObject == citusExtensionOid)
	{
		/*
		 * We do not use Citus hooks during CREATE/ALTER EXTENSION citus
		 * since the objects used by the C code might be not be there yet.
		 */
		return false;
	}

	/* citus extension exists and has been created */
	return true;
}


/*
 * CheckCitusColumnarVersion checks whether there is a version mismatch between the
 * available version and the loaded version or between the installed version
 * and the loaded version. Returns true if compatible, false otherwise.
 *
 * As a side effect, this function also sets citusVersionKnownCompatible_Columnar global
 * variable to true which reduces version check cost of next calls.
 */
bool
CheckCitusColumnarVersion(int elevel)
{
	if (citusVersionKnownCompatibleColumnar ||
		!CitusColumnarHasBeenLoaded() ||
		!EnableVersionChecksColumnar)
	{
		return true;
	}

	if (CheckAvailableVersionColumnar(elevel) && CheckInstalledVersionColumnar(elevel))
	{
		citusVersionKnownCompatibleColumnar = true;
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * CheckAvailableVersion compares CITUS_EXTENSIONVERSION and the currently
 * available version from the citus.control file. If they are not compatible,
 * this function logs an error with the specified elevel and returns false,
 * otherwise it returns true.
 */
bool
CheckAvailableVersionColumnar(int elevel)
{
	if (!EnableVersionChecksColumnar)
	{
		return true;
	}

	char *availableVersion = AvailableExtensionVersionColumnar();

	if (!MajorVersionsCompatibleColumnar(availableVersion, CITUS_EXTENSIONVERSION))
	{
		ereport(elevel, (errmsg("loaded Citus library version differs from latest "
								"available extension version"),
						 errdetail("Loaded library requires %s, but the latest control "
								   "file specifies %s.", CITUS_MAJORVERSION,
								   availableVersion),
						 errhint("Restart the database to load the latest Citus "
								 "library.")));
		pfree(availableVersion);
		return false;
	}
	pfree(availableVersion);
	return true;
}


/*
 * CheckInstalledVersion compares CITUS_EXTENSIONVERSION and the
 * extension's current version from the pg_extension catalog table. If they
 * are not compatible, this function logs an error with the specified elevel,
 * otherwise it returns true.
 */
static bool
CheckInstalledVersionColumnar(int elevel)
{
	Assert(CitusColumnarHasBeenLoaded());
	Assert(EnableVersionChecksColumnar);

	char *installedVersion = InstalledExtensionVersionColumnar();

	if (!MajorVersionsCompatibleColumnar(installedVersion, CITUS_EXTENSIONVERSION))
	{
		ereport(elevel, (errmsg("loaded Citus library version differs from installed "
								"extension version"),
						 errdetail("Loaded library requires %s, but the installed "
								   "extension version is %s.", CITUS_MAJORVERSION,
								   installedVersion),
						 errhint("Run ALTER EXTENSION citus UPDATE and try again.")));
		pfree(installedVersion);
		return false;
	}
	pfree(installedVersion);
	return true;
}


/*
 * MajorVersionsCompatible checks whether both versions are compatible. They
 * are if major and minor version numbers match, the schema version is
 * ignored.  Returns true if compatible, false otherwise.
 */
bool
MajorVersionsCompatibleColumnar(char *leftVersion, char *rightVersion)
{
	const char schemaVersionSeparator = '-';

	char *leftSeperatorPosition = strchr(leftVersion, schemaVersionSeparator);
	char *rightSeperatorPosition = strchr(rightVersion, schemaVersionSeparator);
	int leftComparisionLimit = 0;
	int rightComparisionLimit = 0;

	if (leftSeperatorPosition != NULL)
	{
		leftComparisionLimit = leftSeperatorPosition - leftVersion;
	}
	else
	{
		leftComparisionLimit = strlen(leftVersion);
	}

	if (rightSeperatorPosition != NULL)
	{
		rightComparisionLimit = rightSeperatorPosition - rightVersion;
	}
	else
	{
		rightComparisionLimit = strlen(rightVersion);
	}

	/* we can error out early if hypens are not in the same position */
	if (leftComparisionLimit != rightComparisionLimit)
	{
		return false;
	}

	return strncmp(leftVersion, rightVersion, leftComparisionLimit) == 0;
}


/*
 * AvailableExtensionVersion returns the Citus version from citus.control file. It also
 * saves the result, thus consecutive calls to CitusExtensionAvailableVersion will
 * not read the citus.control file again.
 */
static char *
AvailableExtensionVersionColumnar(void)
{
	LOCAL_FCINFO(fcinfo, 0);
	FmgrInfo flinfo;

	bool goForward = true;
	bool doCopy = false;
	char *availableExtensionVersion;

	EState *estate = CreateExecutorState();
	ReturnSetInfo *extensionsResultSet = makeNode(ReturnSetInfo);
	extensionsResultSet->econtext = GetPerTupleExprContext(estate);
	extensionsResultSet->allowedModes = SFRM_Materialize;

	fmgr_info(F_PG_AVAILABLE_EXTENSIONS, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 0, InvalidOid, NULL,
							 (Node *) extensionsResultSet);

	/* pg_available_extensions returns result set containing all available extensions */
	(*pg_available_extensions)(fcinfo);

	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(
		extensionsResultSet->setDesc,
		&TTSOpsMinimalTuple);
	bool hasTuple = tuplestore_gettupleslot(extensionsResultSet->setResult, goForward,
											doCopy,
											tupleTableSlot);
	while (hasTuple)
	{
		bool isNull = false;

		Datum extensionNameDatum = slot_getattr(tupleTableSlot, 1, &isNull);
		char *extensionName = NameStr(*DatumGetName(extensionNameDatum));
		if (strcmp(extensionName, "citus") == 0)
		{
			Datum availableVersion = slot_getattr(tupleTableSlot, 2, &isNull);


			availableExtensionVersion = text_to_cstring(DatumGetTextPP(availableVersion));


			ExecClearTuple(tupleTableSlot);
			ExecDropSingleTupleTableSlot(tupleTableSlot);

			return availableExtensionVersion;
		}

		ExecClearTuple(tupleTableSlot);
		hasTuple = tuplestore_gettupleslot(extensionsResultSet->setResult, goForward,
										   doCopy, tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("citus extension is not found")));

	return NULL; /* keep compiler happy */
}


/*
 * InstalledExtensionVersion returns the Citus version in PostgreSQL pg_extension table.
 */
static char *
InstalledExtensionVersionColumnar(void)
{
	ScanKeyData entry[1];
	char *installedExtensionVersion = NULL;

	Relation relation = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0], Anum_pg_extension_extname, BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("citus"));

	SysScanDesc scandesc = systable_beginscan(relation, ExtensionNameIndexId, true,
											  NULL, 1, entry);

	HeapTuple extensionTuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(extensionTuple))
	{
		int extensionIndex = Anum_pg_extension_extversion;
		TupleDesc tupleDescriptor = RelationGetDescr(relation);
		bool isNull = false;

		Datum installedVersion = heap_getattr(extensionTuple, extensionIndex,
											  tupleDescriptor, &isNull);

		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("citus extension version is null")));
		}


		installedExtensionVersion = text_to_cstring(DatumGetTextPP(installedVersion));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("citus extension is not loaded")));
	}

	systable_endscan(scandesc);

	table_close(relation, AccessShareLock);

	return installedExtensionVersion;
}


/*
 * GetExtensionOption returns DefElem * node with "defname" from "options" list
 */
DefElem *
GetExtensionOption(List *extensionOptions, const char *defname)
{
	DefElem *defElement = NULL;
	foreach_declared_ptr(defElement, extensionOptions)
	{
		if (IsA(defElement, DefElem) &&
			strncmp(defElement->defname, defname, NAMEDATALEN) == 0)
		{
			return defElement;
		}
	}

	return NULL;
}
