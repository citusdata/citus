#include "citus_version.h"
#if USE_TABLEAM

#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#if PG_VERSION_NUM >= 130000
#include "access/heaptoast.h"
#else
#include "access/tuptoaster.h"
#endif
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_trigger.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
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
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "columnar/cstore.h"
#include "columnar/cstore_customscan.h"
#include "columnar/cstore_tableam.h"
#include "columnar/cstore_version_compat.h"

#define CSTORE_TABLEAM_NAME "cstore_tableam"

/*
 * Timing parameters for truncate locking heuristics.
 *
 * These are the same values from src/backend/access/heap/vacuumlazy.c
 */
#define VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL 50       /* ms */
#define VACUUM_TRUNCATE_LOCK_TIMEOUT 5000               /* ms */

typedef struct CStoreScanDescData
{
	TableScanDescData cs_base;
	TableReadState *cs_readState;
} CStoreScanDescData;

typedef struct CStoreScanDescData *CStoreScanDesc;

static TableWriteState *CStoreWriteState = NULL;
static ExecutorEnd_hook_type PreviousExecutorEndHook = NULL;
static MemoryContext CStoreContext = NULL;
static object_access_hook_type prevObjectAccessHook = NULL;
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

/* forward declaration for static functions */
static void CStoreTableAMObjectAccessHook(ObjectAccessType access, Oid classId, Oid
										  objectId, int subId,
										  void *arg);
#if PG_VERSION_NUM >= 130000
static void CStoreTableAMProcessUtility(PlannedStmt *plannedStatement,
										const char *queryString,
										ProcessUtilityContext context,
										ParamListInfo paramListInfo,
										QueryEnvironment *queryEnvironment,
										DestReceiver *destReceiver,
										QueryCompletion *qc);
#else
static void CStoreTableAMProcessUtility(PlannedStmt *plannedStatement,
										const char *queryString,
										ProcessUtilityContext context,
										ParamListInfo paramListInfo,
										QueryEnvironment *queryEnvironment,
										DestReceiver *destReceiver,
										char *completionTag);
#endif

static bool IsCStoreTableAmTable(Oid relationId);
static bool ConditionalLockRelationWithTimeout(Relation rel, LOCKMODE lockMode,
											   int timeout, int retryInterval);
static void LogRelationStats(Relation rel, int elevel);
static void TruncateCStore(Relation rel, int elevel);


/*
 * CStoreTableAMDefaultOptions returns the default options for a cstore table am table.
 * These options are based on the GUC's controlling the defaults.
 */
static CStoreOptions *
CStoreTableAMDefaultOptions()
{
	CStoreOptions *cstoreOptions = palloc0(sizeof(CStoreOptions));
	cstoreOptions->compressionType = cstore_compression;
	cstoreOptions->stripeRowCount = cstore_stripe_row_count;
	cstoreOptions->blockRowCount = cstore_block_row_count;
	return cstoreOptions;
}


/*
 * CStoreTableAMGetOptions returns the options based on a relation. It is advised the
 * relation is a cstore table am table, if not it will raise an error
 */
static CStoreOptions *
CStoreTableAMGetOptions(Relation rel)
{
	CStoreOptions *cstoreOptions = NULL;
	DataFileMetadata *metadata = NULL;

	Assert(rel != NULL);

	cstoreOptions = palloc0(sizeof(CStoreOptions));
	metadata = ReadDataFileMetadata(rel->rd_node.relNode, false);
	cstoreOptions->compressionType = metadata->compression;
	cstoreOptions->stripeRowCount = metadata->stripeRowCount;
	cstoreOptions->blockRowCount = metadata->blockRowCount;
	return cstoreOptions;
}


static MemoryContext
GetCStoreMemoryContext()
{
	if (CStoreContext == NULL)
	{
		CStoreContext = AllocSetContextCreate(TopMemoryContext, "cstore context",
											  ALLOCSET_DEFAULT_SIZES);
	}
	return CStoreContext;
}


static void
ResetCStoreMemoryContext()
{
	if (CStoreContext != NULL)
	{
		MemoryContextReset(CStoreContext);
	}
}


static void
cstore_init_write_state(Relation relation)
{
	if (CStoreWriteState != NULL)
	{
		/* TODO: consider whether it's possible for a new write to start */
		/* before an old one is flushed */
		Assert(CStoreWriteState->relation->rd_id == relation->rd_id);
	}

	if (CStoreWriteState == NULL)
	{
		CStoreOptions *cstoreOptions = CStoreTableAMGetOptions(relation);
		TupleDesc tupdesc = RelationGetDescr(relation);

		elog(LOG, "initializing write state for relation %d", relation->rd_id);
		CStoreWriteState = CStoreBeginWrite(relation,
											cstoreOptions->compressionType,
											cstoreOptions->stripeRowCount,
											cstoreOptions->blockRowCount,
											tupdesc);
	}
}


static void
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


static List *
RelationColumnList(Relation rel)
{
	List *columnList = NIL;
	TupleDesc tupdesc = RelationGetDescr(rel);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Index varno = 1;
		AttrNumber varattno = i + 1;
		Oid vartype = tupdesc->attrs[i].atttypid;
		int32 vartypmod = tupdesc->attrs[i].atttypmod;
		Oid varcollid = tupdesc->attrs[i].attcollation;
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

	return columnList;
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
	TableScanDesc scandesc;
	int natts = relation->rd_att->natts;
	Bitmapset *attr_needed = NULL;

	attr_needed = bms_add_range(attr_needed, 0, natts - 1);

	/* the cstore access method does not use the flags, they are specific to heap */
	flags = 0;

	scandesc = cstore_beginscan_extended(relation, snapshot, nkeys, key, parallel_scan,
										 flags, attr_needed, NULL);

	pfree(attr_needed);

	return scandesc;
}


TableScanDesc
cstore_beginscan_extended(Relation relation, Snapshot snapshot,
						  int nkeys, ScanKey key,
						  ParallelTableScanDesc parallel_scan,
						  uint32 flags, Bitmapset *attr_needed, List *scanQual)
{
	TupleDesc tupdesc = relation->rd_att;
	TableReadState *readState = NULL;
	CStoreScanDesc scan = palloc(sizeof(CStoreScanDescData));
	List *columnList = NIL;
	List *neededColumnList = NIL;
	MemoryContext oldContext = MemoryContextSwitchTo(GetCStoreMemoryContext());
	ListCell *columnCell = NULL;

	scan->cs_base.rs_rd = relation;
	scan->cs_base.rs_snapshot = snapshot;
	scan->cs_base.rs_nkeys = nkeys;
	scan->cs_base.rs_key = key;
	scan->cs_base.rs_flags = flags;
	scan->cs_base.rs_parallel = parallel_scan;

	columnList = RelationColumnList(relation);

	/* only collect columns that we need for the scan */
	foreach(columnCell, columnList)
	{
		Var *var = castNode(Var, lfirst(columnCell));
		if (bms_is_member(var->varattno - 1, attr_needed))
		{
			neededColumnList = lappend(neededColumnList, var);
		}
	}

	readState = CStoreBeginRead(relation, tupdesc, neededColumnList, scanQual);

	scan->cs_readState = readState;

	MemoryContextSwitchTo(oldContext);
	return ((TableScanDesc) scan);
}


static void
cstore_endscan(TableScanDesc sscan)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;
	CStoreEndRead(scan->cs_readState);
	scan->cs_readState = NULL;
}


static void
cstore_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
			  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;
	CStoreRescan(scan->cs_readState);
}


static bool
cstore_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;
	bool nextRowFound;
	MemoryContext oldContext = MemoryContextSwitchTo(GetCStoreMemoryContext());

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
	MemoryContext oldContext = MemoryContextSwitchTo(GetCStoreMemoryContext());

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
	MemoryContext oldContext = MemoryContextSwitchTo(GetCStoreMemoryContext());

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
	DataFileMetadata *metadata = ReadDataFileMetadata(rel->rd_node.relNode, true);
	uint64 blockRowCount = 0;
	uint64 stripeRowCount = 0;
	CompressionType compression = 0;

	if (metadata != NULL)
	{
		/* existing table (e.g. TRUNCATE), use existing blockRowCount */
		blockRowCount = metadata->blockRowCount;
		stripeRowCount = metadata->stripeRowCount;
		compression = metadata->compression;
	}
	else
	{
		/* new table, use options */
		CStoreOptions *options = CStoreTableAMDefaultOptions();
		blockRowCount = options->blockRowCount;
		stripeRowCount = options->stripeRowCount;
		compression = options->compressionType;
	}

	/* delete old relfilenode metadata */
	DeleteDataFileMetadataRowIfExists(rel->rd_node.relNode);

	Assert(persistence == RELPERSISTENCE_PERMANENT);
	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();
	srel = RelationCreateStorage(*newrnode, persistence);
	InitCStoreDataFileMetadata(newrnode->relNode, blockRowCount, stripeRowCount,
							   compression);
	smgrclose(srel);
}


static void
cstore_relation_nontransactional_truncate(Relation rel)
{
	DataFileMetadata *metadata = ReadDataFileMetadata(rel->rd_node.relNode, false);

	/*
	 * No need to set new relfilenode, since the table was created in this
	 * transaction and no other transaction can see this relation yet. We
	 * can just truncate the relation.
	 *
	 * This is similar to what is done in heapam_relation_nontransactional_truncate.
	 */
	RelationTruncate(rel, 0);

	/* Delete old relfilenode metadata and recreate it */
	DeleteDataFileMetadataRowIfExists(rel->rd_node.relNode);
	InitCStoreDataFileMetadata(rel->rd_node.relNode, metadata->blockRowCount,
							   metadata->stripeRowCount, metadata->compression);
}


static void
cstore_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "cstore_relation_copy_data not implemented");
}


/*
 * cstore_relation_copy_for_cluster is called on VACUUM FULL, at which
 * we should copy data from OldHeap to NewHeap.
 *
 * In general TableAM case this can also be called for the CLUSTER command
 * which is not applicable for cstore since it doesn't support indexes.
 */
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
	TableWriteState *writeState = NULL;
	TableReadState *readState = NULL;
	CStoreOptions *cstoreOptions = NULL;
	Datum *values = NULL;
	bool *nulls = NULL;
	TupleDesc sourceDesc = RelationGetDescr(OldHeap);
	TupleDesc targetDesc = RelationGetDescr(NewHeap);

	if (OldIndex != NULL || use_sort)
	{
		ereport(ERROR, (errmsg(CSTORE_TABLEAM_NAME " doesn't support indexes")));
	}

	/*
	 * copy_table_data in cluster.c assumes tuple descriptors are exactly
	 * the same. Even dropped columns exist and are marked as attisdropped
	 * in the target relation.
	 */
	Assert(sourceDesc->natts == targetDesc->natts);

	/*
	 * Since we are copying into a new relation we need to copy the settings from the old
	 * relation first.
	 */

	cstoreOptions = CStoreTableAMGetOptions(OldHeap);

	UpdateCStoreDataFileMetadata(NewHeap->rd_node.relNode,
								 cstoreOptions->blockRowCount,
								 cstoreOptions->stripeRowCount,
								 cstoreOptions->compressionType);

	cstoreOptions = CStoreTableAMGetOptions(NewHeap);

	writeState = CStoreBeginWrite(NewHeap,
								  cstoreOptions->compressionType,
								  cstoreOptions->stripeRowCount,
								  cstoreOptions->blockRowCount,
								  targetDesc);

	readState = CStoreBeginRead(OldHeap, sourceDesc, RelationColumnList(OldHeap), NULL);

	values = palloc0(sourceDesc->natts * sizeof(Datum));
	nulls = palloc0(sourceDesc->natts * sizeof(bool));

	*num_tuples = 0;

	while (CStoreReadNextRow(readState, values, nulls))
	{
		CStoreWriteRow(writeState, values, nulls);
		(*num_tuples)++;
	}

	*tups_vacuumed = 0;

	CStoreEndWrite(writeState);
	CStoreEndRead(readState);
}


/*
 * cstore_vacuum_rel implements VACUUM without FULL option.
 */
static void
cstore_vacuum_rel(Relation rel, VacuumParams *params,
				  BufferAccessStrategy bstrategy)
{
	int elevel = (params->options & VACOPT_VERBOSE) ? INFO : DEBUG2;

	/* this should have been resolved by vacuum.c until now */
	Assert(params->truncate != VACOPT_TERNARY_DEFAULT);

	LogRelationStats(rel, elevel);

	/*
	 * We don't have updates, deletes, or concurrent updates, so all we
	 * care for now is truncating the unused space at the end of storage.
	 */
	if (params->truncate == VACOPT_TERNARY_ENABLED)
	{
		TruncateCStore(rel, elevel);
	}
}


static void
LogRelationStats(Relation rel, int elevel)
{
	DataFileMetadata *datafileMetadata = NULL;
	ListCell *stripeMetadataCell = NULL;
	Oid relfilenode = rel->rd_node.relNode;
	StringInfo infoBuf = makeStringInfo();

	int compressionStats[COMPRESSION_COUNT] = { 0 };
	uint64 totalStripeLength = 0;
	uint64 tupleCount = 0;
	uint64 blockCount = 0;
	uint64 relPages = 0;
	int stripeCount = 0;
	TupleDesc tupdesc = RelationGetDescr(rel);
	uint64 droppedBlocksWithData = 0;

	datafileMetadata = ReadDataFileMetadata(relfilenode, false);
	stripeCount = list_length(datafileMetadata->stripeMetadataList);

	foreach(stripeMetadataCell, datafileMetadata->stripeMetadataList)
	{
		StripeMetadata *stripe = lfirst(stripeMetadataCell);
		StripeSkipList *skiplist = ReadStripeSkipList(relfilenode, stripe->id,
													  RelationGetDescr(rel),
													  stripe->blockCount);
		for (uint32 column = 0; column < skiplist->columnCount; column++)
		{
			bool attrDropped = tupdesc->attrs[column].attisdropped;
			for (uint32 block = 0; block < skiplist->blockCount; block++)
			{
				ColumnBlockSkipNode *skipnode =
					&skiplist->blockSkipNodeArray[column][block];

				/* ignore zero length blocks for dropped attributes */
				if (skipnode->valueLength > 0)
				{
					compressionStats[skipnode->valueCompressionType]++;
					blockCount++;

					if (attrDropped)
					{
						droppedBlocksWithData++;
					}
				}
			}
		}

		tupleCount += stripe->rowCount;
		totalStripeLength += stripe->dataLength;
	}

	RelationOpenSmgr(rel);
	relPages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	RelationCloseSmgr(rel);

	appendStringInfo(infoBuf, "total file size: %ld, total data size: %ld\n",
					 relPages * BLCKSZ, totalStripeLength);
	appendStringInfo(infoBuf,
					 "total row count: %ld, stripe count: %d, "
					 "average rows per stripe: %ld\n",
					 tupleCount, stripeCount, tupleCount / stripeCount);
	appendStringInfo(infoBuf,
					 "block count: %ld"
					 ", containing data for dropped columns: %ld",
					 blockCount, droppedBlocksWithData);
	for (int compressionType = 0; compressionType < COMPRESSION_COUNT; compressionType++)
	{
		appendStringInfo(infoBuf,
						 ", %s compressed: %d",
						 CompressionTypeStr(compressionType),
						 compressionStats[compressionType]);
	}
	appendStringInfoString(infoBuf, "\n");

	ereport(elevel, (errmsg("statistics for \"%s\":\n%s", RelationGetRelationName(rel),
							infoBuf->data)));
}


/*
 * TruncateCStore truncates the unused space at the end of main fork for
 * a cstore table. This unused space can be created by aborted transactions.
 *
 * This implementation is based on heap_vacuum_rel in vacuumlazy.c with some
 * changes so it suits columnar store relations.
 */
static void
TruncateCStore(Relation rel, int elevel)
{
	PGRUsage ru0;
	BlockNumber old_rel_pages = 0;
	BlockNumber new_rel_pages = 0;
	SmgrAddr highestPhysicalAddress;

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

	RelationOpenSmgr(rel);
	old_rel_pages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	RelationCloseSmgr(rel);

	/*
	 * Due to the AccessExclusive lock there's no danger that
	 * new stripes be added beyond highestPhysicalAddress while
	 * we're truncating.
	 */
	highestPhysicalAddress =
		logical_to_smgr(GetHighestUsedAddress(rel->rd_node.relNode));

	new_rel_pages = highestPhysicalAddress.blockno + 1;
	if (new_rel_pages == old_rel_pages)
	{
		UnlockRelation(rel, AccessExclusiveLock);
		return;
	}

	/*
	 * Truncate the storage. Note that RelationTruncate() takes care of
	 * Write Ahead Logging.
	 */
	RelationTruncate(rel, new_rel_pages);

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
cstore_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
							   BufferAccessStrategy bstrategy)
{
	/*
	 * Our access method is not pages based, i.e. tuples are not confined
	 * to pages boundaries. So not much to do here. We return true anyway
	 * so acquire_sample_rows() in analyze.c would call our
	 * cstore_scan_analyze_next_tuple() callback.
	 */
	return true;
}


static bool
cstore_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
							   double *liverows, double *deadrows,
							   TupleTableSlot *slot)
{
	/*
	 * Currently we don't do anything smart to reduce number of rows returned
	 * for ANALYZE. The TableAM API's ANALYZE functions are designed for page
	 * based access methods where it chooses random pages, and then reads
	 * tuples from those pages.
	 *
	 * We could do something like that here by choosing sample stripes or blocks,
	 * but getting that correct might need quite some work. Since cstore_fdw's
	 * ANALYZE scanned all rows, as a starter we do the same here and scan all
	 * rows.
	 */
	if (cstore_getnextslot(scan, ForwardScanDirection, slot))
	{
		(*liverows)++;
		return true;
	}

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
	RelationOpenSmgr(rel);
	*pages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	*tuples = CStoreTableRowCount(rel);

	/*
	 * Append-only, so everything is visible except in-progress or rolled-back
	 * transactions.
	 */
	*allvisfrac = 1.0;

	get_rel_data_width(rel, attr_widths);
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
	ResetCStoreMemoryContext();
}


#if PG_VERSION_NUM >= 130000
static void
CStoreTableAMProcessUtility(PlannedStmt *plannedStatement,
							const char *queryString,
							ProcessUtilityContext context,
							ParamListInfo paramListInfo,
							QueryEnvironment *queryEnvironment,
							DestReceiver *destReceiver,
							QueryCompletion *queryCompletion)
#else
static void
CStoreTableAMProcessUtility(PlannedStmt * plannedStatement,
							const char * queryString,
							ProcessUtilityContext context,
							ParamListInfo paramListInfo,
							QueryEnvironment * queryEnvironment,
							DestReceiver * destReceiver,
							char * completionTag)
#endif
{
	Node *parseTree = plannedStatement->utilityStmt;

	if (nodeTag(parseTree) == T_CreateTrigStmt)
	{
		CreateTrigStmt *createTrigStmt = (CreateTrigStmt *) parseTree;
		Relation rel;
		bool isCStore;

		rel = relation_openrv(createTrigStmt->relation, AccessShareLock);
		isCStore = rel->rd_tableam == GetCstoreTableAmRoutine();
		relation_close(rel, AccessShareLock);

		if (isCStore &&
			createTrigStmt->row &&
			createTrigStmt->timing == TRIGGER_TYPE_AFTER)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg(
								"AFTER ROW triggers are not supported for columnstore access method"),
							errhint("Consider an AFTER STATEMENT trigger instead.")));
		}
	}

	CALL_PREVIOUS_UTILITY();
}


void
cstore_tableam_init()
{
	PreviousExecutorEndHook = ExecutorEnd_hook;
	ExecutorEnd_hook = CStoreExecutorEnd;
	PreviousProcessUtilityHook = (ProcessUtility_hook != NULL) ?
								 ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = CStoreTableAMProcessUtility;
	prevObjectAccessHook = object_access_hook;
	object_access_hook = CStoreTableAMObjectAccessHook;

	cstore_customscan_init();
}


void
cstore_tableam_finish()
{
	ExecutorEnd_hook = PreviousExecutorEndHook;
}


/*
 * Implements object_access_hook. One of the places this is called is just
 * before dropping an object, which allows us to clean-up resources for
 * cstore tables.
 *
 * See the comments for CStoreFdwObjectAccessHook for more details.
 */
static void
CStoreTableAMObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId, int
							  subId,
							  void *arg)
{
	if (prevObjectAccessHook)
	{
		prevObjectAccessHook(access, classId, objectId, subId, arg);
	}

	/*
	 * Do nothing if this is not a DROP relation command.
	 */
	if (access != OAT_DROP || classId != RelationRelationId || OidIsValid(subId))
	{
		return;
	}

	/*
	 * Lock relation to prevent it from being dropped and to avoid
	 * race conditions in the next if block.
	 */
	LockRelationOid(objectId, AccessShareLock);

	if (IsCStoreTableAmTable(objectId))
	{
		/*
		 * Drop metadata. No need to drop storage here since for
		 * tableam tables storage is managed by postgres.
		 */
		Relation rel = table_open(objectId, AccessExclusiveLock);
		DeleteDataFileMetadataRowIfExists(rel->rd_node.relNode);

		/* keep the lock since we did physical changes to the relation */
		table_close(rel, NoLock);
	}
}


/*
 * IsCStoreTableAmTable returns true if relation has cstore_tableam
 * access method. This can be called before extension creation.
 */
static bool
IsCStoreTableAmTable(Oid relationId)
{
	bool result;
	Relation rel;

	if (!OidIsValid(relationId))
	{
		return false;
	}

	/*
	 * Lock relation to prevent it from being dropped &
	 * avoid race conditions.
	 */
	rel = relation_open(relationId, AccessShareLock);
	result = rel->rd_tableam == GetCstoreTableAmRoutine();
	relation_close(rel, NoLock);

	return result;
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
	.relation_vacuum = cstore_vacuum_rel,
	.scan_analyze_next_block = cstore_scan_analyze_next_block,
	.scan_analyze_next_tuple = cstore_scan_analyze_next_tuple,
	.index_build_range_scan = cstore_index_build_range_scan,
	.index_validate_scan = cstore_index_validate_scan,

	.relation_size = cstore_relation_size,
	.relation_needs_toast_table = cstore_relation_needs_toast_table,

	.relation_estimate_size = cstore_estimate_rel_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
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


/*
 * alter_cstore_table_set is a UDF exposed in postgres to change settings on a columnar
 * table. Calling this function on a non-columnar table gives an error.
 *
 * sql syntax:
 *   pg_catalog.alter_cstore_table_set(
 *        table_name regclass,
 *        block_row_count int DEFAULT NULL,
 *        stripe_row_count int DEFAULT NULL,
 *        compression name DEFAULT null)
 *
 * All arguments except the table name are optional. The UDF is supposed to be called
 * like:
 *   SELECT alter_cstore_table_set('table', compression => 'pglz');
 *
 * This will only update the compression of the table, keeping all other settings the
 * same. Multiple settings can be changed at the same time by providing multiple
 * arguments. Calling the argument with the NULL value will be interperted as not having
 * provided the argument.
 */
PG_FUNCTION_INFO_V1(alter_cstore_table_set);
Datum
alter_cstore_table_set(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	int blockRowCount = 0;
	int stripeRowCount = 0;
	CompressionType compression = COMPRESSION_TYPE_INVALID;

	Relation rel = table_open(relationId, AccessExclusiveLock); /* ALTER TABLE LOCK */
	DataFileMetadata *metadata = ReadDataFileMetadata(rel->rd_node.relNode, true);
	if (!metadata)
	{
		ereport(ERROR, (errmsg("table %s is not a cstore table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	blockRowCount = metadata->blockRowCount;
	stripeRowCount = metadata->stripeRowCount;
	compression = metadata->compression;

	/* block_row_count => not null */
	if (!PG_ARGISNULL(1))
	{
		blockRowCount = PG_GETARG_INT32(1);
		ereport(DEBUG1, (errmsg("updating block row count to %d", blockRowCount)));
	}

	/* stripe_row_count => not null */
	if (!PG_ARGISNULL(2))
	{
		stripeRowCount = PG_GETARG_INT32(2);
		ereport(DEBUG1, (errmsg("updating stripe row count to %d", stripeRowCount)));
	}

	/* compression => not null */
	if (!PG_ARGISNULL(3))
	{
		Name compressionName = PG_GETARG_NAME(3);
		compression = ParseCompressionType(NameStr(*compressionName));
		if (compression == COMPRESSION_TYPE_INVALID)
		{
			ereport(ERROR, (errmsg("unknown compression type for cstore table: %s",
								   quote_identifier(NameStr(*compressionName)))));
		}
		ereport(DEBUG1, (errmsg("updating compression to %s",
								CompressionTypeStr(compression))));
	}

	UpdateCStoreDataFileMetadata(rel->rd_node.relNode, blockRowCount, stripeRowCount,
								 compression);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(alter_cstore_table_reset);
Datum
alter_cstore_table_reset(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	int blockRowCount = 0;
	int stripeRowCount = 0;
	CompressionType compression = COMPRESSION_TYPE_INVALID;

	Relation rel = table_open(relationId, AccessExclusiveLock); /* ALTER TABLE LOCK */
	DataFileMetadata *metadata = ReadDataFileMetadata(rel->rd_node.relNode, true);
	if (!metadata)
	{
		ereport(ERROR, (errmsg("table %s is not a cstore table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	blockRowCount = metadata->blockRowCount;
	stripeRowCount = metadata->stripeRowCount;
	compression = metadata->compression;

	/* block_row_count => true */
	if (!PG_ARGISNULL(1) && PG_GETARG_BOOL(1))
	{
		blockRowCount = cstore_block_row_count;
		ereport(DEBUG1, (errmsg("resetting block row count to %d", blockRowCount)));
	}

	/* stripe_row_count => true */
	if (!PG_ARGISNULL(2) && PG_GETARG_BOOL(2))
	{
		stripeRowCount = cstore_stripe_row_count;
		ereport(DEBUG1, (errmsg("resetting stripe row count to %d", stripeRowCount)));
	}

	/* compression => true */
	if (!PG_ARGISNULL(3) && PG_GETARG_BOOL(3))
	{
		compression = cstore_compression;
		ereport(DEBUG1, (errmsg("resetting compression to %s",
								CompressionTypeStr(compression))));
	}

	UpdateCStoreDataFileMetadata(rel->rd_node.relNode, blockRowCount, stripeRowCount,
								 compression);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}


#endif
