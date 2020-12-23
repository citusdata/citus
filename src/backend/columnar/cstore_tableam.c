#include "citus_version.h"
#if HAS_TABLEAM

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
#include "catalog/pg_publication.h"
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
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "columnar/cstore.h"
#include "columnar/cstore_customscan.h"
#include "columnar/cstore_tableam.h"
#include "columnar/cstore_version_compat.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"

#define CSTORE_TABLEAM_NAME "columnar"

/*
 * Timing parameters for truncate locking heuristics.
 *
 * These are the same values from src/backend/access/heap/vacuumlazy.c
 */
#define VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL 50       /* ms */
#define VACUUM_TRUNCATE_LOCK_TIMEOUT 4500               /* ms */

/*
 * CStoreScanDescData is the scan state passed between beginscan(),
 * getnextslot(), rescan(), and endscan() calls.
 */
typedef struct CStoreScanDescData
{
	TableScanDescData cs_base;
	TableReadState *cs_readState;

	/*
	 * We initialize cs_readState lazily in the first getnextslot() call. We
	 * need the following for initialization. We save them in beginscan().
	 */
	MemoryContext scanContext;
	Bitmapset *attr_needed;
	List *scanQual;

	/*
	 * ANALYZE requires an item pointer for sorting. We keep track of row
	 * number so we can construct an item pointer based on that.
	 */
	int rowNumber;
} CStoreScanDescData;

typedef struct CStoreScanDescData *CStoreScanDesc;

static object_access_hook_type PrevObjectAccessHook = NULL;
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

static bool ConditionalLockRelationWithTimeout(Relation rel, LOCKMODE lockMode,
											   int timeout, int retryInterval);
static void LogRelationStats(Relation rel, int elevel);
static void TruncateCStore(Relation rel, int elevel);
static HeapTuple ColumnarSlotCopyHeapTuple(TupleTableSlot *slot);
static void ColumnarCheckLogicalReplication(Relation rel);

/* Custom tuple slot ops used for columnar. Initialized in cstore_tableam_init(). */
TupleTableSlotOps TTSOpsColumnar;

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

		if (tupdesc->attrs[i].attisdropped)
		{
			continue;
		}

		Var *var = makeVar(varno, varattno, vartype, vartypmod,
						   varcollid, varlevelsup);
		columnList = lappend(columnList, var);
	}

	return columnList;
}


static const TupleTableSlotOps *
cstore_slot_callbacks(Relation relation)
{
	return &TTSOpsColumnar;
}


static TableScanDesc
cstore_beginscan(Relation relation, Snapshot snapshot,
				 int nkeys, ScanKey key,
				 ParallelTableScanDesc parallel_scan,
				 uint32 flags)
{
	int natts = relation->rd_att->natts;
	Bitmapset *attr_needed = NULL;

	attr_needed = bms_add_range(attr_needed, 0, natts - 1);

	/* the cstore access method does not use the flags, they are specific to heap */
	flags = 0;

	TableScanDesc scandesc = cstore_beginscan_extended(relation, snapshot, nkeys, key,
													   parallel_scan,
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
	Oid relfilenode = relation->rd_node.relNode;

	/*
	 * A memory context to use for scan-wide data, including the lazily
	 * initialized read state. We assume that beginscan is called in a
	 * context that will last until end of scan.
	 */
	MemoryContext scanContext =
		AllocSetContextCreate(
			CurrentMemoryContext,
			"Column Store Scan Context",
			ALLOCSET_DEFAULT_SIZES);

	MemoryContext oldContext = MemoryContextSwitchTo(scanContext);

	CStoreScanDesc scan = palloc(sizeof(CStoreScanDescData));
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

	if (PendingWritesInUpperTransactions(relfilenode, GetCurrentSubTransactionId()))
	{
		elog(ERROR,
			 "cannot read from table when there is unflushed data in upper transactions");
	}

	FlushWriteStateForRelfilenode(relfilenode, GetCurrentSubTransactionId());

	MemoryContextSwitchTo(oldContext);

	return ((TableScanDesc) scan);
}


/*
 * init_cstore_read_state initializes a column store table read and returns the
 * state.
 */
static TableReadState *
init_cstore_read_state(Relation relation, TupleDesc tupdesc, Bitmapset *attr_needed,
					   List *scanQual)
{
	List *columnList = RelationColumnList(relation);
	ListCell *columnCell = NULL;

	List *neededColumnList = NIL;

	/* only collect columns that we need for the scan */
	foreach(columnCell, columnList)
	{
		Var *var = castNode(Var, lfirst(columnCell));
		if (bms_is_member(var->varattno - 1, attr_needed))
		{
			neededColumnList = lappend(neededColumnList, var);
		}
	}

	TableReadState *readState = CStoreBeginRead(relation, tupdesc, neededColumnList,
												scanQual);

	return readState;
}


static void
cstore_endscan(TableScanDesc sscan)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;
	if (scan->cs_readState != NULL)
	{
		CStoreEndRead(scan->cs_readState);
		scan->cs_readState = NULL;
	}
}


static void
cstore_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
			  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;
	if (scan->cs_readState != NULL)
	{
		CStoreRescan(scan->cs_readState);
	}
}


static bool
cstore_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	CStoreScanDesc scan = (CStoreScanDesc) sscan;

	/*
	 * if this is the first row, initialize read state.
	 */
	if (scan->cs_readState == NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(scan->scanContext);
		scan->cs_readState =
			init_cstore_read_state(scan->cs_base.rs_rd, slot->tts_tupleDescriptor,
								   scan->attr_needed, scan->scanQual);
		MemoryContextSwitchTo(oldContext);
	}

	ExecClearTuple(slot);

	bool nextRowFound = CStoreReadNextRow(scan->cs_readState, slot->tts_values,
										  slot->tts_isnull);

	if (!nextRowFound)
	{
		return false;
	}

	ExecStoreVirtualTuple(slot);

	/*
	 * Set slot's item pointer block & offset to non-zero. These are
	 * used just for sorting in acquire_sample_rows(), so rowNumber
	 * is good enough. See ColumnarSlotCopyHeapTuple for more info.
	 *
	 * offset is 16-bits, so use the first 15 bits for offset and
	 * rest as block number.
	 */
	ItemPointerSetBlockNumber(&(slot->tts_tid), scan->rowNumber / (32 * 1024) + 1);
	ItemPointerSetOffsetNumber(&(slot->tts_tid), scan->rowNumber % (32 * 1024) + 1);

	scan->rowNumber++;

	return true;
}


static Size
cstore_parallelscan_estimate(Relation rel)
{
	elog(ERROR, "columnar_parallelscan_estimate not implemented");
}


static Size
cstore_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnar_parallelscan_initialize not implemented");
}


static void
cstore_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnar_parallelscan_reinitialize not implemented");
}


static IndexFetchTableData *
cstore_index_fetch_begin(Relation rel)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("indexes not supported for columnar tables")));
}


static void
cstore_index_fetch_reset(IndexFetchTableData *scan)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("indexes not supported for columnar tables")));
}


static void
cstore_index_fetch_end(IndexFetchTableData *scan)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("indexes not supported for columnar tables")));
}


static bool
cstore_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *call_again, bool *all_dead)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("indexes not supported for columnar tables")));
}


static bool
cstore_fetch_row_version(Relation relation,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot)
{
	elog(ERROR, "columnar_fetch_row_version not implemented");
}


static void
cstore_get_latest_tid(TableScanDesc sscan,
					  ItemPointer tid)
{
	elog(ERROR, "columnar_get_latest_tid not implemented");
}


static bool
cstore_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "columnar_tuple_tid_valid not implemented");
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
	elog(ERROR, "columnar_compute_xid_horizon_for_tuples not implemented");
}


static void
cstore_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					int options, BulkInsertState bistate)
{
	/*
	 * cstore_init_write_state allocates the write state in a longer
	 * lasting context, so no need to worry about it.
	 */
	TableWriteState *writeState = cstore_init_write_state(relation,
														  RelationGetDescr(relation),
														  GetCurrentSubTransactionId());

	MemoryContext oldContext = MemoryContextSwitchTo(writeState->perTupleContext);

	HeapTuple heapTuple = ExecCopySlotHeapTuple(slot);

	ColumnarCheckLogicalReplication(relation);
	if (HeapTupleHasExternal(heapTuple))
	{
		/* detoast any toasted attributes */
		HeapTuple newTuple = toast_flatten_tuple(heapTuple,
												 slot->tts_tupleDescriptor);

		ExecForceStoreHeapTuple(newTuple, slot, true);
	}

	slot_getallattrs(slot);

	CStoreWriteRow(writeState, slot->tts_values, slot->tts_isnull);

	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(writeState->perTupleContext);
}


static void
cstore_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								CommandId cid, int options,
								BulkInsertState bistate, uint32 specToken)
{
	elog(ERROR, "columnar_tuple_insert_speculative not implemented");
}


static void
cstore_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								  uint32 specToken, bool succeeded)
{
	elog(ERROR, "columnar_tuple_complete_speculative not implemented");
}


static void
cstore_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					CommandId cid, int options, BulkInsertState bistate)
{
	TableWriteState *writeState = cstore_init_write_state(relation,
														  RelationGetDescr(relation),
														  GetCurrentSubTransactionId());

	ColumnarCheckLogicalReplication(relation);
	for (int i = 0; i < ntuples; i++)
	{
		TupleTableSlot *tupleSlot = slots[i];
		MemoryContext oldContext = MemoryContextSwitchTo(writeState->perTupleContext);
		HeapTuple heapTuple = ExecCopySlotHeapTuple(tupleSlot);

		if (HeapTupleHasExternal(heapTuple))
		{
			/* detoast any toasted attributes */
			HeapTuple newTuple = toast_flatten_tuple(heapTuple,
													 tupleSlot->tts_tupleDescriptor);

			ExecForceStoreHeapTuple(newTuple, tupleSlot, true);
		}

		slot_getallattrs(tupleSlot);

		CStoreWriteRow(writeState, tupleSlot->tts_values, tupleSlot->tts_isnull);
		MemoryContextSwitchTo(oldContext);
	}

	MemoryContextReset(writeState->perTupleContext);
}


static TM_Result
cstore_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					Snapshot snapshot, Snapshot crosscheck, bool wait,
					TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "columnar_tuple_delete not implemented");
}


static TM_Result
cstore_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd,
					LockTupleMode *lockmode, bool *update_indexes)
{
	elog(ERROR, "columnar_tuple_update not implemented");
}


static TM_Result
cstore_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				  LockWaitPolicy wait_policy, uint8 flags,
				  TM_FailureData *tmfd)
{
	elog(ERROR, "columnar_tuple_lock not implemented");
}


static void
cstore_finish_bulk_insert(Relation relation, int options)
{
	/*
	 * Nothing to do here. We keep write states live until transaction end.
	 */
}


static void
cstore_relation_set_new_filenode(Relation rel,
								 const RelFileNode *newrnode,
								 char persistence,
								 TransactionId *freezeXid,
								 MultiXactId *minmulti)
{
	Oid oldRelfilenode = rel->rd_node.relNode;

	MarkRelfilenodeDropped(oldRelfilenode, GetCurrentSubTransactionId());

	/* delete old relfilenode metadata */
	DeleteMetadataRows(rel->rd_node);

	Assert(persistence == RELPERSISTENCE_PERMANENT);
	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();
	SMgrRelation srel = RelationCreateStorage(*newrnode, persistence);

	InitColumnarOptions(rel->rd_id);

	smgrclose(srel);

	/* we will lazily initialize metadata in first stripe reservation */
}


static void
cstore_relation_nontransactional_truncate(Relation rel)
{
	RelFileNode relfilenode = rel->rd_node;

	NonTransactionDropWriteState(relfilenode.relNode);

	/* Delete old relfilenode metadata */
	DeleteMetadataRows(relfilenode);

	/*
	 * No need to set new relfilenode, since the table was created in this
	 * transaction and no other transaction can see this relation yet. We
	 * can just truncate the relation.
	 *
	 * This is similar to what is done in heapam_relation_nontransactional_truncate.
	 */
	RelationTruncate(rel, 0);

	/* we will lazily initialize new metadata in first stripe reservation */
}


static void
cstore_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "columnar_relation_copy_data not implemented");
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
	TupleDesc sourceDesc = RelationGetDescr(OldHeap);
	TupleDesc targetDesc = RelationGetDescr(NewHeap);

	if (OldIndex != NULL || use_sort)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("indexes not supported for columnar tables")));
	}

	/*
	 * copy_table_data in cluster.c assumes tuple descriptors are exactly
	 * the same. Even dropped columns exist and are marked as attisdropped
	 * in the target relation.
	 */
	Assert(sourceDesc->natts == targetDesc->natts);

	/* read settings from old heap, relfilenode will be swapped at the end */
	ColumnarOptions cstoreOptions = { 0 };
	ReadColumnarOptions(OldHeap->rd_id, &cstoreOptions);

	TableWriteState *writeState = CStoreBeginWrite(NewHeap->rd_node,
												   cstoreOptions,
												   targetDesc);

	TableReadState *readState = CStoreBeginRead(OldHeap, sourceDesc,
												RelationColumnList(OldHeap), NULL);

	Datum *values = palloc0(sourceDesc->natts * sizeof(Datum));
	bool *nulls = palloc0(sourceDesc->natts * sizeof(bool));

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


/*
 * LogRelationStats logs statistics as the output of the VACUUM VERBOSE.
 */
static void
LogRelationStats(Relation rel, int elevel)
{
	ListCell *stripeMetadataCell = NULL;
	RelFileNode relfilenode = rel->rd_node;
	StringInfo infoBuf = makeStringInfo();

	int compressionStats[COMPRESSION_COUNT] = { 0 };
	uint64 totalStripeLength = 0;
	uint64 tupleCount = 0;
	uint64 chunkCount = 0;
	TupleDesc tupdesc = RelationGetDescr(rel);
	uint64 droppedChunksWithData = 0;
	uint64 totalDecompressedLength = 0;

	List *stripeList = StripesForRelfilenode(relfilenode);
	int stripeCount = list_length(stripeList);

	foreach(stripeMetadataCell, stripeList)
	{
		StripeMetadata *stripe = lfirst(stripeMetadataCell);
		StripeSkipList *skiplist = ReadStripeSkipList(relfilenode, stripe->id,
													  RelationGetDescr(rel),
													  stripe->chunkCount);
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

	RelationOpenSmgr(rel);
	uint64 relPages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
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
	BlockNumber old_rel_pages = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	RelationCloseSmgr(rel);

	/*
	 * Due to the AccessExclusive lock there's no danger that
	 * new stripes be added beyond highestPhysicalAddress while
	 * we're truncating.
	 */
	SmgrAddr highestPhysicalAddress =
		logical_to_smgr(GetHighestUsedAddress(rel->rd_node));

	/*
	 * Unlock and return if truncation won't reduce data file's size.
	 */
	BlockNumber new_rel_pages = Min(old_rel_pages,
									highestPhysicalAddress.blockno + 1);
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
	 * We could do something like that here by choosing sample stripes or chunks,
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
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("indexes not supported for columnar tables")));
}


static void
cstore_index_validate_scan(Relation heapRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   Snapshot snapshot,
						   ValidateIndexState *state)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("indexes not supported for columnar tables")));
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
	elog(ERROR, "columnar_scan_sample_next_block not implemented");
}


static bool
cstore_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
							  TupleTableSlot *slot)
{
	elog(ERROR, "columnar_scan_sample_next_tuple not implemented");
}


static void
CStoreXactCallback(XactEvent event, void *arg)
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
CStoreSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
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

		Relation rel = relation_openrv(createTrigStmt->relation, AccessShareLock);
		bool isCStore = rel->rd_tableam == GetColumnarTableAmRoutine();
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
	RegisterXactCallback(CStoreXactCallback, NULL);
	RegisterSubXactCallback(CStoreSubXactCallback, NULL);

	PreviousProcessUtilityHook = (ProcessUtility_hook != NULL) ?
								 ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = CStoreTableAMProcessUtility;
	PrevObjectAccessHook = object_access_hook;
	object_access_hook = CStoreTableAMObjectAccessHook;

	cstore_customscan_init();

	TTSOpsColumnar = TTSOpsVirtual;
	TTSOpsColumnar.copy_heap_tuple = ColumnarSlotCopyHeapTuple;
}


void
cstore_tableam_finish()
{
	object_access_hook = PrevObjectAccessHook;
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

	/*
	 * We need to set item pointer, since implementation of ANALYZE
	 * requires it. See the qsort in acquire_sample_rows() and
	 * also compare_rows in backend/commands/analyze.c.
	 *
	 * slot->tts_tid is filled in cstore_getnextslot.
	 */
	tuple->t_self = slot->tts_tid;

	return tuple;
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
	if (PrevObjectAccessHook)
	{
		PrevObjectAccessHook(access, classId, objectId, subId, arg);
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
		RelFileNode relfilenode = rel->rd_node;
		DeleteMetadataRows(relfilenode);
		DeleteColumnarTableOptions(rel->rd_id, true);

		MarkRelfilenodeDropped(relfilenode.relNode, GetCurrentSubTransactionId());

		/* keep the lock since we did physical changes to the relation */
		table_close(rel, NoLock);
	}
}


/*
 * IsCStoreTableAmTable returns true if relation has cstore_tableam
 * access method. This can be called before extension creation.
 */
bool
IsCStoreTableAmTable(Oid relationId)
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
GetColumnarTableAmRoutine(void)
{
	return &cstore_am_methods;
}


PG_FUNCTION_INFO_V1(columnar_handler);
Datum
columnar_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&cstore_am_methods);
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
	if (!is_publishable_relation(rel))
	{
		return;
	}

	if (rel->rd_pubactions == NULL)
	{
		GetRelationPublicationActions(rel);
		Assert(rel->rd_pubactions != NULL);
	}

	if (rel->rd_pubactions->pubinsert)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"cannot insert into columnar table that is a part of a publication")));
	}
}


/*
 * CitusCreateAlterColumnarTableSet generates a portable
 */
static char *
CitusCreateAlterColumnarTableSet(char *qualifiedRelationName,
								 const ColumnarOptions *options)
{
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf,
					 "SELECT alter_columnar_table_set(%s, "
					 "chunk_row_count => %d, "
					 "stripe_row_count => %lu, "
					 "compression_level => %d, "
					 "compression => %s);",
					 quote_literal_cstr(qualifiedRelationName),
					 options->chunkRowCount,
					 options->stripeRowCount,
					 options->compressionLevel,
					 quote_literal_cstr(CompressionTypeStr(options->compressionType)));

	return buf.data;
}


/*
 * ColumnarTableDDLContext holds the instance variable for the TableDDLCommandFunction
 * instance described below.
 */
typedef struct ColumnarTableDDLContext
{
	char *schemaName;
	char *relationName;
	ColumnarOptions options;
} ColumnarTableDDLContext;


/*
 * GetTableDDLCommandColumnar is an internal function used to turn a
 * ColumnarTableDDLContext stored on the context of a TableDDLCommandFunction into a sql
 * command that will be executed against a table. The resulting command will set the
 * options of the table to the same options as the relation on the coordinator.
 */
static char *
GetTableDDLCommandColumnar(void *context)
{
	ColumnarTableDDLContext *tableDDLContext = (ColumnarTableDDLContext *) context;

	char *qualifiedShardName = quote_qualified_identifier(tableDDLContext->schemaName,
														  tableDDLContext->relationName);

	return CitusCreateAlterColumnarTableSet(qualifiedShardName,
											&tableDDLContext->options);
}


/*
 * GetShardedTableDDLCommandColumnar is an internal function used to turn a
 * ColumnarTableDDLContext stored on the context of a TableDDLCommandFunction into a sql
 * command that will be executed against a shard. The resulting command will set the
 * options of the shard to the same options as the relation the shard is based on.
 */
static char *
GetShardedTableDDLCommandColumnar(uint64 shardId, void *context)
{
	ColumnarTableDDLContext *tableDDLContext = (ColumnarTableDDLContext *) context;

	/*
	 * AppendShardId is destructive of the original cahr *, given we want to serialize
	 * more than once we copy it before appending the shard id.
	 */
	char *relationName = pstrdup(tableDDLContext->relationName);
	AppendShardIdToName(&relationName, shardId);

	char *qualifiedShardName = quote_qualified_identifier(tableDDLContext->schemaName,
														  relationName);

	return CitusCreateAlterColumnarTableSet(qualifiedShardName,
											&tableDDLContext->options);
}


/*
 * ColumnarGetCustomTableOptionsDDL returns a TableDDLCommand representing a command that
 * will apply the passed columnar options to the relation identified by relationId on a
 * new table or shard.
 */
static TableDDLCommand *
ColumnarGetCustomTableOptionsDDL(char *schemaName, char *relationName,
								 ColumnarOptions options)
{
	ColumnarTableDDLContext *context = (ColumnarTableDDLContext *) palloc0(
		sizeof(ColumnarTableDDLContext));

	/* build the context */
	context->schemaName = schemaName;
	context->relationName = relationName;
	context->options = options;

	/* create TableDDLCommand based on the context build above */
	return makeTableDDLCommandFunction(
		GetTableDDLCommandColumnar,
		GetShardedTableDDLCommandColumnar,
		context);
}


/*
 * ColumnarGetTableOptionsDDL returns a TableDDLCommand representing a command that will
 * apply the columnar options currently applicable to the relation identified by
 * relationId on a new table or shard.
 */
TableDDLCommand *
ColumnarGetTableOptionsDDL(Oid relationId)
{
	Oid namespaceId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(namespaceId);
	char *relationName = get_rel_name(relationId);

	ColumnarOptions options = { 0 };
	ReadColumnarOptions(relationId, &options);

	return ColumnarGetCustomTableOptionsDDL(schemaName, relationName, options);
}


/*
 * alter_columnar_table_set is a UDF exposed in postgres to change settings on a columnar
 * table. Calling this function on a non-columnar table gives an error.
 *
 * sql syntax:
 *   pg_catalog.alter_columnar_table_set(
 *        table_name regclass,
 *        chunk_row_count int DEFAULT NULL,
 *        stripe_row_count int DEFAULT NULL,
 *        compression name DEFAULT null)
 *
 * All arguments except the table name are optional. The UDF is supposed to be called
 * like:
 *   SELECT alter_columnar_table_set('table', compression => 'pglz');
 *
 * This will only update the compression of the table, keeping all other settings the
 * same. Multiple settings can be changed at the same time by providing multiple
 * arguments. Calling the argument with the NULL value will be interperted as not having
 * provided the argument.
 */
PG_FUNCTION_INFO_V1(alter_columnar_table_set);
Datum
alter_columnar_table_set(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation rel = table_open(relationId, AccessExclusiveLock); /* ALTER TABLE LOCK */
	if (!IsCStoreTableAmTable(relationId))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	ColumnarOptions options = { 0 };
	if (!ReadColumnarOptions(relationId, &options))
	{
		ereport(ERROR, (errmsg("unable to read current options for table")));
	}

	/* chunk_row_count => not null */
	if (!PG_ARGISNULL(1))
	{
		options.chunkRowCount = PG_GETARG_INT32(1);
		ereport(DEBUG1,
				(errmsg("updating chunk row count to %d", options.chunkRowCount)));
	}

	/* stripe_row_count => not null */
	if (!PG_ARGISNULL(2))
	{
		options.stripeRowCount = PG_GETARG_INT32(2);
		ereport(DEBUG1, (errmsg(
							 "updating stripe row count to " UINT64_FORMAT,
							 options.stripeRowCount)));
	}

	/* compression => not null */
	if (!PG_ARGISNULL(3))
	{
		Name compressionName = PG_GETARG_NAME(3);
		options.compressionType = ParseCompressionType(NameStr(*compressionName));
		if (options.compressionType == COMPRESSION_TYPE_INVALID)
		{
			ereport(ERROR, (errmsg("unknown compression type for cstore table: %s",
								   quote_identifier(NameStr(*compressionName)))));
		}
		ereport(DEBUG1, (errmsg("updating compression to %s",
								CompressionTypeStr(options.compressionType))));
	}

	/* compression_level => not null */
	if (!PG_ARGISNULL(4))
	{
		options.compressionLevel = PG_GETARG_INT32(4);
		if (options.compressionLevel < COMPRESSION_LEVEL_MIN ||
			options.compressionLevel > COMPRESSION_LEVEL_MAX)
		{
			ereport(ERROR, (errmsg("compression level out of range"),
							errhint("compression level must be between %d and %d",
									COMPRESSION_LEVEL_MIN,
									COMPRESSION_LEVEL_MAX)));
		}

		ereport(DEBUG1, (errmsg("updating compression level to %d",
								options.compressionLevel)));
	}

	if (EnableDDLPropagation && IsCitusTable(relationId))
	{
		/* when a columnar table is distributed update all settings on the shards */
		Oid namespaceId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(namespaceId);
		char *relationName = get_rel_name(relationId);
		TableDDLCommand *command = ColumnarGetCustomTableOptionsDDL(schemaName,
																	relationName,
																	options);
		DDLJob *ddljob = CreateCustomDDLTaskList(relationId, command);

		ExecuteDistributedDDLJob(ddljob);
	}

	SetColumnarOptions(relationId, &options);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}


/*
 * alter_columnar_table_reset is a UDF exposed in postgres to reset the settings on a
 * columnar table. Calling this function on a non-columnar table gives an error.
 *
 * sql syntax:
 *   pg_catalog.alter_columnar_table_re
 *   teset(
 *        table_name regclass,
 *        chunk_row_count bool DEFAULT FALSE,
 *        stripe_row_count bool DEFAULT FALSE,
 *        compression bool DEFAULT FALSE)
 *
 * All arguments except the table name are optional. The UDF is supposed to be called
 * like:
 *   SELECT alter_columnar_table_set('table', compression => true);
 *
 * All options set to true will be reset to the default system value.
 */
PG_FUNCTION_INFO_V1(alter_columnar_table_reset);
Datum
alter_columnar_table_reset(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation rel = table_open(relationId, AccessExclusiveLock); /* ALTER TABLE LOCK */
	if (!IsCStoreTableAmTable(relationId))
	{
		ereport(ERROR, (errmsg("table %s is not a columnar table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

	ColumnarOptions options = { 0 };
	if (!ReadColumnarOptions(relationId, &options))
	{
		ereport(ERROR, (errmsg("unable to read current options for table")));
	}

	/* chunk_row_count => true */
	if (!PG_ARGISNULL(1) && PG_GETARG_BOOL(1))
	{
		options.chunkRowCount = cstore_chunk_row_count;
		ereport(DEBUG1,
				(errmsg("resetting chunk row count to %d", options.chunkRowCount)));
	}

	/* stripe_row_count => true */
	if (!PG_ARGISNULL(2) && PG_GETARG_BOOL(2))
	{
		options.stripeRowCount = cstore_stripe_row_count;
		ereport(DEBUG1,
				(errmsg("resetting stripe row count to " UINT64_FORMAT,
						options.stripeRowCount)));
	}

	/* compression => true */
	if (!PG_ARGISNULL(3) && PG_GETARG_BOOL(3))
	{
		options.compressionType = cstore_compression;
		ereport(DEBUG1, (errmsg("resetting compression to %s",
								CompressionTypeStr(options.compressionType))));
	}

	/* compression_level => true */
	if (!PG_ARGISNULL(4) && PG_GETARG_BOOL(4))
	{
		options.compressionLevel = columnar_compression_level;
		ereport(DEBUG1, (errmsg("reseting compression level to %d",
								columnar_compression_level)));
	}

	if (EnableDDLPropagation && IsCitusTable(relationId))
	{
		/* when a columnar table is distributed update all settings on the shards */
		Oid namespaceId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(namespaceId);
		char *relationName = get_rel_name(relationId);
		TableDDLCommand *command = ColumnarGetCustomTableOptionsDDL(schemaName,
																	relationName,
																	options);
		DDLJob *ddljob = CreateCustomDDLTaskList(relationId, command);

		ExecuteDistributedDDLJob(ddljob);
	}

	SetColumnarOptions(relationId, &options);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}


#endif
