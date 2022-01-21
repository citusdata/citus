#ifndef COLUMNAR_TABLEAM_H
#define COLUMNAR_TABLEAM_H

#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "access/skey.h"
#include "nodes/bitmapset.h"
#include "access/heapam.h"
#include "catalog/indexing.h"

/*
 * Number of valid ItemPointer Offset's for "row number" <> "ItemPointer"
 * mapping.
 *
 * Postgres has some asserts calling either ItemPointerIsValid or
 * OffsetNumberIsValid. That constraints itemPointer.offsetNumber
 * for columnar tables to the following interval:
 * [FirstOffsetNumber, MaxOffsetNumber].
 *
 * However, bitmap scan logic assumes that itemPointer.offsetNumber cannot
 * be larger than MaxHeapTuplesPerPage (see tbm_add_tuples).
 *
 * For this reason, we restrict itemPointer.offsetNumber
 * to the following interval: [FirstOffsetNumber, MaxHeapTuplesPerPage].
 */
#define VALID_ITEMPOINTER_OFFSETS \
	((uint64) (MaxHeapTuplesPerPage - FirstOffsetNumber + 1))

/*
 * Number of valid ItemPointer BlockNumber's for "row number" <> "ItemPointer"
 * mapping.
 *
 * Similar to VALID_ITEMPOINTER_OFFSETS, due to asserts around
 * itemPointer.blockNumber, we can only use values upto and including
 * MaxBlockNumber.
 * Note that postgres doesn't restrict blockNumber to a lower boundary.
 *
 * For this reason, we restrict itemPointer.blockNumber
 * to the following interval: [0, MaxBlockNumber].
 */
#define VALID_BLOCKNUMBERS ((uint64) (MaxBlockNumber + 1))


struct ColumnarScanDescData;
typedef struct ColumnarScanDescData *ColumnarScanDesc;


const TableAmRoutine * GetColumnarTableAmRoutine(void);
extern void columnar_tableam_init(void);
extern void columnar_tableam_finish(void);
extern void CreateTruncateTrigger(Oid relationId);
extern void EnsureTableOwner(Oid relationId);
extern bool CheckCitusVersion(int elevel);

extern TableScanDesc columnar_beginscan_extended(Relation relation, Snapshot snapshot,
												 int nkeys, ScanKey key,
												 ParallelTableScanDesc parallel_scan,
												 uint32 flags, Bitmapset *attr_needed,
												 List *scanQual);
extern int64 ColumnarScanChunkGroupsFiltered(ColumnarScanDesc columnarScanDesc);
extern bool ColumnarSupportsIndexAM(char *indexAMName);
extern bool IsColumnarTableAmTable(Oid relationId);


#endif /* COLUMNAR_TABLEAM_H */
