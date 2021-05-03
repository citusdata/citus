#include "citus_version.h"

#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "access/skey.h"
#include "nodes/bitmapset.h"

#include "distributed/coordinator_protocol.h"


/*
 * Number of valid ItemPointer Offset's for "row number" <> "ItemPointer"
 * mapping.
 *
 * Postgres has some asserts calling either ItemPointerIsValid or
 * OffsetNumberIsValid. That constraints itemPointer.offsetNumber
 * for columnar tables to the following interval:
 * [FirstOffsetNumber, MaxOffsetNumber].
 *
 * However, for GIN indexes, Postgres also asserts the following in
 * itemptr_to_uint64 function:
 * "GinItemPointerGetOffsetNumber(iptr) < (1 << MaxHeapTuplesPerPageBits)",
 * where MaxHeapTuplesPerPageBits = 11.
 * That means, offsetNumber for columnar tables can't be equal to
 * 2**11 = 2048 = MaxOffsetNumber.
 * Hence we can't use MaxOffsetNumber as offsetNumber too.
 *
 * For this reason, we restrict itemPointer.offsetNumber
 * to the following interval: [FirstOffsetNumber, MaxOffsetNumber).
 */
#define VALID_ITEMPOINTER_OFFSETS ((uint64) (MaxOffsetNumber - FirstOffsetNumber))

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


const TableAmRoutine * GetColumnarTableAmRoutine(void);
extern void columnar_tableam_init(void);
extern void columnar_tableam_finish(void);

extern TableScanDesc columnar_beginscan_extended(Relation relation, Snapshot snapshot,
												 int nkeys, ScanKey key,
												 ParallelTableScanDesc parallel_scan,
												 uint32 flags, Bitmapset *attr_needed,
												 List *scanQual);
extern int64 ColumnarScanChunkGroupsFiltered(TableScanDesc scanDesc);
extern bool IsColumnarTableAmTable(Oid relationId);
extern TableDDLCommand * ColumnarGetTableOptionsDDL(Oid relationId);
extern char * GetShardedTableDDLCommandColumnar(uint64 shardId, void *context);
