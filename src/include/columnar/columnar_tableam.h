#include "citus_version.h"
#if HAS_TABLEAM

#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "access/skey.h"
#include "nodes/bitmapset.h"

#include "distributed/coordinator_protocol.h"

const TableAmRoutine * GetColumnarTableAmRoutine(void);
extern void columnar_tableam_init(void);
extern void columnar_tableam_finish(void);

extern TableScanDesc columnar_beginscan_extended(Relation relation, Snapshot snapshot,
												 int nkeys, ScanKey key,
												 ParallelTableScanDesc parallel_scan,
												 uint32 flags, Bitmapset *attr_needed,
												 List *scanQual);
extern int64 ColumnarGetChunkGroupsFiltered(TableScanDesc scanDesc);
extern bool IsColumnarTableAmTable(Oid relationId);
extern TableDDLCommand * ColumnarGetTableOptionsDDL(Oid relationId);
extern char * GetShardedTableDDLCommandColumnar(uint64 shardId, void *context);
#endif
