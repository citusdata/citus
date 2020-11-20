#include "citus_version.h"
#if HAS_TABLEAM

#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "access/skey.h"
#include "nodes/bitmapset.h"

const TableAmRoutine * GetColumnarTableAmRoutine(void);
extern void cstore_tableam_init(void);
extern void cstore_tableam_finish(void);

extern TableScanDesc cstore_beginscan_extended(Relation relation, Snapshot snapshot,
											   int nkeys, ScanKey key,
											   ParallelTableScanDesc parallel_scan,
											   uint32 flags, Bitmapset *attr_needed,
											   List *scanQual);
#endif
