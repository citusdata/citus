#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"

const TableAmRoutine * GetCstoreTableAmRoutine(void);
extern void cstore_tableam_init(void);
extern void cstore_tableam_finish(void);
