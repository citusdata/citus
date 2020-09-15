#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"

const TableAmRoutine *GetCstoreTableAmRoutine(void);
Datum cstore_tableam_handler(PG_FUNCTION_ARGS);
extern void cstore_free_write_state(void);
extern void cstore_tableam_init(void);
extern void cstore_tableam_finish(void);
