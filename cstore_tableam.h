#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"

const TableAmRoutine *GetCstoreTableAmRoutine(void);
Datum cstore_tableam_handler(PG_FUNCTION_ARGS);
