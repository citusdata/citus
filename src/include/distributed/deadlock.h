/*-------------------------------------------------------------------------
 *
 * deadlock.h
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DEADLOCK_H
#define DEADLOCK_H

#include "fmgr.h"
#include "utils/elog.h"

extern Datum this_machine_kills_deadlocks(PG_FUNCTION_ARGS);

extern void DeadlockLogHook(ErrorData *edata);

#endif /* DEADLOCK_H */
