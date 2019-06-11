/*-------------------------------------------------------------------------
 *
 * insert_select_executor.h
 *
 * Declarations for public functions and types related to executing
 * INSERT..SELECT commands.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef INSERT_SELECT_EXECUTOR_H
#define INSERT_SELECT_EXECUTOR_H


#include "executor/execdesc.h"
#include "distributed/citus_custom_scan.h"


extern TupleTableSlot * CoordinatorInsertSelectExecScan(CustomScanState *node);
extern void CoordinatorInsertSelectExec(CitusScanState *scanState);


#endif /* INSERT_SELECT_EXECUTOR_H */
