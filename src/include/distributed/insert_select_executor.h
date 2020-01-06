/*-------------------------------------------------------------------------
 *
 * insert_select_executor.h
 *
 * Declarations for public functions and types related to executing
 * INSERT..SELECT commands.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef INSERT_SELECT_EXECUTOR_H
#define INSERT_SELECT_EXECUTOR_H


#include "executor/execdesc.h"


extern TupleTableSlot * CoordinatorInsertSelectExecScan(CustomScanState *node);
extern bool ExecutingInsertSelect(void);
extern Query * BuildSelectForInsertSelect(Query *insertSelectQuery);


#endif /* INSERT_SELECT_EXECUTOR_H */
