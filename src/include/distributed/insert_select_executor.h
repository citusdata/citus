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

extern bool EnableRepartitionedInsertSelect;

extern TupleTableSlot * NonPushableInsertSelectExecScan(CustomScanState *node);
extern Query * BuildSelectForInsertSelect(Query *insertSelectQuery);
extern bool IsSupportedRedistributionTarget(Oid targetRelationId);
extern bool IsRedistributablePlan(Plan *selectPlan);


#endif /* INSERT_SELECT_EXECUTOR_H */
