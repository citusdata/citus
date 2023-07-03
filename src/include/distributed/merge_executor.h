/*-------------------------------------------------------------------------
 *
 * merge_executor.h
 *
 * Declarations for public functions and types related to executing
 * MERGE INTO ...  SQL commands.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MERGE_EXECUTOR_H
#define MERGE_EXECUTOR_H

extern TupleTableSlot * NonPushableMergeCommandExecScan(CustomScanState *node);

#endif /* MERGE_EXECUTOR_H */
