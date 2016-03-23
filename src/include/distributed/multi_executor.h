/*-------------------------------------------------------------------------
 *
 * multi_executor.h
 *	  Executor support for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_EXECUTOR_H
#define MULTI_EXECUTOR_H

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"

/* signal currently executed statement is a master select statement or router execution */
#define EXEC_FLAG_CITUS_MASTER_SELECT 0x100
#define EXEC_FLAG_CITUS_ROUTER_EXECUTOR 0x200

extern void multi_ExecutorStart(QueryDesc *queryDesc, int eflags);
extern void multi_ExecutorRun(QueryDesc *queryDesc,
							  ScanDirection direction, long count);
extern void multi_ExecutorFinish(QueryDesc *queryDesc);
extern void multi_ExecutorEnd(QueryDesc *queryDesc);

#endif /* MULTI_EXECUTOR_H */
