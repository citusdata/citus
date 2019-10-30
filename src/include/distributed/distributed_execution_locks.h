/*-------------------------------------------------------------------------
 *
 * distributed_execution_locks.h
 *	  Locking Infrastructure for distributed execution.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_EXECUTION_LOCKS_H
#define DISTRIBUTED_EXECUTION_LOCKS_H

#include "postgres.h"

#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "distributed/multi_physical_planner.h"

extern void AcquireExecutorShardLocks(Task *task, RowModifyLevel modLevel);
extern void AcquireExecutorMultiShardLocks(List *taskList);
extern void AcquireMetadataLocks(List *taskList);
extern void LockPartitionsInRelationList(List *relationIdList, LOCKMODE lockmode);
extern void LockPartitionRelations(Oid relationId, LOCKMODE lockMode);

#endif /* DISTRIBUTED_EXECUTION_LOCKS_H */
