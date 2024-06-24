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

extern void AcquireExecutorShardLocksForExecution(RowModifyLevel modLevel,
												  List *taskList);
extern void AcquireExecutorShardLocksForRelationRowLockList(List *relationRowLockList);
extern bool RequiresConsistentSnapshot(Task *task);
extern void AcquireMetadataLocks(List *taskList);
extern void LockPartitionsInRelationList(List *relationIdList, LOCKMODE lockmode);
extern void LockPartitionRelations(Oid relationId, LOCKMODE lockMode);
extern void LockPartitionsForDistributedPlan(DistributedPlan *distributedPlan);


#endif /* DISTRIBUTED_EXECUTION_LOCKS_H */
