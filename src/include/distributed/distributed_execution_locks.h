#ifndef _DISTRIBUTED_EXECUTION_LOCKS_H
#define _DISTRIBUTED_EXECUTION_LOCKS_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "distributed/multi_physical_planner.h"

extern void LockPartitionsInRelationList(List *relationIdList, LOCKMODE lockmode);
extern void LockPartitionRelations(Oid relationId, LOCKMODE lockMode);

extern void AcquireExecutorShardLocks(Task *task, RowModifyLevel modLevel);
extern void AcquireExecutorMultiShardLocks(List *taskList);
extern void AcquireMetadataLocks(List *taskList);

#endif /* PLACEMENT_ACCESS_H */
