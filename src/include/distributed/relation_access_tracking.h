/*
 * relation_access_tracking.h
 *
 * Function declartions for transaction access tracking.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 */

#ifndef RELATION_ACCESS_TRACKING_H_
#define RELATION_ACCESS_TRACKING_H_

#include "distributed/master_metadata_utility.h"
#include "distributed/multi_physical_planner.h" /* access Task struct */
#include "distributed/placement_connection.h"

/* forward declare, to avoid dependency on ShardPlacement definition */
struct ShardPlacement;

typedef enum RelationAccessMode
{
	RELATION_NOT_ACCESSED,
	RELATION_SEQUENTIAL_ACCESSED,
	RELATION_PARALLEL_ACCESSED
} RelationAccessMode;

extern void AllocateRelationAccessHash(void);
extern void ResetRelationAccessHash(void);
extern void AssociatePlacementAccessWithRelation(ShardPlacement *placement,
												 ShardPlacementAccessType accessType);
extern void RecordParallelSelectAccess(Oid relationId);
extern void RecordRelationMultiShardSelectAccessForTask(Task *task);
extern void RecordRelationMultiShardModifyAccessForTask(Task *task);
extern void RecordParallelModifyAccess(Oid relationId);
extern void RecordParallelDDLAccess(Oid relationId);
extern void RecordRelationMultiShardDDLAccessForTask(Task *task);
extern RelationAccessMode GetRelationDDLAccessMode(Oid relationId);
extern RelationAccessMode GetRelationDMLAccessMode(Oid relationId);
extern RelationAccessMode GetRelationSelectAccessMode(Oid relationId);


#endif /* RELATION_ACCESS_TRACKING_H_ */
