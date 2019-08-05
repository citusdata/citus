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

/* Config variables managed via guc.c */
extern bool EnforceForeignKeyRestrictions;


/* forward declare, to avoid dependency on ShardPlacement definition */
struct ShardPlacement;

typedef enum RelationAccessMode
{
	RELATION_NOT_ACCESSED,

	/* only valid for reference tables */
	RELATION_REFERENCE_ACCESSED,

	/*
	 * Only valid for distributed tables and set
	 * if table is accessed in parallel mode
	 */
	RELATION_PARALLEL_ACCESSED
} RelationAccessMode;

extern void AllocateRelationAccessHash(void);
extern void ResetRelationAccessHash(void);
extern void AssociatePlacementAccessWithRelation(ShardPlacement *placement,
												 ShardPlacementAccessType accessType);
extern void RecordParallelRelationAccessForTaskList(List *taskList);
extern void RecordParallelSelectAccess(Oid relationId);
extern void RecordParallelModifyAccess(Oid relationId);
extern void RecordParallelDDLAccess(Oid relationId);
extern RelationAccessMode GetRelationDDLAccessMode(Oid relationId);
extern RelationAccessMode GetRelationDMLAccessMode(Oid relationId);
extern RelationAccessMode GetRelationSelectAccessMode(Oid relationId);
extern bool ShouldRecordRelationAccess(void);
extern bool ParallelQueryExecutedInTransaction(void);


#endif /* RELATION_ACCESS_TRACKING_H_ */
