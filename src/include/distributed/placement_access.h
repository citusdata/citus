/*-------------------------------------------------------------------------
 *
 * placement_access.h
 *      Declarations of the structs and functions used in generating the
 *      placement accesses for distributed query execution.
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef PLACEMENT_ACCESS_H
#define PLACEMENT_ACCESS_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "distributed/multi_physical_planner.h"

/* forward declare, to avoid dependency on ShardPlacement definition */
struct ShardPlacement;

/* represents the way in which a placement is accessed */
typedef enum ShardPlacementAccessType
{
	/* read from placement */
	PLACEMENT_ACCESS_SELECT,

	/* modify rows in placement */
	PLACEMENT_ACCESS_DML,

	/* modify placement schema */
	PLACEMENT_ACCESS_DDL
} ShardPlacementAccessType;

/* represents access to a placement */
typedef struct ShardPlacementAccess
{
	/* placement that is accessed */
	struct ShardPlacement *placement;

	/* the way in which the placement is accessed */
	ShardPlacementAccessType accessType;
} ShardPlacementAccess;


extern List * PlacementAccessListForTask(Task *task, ShardPlacement *taskPlacement);
extern ShardPlacementAccess * CreatePlacementAccess(ShardPlacement *placement,
													ShardPlacementAccessType accessType);

#endif /* PLACEMENT_ACCESS_H */
