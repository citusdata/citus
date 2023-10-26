/*-------------------------------------------------------------------------
 *
 * pg_dist_shardgroup.h
 *	  definition of the "shardgroup" relation (pg_dist_shardgroup).
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_SHARDGROUP_H
#define PG_DIST_SHARDGROUP_H

#include "distributed/shardgroup.h"

/* ----------------
 *		pg_dist_shardgroup definition.
 * ----------------
 */
typedef struct FormData_pg_dist_shardgroup
{
	ShardgroupID shardgroupid;
	uint32 colocationid;
} FormData_pg_dist_shardgroup;

/* ----------------
 *      Form_pg_dist_shardgroup corresponds to a pointer to a tuple with
 *      the format of pg_dist_shardgroup relation.
 * ----------------
 */
typedef FormData_pg_dist_shardgroup *Form_pg_dist_shardgroup;

/* ----------------
 *      compiler constants for pg_dist_shardgroup
 * ----------------
 */
#define Natts_pg_dist_shardgroup 2
#define Anum_pg_dist_shardgroup_shardgroupid 1
#define Anum_pg_dist_shardgroup_colocationid 2

#endif   /* PG_DIST_SHARDGROUP_H */
