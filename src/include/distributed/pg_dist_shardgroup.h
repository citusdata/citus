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

#include  "postgres.h"

/* ----------------
 *		pg_dist_shardgroup definition.
 * ----------------
 */
typedef struct FormData_pg_dist_shardgroup
{
	int64 shardgroupid;
	int32 colocationid;
#ifdef CATALOG_VARLEN          /* variable-length fields start here */
	text shardminvalue;        /* partition key's minimum value in shard */
	text shardmaxvalue;        /* partition key's maximum value in shard */
#endif
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
#define Natts_pg_dist_shardgroup 4
#define Anum_pg_dist_shardgroup_shardgroupid 1
#define Anum_pg_dist_shardgroup_colocationid 2
#define Anum_pg_dist_shardgroup_shardminvalue 3
#define Anum_pg_dist_shardgroup_shardmaxvalue 4

#endif   /* PG_DIST_SHARD_H */
