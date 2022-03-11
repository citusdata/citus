/*-------------------------------------------------------------------------
 *
 * pg_dist_local_group.h
 *	  definition of the relation that holds the local group id (pg_dist_local_group).
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_LOCAL_GROUP_H
#define PG_DIST_LOCAL_GROUP_H

/* ----------------
 *		pg_dist_local_group definition.
 * ----------------
 */
typedef struct FormData_pg_dist_local_group
{
	int groupid;
	uint64 logical_clock_value;
} FormData_pg_dist_local_group;

/* ----------------
 *      FormData_pg_dist_local_group corresponds to a pointer to a tuple with
 *      the format of pg_dist_local_group relation.
 * ----------------
 */
typedef FormData_pg_dist_local_group *Form_pg_dist_local_group;

/* ----------------
 *      compiler constants for pg_dist_local_group
 * ----------------
 */
#define Natts_pg_dist_local_group 2
#define Anum_pg_dist_local_groupid 1
#define Anum_pg_dist_local_logical_clock_value 2

#endif /* PG_DIST_LOCAL_GROUP_H */
