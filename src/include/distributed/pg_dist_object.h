/*-------------------------------------------------------------------------
 *
 * pg_dist_object.h
 *	  definition of the relation that holds the object information on the
 *	  cluster (pg_dist_object).
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_OBJECT_H
#define PG_DIST_OBJECT_H

/* ----------------
 *      compiler constants for pg_dist_object
 * ----------------
 */
#define Natts_pg_dist_object 8
#define Anum_pg_dist_object_classid 1
#define Anum_pg_dist_object_objid 2
#define Anum_pg_dist_object_objsubid 3
#define Anum_pg_dist_object_type 4
#define Anum_pg_dist_object_object_names 5
#define Anum_pg_dist_object_object_args 6
#define Anum_pg_dist_object_distribution_arg_index 7
#define Anum_pg_dist_object_colocation_id 8

#endif /* PG_DIST_OBJECT_H */
