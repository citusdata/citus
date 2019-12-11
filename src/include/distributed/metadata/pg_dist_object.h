/*-------------------------------------------------------------------------
 *
 * pg_dist_object.h
 *	  definition of the system distributed objects relation (pg_dist_object).
 *
 * This table keeps metadata on all postgres objects that are distributed
 * to all the nodes in the network. Objects in this table should all be
 * present on all workers and kept in sync throughout their existance.
 * This also means that all nodes joining the network are assumed to
 * recreate all these objects.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_OBJECT_H
#define PG_DIST_OBJECT_H


/* ----------------
 *		pg_dist_object definition.
 * ----------------
 */
typedef struct FormData_pg_dist_object
{
	Oid classid;      /* class of the distributed object */
	Oid objid;        /* object id of the distributed object */
	int32 objsubid;   /* object sub id of the distributed object, eg. attnum */

#ifdef CATALOG_VARLEN           /* variable-length fields start here */
	text type;
	text[] object_names;
	text[] object_arguments;

	uint32 distribution_argument_index; /* only valid for distributed functions/procedures */
	uint32 colocationid;            /* only valid for distributed functions/procedures */
#endif
} FormData_pg_dist_object;

/* ----------------
 *      Form_pg_dist_partitions corresponds to a pointer to a tuple with
 *      the format of pg_dist_partitions relation.
 * ----------------
 */
typedef FormData_pg_dist_object *Form_pg_dist_object;

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
#define Anum_pg_dist_object_distribution_argument_index 7
#define Anum_pg_dist_object_colocationid 8

#endif /* PG_DIST_OBJECT_H */
