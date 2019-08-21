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
 * Copyright (c) 2019, Citus Data, Inc.
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
#define Natts_pg_dist_object 3
#define Anum_pg_dist_object_classid 1
#define Anum_pg_dist_object_objid 2
#define Anum_pg_dist_object_objsubid 3

#endif /* PG_DIST_OBJECT_H */
