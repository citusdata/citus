/*-------------------------------------------------------------------------
 *
 * pg_dist_colocation.h
 *	  definition of the relation that holds the colocation information on the
 *	  cluster (pg_dist_colocation).
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_COLOCATION_H
#define PG_DIST_COLOCATION_H

/* ----------------
 *		pg_dist_colocation definition.
 * ----------------
 */
typedef struct FormData_pg_dist_colocation
{
	uint32 colocationid;
	uint32 shardcount;
	uint32 replicationfactor;
	Oid distributioncolumntype;
	bool defaultgroup;
} FormData_pg_dist_colocation;

/* ----------------
 *      Form_pg_dist_colocation corresponds to a pointer to a tuple with
 *      the format of pg_dist_colocation relation.
 * ----------------
 */
typedef FormData_pg_dist_colocation *Form_pg_dist_colocation;

/* ----------------
 *      compiler constants for pg_dist_colocation
 * ----------------
 */
#define Natts_pg_dist_colocation 5
#define Anum_pg_dist_colocation_colocationid 1
#define Anum_pg_dist_colocation_shardcount 2
#define Anum_pg_dist_colocation_replicationfactor 3
#define Anum_pg_dist_colocation_distributioncolumntype 4
#define Anum_pg_dist_colocation_defaultgroup 5

#define COLOCATIONID_SEQUENCE_NAME "pg_dist_colocationid_seq"


#endif /* PG_DIST_COLOCATION_H */
