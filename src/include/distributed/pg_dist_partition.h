/*-------------------------------------------------------------------------
 *
 * pg_dist_partition.h
 *	  definition of the system "remote partition" relation (pg_dist_partition).
 *
 * This table keeps metadata on logical tables that the user requested remote
 * partitioning for (smaller physical tables that we partition data to are
 * handled in another system catalog).
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_PARTITION_H
#define PG_DIST_PARTITION_H

/* ----------------
 *		pg_dist_partition definition.
 * ----------------
 */
typedef struct FormData_pg_dist_partition
{
	Oid logicalrelid;              /* logical relation id; references pg_class oid */
	char partmethod;               /* partition method; see codes below */
	text partkey;                  /* partition key expression */
} FormData_pg_dist_partition;

/* ----------------
 *      Form_pg_dist_partitions corresponds to a pointer to a tuple with
 *      the format of pg_dist_partitions relation.
 * ----------------
 */
typedef FormData_pg_dist_partition *Form_pg_dist_partition;

/* ----------------
 *      compiler constants for pg_dist_partitions
 * ----------------
 */
#define Natts_pg_dist_partition 3
#define Anum_pg_dist_partition_logicalrelid 1
#define Anum_pg_dist_partition_partmethod 2
#define Anum_pg_dist_partition_partkey 3

/* valid values for partmethod include append, hash, and range */
#define DISTRIBUTE_BY_APPEND 'a'
#define DISTRIBUTE_BY_HASH 'h'
#define DISTRIBUTE_BY_RANGE 'r'
#define REDISTRIBUTE_BY_HASH 'x'


#endif   /* PG_DIST_PARTITION_H */
