/*-------------------------------------------------------------------------
 *
 * pg_dist_partition.h
 *	  definition of the system "remote partition" relation (pg_dist_partition).
 *
 * This table keeps metadata on logical tables that the user requested remote
 * partitioning for (smaller physical tables that we partition data to are
 * handled in another system catalog).
 *
 * Copyright (c) Citus Data, Inc.
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
	Oid logicalrelid;    /* logical relation id; references pg_class oid */
	char partmethod;     /* partition method; see codes below */
#ifdef CATALOG_VARLEN    /* variable-length fields start here */
	text partkey;        /* partition key expression */
	uint32 colocationid; /* id of the co-location group of particular table belongs to */
	char repmodel;       /* replication model; see codes below */
#endif
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
#define Natts_pg_dist_partition 5
#define Anum_pg_dist_partition_logicalrelid 1
#define Anum_pg_dist_partition_partmethod 2
#define Anum_pg_dist_partition_partkey 3
#define Anum_pg_dist_partition_colocationid 4
#define Anum_pg_dist_partition_repmodel 5

/* valid values for partmethod include append, hash, and range */
#define DISTRIBUTE_BY_APPEND 'a'
#define DISTRIBUTE_BY_HASH 'h'
#define DISTRIBUTE_BY_RANGE 'r'
#define DISTRIBUTE_BY_NONE 'n'
#define REDISTRIBUTE_BY_HASH 'x'

/*
 * Valid values for repmodel are 'c' for coordinator, 's' for streaming
 * and 't' for two-phase-commit. We also use an invalid replication model
 * ('i') for distinguishing uninitialized variables where necessary.
 */
#define REPLICATION_MODEL_COORDINATOR 'c'
#define REPLICATION_MODEL_STREAMING 's'
#define REPLICATION_MODEL_2PC 't'
#define REPLICATION_MODEL_INVALID 'i'


#endif   /* PG_DIST_PARTITION_H */
