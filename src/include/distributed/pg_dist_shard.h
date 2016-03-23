/*-------------------------------------------------------------------------
 *
 * pg_dist_shard.h
 *	  definition of the "shard" relation (pg_dist_shard).
 *
 * This table maps logical tables to their remote partitions (from this point
 * on, we use the terms remote partition and shard interchangeably). All changes
 * concerning the creation, deletion, merging, and split of remote partitions
 * reference this table.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_SHARD_H
#define PG_DIST_SHARD_H

/* ----------------
 *		pg_dist_shard definition.
 * ----------------
 */
typedef struct FormData_pg_dist_shard
{
	Oid logicalrelid;         /* logical relation id; references pg_class oid */
	int64 shardid;            /* global shardId representing remote partition */
	char shardstorage;        /* shard storage type; see codes below */
#ifdef CATALOG_VARLEN           /* variable-length fields start here */
	text shardalias;           /* user specified table name for shard, if any */
	text shardminvalue;        /* partition key's minimum value in shard */
	text shardmaxvalue;        /* partition key's maximum value in shard */
#endif
} FormData_pg_dist_shard;

/* ----------------
 *      Form_pg_dist_shards corresponds to a pointer to a tuple with
 *      the format of pg_dist_shards relation.
 * ----------------
 */
typedef FormData_pg_dist_shard *Form_pg_dist_shard;

/* ----------------
 *      compiler constants for pg_dist_shards
 * ----------------
 */
#define Natts_pg_dist_shard 6
#define Anum_pg_dist_shard_logicalrelid 1
#define Anum_pg_dist_shard_shardid 2
#define Anum_pg_dist_shard_shardstorage 3
#define Anum_pg_dist_shard_shardalias 4
#define Anum_pg_dist_shard_shardminvalue 5
#define Anum_pg_dist_shard_shardmaxvalue 6

/*
 * Valid values for shard storage types include relay file, foreign table,
 * (standard) table and columnar table. Relay file types are currently unused.
 */
#define SHARD_STORAGE_RELAY 'r'
#define SHARD_STORAGE_FOREIGN 'f'
#define SHARD_STORAGE_TABLE 't'
#define SHARD_STORAGE_COLUMNAR 'c'


#endif   /* PG_DIST_SHARD_H */
