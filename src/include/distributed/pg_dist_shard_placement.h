/*-------------------------------------------------------------------------
 *
 * pg_dist_shard_placement.h
 *	  definition of the "server" relation (pg_dist_shard_placement).
 *
 * This table keeps information on remote shards and their whereabouts on the
 * master node. The table's contents are updated and used as follows: (i) the
 * worker nodes send periodic reports about the shards they contain, and (ii)
 * the master reconciles these shard reports, and determines outdated, under-
 * and over-replicated shards.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DIST_SHARD_PLACEMENT_H
#define PG_DIST_SHARD_PLACEMENT_H

/* ----------------
 *		pg_dist_shard_placement definition.
 * ----------------
 */
typedef struct FormData_pg_dist_shard_placement
{
	int64 shardid;              /* global shardId on remote node */
	int32 shardstate;           /* shard state on remote node; see RelayFileState */
	int64 shardlength;          /* shard length on remote node; stored as bigint */
#ifdef CATALOG_VARLEN           /* variable-length fields start here */
	text nodename;              /* remote node's host name */
	int32 nodeport;             /* remote node's port number */
#endif
} FormData_pg_dist_shard_placement;

/* ----------------
 *		Form_pg_dist_shard_placement corresponds to a pointer to a tuple with
 *		the format of pg_dist_shard_placement relation.
 * ----------------
 */
typedef FormData_pg_dist_shard_placement *Form_pg_dist_shard_placement;

/* ----------------
 *		compiler constants for pg_dist_shard_placement
 * ----------------
 */
#define Natts_pg_dist_shard_placement 5
#define Anum_pg_dist_shard_placement_shardid 1
#define Anum_pg_dist_shard_placement_shardstate 2
#define Anum_pg_dist_shard_placement_shardlength 3
#define Anum_pg_dist_shard_placement_nodename 4
#define Anum_pg_dist_shard_placement_nodeport 5


#endif   /* PG_DIST_SHARD_PLACEMENT_H */
