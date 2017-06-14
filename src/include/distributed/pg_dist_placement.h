/*-------------------------------------------------------------------------
 *
 * pg_dist_placement.h
 *	  definition of the "server" relation (pg_dist_placement).
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

#ifndef PG_DIST_PLACEMENT_H
#define PG_DIST_PLACEMENT_H

/* ----------------
 *		pg_dist_placement definition.
 * ----------------
 */
typedef struct FormData_pg_dist_placement
{
	int64 placementid;          /* global placementId on remote node */
	int64 shardid;              /* global shardId on remote node */
	int32 shardstate;           /* shard state on remote node; see RelayFileState */
	int64 shardlength;          /* shard length on remote node; stored as bigint */
	int32 groupid;              /* the group the shard is placed on */
} FormData_pg_dist_placement;

/* ----------------
 *		Form_pg_dist_placement corresponds to a pointer to a tuple with
 *		the format of pg_dist_placement relation.
 * ----------------
 */
typedef FormData_pg_dist_placement *Form_pg_dist_placement;

/* ----------------
 *		compiler constants for pg_dist_placement
 * ----------------
 */
#define Natts_pg_dist_placement 5
#define Anum_pg_dist_placement_placementid 1
#define Anum_pg_dist_placement_shardid 2
#define Anum_pg_dist_placement_shardstate 3
#define Anum_pg_dist_placement_shardlength 4
#define Anum_pg_dist_placement_groupid 5


#endif   /* PG_DIST_PLACEMENT_H */
