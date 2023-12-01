/*-------------------------------------------------------------------------
 *
 * shardgroup.h
 *   Shardgroups are a logical unit of colocated shards from different
 *   tables belonging to the same colocation group. When shards belong
 *   to the same shardgroup they move as one logical unit during
 *   shardmoves, which are better described as shardgroupmoves.
 *
 *   This header defines functions operating on shardgroups as well as
 *   helpers to work with ShardgroupID's that identify shardgroups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDGROUP_H
#define SHARDGROUP_H

#include "postgres.h"

#include "fmgr.h"

typedef int64 ShardgroupID;
#define SHARDGROUPID_FORMAT INT64_FORMAT
#define SHARDGROUPID_SQL_FORMAT SHARDGROUPID_FORMAT "::bigint"
#define InvalidShardgroupID ((ShardgroupID) 0)
#define IsShardgroupIDValid(shardgroupID) ((shardgroupID) != InvalidShardgroupID)

/* helper functions to get a typed ShardgroupID to and from a Datum */
#define DatumGetShardgroupID(datum) ((ShardgroupID) DatumGetInt64((datum)))
#define ShardgroupIDGetDatum(shardgroupID) Int64GetDatum(((int64) (shardgroupID)))

#define PG_GETARG_SHARDGROUPID(n) DatumGetShardgroupID(PG_GETARG_DATUM(n))

#endif   /* SHARDGROUP_H */
