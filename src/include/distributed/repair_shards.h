/*-------------------------------------------------------------------------
 *
 * repair_shards.h
 *	  Code used to move shards around.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/pg_list.h"

extern int ForceDiskAvailableInBytes;
extern int ForceDiskSizeInBytes;
extern int64 ColocationSizeInBytes(List *colocatedShardList,
								   char *workerNodeName, uint32 workerNodePort);
