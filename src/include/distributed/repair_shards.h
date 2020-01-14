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

extern int ForceDiskAvailable;
extern int ForceDiskSize;
extern int64 ColocationSize(List *colocatedShardList,
							char *workerNodeName, uint32 workerNodePort);
