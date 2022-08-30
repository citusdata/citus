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

extern uint64 ShardListSizeInBytes(List *colocatedShardList,
								   char *workerNodeName, uint32 workerNodePort);
extern void ErrorIfMoveUnsupportedTableType(Oid relationId);
extern void CopyShardsToNode(WorkerNode *sourceNode, WorkerNode *targetNode,
							 List *shardIntervalList, char *snapshotName);
extern void VerifyTablesHaveReplicaIdentity(List *colocatedTableList);
extern bool RelationCanPublishAllModifications(Oid relationId);
