/*-------------------------------------------------------------------------
 *
 * reference_table_utils.h
 *
 * Declarations for public utility functions related to reference tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REFERENCE_TABLE_UTILS_H_
#define REFERENCE_TABLE_UTILS_H_

#include "postgres.h"

#include "listutils.h"

#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"

extern void EnsureReferenceTablesExistOnAllNodes(void);
extern void EnsureReferenceTablesExistOnAllNodesExtended(char transferMode);
extern bool HasNodesWithMissingReferenceTables(List **referenceTableList);
extern uint32 CreateReferenceTableColocationId(void);
extern uint32 GetReferenceTableColocationId(void);
extern List * GetAllReplicatedTableList(void);
extern List * ReplicatedPlacementsForNodeGroup(int32 groupId);
extern char * DeleteShardPlacementCommand(uint64 placementId);
extern void DeleteAllReplicatedTablePlacementsFromNodeGroup(int32 groupId,
															bool localOnly);
extern void DeleteAllReplicatedTablePlacementsFromNodeGroupViaMetadataContext(
	MetadataSyncContext *context, int32 groupId, bool localOnly);
extern int CompareOids(const void *leftElement, const void *rightElement);
extern void ReplicateAllReferenceTablesToNode(WorkerNode *workerNode);
extern void ErrorIfNotAllNodesHaveReferenceTableReplicas(List *workerNodeList);

#endif /* REFERENCE_TABLE_UTILS_H_ */
