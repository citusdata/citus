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

extern void EnsureReferenceTablesExistOnAllNodes(void);
extern void EnsureReferenceTablesExistOnAllNodesExtended(char transferMode);
extern uint32 CreateReferenceTableColocationId(void);
extern void DeleteAllReferenceTablePlacementsFromNodeGroup(int32 groupId);
extern int CompareOids(const void *leftElement, const void *rightElement);
extern int ReferenceTableReplicationFactor(void);
extern void ReplicateAllReferenceTablesToNode(char *nodeName, int nodePort);

#endif /* REFERENCE_TABLE_UTILS_H_ */
