/*-------------------------------------------------------------------------
 *
 * reference_table_utils.h
 *
 * Declarations for public utility functions related to reference tables.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REFERENCE_TABLE_UTILS_H_
#define REFERENCE_TABLE_UTILS_H_

#include "postgres.h"

#include "listutils.h"

extern uint32 CreateReferenceTableColocationId(void);
extern void ReplicateAllReferenceTablesToNode(char *nodeName, int nodePort);
extern void DeleteAllReferenceTablePlacementsFromNodeGroup(int32 groupId);
extern List * ReferenceTableOidList(void);
extern int CompareOids(const void *leftElement, const void *rightElement);
extern int ReferenceTableReplicationFactor(void);


#endif /* REFERENCE_TABLE_UTILS_H_ */
