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

extern uint32 CreateReferenceTableColocationId(void);
extern void ReplicateAllReferenceTablesToNode(char *nodeName, int nodePort);
extern void DeleteAllReferenceTablePlacementsFromNode(char *workerName,
													  uint32 workerPort);
extern List * ReferenceTableOidList(void);

#endif /* REFERENCE_TABLE_UTILS_H_ */
