/*-------------------------------------------------------------------------
 *
 * shard_split.h
 *
 * API for shard splits.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDSPLIT_H_
#define SHARDSPLIT_H_

/* Split Modes supported by Shard Split API */
typedef enum SplitMode
{
	BLOCKING_SPLIT = 0,
	NON_BLOCKING_SPLIT = 1,
	AUTO_SPLIT = 2
} SplitMode;


/*
 * User Scenario calling Split Shard API.
 * The 'SplitOperation' type is used to customize info/error messages based on user scenario.
 */
typedef enum SplitOperation
{
	SHARD_SPLIT_API = 0,
	ISOLATE_TENANT_TO_NEW_SHARD
} SplitOperation;

typedef enum SplitArtifactType
{
	SPLIT_CHILD_SHARD = 0,
	SPLIT_DUMMY_SHARD = 1,
	SPLIT_PUBLICATION = 2,
	SPLIT_SUBSCRIPTION = 3,
	SPLIT_REPLICATION_SLOT = 4
} SplitArtifactType;


/*
 * SplitShard API to split a given shard (or shard group) using split mode and
 * specified split points to a set of destination nodes.
 */
extern void SplitShard(SplitMode splitMode,
					   SplitOperation splitOperation,
					   uint64 shardIdToSplit,
					   List *shardSplitPointsList,
					   List *nodeIdsForPlacementList);

extern void DropShardList(List *shardIntervalList);

extern SplitMode LookupSplitMode(Oid shardTransferModeOid);
extern StringInfo CreateArtifactEntryCommand(uint32 operationId, SplitArtifactType splitArtifact, char* artifactName);
extern StringInfo CreateDeleteArtifactCommand(SplitArtifactType splitArtifact, char* artifactName);
#endif /* SHARDSPLIT_H_ */
