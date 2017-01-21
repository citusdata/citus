/*-------------------------------------------------------------------------
 *
 * multi_shard_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_SHARD_TRANSACTION_H
#define MULTI_SHARD_TRANSACTION_H


#include "utils/hsearch.h"
#include "nodes/pg_list.h"


/* ShardConnections represents a set of connections for each placement of a shard */
typedef struct ShardConnections
{
	int64 shardId;

	/* list of MultiConnection structs */
	List *connectionList;
} ShardConnections;


extern HTAB * OpenTransactionsToAllShardPlacements(List *shardIdList,
												   int connectionFlags);
extern HTAB * CreateShardConnectionHash(MemoryContext memoryContext);
extern ShardConnections * GetShardHashConnections(HTAB *connectionHash, int64 shardId,
												  bool *connectionsFound);
extern List * ShardConnectionList(HTAB *connectionHash);
extern void ResetShardPlacementTransactionState(void);
extern void UnclaimAllShardConnections(HTAB *shardConnectionHash);


#endif /* MULTI_SHARD_TRANSACTION_H */
