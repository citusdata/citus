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


extern void OpenTransactionsToAllShardPlacements(List *shardIdList, char *relationOwner);
extern HTAB * CreateShardConnectionHash(MemoryContext memoryContext);
extern ShardConnections * GetShardConnections(int64 shardId, bool *shardConnectionsFound);
extern ShardConnections * GetShardHashConnections(HTAB *connectionHash, int64 shardId,
												  bool *connectionsFound);
extern List * ShardConnectionList(HTAB *connectionHash);
extern void CloseConnections(List *connectionList);
extern void ResetShardPlacementTransactionState(void);


#endif /* MULTI_SHARD_TRANSACTION_H */
