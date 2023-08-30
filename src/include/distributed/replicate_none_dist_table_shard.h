/*-------------------------------------------------------------------------
 *
 * replicate_none_dist_table_shard.h
 *	  Routines to replicate shard of none-distributed table to
 *    a remote node.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REPLICA_LOCAL_TABLE_SHARD_H
#define REPLICA_LOCAL_TABLE_SHARD_H

extern void NoneDistTableReplicateCoordinatorPlacement(Oid noneDistTableId,
													   List *targetNodeList);
extern void NoneDistTableDeleteCoordinatorPlacement(Oid noneDistTableId);
extern void NoneDistTableDropCoordinatorPlacementTable(Oid noneDistTableId);

#endif /* REPLICA_LOCAL_TABLE_SHARD_H */
