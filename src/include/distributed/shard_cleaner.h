/*-------------------------------------------------------------------------
 *
 * shard_cleaner.h
 *	  Type and function declarations used in background shard cleaning
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_SHARD_CLEANER_H
#define CITUS_SHARD_CLEANER_H

/* GUC to configure deferred shard deletion */
extern int DeferShardDeleteInterval;
extern bool DeferShardDeleteOnMove;
extern double DesiredPercentFreeAfterMove;
extern bool CheckAvailableSpaceBeforeMove;

extern int TryDropMarkedShards(bool waitForLocks);
extern int DropMarkedShards(bool waitForLocks);

#endif /*CITUS_SHARD_CLEANER_H */
