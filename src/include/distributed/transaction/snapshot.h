/*-------------------------------------------------------------------------
 *
 * snapshot.h
 *
 * Functions for managing distributed snapshots.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_SNAPSHOT_H
#define DISTRIBUTED_SNAPSHOT_H


extern char * GetSnapshotNameForNode(char *hostname, int port, char *userName,
									 char *databaseName);
extern void ResetExportedSnapshots(void);


#endif /* DISTRIBUTED_SNAPSHOT_H */
