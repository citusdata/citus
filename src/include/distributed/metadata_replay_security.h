/*-------------------------------------------------------------------------
 *
 * metadata_replay_security.h
 *    Security boundary for evaluating expressions during metadata replay.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef METADATA_REPLAY_SECURITY_H
#define METADATA_REPLAY_SECURITY_H

#include "postgres.h"

#include "catalog/objectaddress.h"

typedef struct MetadataReplayOwnerContext
{
	Oid ownerId;
	Oid savedUserId;
	int savedSecurityContext;
	int gucNestLevel;
} MetadataReplayOwnerContext;

typedef enum
{
	METADATA_REPLAY_PROTECTION_OFF = 0,
	METADATA_REPLAY_PROTECTION_WARN = 1,
	METADATA_REPLAY_PROTECTION_REQUIRE = 2
} MetadataReplayProtectionMode;

extern char *MetadataReplayOwner;
extern int MetadataReplayProtection;

extern List * ProtectMetadataReplayCommandList(List *commandList,
											   const ObjectAddress *object);
extern bool MetadataReplayStatementIsMarked(void);
extern char * SaveAndClearMetadataReplayMarker(void);
extern void RestoreMetadataReplayMarker(const char *ownerName);
extern void EnterMetadataReplayOwnerContext(MetadataReplayOwnerContext *ownerContext);
extern void ExitMetadataReplayOwnerContext(MetadataReplayOwnerContext *ownerContext);

#endif /* METADATA_REPLAY_SECURITY_H */
