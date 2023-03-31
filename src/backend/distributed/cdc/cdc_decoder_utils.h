/*-------------------------------------------------------------------------
 *
 * cdc_decoder_utils.h
 *	  Utility functions and declerations for cdc decoder.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CDC_DECODER_H
#define CITUS_CDC_DECODER_H

#include "postgres.h"
#include "fmgr.h"
#include "replication/logical.h"
#include "c.h"

#define InvalidRepOriginId 0
#define INVALID_SHARD_ID 0

bool CdcIsCoordinator(void);

uint64 CdcExtractShardIdFromTableName(const char *tableName, bool missingOk);

Oid CdcLookupShardRelationFromCatalog(int64 shardId, bool missingOk);

char CdcPartitionMethodViaCatalog(Oid relationId);

bool CdcCitusHasBeenLoaded(void);

char * RemoveCitusDecodersFromPaths(char *paths);

#endif   /* CITUS_CDC_DECODER_UTILS_H */
