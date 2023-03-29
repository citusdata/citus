/*-------------------------------------------------------------------------
 *
 * cdc_decoder..h
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


void PublishDistributedTableChanges(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
									Relation relation, ReorderBufferChange *change);

void InitCDCDecoder(OutputPluginCallbacks *cb, LogicalDecodeChangeCB changeCB);

/* used in the replication_origin_filter_cb function. */
#define InvalidRepOriginId 0

#endif   /* CITUS_CDC_DECODER_H */
