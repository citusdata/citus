/*-------------------------------------------------------------------------
 *
 * timeseries_utils.h
 *
 * Declarations for public utility functions related timeseries
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TIMESERIES_UTILS_H_
#define TIMESERIES_UTILS_H_

#include "postgres.h"
#include "server/datatype/timestamp.h"
#include "server/partitioning/partdefs.h"

extern Oid CitusTimeseriesTablesRelationId(void);
extern Oid TimeseriesNamespaceId(void);
extern bool CheckIntervalAlignmentWithThresholds(Interval *partitionInterval,
												 Interval *compressionThreshold,
												 Interval *retentionThreshold);
extern bool CheckIntervalAlignnmentWithPartitionKey(PartitionKey partitionKey,
													Interval *partitionInterval);

#endif /* TIMESERIES_UTILS_H_ */
