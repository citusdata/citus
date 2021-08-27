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

#define Natts_citus_timeseries_tables 7
#define Anum_citus_timeseries_table_relation_id 1
#define Anum_citus_timeseries_table_partition_interval 2
#define Anum_citus_timeseries_table_postmake_interval_count 3
#define Anum_citus_timeseries_table_premake_interval_count 4
#define Anum_citus_timeseries_table_start_from 5
#define Anum_citus_timeseries_table_compression_threshold 6
#define Anum_citus_timeseries_table_retention_threshold 7

/*
 * Taken from the Postgres code. Postgres' uses that conversion
 * for internal interval conparison.
 */
#define INTERVAL_TO_SEC(ivp) \
	(((double) (ivp)->time) / ((double) USECS_PER_SEC) + \
	 (ivp)->day * (24.0 * SECS_PER_HOUR) + \
	 (ivp)->month * (30.0 * SECS_PER_DAY))

extern Oid CitusTimeseriesTablesRelationId(void);
extern Oid CitusTimeseriesTablesPKeyIndexRelationId(void);
extern Oid TimeseriesNamespaceId(void);
extern bool CheckIntervalAlignmentWithThresholds(Interval *partitionInterval,
												 Interval *compressionThreshold,
												 Interval *retentionThreshold);
extern bool CheckIntervalAlignnmentWithPartitionKey(PartitionKey partitionKey,
													Interval *partitionInterval);

#endif /* TIMESERIES_UTILS_H_ */
