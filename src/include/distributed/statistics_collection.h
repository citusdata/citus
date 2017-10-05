/*-------------------------------------------------------------------------
 *
 * statistics_collection.h
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef STATISTICS_COLLECTION_H
#define STATISTICS_COLLECTION_H

#if HAVE_LIBCURL

#define STATS_COLLECTION_URL "http://localhost:5000/collect_stats"
#define HTTP_TIMEOUT_SECONDS 5

/* Config variables managed via guc.c */
extern bool EnableStatisticsCollection;

extern void WarnIfSyncDNS(void);
extern bool CollectBasicUsageStatistics(void);

#endif /* HAVE_LIBCURL */

#endif /* STATISTICS_COLLECTION_H */
