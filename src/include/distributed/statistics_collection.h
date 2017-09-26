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

/* Config variables managed via guc.c */
extern bool EnableStatisticsCollection;

extern void CollectBasicUsageStatistics(void);

#endif /* HAVE_LIBCURL */

#endif /* STATISTICS_COLLECTION_H */
