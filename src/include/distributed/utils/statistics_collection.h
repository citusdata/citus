/*-------------------------------------------------------------------------
 *
 * statistics_collection.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef STATISTICS_COLLECTION_H
#define STATISTICS_COLLECTION_H

#include "citus_version.h"

/*
 * Append USED_WITH_LIBCURL_ONLY to definitions of variables that are only used
 * when compiled with libcurl, to avoid compiler warnings about unused variables
 * when built without libcurl.
 */
#ifdef HAVE_LIBCURL
#define USED_WITH_LIBCURL_ONLY
#else
#define USED_WITH_LIBCURL_ONLY pg_attribute_unused()
#endif

/* Config variables managed via guc.c */
extern bool EnableStatisticsCollection;

#ifdef HAVE_LIBCURL

#define HTTP_TIMEOUT_SECONDS 5

extern void WarnIfSyncDNS(void);
extern bool CollectBasicUsageStatistics(void);

#endif /* HAVE_LIBCURL */

#endif /* STATISTICS_COLLECTION_H */
