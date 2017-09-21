/*-------------------------------------------------------------------------
 *
 * statistics_collection.c
 *	  Anonymous reports and statistics collection.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/statistics_collection.h"

#if HAVE_LIBCURL == 0

/* if we don't have libcurl, CallHome is no-op. */
void
CallHome(void) { }


#else

#include "postgres.h"

#include <curl/curl.h>
#include <sys/utsname.h>

#include "access/xact.h"
#include "citus_version.h"
#include "distributed/metadata_cache.h"
#include "distributed/worker_manager.h"
#include "lib/stringinfo.h"

static uint64_t NextPow2(uint64_t n);
static uint64_t ClusterSize(List *distributedTableList);
static bool SendHttpPostRequest(const char *url, const char *postFields);

void
CallHome(void)
{
	List *distributedTables = NIL;
	uint64_t roundedDistTableCount = 0;
	uint64_t roundedClusterSize = 0;
	uint32_t workerNodeCount = 0;
	struct utsname unameData;
	StringInfo fields = makeStringInfo();

	elog(WARNING, "Calling home!");

	StartTransactionCommand();
	distributedTables = DistributedTableList();
	roundedDistTableCount = NextPow2(list_length(distributedTables));
	roundedClusterSize = NextPow2(ClusterSize(distributedTables));
	workerNodeCount = ActivePrimaryNodeCount();
	CommitTransactionCommand();

	uname(&unameData);

	appendStringInfo(fields, "citus_version=%s", CITUS_VERSION);
	appendStringInfo(fields, "&table_count=" UINT64_FORMAT, roundedDistTableCount);
	appendStringInfo(fields, "&cluster_size=" UINT64_FORMAT, roundedClusterSize);
	appendStringInfo(fields, "&worker_node_count=%u", workerNodeCount);
	appendStringInfo(fields, "&os_name=%s&os_release=%s&hwid=%s",
					 unameData.sysname, unameData.release, unameData.machine);

	SendHttpPostRequest("http://localhost:5000/collect_stats", fields->data);
}


/*
 * ClusterSize returns total size of data store in the cluster consisting of
 * given distributed tables. We ignore tables which we cannot get their size.
 */
static uint64_t
ClusterSize(List *distributedTableList)
{
	uint64_t clusterSize = 0;
	ListCell *distTableCacheEntryCell = NULL;

	foreach(distTableCacheEntryCell, distributedTableList)
	{
		DistTableCacheEntry *distTableCacheEntry = lfirst(distTableCacheEntryCell);
		Oid relationId = distTableCacheEntry->relationId;

		PG_TRY();
		{
			Datum distTableSizeDatum = DirectFunctionCall1(citus_table_size,
														   ObjectIdGetDatum(relationId));
			clusterSize += DatumGetInt64(distTableSizeDatum);
		}
		PG_CATCH();
		{
			FlushErrorState();
		}
		PG_END_TRY();
	}

	return clusterSize;
}


/*
 * NextPow2 returns smallest power of 2 less than or equal to n. If n is greater
 * than 2^63, it returns 2^63.
 */
static uint64_t
NextPow2(uint64_t n)
{
	uint64_t result = 1;

	/* if there is no 64-bit power of 2 greater than n, return 2^63 */
	if (n > (1ull << 63))
	{
		return (1ull << 63);
	}

	while (result < n)
	{
		result *= 2;
	}

	return result;
}


/*
 * SendHttpPostRequest sends a HTTP/HTTPS POST request to the given URL with the
 * given POST fields.
 */
static bool
SendHttpPostRequest(const char *url, const char *postFields)
{
	bool requestSent = false;
	CURLcode curlCode = false;
	CURL *curl = NULL;

	curl_global_init(CURL_GLOBAL_DEFAULT);
	curl = curl_easy_init();
	if (curl)
	{
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields);

		curlCode = curl_easy_perform(curl);
		if (curlCode == CURLE_OK)
		{
			requestSent = true;
		}

		curl_easy_cleanup(curl);
	}

	curl_global_cleanup();

	return requestSent;
}


#endif
