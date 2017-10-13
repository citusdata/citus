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

bool EnableStatisticsCollection = true; /* send basic usage statistics to Citus */

#if HAVE_LIBCURL

#include <curl/curl.h>
#include <sys/utsname.h>

#include "access/xact.h"
#include "citus_version.h"
#include "distributed/metadata_cache.h"
#include "distributed/statistics_collection.h"
#include "distributed/worker_manager.h"
#include "lib/stringinfo.h"
#include "utils/json.h"

static uint64 NextPow2(uint64 n);
static uint64 ClusterSize(List *distributedTableList);
static bool SendHttpPostJsonRequest(const char *url, const char *postFields, long
									timeoutSeconds);

/* WarnIfSyncDNS warns if libcurl is compiled with synchronous DNS. */
void
WarnIfSyncDNS(void)
{
	curl_version_info_data *versionInfo = curl_version_info(CURLVERSION_NOW);
	if (!(versionInfo->features & CURL_VERSION_ASYNCHDNS))
	{
		ereport(WARNING, (errmsg("your current libcurl version doesn't support "
								 "asynchronous DNS, which might cause unexpected "
								 "delays in the operation of Citus"),
						  errhint("Install a libcurl version with asynchronous DNS "
								  "support.")));
	}
}


/*
 * CollectBasicUsageStatistics sends basic usage statistics to Citus servers.
 * This includes Citus version, table count rounded to next power of 2, cluster
 * size rounded to next power of 2, worker node count, and uname data. Returns
 * true if we actually have sent statistics to the server.
 */
bool
CollectBasicUsageStatistics(void)
{
	List *distributedTables = NIL;
	uint64 roundedDistTableCount = 0;
	uint64 roundedClusterSize = 0;
	uint32 workerNodeCount = 0;
	StringInfo fields = makeStringInfo();
	struct utsname unameData;
	memset(&unameData, 0, sizeof(unameData));

	StartTransactionCommand();
	distributedTables = DistributedTableList();
	roundedDistTableCount = NextPow2(list_length(distributedTables));
	roundedClusterSize = NextPow2(ClusterSize(distributedTables));
	workerNodeCount = ActivePrimaryNodeCount();
	CommitTransactionCommand();

	uname(&unameData);

	appendStringInfoString(fields, "{\"citus_version\": ");
	escape_json(fields, CITUS_VERSION);
	appendStringInfo(fields, ",\"table_count\": " UINT64_FORMAT, roundedDistTableCount);
	appendStringInfo(fields, ",\"cluster_size\": " UINT64_FORMAT, roundedClusterSize);
	appendStringInfo(fields, ",\"worker_node_count\": %u", workerNodeCount);
	appendStringInfoString(fields, ",\"os_name\": ");
	escape_json(fields, unameData.sysname);
	appendStringInfoString(fields, ",\"os_release\": ");
	escape_json(fields, unameData.release);
	appendStringInfoString(fields, ",\"hwid\": ");
	escape_json(fields, unameData.machine);
	appendStringInfoString(fields, "}");

	return SendHttpPostJsonRequest(STATS_COLLECTION_HOST "/v1/usage_reports",
								   fields->data, HTTP_TIMEOUT_SECONDS);
}


/*
 * ClusterSize returns total size of data store in the cluster consisting of
 * given distributed tables. We ignore tables which we cannot get their size.
 */
static uint64
ClusterSize(List *distributedTableList)
{
	uint64 clusterSize = 0;
	ListCell *distTableCacheEntryCell = NULL;

	foreach(distTableCacheEntryCell, distributedTableList)
	{
		DistTableCacheEntry *distTableCacheEntry = lfirst(distTableCacheEntryCell);
		Oid relationId = distTableCacheEntry->relationId;
		MemoryContext savedContext = CurrentMemoryContext;

		PG_TRY();
		{
			Datum distTableSizeDatum = DirectFunctionCall1(citus_table_size,
														   ObjectIdGetDatum(relationId));
			clusterSize += DatumGetInt64(distTableSizeDatum);
		}
		PG_CATCH();
		{
			FlushErrorState();

			/* citus_table_size() throws an error while the memory context is changed */
			MemoryContextSwitchTo(savedContext);
		}
		PG_END_TRY();
	}

	return clusterSize;
}


/*
 * NextPow2 returns smallest power of 2 less than or equal to n. If n is greater
 * than 2^63, it returns 2^63. Returns 0 when n is 0.
 */
static uint64
NextPow2(uint64 n)
{
	uint64 result = 1;

	if (n == 0)
	{
		return 0;
	}

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
 * SendHttpPostJsonRequest sends a HTTP/HTTPS POST request to the given URL with
 * the given json object.
 */
static bool
SendHttpPostJsonRequest(const char *url, const char *jsonObj, long timeoutSeconds)
{
	bool success = false;
	CURLcode curlCode = false;
	CURL *curl = NULL;

	curl_global_init(CURL_GLOBAL_DEFAULT);
	curl = curl_easy_init();
	if (curl)
	{
		struct curl_slist *headers = NULL;
		headers = curl_slist_append(headers, "Content-Type: application/json");
		headers = curl_slist_append(headers, "charsets: utf-8");

		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonObj);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeoutSeconds);
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

		curlCode = curl_easy_perform(curl);
		if (curlCode == CURLE_OK)
		{
			int64 httpCode = 0;
			curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
			if (httpCode == 200)
			{
				success = true;
			}
			else if (httpCode >= 400 && httpCode < 500)
			{
				ereport(WARNING, (errmsg("HTTP request failed."),
								  errhint("HTTP response code: " INT64_FORMAT,
										  httpCode)));
			}
		}
		else
		{
			ereport(WARNING, (errmsg("Sending HTTP POST request failed."),
							  errhint("Error code: %s.", curl_easy_strerror(curlCode))));
		}

		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
	}

	curl_global_cleanup();

	return success;
}


#endif /* HAVE_LIBCURL */
