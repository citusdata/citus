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

#include "citus_version.h"
#include "fmgr.h"
#include "utils/uuid.h"

bool EnableStatisticsCollection = true; /* send basic usage statistics to Citus */

PG_FUNCTION_INFO_V1(citus_server_id);

#ifdef HAVE_LIBCURL

#include <curl/curl.h>
#ifndef WIN32
#include <sys/utsname.h>
#else
typedef struct utsname
{
	char sysname[65];
	char release[65];
	char version[65];
	char machine[65];
} utsname;
#endif

#include "access/xact.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/statistics_collection.h"
#include "distributed/worker_manager.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/fmgrprotos.h"

static size_t StatisticsCallback(char *contents, size_t size, size_t count,
								 void *userData);
static size_t CheckForUpdatesCallback(char *contents, size_t size, size_t count,
									  void *userData);
static bool JsonbFieldInt32(Jsonb *jsonb, const char *fieldName, int32 *result);
static bool JsonbFieldStr(Jsonb *jsonb, const char *fieldName, StringInfo result);
static uint64 NextPow2(uint64 n);
static uint64 DistributedTablesSize(List *distTableOids);
static bool UrlEncode(StringInfo buf, const char *str);
static bool SendHttpPostJsonRequest(const char *url, const char *postFields,
									long timeoutSeconds,
									curl_write_callback responseCallback);
static bool SendHttpGetJsonRequest(const char *url, long timeoutSeconds,
								   curl_write_callback responseCallback);
static bool PerformHttpRequest(CURL *curl);
#ifdef WIN32
static int uname(struct utsname *buf);
#endif


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
	List *distTableOids = NIL;
	uint64 roundedDistTableCount = 0;
	uint64 roundedClusterSize = 0;
	uint32 workerNodeCount = 0;
	StringInfo fields = makeStringInfo();
	Datum metadataJsonbDatum = 0;
	char *metadataJsonbStr = NULL;
	MemoryContext savedContext = CurrentMemoryContext;
	int unameResult PG_USED_FOR_ASSERTS_ONLY = 0;
	bool metadataCollectionFailed = false;
	struct utsname unameData;
	memset(&unameData, 0, sizeof(unameData));

	/*
	 * Start a subtransaction so we can rollback database's state to it in case
	 * of error.
	 */
	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		distTableOids = DistTableOidList();
		roundedDistTableCount = NextPow2(list_length(distTableOids));
		roundedClusterSize = NextPow2(DistributedTablesSize(distTableOids));
		workerNodeCount = ActivePrimaryNodeCount();
		metadataJsonbDatum = DistNodeMetadata();
		metadataJsonbStr = DatumGetCString(DirectFunctionCall1(jsonb_out,
															   metadataJsonbDatum));

		/*
		 * Releasing a subtransaction doesn't free its memory context, since the
		 * data it contains will be needed at upper commit. See the comments for
		 * AtSubCommit_Memory() at postgres/src/backend/access/transam/xact.c.
		 */
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		ErrorData *edata = NULL;
		MemoryContextSwitchTo(savedContext);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();

		/* rethrow as WARNING */
		edata->elevel = WARNING;
		ThrowErrorData(edata);
		metadataCollectionFailed = true;
	}
	PG_END_TRY();

	/*
	 * Returning here instead of in PG_CATCH() since PG_END_TRY() resets couple
	 * of global variables.
	 */
	if (metadataCollectionFailed)
	{
		return false;
	}

	unameResult = uname(&unameData);
	Assert(unameResult == 0);  /* uname() always succeeds if we pass valid buffer */

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
	appendStringInfo(fields, ",\"node_metadata\": %s", metadataJsonbStr);
	appendStringInfoString(fields, "}");

	return SendHttpPostJsonRequest(REPORTS_BASE_URL "/v1/usage_reports",
								   fields->data, HTTP_TIMEOUT_SECONDS,
								   StatisticsCallback);
}


/*
 * StatisticsCallback receives the response for the request sent by
 * CollectBasicUsageStatistics. For now, it doesn't check the contents of the
 * response and succeeds for any response.
 */
static size_t
StatisticsCallback(char *contents, size_t size, size_t count, void *userData)
{
	return size * count;
}


/* CheckForUpdates queries Citus servers for newer releases of Citus. */
void
CheckForUpdates(void)
{
	StringInfo url = makeStringInfo();
	appendStringInfoString(url, REPORTS_BASE_URL "/v1/releases/latest?edition=");

	if (!UrlEncode(url, CITUS_EDITION))
	{
		ereport(WARNING, (errmsg("url encoding '%s' failed", CITUS_EDITION)));
		return;
	}

	if (!SendHttpGetJsonRequest(url->data, HTTP_TIMEOUT_SECONDS,
								&CheckForUpdatesCallback))
	{
		ereport(WARNING, (errmsg("checking for updates failed")));
	}
}


/*
 * CheckForUpdatesCallback receives the response for the request sent by
 * CheckForUpdates(). It processes the response, and if there is a newer release
 * of Citus available, logs a LOG message. This function returns 0 if there are
 * any errors in the received response, which means we didn't consume the data.
 * Otherwise, it returns (size * count) which means we consumed all of the data.
 */
static size_t
CheckForUpdatesCallback(char *contents, size_t size, size_t count, void *userData)
{
	const int32 citusVersionMajor = CITUS_VERSION_NUM / 10000;
	const int32 citusVersionMinor = (CITUS_VERSION_NUM / 100) % 100;
	const int32 citusVersionPatch = CITUS_VERSION_NUM % 100;
	Jsonb *responseJsonb = NULL;
	StringInfo releaseVersion = makeStringInfo();
	int32 releaseMajor = 0;
	int32 releaseMinor = 0;
	int32 releasePatch = 0;
	char *updateType = NULL;
	MemoryContext savedContext = CurrentMemoryContext;

	StringInfo responseNullTerminated = makeStringInfo();
	appendBinaryStringInfo(responseNullTerminated, contents, size * count);

	/*
	 * Start a subtransaction so we can rollback database's state to it in case
	 * of error.
	 */
	BeginInternalSubTransaction(NULL);

	/* jsonb_in can throw errors */
	PG_TRY();
	{
		Datum responseCStringDatum = CStringGetDatum(responseNullTerminated->data);
		Datum responseJasonbDatum = DirectFunctionCall1(jsonb_in, responseCStringDatum);
		responseJsonb = DatumGetJsonbP(responseJasonbDatum);
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		responseJsonb = NULL;
	}
	PG_END_TRY();

	/*
	 * Returning here instead of in PG_CATCH() because PG_END_TRY() resets
	 * couple of global variables.
	 */
	if (responseJsonb == NULL)
	{
		return 0;
	}

	if (!JsonbFieldStr(responseJsonb, "version", releaseVersion) ||
		!JsonbFieldInt32(responseJsonb, "major", &releaseMajor) ||
		!JsonbFieldInt32(responseJsonb, "minor", &releaseMinor) ||
		!JsonbFieldInt32(responseJsonb, "patch", &releasePatch))
	{
		return 0;
	}

	if ((releaseMajor > citusVersionMajor) ||
		(releaseMajor == citusVersionMajor && releaseMinor > citusVersionMinor))
	{
		updateType = "major";
	}
	else if (releaseMajor == citusVersionMajor &&
			 releaseMinor == citusVersionMinor &&
			 releasePatch > citusVersionPatch)
	{
		updateType = "patch";
	}

	if (updateType != NULL)
	{
		ereport(LOG, (errmsg("a new %s release of Citus (%s) is available",
							 updateType, releaseVersion->data)));
	}

	return size * count;
}


/*
 * JsonbFieldInt32 sets the given output variable to the int32 value of the given
 * field in the given JSONB object. If the field doesn't exist or its value is
 * not an integer that fits in 32-bits, this function returns false.
 */
static bool
JsonbFieldInt32(Jsonb *jsonb, const char *fieldName, int32 *result)
{
	MemoryContext savedContext = CurrentMemoryContext;
	bool success = false;
	JsonbValue *fieldValue = NULL;
	JsonbValue key;
	memset(&key, 0, sizeof(key));
	key.type = jbvString;
	key.val.string.len = strlen(fieldName);
	key.val.string.val = (char *) fieldName;

	fieldValue = findJsonbValueFromContainer(&(jsonb->root), JB_FOBJECT, &key);
	if (fieldValue == NULL || fieldValue->type != jbvNumeric)
	{
		return false;
	}

	/*
	 * Start a subtransaction so we can rollback database's state to it in case
	 * of error.
	 */
	BeginInternalSubTransaction(NULL);

	/* numeric_int4 can throw errors */
	PG_TRY();
	{
		Datum resultNumericDatum = NumericGetDatum(fieldValue->val.numeric);
		*result = DatumGetInt32(DirectFunctionCall1(numeric_int4, resultNumericDatum));
		success = true;
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		FlushErrorState();
		success = false;
		RollbackAndReleaseCurrentSubTransaction();
	}
	PG_END_TRY();

	return success;
}


/*
 * JsonbFieldStr appends string value of the given field in the given JSONB
 * object to the given string buffer. If the field doesn't exist or its value is
 * not string, this function returns false. Otherwise it returns true.
 */
static bool
JsonbFieldStr(Jsonb *jsonb, const char *fieldName, StringInfo result)
{
	JsonbValue *fieldValue = NULL;
	JsonbValue key;
	memset(&key, 0, sizeof(key));
	key.type = jbvString;
	key.val.string.len = strlen(fieldName);
	key.val.string.val = (char *) fieldName;

	fieldValue = findJsonbValueFromContainer(&(jsonb->root), JB_FOBJECT, &key);
	if (fieldValue == NULL || fieldValue->type != jbvString)
	{
		return false;
	}

	appendBinaryStringInfo(result, fieldValue->val.string.val,
						   fieldValue->val.string.len);
	return true;
}


/*
 * DistributedTablesSize returns total size of data store in the cluster consisting
 * of given distributed tables. We ignore tables which we cannot get their size.
 */
static uint64
DistributedTablesSize(List *distTableOids)
{
	uint64 totalSize = 0;
	ListCell *distTableOidCell = NULL;

	foreach(distTableOidCell, distTableOids)
	{
		Oid relationId = lfirst_oid(distTableOidCell);
		Datum tableSizeDatum = 0;

		/*
		 * Relations can get dropped after getting the Oid list and before we
		 * reach here. Acquire a lock to make sure the relation is available
		 * while we are getting its size.
		 */
		Relation relation = try_relation_open(relationId, AccessShareLock);
		if (relation == NULL)
		{
			continue;
		}

		/*
		 * Ignore hash partitioned tables with size greater than 1, since
		 * citus_table_size() doesn't work on them.
		 */
		if (PartitionMethod(relationId) == DISTRIBUTE_BY_HASH &&
			!SingleReplicatedTable(relationId))
		{
			heap_close(relation, AccessShareLock);
			continue;
		}

		tableSizeDatum = DirectFunctionCall1(citus_table_size,
											 ObjectIdGetDatum(relationId));
		totalSize += DatumGetInt64(tableSizeDatum);
		heap_close(relation, AccessShareLock);
	}

	return totalSize;
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
 * UrlEncode URL encodes the given string and appends it to the given buffer.
 * If either libcurl initialization or encoding fails, returns false.
 */
static bool
UrlEncode(StringInfo buf, const char *str)
{
	bool success = false;
	CURL *curl = NULL;

	curl_global_init(CURL_GLOBAL_DEFAULT);
	curl = curl_easy_init();
	if (curl)
	{
		char *urlEncodedStr = curl_easy_escape(curl, str, strlen(str));
		if (urlEncodedStr)
		{
			appendStringInfoString(buf, urlEncodedStr);
			curl_free(urlEncodedStr);
			success = true;
		}

		curl_easy_cleanup(curl);
	}

	curl_global_cleanup();
	return success;
}


/*
 * SendHttpPostJsonRequest sends a HTTP/HTTPS POST request to the given URL with
 * the given json object. responseCallback is called with the content of response.
 */
static bool
SendHttpPostJsonRequest(const char *url, const char *jsonObj, long timeoutSeconds,
						curl_write_callback responseCallback)
{
	bool success = false;
	CURL *curl = NULL;

	curl_global_init(CURL_GLOBAL_DEFAULT);
	curl = curl_easy_init();
	if (curl)
	{
		struct curl_slist *headers = NULL;
		headers = curl_slist_append(headers, "Accept: application/json");
		headers = curl_slist_append(headers, "Content-Type: application/json");
		headers = curl_slist_append(headers, "charsets: utf-8");

		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonObj);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeoutSeconds);
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, responseCallback);

		success = PerformHttpRequest(curl);

		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
	}

	curl_global_cleanup();

	return success;
}


/*
 * SendHttpGetJsonRequest sends an HTTP/HTTPS GET request to the given URL, and
 * expects a JSON response from server. GET parameters should be added to the url.
 * responseCallback is called with the content of response.
 */
static bool
SendHttpGetJsonRequest(const char *url, long timeoutSeconds,
					   curl_write_callback responseCallback)
{
	bool success = false;
	CURL *curl = NULL;

	curl_global_init(CURL_GLOBAL_DEFAULT);
	curl = curl_easy_init();
	if (curl)
	{
		struct curl_slist *headers = NULL;
		headers = curl_slist_append(headers, "Accept: application/json");

		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeoutSeconds);
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, responseCallback);

		success = PerformHttpRequest(curl);

		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
	}

	curl_global_cleanup();

	return success;
}


/*
 * PerformHttpRequest sends the HTTP request with the parameters set in the given
 * curl object, and returns if it was successful or not. If the request was not
 * successful, it may log some warnings. This method expects to take place after
 * curl_easy_init() but before curl_easy_cleanup().
 */
static bool
PerformHttpRequest(CURL *curl)
{
	bool success = false;
	CURLcode curlCode = curl_easy_perform(curl);
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
		ereport(WARNING, (errmsg("Sending HTTP request failed."),
						  errhint("Error code: %s.", curl_easy_strerror(curlCode))));
	}

	return success;
}


#endif /* HAVE_LIBCURL */

/*
 * citus_server_id returns a random UUID value as server identifier. This is
 * modeled after PostgreSQL's pg_random_uuid().
 */
Datum
citus_server_id(PG_FUNCTION_ARGS)
{
	uint8 *buf = (uint8 *) palloc(UUID_LEN);

	/*
	 * If pg_strong_random() fails, fall-back to using random(). In previous
	 * versions of postgres we don't have pg_strong_random(), so use it by
	 * default in that case.
	 */
	if (!pg_strong_random((char *) buf, UUID_LEN))
	{
		int bufIdx = 0;
		for (bufIdx = 0; bufIdx < UUID_LEN; bufIdx++)
		{
			buf[bufIdx] = (uint8) (random() & 0xFF);
		}
	}

	/*
	 * Set magic numbers for a "version 4" (pseudorandom) UUID, see
	 * http://tools.ietf.org/html/rfc4122#section-4.4
	 */
	buf[6] = (buf[6] & 0x0f) | 0x40;    /* "version" field */
	buf[8] = (buf[8] & 0x3f) | 0x80;    /* "variant" field */

	PG_RETURN_UUID_P((pg_uuid_t *) buf);
}


#ifdef WIN32

/*
 * Inspired by perl5's win32_uname
 * https://github.com/Perl/perl5/blob/69374fe705978962b85217f3eb828a93f836fd8d/win32/win32.c#L2057
 */
static int
uname(struct utsname *buf)
{
	OSVERSIONINFO ver;

	ver.dwOSVersionInfoSize = sizeof(ver);
	GetVersionEx(&ver);

	switch (ver.dwPlatformId)
	{
		case VER_PLATFORM_WIN32_WINDOWS:
		{
			strcpy(buf->sysname, "Windows");
			break;
		}

		case VER_PLATFORM_WIN32_NT:
		{
			strcpy(buf->sysname, "Windows NT");
			break;
		}

		case VER_PLATFORM_WIN32s:
		{
			strcpy(buf->sysname, "Win32s");
			break;
		}

		default:
		{
			strcpy(buf->sysname, "Win32 Unknown");
			break;
		}
	}

	sprintf(buf->release, "%d.%d", ver.dwMajorVersion, ver.dwMinorVersion);

	{
		SYSTEM_INFO info;
		DWORD procarch;
		char *arch;

		GetSystemInfo(&info);
		procarch = info.wProcessorArchitecture;

		switch (procarch)
		{
			case PROCESSOR_ARCHITECTURE_INTEL:
			{
				arch = "x86";
				break;
			}

			case PROCESSOR_ARCHITECTURE_IA64:
			{
				arch = "x86";
				break;
			}

			case PROCESSOR_ARCHITECTURE_AMD64:
			{
				arch = "x86";
				break;
			}

			case PROCESSOR_ARCHITECTURE_UNKNOWN:
			{
				arch = "x86";
				break;
			}

			default:
			{
				arch = NULL;
				break;
			}
		}

		if (arch != NULL)
		{
			strcpy(buf->machine, arch);
		}
		else
		{
			sprintf(buf->machine, "unknown(0x%x)", procarch);
		}
	}

	return 0;
}


#endif
