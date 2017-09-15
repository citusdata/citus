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

#include <curl/curl.h>

static bool SendGETRequest(const char *url);

void
CallHome(void)
{
	elog(NOTICE, "Calling home.");
	SendGETRequest("http://localhost:5000/collect_stats");
}


static bool
SendGETRequest(const char *url)
{
	bool requestSent = false;
	CURLcode curlCode = false;
	CURL *curl = NULL;

	curl_global_init(CURL_GLOBAL_DEFAULT);
	curl = curl_easy_init();
	if (curl)
	{
		curl_easy_setopt(curl, CURLOPT_URL, url);
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
