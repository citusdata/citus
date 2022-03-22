/*-------------------------------------------------------------------------
 *
 * copy_url.c
 *	  COPY from a URL using libcurl
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "citus_version.h"

#include "distributed/commands/copy_url.h"
#include "storage/latch.h"
#include "utils/wait_event.h"

bool EnableCopyFromURL = true;

#ifdef HAVE_LIBCURL

#include <curl/curl.h>


/* HTTP client (libcurl) */
static CURL *curl = NULL;
static int CurrentSocket = 0;


/*
 * OpenURL opens the given URL such that it can subsequently be read via
 * ReadBytesFromURL.
 */
void
OpenURL(char *url)
{
	/* clean up any leftovers */
	CloseURL();

	curl = curl_easy_init();

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_CONNECT_ONLY, 1L);

	CURLcode res = curl_easy_perform(curl);
	if (res != CURLE_OK)
	{
		ereport(ERROR, (errmsg("failed to get URL: %s", curl_easy_strerror(res))));
	}

	res = curl_easy_getinfo(curl, CURLINFO_ACTIVESOCKET, &CurrentSocket);
}


/*
 * ReadBytesFromURL reads bytes from a URL using libcurl.
 */
int
ReadBytesFromURL(void *outbuf, int minread, int maxread)
{
	size_t nread;
	CURLcode res;

	do
	{
		nread = 0;
		res = curl_easy_recv(curl, (char *) outbuf, maxread, &nread);
 
		if (res == CURLE_AGAIN)
		{
			int waitFlags = WL_POSTMASTER_DEATH | WL_LATCH_SET | WL_SOCKET_READABLE;

			int rc = WaitLatchOrSocket(MyLatch, waitFlags, CurrentSocket, 30000,
									   PG_WAIT_IO);
			if (rc & WL_POSTMASTER_DEATH)
			{
				ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
			}

			if (rc & WL_TIMEOUT)
			{
				ereport(ERROR, (errmsg("timeout while requesting URL")));
			}

			if (rc & WL_LATCH_SET)
			{
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
			}
		}
	}
	while (res == CURLE_AGAIN);

elog(NOTICE, "read %d bytes", nread);

	return (int) nread;
}


/*
 * CloseURL closes a currently opened URL.
 */
void
CloseURL(void)
{
	if (curl != NULL)
	{
		curl_easy_cleanup(curl);
		curl = NULL;
	}
}

#endif
