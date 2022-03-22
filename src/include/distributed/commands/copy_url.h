/*-------------------------------------------------------------------------
 *
 * copy_url.h
 *   Function for copying from a URL.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COPY_URL_H
#define COPY_URL_H

#include "citus_version.h"

/* Config variables managed via guc.c */
extern bool EnableCopyFromURL;

#ifdef HAVE_LIBCURL

void OpenURL(char *url);
int ReadBytesFromURL(void *outbuf, int minread, int maxread);
void CloseURL(void);

#endif /* HAVE_LIBCURL */
#endif /* COPY_URL_H */
