/*-------------------------------------------------------------------------
 *
 * safe_lib.h
 *
 * This file contains helper functions to expand on the _s functions from
 * safestringlib.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_safe_lib_H
#define CITUS_safe_lib_H

#include "postgres.h"

#include "safe_lib.h"

extern void ereport_constraint_handler(const char *message, void *pointer, errno_t error);
extern int64 SafeStringToInt64(const char *str);
extern uint64 SafeStringToUint64(const char *str);
extern void SafeQsort(void *ptr, rsize_t count, rsize_t size,
					  int (*comp)(const void *, const void *));
void * SafeBsearch(const void *key, const void *ptr, rsize_t count, rsize_t size,
				   int (*comp)(const void *, const void *));
int SafeSnprintf(char *str, rsize_t count, const char *fmt, ...);

#define memset_struct_0(variable) memset(&variable, 0, sizeof(variable))

#endif
