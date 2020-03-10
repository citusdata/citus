/*-------------------------------------------------------------------------
 *
 * safe_lib.c
 *
 * This file contains all SafeXXXX helper functions that we implement to
 * replace missing xxxx_s functions implemented by safestringlib. It also
 * contains a constraint handler for use in both our SafeXXX and safestringlib
 * its xxxx_s functions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "safe_lib.h"

#include <limits.h>

#include "distributed/citus_safe_lib.h"
#include "lib/stringinfo.h"

/*
 * In PG 11 pg_vsnprintf is not exported and compiled in most cases, in that
 * case use the copied one from pg11_snprintf.c
 * NOTE: Whenever removing this section also remove pg11_snprintf.c
 */
#if PG_VERSION_NUM < 120000
extern int pg11_vsnprintf(char *str, size_t count, const char *fmt, va_list args);
#define citus_vsnprintf pg11_vsnprintf
#else
#define citus_vsnprintf pg_vsnprintf
#endif


/*
 * ereport_constraint_handler is a constraint handler that calls ereport. A
 * constraint handler is called whenever an error occurs in any of the
 * safestringlib xxxx_s functions or our SafeXXXX functions.
 *
 * More info on constraint handlers can be found here:
 * https://en.cppreference.com/w/c/error/set_constraint_handler_s
 */
void
ereport_constraint_handler(const char *message,
						   void *pointer,
						   errno_t error)
{
	if (message && error)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg(
							"Memory constraint error: %s (errno %d)", message, error)));
	}
	else if (message)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg(
							"Memory constraint error: %s", message)));
	}
	else if (error)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg(
							"Unknown function failed with memory constraint error (errno %d)",
							error)));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg(
							"Unknown function failed with memory constraint error")));
	}
}


/*
 * SafeStringToInt64 converts a string containing a number to a int64. When it
 * fails it calls ereport.
 *
 * The different error cases are inspired by
 * https://stackoverflow.com/a/26083517/2570866
 */
int64
SafeStringToInt64(const char *str)
{
	char *endptr;
	errno = 0;
	long long number = strtoll(str, &endptr, 10);

	if (str == endptr)
	{
		ereport(ERROR, (errmsg("Error parsing %s as int64, no digits found\n", str)));
	}
	else if ((errno == ERANGE && number == LLONG_MIN) || number < INT64_MIN)
	{
		ereport(ERROR, (errmsg("Error parsing %s as int64, underflow occured\n", str)));
	}
	else if ((errno == ERANGE && number == LLONG_MAX) || number > INT64_MAX)
	{
		ereport(ERROR, (errmsg("Error parsing %s as int64, overflow occured\n", str)));
	}
	else if (errno == EINVAL)
	{
		ereport(ERROR, (errmsg(
							"Error parsing %s as int64, base contains unsupported value\n",
							str)));
	}
	else if (errno != 0 && number == 0)
	{
		int err = errno;
		ereport(ERROR, (errmsg("Error parsing %s as int64, errno %d\n", str, err)));
	}
	else if (errno == 0 && str && *endptr != '\0')
	{
		ereport(ERROR, (errmsg(
							"Error parsing %s as int64, aditional characters remain after int64\n",
							str)));
	}
	return number;
}


/*
 * SafeStringToUint64 converts a string containing a number to a uint64. When it
 * fails it calls ereport.
 *
 * The different error cases are inspired by
 * https://stackoverflow.com/a/26083517/2570866
 */
uint64
SafeStringToUint64(const char *str)
{
	char *endptr;
	errno = 0;
	unsigned long long number = strtoull(str, &endptr, 10);

	if (str == endptr)
	{
		ereport(ERROR, (errmsg("Error parsing %s as uint64, no digits found\n", str)));
	}
	else if ((errno == ERANGE && number == ULLONG_MAX) || number > UINT64_MAX)
	{
		ereport(ERROR, (errmsg("Error parsing %s as uint64, overflow occured\n", str)));
	}
	else if (errno == EINVAL)
	{
		ereport(ERROR, (errmsg(
							"Error parsing %s as uint64, base contains unsupported value\n",
							str)));
	}
	else if (errno != 0 && number == 0)
	{
		int err = errno;
		ereport(ERROR, (errmsg("Error parsing %s as uint64, errno %d\n", str, err)));
	}
	else if (errno == 0 && str && *endptr != '\0')
	{
		ereport(ERROR, (errmsg(
							"Error parsing %s as uint64, aditional characters remain after uint64\n",
							str)));
	}
	return number;
}


/*
 * SafeQsort is the non reentrant version of qsort (qsort vs qsort_r), but it
 * does the input checks required for qsort_s:
 *  1. count or size is greater than RSIZE_MAX
 *  2. ptr or comp is a null pointer (unless count is zero)
 * source: https://en.cppreference.com/w/c/algorithm/qsort
 *
 * When it hits these errors it calls the ereport_constraint_handler.
 *
 * NOTE: this functions calls pg_qsort instead of stdlib qsort.
 */
void
SafeQsort(void *ptr, rsize_t count, rsize_t size,
		  int (*comp)(const void *, const void *))
{
	if (count > RSIZE_MAX_MEM)
	{
		ereport_constraint_handler("SafeQsort: count exceeds max",
								   NULL, ESLEMAX);
	}

	if (size > RSIZE_MAX_MEM)
	{
		ereport_constraint_handler("SafeQsort: size exceeds max",
								   NULL, ESLEMAX);
	}
	if (size != 0)
	{
		if (ptr == NULL)
		{
			ereport_constraint_handler("SafeQsort: ptr is NULL", NULL, ESNULLP);
		}
		if (comp == NULL)
		{
			ereport_constraint_handler("SafeQsort: comp is NULL", NULL, ESNULLP);
		}
	}
	pg_qsort(ptr, count, size, comp);
}


/*
 * SafeBsearch is a non reentrant version of bsearch, but it does the
 * input checks required for bsearch_s:
 *  1. count or size is greater than RSIZE_MAX
 *  2. key, ptr or comp is a null pointer (unless count is zero)
 * source: https://en.cppreference.com/w/c/algorithm/bsearch
 *
 * When it hits these errors it calls the ereport_constraint_handler.
 *
 * NOTE: this functions calls pg_qsort instead of stdlib qsort.
 */
void *
SafeBsearch(const void *key, const void *ptr, rsize_t count, rsize_t size,
			int (*comp)(const void *, const void *))
{
	if (count > RSIZE_MAX_MEM)
	{
		ereport_constraint_handler("SafeBsearch: count exceeds max",
								   NULL, ESLEMAX);
	}

	if (size > RSIZE_MAX_MEM)
	{
		ereport_constraint_handler("SafeBsearch: size exceeds max",
								   NULL, ESLEMAX);
	}
	if (size != 0)
	{
		if (key == NULL)
		{
			ereport_constraint_handler("SafeBsearch: key is NULL", NULL, ESNULLP);
		}
		if (ptr == NULL)
		{
			ereport_constraint_handler("SafeBsearch: ptr is NULL", NULL, ESNULLP);
		}
		if (comp == NULL)
		{
			ereport_constraint_handler("SafeBsearch: comp is NULL", NULL, ESNULLP);
		}
	}

	/*
	 * Explanation of IGNORE-BANNED:
	 * bsearch is safe to use here since we check the same thing bsearch_s
	 * does. We cannot use bsearch_s as a replacement, since it's not available
	 * in safestringlib.
	 */
	return bsearch(key, ptr, count, size, comp); /* IGNORE-BANNED */
}


/*
 * SafeSnprintf is a safer replacement for snprintf, which is needed since
 * safestringlib doesn't implement snprintf_s.
 *
 * The required failure modes of snprint_s are as follows (in parentheses if
 * this implements it and how):
 * 1. the conversion specifier %n is present in format (yes, %n is not
 *    supported by pg_vsnprintf)
 * 2. any of the arguments corresponding to %s is a null pointer (half, checked
 *    in postgres when asserts are enabled)
 * 3. format or buffer is a null pointer (yes, checked by this function)
 * 4. bufsz is zero or greater than RSIZE_MAX (yes, checked by this function)
 * 5. encoding errors occur in any of string and character conversion
 *    specifiers (no clue what postgres does in this case)
 * source: https://en.cppreference.com/w/c/io/fprintf
 */
int
SafeSnprintf(char *restrict buffer, rsize_t bufsz, const char *restrict format, ...)
{
	/* failure mode 3 */
	if (buffer == NULL)
	{
		ereport_constraint_handler("SafeSnprintf: buffer is NULL", NULL, ESNULLP);
	}
	if (format == NULL)
	{
		ereport_constraint_handler("SafeSnprintf: format is NULL", NULL, ESNULLP);
	}

	/* failure mode 4 */
	if (bufsz == 0)
	{
		ereport_constraint_handler("SafeSnprintf: bufsz is 0",
								   NULL, ESZEROL);
	}

	if (bufsz > RSIZE_MAX_STR)
	{
		ereport_constraint_handler("SafeSnprintf: bufsz exceeds max",
								   NULL, ESLEMAX);
	}

	va_list args;

	va_start(args, format);
	size_t result = citus_vsnprintf(buffer, bufsz, format, args);
	va_end(args);
	return result;
}
