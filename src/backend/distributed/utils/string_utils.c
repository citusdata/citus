/*-------------------------------------------------------------------------
 *
 * string_utils.c
 *
 * This file contains functions to perform useful operations on strings.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/relay_utility.h"
#include "distributed/string_utils.h"

/*
 * ConvertIntToString returns the string version of given integer.
 */
char *
ConvertIntToString(int val)
{
	StringInfo str = makeStringInfo();

	appendStringInfo(str, "%d", val);

	return str->data;
}
