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

#include "utils/builtins.h"

#include "distributed/listutils.h"
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

char* ConcatenateStringListWithDelimiter(List* stringList, char delimiter) {
	StringInfo result = makeStringInfo();
	
	bool isFirst = true;
	char* string = NULL;
	foreach_ptr(string, stringList) {
		if (!isFirst) {
			appendStringInfoChar(result, delimiter);
		}else {
			isFirst = false;
		}
		appendStringInfoString(result, quote_literal_cstr(string));
	}
	return result->data;
}