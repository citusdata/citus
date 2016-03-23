/*-------------------------------------------------------------------------
 *
 * test/src/create_shards.c
 *
 * This file contains functions to exercise shard creation functionality
 * within Citus.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include <string.h>

#include "distributed/listutils.h"
#include "distributed/test_helper_functions.h" /* IWYU pragma: keep */
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* local function forward declarations */
static int CompareStrings(const void *leftElement, const void *rightElement);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(sort_names);


/*
 * sort_names accepts three strings, places them in a list, then calls PGSSortList
 * to test its sort functionality. Returns a string containing sorted lines.
 */
Datum
sort_names(PG_FUNCTION_ARGS)
{
	char *first = PG_GETARG_CSTRING(0);
	char *second = PG_GETARG_CSTRING(1);
	char *third = PG_GETARG_CSTRING(2);
	List *nameList = SortList(list_make3(first, second, third),
							  (int (*)(const void *, const void *))(&CompareStrings));
	StringInfo sortedNames = makeStringInfo();

	ListCell *nameCell = NULL;
	foreach(nameCell, nameList)
	{
		char *name = lfirst(nameCell);
		appendStringInfo(sortedNames, "%s\n", name);
	}


	PG_RETURN_CSTRING(sortedNames->data);
}


/*
 * A simple wrapper around strcmp suitable for use with PGSSortList or qsort.
 */
static int
CompareStrings(const void *leftElement, const void *rightElement)
{
	const char *leftString = *((const char **) leftElement);
	const char *rightString = *((const char **) rightElement);

	return strcmp(leftString, rightString);
}
