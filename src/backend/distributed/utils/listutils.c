/*-------------------------------------------------------------------------
 *
 * listutils.c
 *
 * This file contains functions to perform useful operations on lists.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "port.h"

#include "distributed/listutils.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"


/*
 * SortList takes in a list of void pointers, and sorts these pointers (and the
 * values they point to) by applying the given comparison function. The function
 * then returns the sorted list of pointers.
 *
 * Because the input list is a list of pointers, and because qsort expects to
 * compare pointers to the list elements, the provided comparison function must
 * compare pointers to pointers to elements. In addition, this sort function
 * naturally exhibits the same lack of stability exhibited by qsort. See that
 * function's man page for more details.
 */
List *
SortList(List *pointerList, int (*comparisonFunction)(const void *, const void *))
{
	List *sortedList = NIL;
	uint32 arrayIndex = 0;
	uint32 arraySize = (uint32) list_length(pointerList);
	void **array = (void **) palloc0(arraySize * sizeof(void *));

	ListCell *pointerCell = NULL;
	foreach(pointerCell, pointerList)
	{
		void *pointer = lfirst(pointerCell);
		array[arrayIndex] = pointer;

		arrayIndex++;
	}

	/* sort the array of pointers using the comparison function */
	qsort(array, arraySize, sizeof(void *), comparisonFunction);

	/* convert the sorted array of pointers back to a sorted list */
	for (arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
	{
		void *sortedPointer = array[arrayIndex];
		sortedList = lappend(sortedList, sortedPointer);
	}

	pfree(array);

	return sortedList;
}
