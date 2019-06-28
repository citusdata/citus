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

#include "utils/lsyscache.h"
#include "lib/stringinfo.h"
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

	if (sortedList != NIL)
	{
		sortedList->type = pointerList->type;
	}

	return sortedList;
}


/*
 * PointerArrayFromList converts a list of pointers to an array of pointers.
 */
void **
PointerArrayFromList(List *pointerList)
{
	int pointerCount = list_length(pointerList);
	void **pointerArray = (void **) palloc0(pointerCount * sizeof(void *));
	ListCell *pointerCell = NULL;
	int pointerIndex = 0;

	foreach(pointerCell, pointerList)
	{
		pointerArray[pointerIndex] = (void *) lfirst(pointerCell);
		pointerIndex += 1;
	}

	return pointerArray;
}


/*
 * DatumArrayToArrayType converts the provided Datum array (of the specified
 * length and type) into an ArrayType suitable for returning from a UDF.
 */
ArrayType *
DatumArrayToArrayType(Datum *datumArray, int datumCount, Oid datumTypeId)
{
	ArrayType *arrayObject = NULL;
	int16 typeLength = 0;
	bool typeByValue = false;
	char typeAlignment = 0;

	get_typlenbyvalalign(datumTypeId, &typeLength, &typeByValue, &typeAlignment);
	arrayObject = construct_array(datumArray, datumCount, datumTypeId,
								  typeLength, typeByValue, typeAlignment);

	return arrayObject;
}


/*
 * ListToHashSet creates a hash table in which the keys are copied from
 * from itemList and the values are the same as the keys. This can
 * be used for fast lookups of the presence of a byte array in a set.
 * itemList should be a list of pointers.
 *
 * If isStringList is true, then look-ups are performed through string
 * comparison of strings up to keySize in length. If isStringList is
 * false, then look-ups are performed by comparing exactly keySize
 * bytes pointed to by the pointers in itemList.
 */
HTAB *
ListToHashSet(List *itemList, Size keySize, bool isStringList)
{
	HASHCTL info;
	HTAB *itemSet = NULL;
	ListCell *itemCell = NULL;
	int flags = HASH_ELEM | HASH_CONTEXT;

	/* allocate sufficient capacity for O(1) expected look-up time */
	int capacity = (int) (list_length(itemList) / 0.75) + 1;

	/* initialise the hash table for looking up keySize bytes */
	memset(&info, 0, sizeof(info));
	info.keysize = keySize;
	info.entrysize = keySize;
	info.hcxt = CurrentMemoryContext;

	if (!isStringList)
	{
		flags |= HASH_BLOBS;
	}

	itemSet = hash_create("ListToHashSet", capacity, &info, flags);

	foreach(itemCell, itemList)
	{
		void *item = lfirst(itemCell);
		bool foundInSet = false;

		hash_search(itemSet, item, HASH_ENTER, &foundInSet);
	}

	return itemSet;
}


/*
 * StringJoin gets a list of char * and then simply
 * returns a newly allocated char * joined with the
 * given delimiter.
 */
char *
StringJoin(List *stringList, char delimiter)
{
	ListCell *stringCell = NULL;
	StringInfo joinedString = makeStringInfo();

	foreach(stringCell, stringList)
	{
		const char *command = lfirst(stringCell);

		appendStringInfoString(joinedString, command);
		appendStringInfoChar(joinedString, delimiter);
	}

	return joinedString->data;
}
