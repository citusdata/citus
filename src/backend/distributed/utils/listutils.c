/*-------------------------------------------------------------------------
 *
 * listutils.c
 *
 * This file contains functions to perform useful operations on lists.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"
#include "port.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"


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

	void *pointer = NULL;
	foreach_declared_ptr(pointer, pointerList)
	{
		array[arrayIndex] = pointer;

		arrayIndex++;
	}

	/* sort the array of pointers using the comparison function */
	SafeQsort(array, arraySize, sizeof(void *), comparisonFunction);

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
	int pointerIndex = 0;

	void *pointer = NULL;
	foreach_declared_ptr(pointer, pointerList)
	{
		pointerArray[pointerIndex] = pointer;
		pointerIndex += 1;
	}

	return pointerArray;
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
	int flags = HASH_ELEM | HASH_CONTEXT;

	/* allocate sufficient capacity for O(1) expected look-up time */
	int capacity = (int) (list_length(itemList) / 0.75) + 1;

	/* initialise the hash table for looking up keySize bytes */
	memset(&info, 0, sizeof(info));
	info.keysize = keySize;
	info.entrysize = keySize;
	info.hcxt = CurrentMemoryContext;

	if (isStringList)
	{
		flags |= HASH_STRINGS;
	}
	else
	{
		flags |= HASH_BLOBS;
	}

	HTAB *itemSet = hash_create("ListToHashSet", capacity, &info, flags);

	void *item = NULL;
	foreach_declared_ptr(item, itemList)
	{
		bool foundInSet = false;

		hash_search(itemSet, item, HASH_ENTER, &foundInSet);
	}

	return itemSet;
}


/*
 * GeneratePositiveIntSequenceList generates a positive int
 * sequence list up to the given number. The list will have:
 * [1:upto]
 */
List *
GeneratePositiveIntSequenceList(int upTo)
{
	List *intList = NIL;
	for (int i = 1; i <= upTo; i++)
	{
		intList = lappend_int(intList, i);
	}
	return intList;
}


/*
 * StringJoin gets a list of char * and then simply
 * returns a newly allocated char * joined with the
 * given delimiter. It uses ';' as the delimiter by
 * default.
 */
char *
StringJoin(List *stringList, char delimiter)
{
	return StringJoinParams(stringList, delimiter, NULL, NULL);
}


/*
 * StringJoin gets a list of char * and then simply
 * returns a newly allocated char * joined with the
 * given delimiter, prefix and postfix.
 */
char *
StringJoinParams(List *stringList, char delimiter, char *prefix, char *postfix)
{
	StringInfo joinedString = makeStringInfo();

	if (prefix != NULL)
	{
		appendStringInfoString(joinedString, prefix);
	}

	const char *command = NULL;
	int curIndex = 0;
	foreach_declared_ptr(command, stringList)
	{
		if (curIndex > 0)
		{
			appendStringInfoChar(joinedString, delimiter);
		}
		appendStringInfoString(joinedString, command);
		curIndex++;
	}

	if (postfix != NULL)
	{
		appendStringInfoString(joinedString, postfix);
	}

	return joinedString->data;
}


/*
 * ListTake returns the first size elements of given list. If size is greater
 * than list's length, it returns all elements of list. This is modeled after
 * the "take" function used in some Scheme implementations.
 */
List *
ListTake(List *pointerList, int size)
{
	List *result = NIL;
	int listIndex = 0;

	void *pointer = NULL;
	foreach_declared_ptr(pointer, pointerList)
	{
		result = lappend(result, pointer);
		listIndex++;
		if (listIndex >= size)
		{
			break;
		}
	}

	return result;
}


/*
 * safe_list_nth first checks if given index is valid and errors out if it is
 * not. Otherwise, it directly calls list_nth.
 */
void *
safe_list_nth(const List *list, int index)
{
	int listLength = list_length(list);
	if (index < 0 || index >= listLength)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("invalid list access: list length was %d but "
							   "element at index %d was requested ",
							   listLength, index)));
	}

	return list_nth(list, index);
}


/*
 * GenerateListFromElement returns a new list with length of listLength
 * such that all the elements are identical with input listElement pointer.
 */
List *
GenerateListFromElement(void *listElement, int listLength)
{
	List *list = NIL;
	for (int i = 0; i < listLength; i++)
	{
		list = lappend(list, listElement);
	}

	return list;
}


/*
 * list_filter_oid filters a list of oid-s based on a keepElement
 * function
 */
List *
list_filter_oid(List *list, bool (*keepElement)(Oid element))
{
	List *result = NIL;
	Oid element = InvalidOid;
	foreach_declared_oid(element, list)
	{
		if (keepElement(element))
		{
			result = lappend_oid(result, element);
		}
	}

	return result;
}
