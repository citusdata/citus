/*-------------------------------------------------------------------------
 *
 * listutils.h
 *
 * Declarations for public utility functions related to lists.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_LISTUTILS_H
#define CITUS_LISTUTILS_H

#include "postgres.h"
#include "c.h"

#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/hsearch.h"


/* utility functions declaration shared within this module */
extern List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));
extern void ** PointerArrayFromList(List *pointerList);
extern ArrayType * DatumArrayToArrayType(Datum *datumArray, int datumCount,
										 Oid datumTypeId);
extern HTAB * ListToHashSet(List *pointerList, Size keySize, bool isStringList);
extern char * StringJoin(List *stringList, char delimiter);

#endif /* CITUS_LISTUTILS_H */
