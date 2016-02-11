/*-------------------------------------------------------------------------
 *
 * listutils.h
 *
 * Declarations for public utility functions related to lists.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUSDB_LISTUTILS_H
#define CITUSDB_LISTUTILS_H

#include "postgres.h"
#include "c.h"

#include "nodes/pg_list.h"


/* utility functions declaration shared within this module */
extern List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));


#endif /* CITUSDB_LISTUTILS_H */
