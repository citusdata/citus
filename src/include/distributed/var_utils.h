/*-------------------------------------------------------------------------
 *
 * var_utils.h
 *	  Utilities regarding vars
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef VAR_UTILS
#define VAR_UTILS
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"

List * PullAllRangeTablesEntries(Query *query, List *rtableList);
#endif
