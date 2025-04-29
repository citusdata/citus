/*-------------------------------------------------------------------------
 *
 * query_utils.h
 *
 * Declarations for query-walker utility functions and related types.
 *
 * Copyright (c), Citus Data, Inc.
 *
 **---------------------------------------------------------------------------
 */
#ifndef QUERY_UTILS_H
#define QUERY_UTILS_H

#include "postgres.h"

#include "nodes/pg_list.h"
#include "nodes/primnodes.h"

/* Enum to define execution flow of ExtractRangeTableList */
typedef enum ExtractRangeTableMode
{
	EXTRACT_RELATION_ENTRIES, /* inclduding local, foreign and partitioned tables */
	EXTRACT_ALL_ENTRIES
} ExtractRangeTableMode;

/* Struct to pass rtable list and execution flow flag to query_walker_tree */
typedef struct ExtractRangeTableWalkerContext
{
	List **rangeTableList;
	ExtractRangeTableMode walkerMode;
} ExtractRangeTableWalkerContext;

/* Struct to pass rtable list and the result list to walker */
typedef struct ExtractRangeTableIdsContext
{
	List **idList;
	List *rtable;
} ExtractRangeTableIdsContext;

/* Function declarations for query-walker utility functions */
extern bool ExtractRangeTableList(Node *node, ExtractRangeTableWalkerContext *context);

/* Below two functions wrap ExtractRangeTableList function to determine the execution flow */
extern bool ExtractRangeTableRelationWalker(Node *node, List **rangeTableList);
extern bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);

extern bool ExtractRangeTableIndexWalker(Node *node, List **rangeTableIndexList);
extern bool ExtractRangeTableIds(Node *node, ExtractRangeTableIdsContext *context);
extern bool CheckIfAllCitusRTEsAreColocated(Node *node, List *rtable, RangeTblEntry **rte);
#endif /* QUERY_UTILS_H */
