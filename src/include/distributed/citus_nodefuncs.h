/*-------------------------------------------------------------------------
 *
 * citus_nodefuncs.h
 *	  Node (de-)serialization support for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_NODEFUNCS_H
#define CITUS_NODEFUNCS_H

#include "distributed/multi_physical_planner.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

/* citus_nodefuncs.c */
extern void SetRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
								 char *fragmentSchemaName, char *fragmentTableName,
								 List *tableIdList);
extern void ModifyRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
									char *fragmentSchemaName, char *fragmentTableName,
									List *tableIdList);
extern void ExtractRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind *rteKind,
									 char **fragmentSchemaName, char **fragmentTableName,
									 List **tableIdList);
extern CitusRTEKind GetRangeTblKind(RangeTblEntry *rte);

/* citus_outfuncs.c */
extern char * CitusNodeToString(const void *obj);

/* citus_read.c */
extern void * CitusStringToNode(char *str);
extern char * citus_pg_strtok(int *length);
extern void * CitusNodeRead(char *token, int tok_len);

/* citus_readfuncs.c */
extern Node * CitusParseNodeString(void);


#endif /* CITUS_NODEFUNCS_H */
