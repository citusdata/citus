/*-------------------------------------------------------------------------
 *
 * pg_dump.h
 *	  definition of pg_dump functions
 *
 * Copyright (c), Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DUMP_H
#define PG_DUMP_H

#include "nodes/pg_list.h"

char * GetPgDumpPath(void);
char * RunPgDump(char *sourceConnectionString, char *snapshotName, List *schemaList,
				 List *excludeTableList, bool dropIfExists);

#endif
