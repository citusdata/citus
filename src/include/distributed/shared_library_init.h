/*-------------------------------------------------------------------------
 *
 * shared_library_init.h
 *	  Functionality related to the initialization of the Citus extension.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARED_LIBRARY_INIT_H
#define SHARED_LIBRARY_INIT_H

#include "columnar/columnar.h"

#define GUC_STANDARD 0
#define MAX_SHARD_COUNT 64000
#define MAX_SHARD_REPLICATION_FACTOR 100

extern char *CitusMainDatabase;

extern ColumnarSupportsIndexAM_type extern_ColumnarSupportsIndexAM;
extern CompressionTypeStr_type extern_CompressionTypeStr;
extern IsColumnarTableAmTable_type extern_IsColumnarTableAmTable;
extern ReadColumnarOptions_type extern_ReadColumnarOptions;

extern void StartupCitusBackend(void);
extern const char * GetClientMinMessageLevelNameForValue(int minMessageLevel);

#endif /* SHARED_LIBRARY_INIT_H */
