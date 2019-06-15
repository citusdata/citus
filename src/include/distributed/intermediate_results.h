/*-------------------------------------------------------------------------
 *
 * intermediate_results.h
 *   Functions for writing and reading intermediate results.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef INTERMEDIATE_RESULTS_H
#define INTERMEDIATE_RESULTS_H


#include "fmgr.h"

#include "distributed/commands/multi_copy.h"
#include "distributed/sharding.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/palloc.h"


extern char * CreateIntermediateResultsDirectory(void);
extern char * IntermediateResultsDirectory(void);
extern char * QueryResultFileName(const char *resultId);
extern DestReceiver * CreateRemoteFileDestReceiver(char *resultId, EState *executorState,
												   List *initialNodeList, bool
												   writeLocalFile);
extern void SendQueryResultViaCopy(const char *resultId);
extern void ReceiveQueryResultViaCopy(const char *resultId);
extern void RemoveIntermediateResultsDirectory(void);
extern int64 IntermediateResultSize(char *resultId);
extern Query * BuildReadIntermediateResultsQuery(List *targetEntryList,
												 List *columnAliasList,
												 Const *resultIdConst);
extern Query * BuildReadIntermediateResultsArrayQuery(List *targetEntryList,
													  List *columnAliasList,
													  List *resultNames);

#endif /* INTERMEDIATE_RESULTS_H */
