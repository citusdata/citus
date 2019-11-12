/*-------------------------------------------------------------------------
 *
 * intermediate_results.h
 *   Functions for writing and reading intermediate results.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef INTERMEDIATE_RESULTS_H
#define INTERMEDIATE_RESULTS_H


#include "fmgr.h"

#include "distributed/commands/multi_copy.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/palloc.h"


extern DestReceiver * CreateRemoteFileDestReceiver(char *resultId, EState *executorState,
												   List *initialNodeList, bool
												   writeLocalFile);
extern void ReceiveQueryResultViaCopy(const char *resultId);
extern void RemoveIntermediateResultsDirectory(void);
extern int64 IntermediateResultSize(char *resultId);


#endif /* INTERMEDIATE_RESULTS_H */
