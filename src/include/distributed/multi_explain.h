/*-------------------------------------------------------------------------
 *
 * multi_explain.h
 *	  Explain support for Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_EXPLAIN_H
#define MULTI_EXPLAIN_H

#include "executor/executor.h"

/* Config variables managed via guc.c to explain distributed query plans */
extern bool ExplainDistributedQueries;
extern bool ExplainAllTasks;

/* internal state, not a GUC */
extern bool ExplainStatementRunning;

#endif /* MULTI_EXPLAIN_H */
