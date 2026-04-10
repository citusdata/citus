/*-------------------------------------------------------------------------
 * procedure_body_analysis.h
 *
 * Declarations for PLpgSQL procedure body analysis used to determine
 * whether a stored procedure is eligible for the single-statement
 * transaction optimization (skip coordinated 2PC).
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROCEDURE_BODY_ANALYSIS_H
#define PROCEDURE_BODY_ANALYSIS_H

#include "postgres.h"

extern bool ProcedureBodyIsSingleStatement;

extern void InstallProcedureBodyAnalysisPlugin(void);

#endif /* PROCEDURE_BODY_ANALYSIS_H */
