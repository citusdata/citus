/*-------------------------------------------------------------------------
 *
 * function_utils.h
 *	  Utilities regarding calls to PG functions
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_FUNCTION_UTILS_H
#define CITUS_FUNCTION_UTILS_H

#include "postgres.h"

#include "nodes/execnodes.h"


/* Function declaration for getting oid for the given function name */
extern Oid FunctionOid(const char *schemaName, const char *functionName,
					   int argumentCount);
extern Oid FunctionOidExtended(const char *schemaName, const char *functionName, int
							   argumentCount, bool missingOK);
extern ReturnSetInfo * FunctionCallGetTupleStore1(PGFunction function, Oid functionId,
												  Datum argument);

#endif /* CITUS_FUNCTION_UTILS_H */
