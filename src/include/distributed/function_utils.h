/*-------------------------------------------------------------------------
 *
 * function_utils.h
 *	  Utilities regarding calls to PG functions
 *
 * Copyright (c) 2012-2018, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#include "nodes/execnodes.h"


/* Function declaration for getting oid for the given function name */
extern Oid FunctionOid(const char *schemaName, const char *functionName,
					   int argumentCount);
extern ReturnSetInfo * FunctionCallGetTupleStore1(PGFunction function, Oid functionId,
												  Datum argument);
