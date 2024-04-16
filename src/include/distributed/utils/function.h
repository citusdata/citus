/*-------------------------------------------------------------------------
 *
 * function.h
 *	  Utility functions for dealing with functions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_FUNCTION_H
#define CITUS_FUNCTION_H

#include "postgres.h"

#include "fmgr.h"


extern FmgrInfo * GetFunctionInfo(Oid typeId, Oid accessMethodId, int16 procedureId);


#endif   /* CITUS_FUNCTION_H */
