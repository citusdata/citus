/*
 * errormessage.c
 *	  Error handling related support functionality.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"

#include "common/sha2.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "distributed/citus_nodes.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"


/*
 * DeferredErrorInternal is a helper function for DeferredError().
 */
DeferredErrorMessage *
DeferredErrorInternal(int code, const char *message, const char *detail, const char *hint,
					  const char *filename, int linenumber, const char *functionname)
{
	DeferredErrorMessage *error = CitusMakeNode(DeferredErrorMessage);

	Assert(message != NULL);

	error->code = code;
	error->message = message;
	error->detail = detail;
	error->hint = hint;
	error->filename = filename;
	error->linenumber = linenumber;
	error->functionname = functionname;
	return error;
}


/*
 * RaiseDeferredErrorInternal is a helper function for RaiseDeferredError().
 */
void
RaiseDeferredErrorInternal(DeferredErrorMessage *error, int elevel)
{
	ErrorData *errorData = palloc0(sizeof(ErrorData));

	errorData->sqlerrcode = error->code;
	errorData->elevel = elevel;
	errorData->message = pstrdup(error->message);
	if (error->detail)
	{
		errorData->detail = pstrdup(error->detail);
	}
	if (error->hint)
	{
		errorData->hint = pstrdup(error->hint);
	}
	errorData->filename = pstrdup(error->filename);
	errorData->lineno = error->linenumber;
	errorData->funcname = error->functionname;

	errorData->assoc_context = ErrorContext;

	ThrowErrorData(errorData);
}
