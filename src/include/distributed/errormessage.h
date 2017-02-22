/*-------------------------------------------------------------------------
 *
 * errormessage.h
 *	  Error handling related support functionality.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef ERRORMESSAGE_H
#define ERRORMESSAGE_H


#include "distributed/citus_nodes.h"


typedef struct DeferredErrorMessage
{
	CitusNode tag;

	int code;
	const char *message;
	const char *detail;
	const char *hint;
	const char *filename;
	int linenumber;
	const char *functionname;
} DeferredErrorMessage;


/*
 * DeferredError allocates a deferred error message, that can later be emitted
 * using RaiseDeferredError().  These error messages can be
 * serialized/copied/deserialized, i.e. can be embedded in plans and such.
 */
#define DeferredError(code, message, detail, hint) \
	DeferredErrorInternal(code, message, detail, hint, __FILE__, __LINE__, __func__)

DeferredErrorMessage * DeferredErrorInternal(int code, const char *message, const
											 char *detail, const char *hint,
											 const char *filename, int linenumber, const
											 char *functionname);

/*
 * RaiseDeferredError emits a previously allocated error using the specified
 * severity.
 *
 * The trickery with __builtin_constant_p/pg_unreachable aims to have the
 * compiler understand that the function will not return if elevel >= ERROR.
 */
#define RaiseDeferredError(error, elevel) \
	do { \
		RaiseDeferredErrorInternal(error, elevel); \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) { \
			pg_unreachable(); } \
	} \
	while (0)

void RaiseDeferredErrorInternal(DeferredErrorMessage *error, int elevel);

#endif
