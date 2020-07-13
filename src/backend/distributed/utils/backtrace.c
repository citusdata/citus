/*
 * backtrace.c
 *
 * Utilities related to backtrace
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"
#include "backtrace.h"
#include "lib/stringinfo.h"

#include "distributed/backtrace.h"

#define BACKTRACE_HEADER "\nBACKTRACE:\n"
#define BACKTRACE_SKIP 2

static int BacktraceFullCallback(void *data, uintptr_t pc,
								 const char *filename, int lineno,
								 const char *function);

static void BacktraceErrorCallback(void *data, const char *msg, int errnum);
static void InitBackTrace(void);
static bool ShouldLogBacktrace(int elevel);
static char * GenerateBackTrace(void);

static struct backtrace_state *backTracestate;

static void
InitBackTrace(void)
{
	const char *filename = NULL;
	void *data = NULL;
	backTracestate = backtrace_create_state(filename, 0,
											BacktraceErrorCallback, data);
}


static bool
ShouldLogBacktrace(int elevel)
{
	return elevel >= ERROR;
}


void
Backtrace(int elevel)
{
	if (!ShouldLogBacktrace(elevel))
	{
		return;
	}
	errdetail("%s", GenerateBackTrace());
}

void AssertBacktrace(void) {
	const char * backtrace = GenerateBackTrace();
    ereport(ERROR, (errmsg("%s", backtrace)));
}


static char *
GenerateBackTrace(void)
{
	if (backTracestate == NULL)
	{
		InitBackTrace();
	}
	StringInfo msgWithBacktrace = makeStringInfo();

	appendStringInfoString(msgWithBacktrace, BACKTRACE_HEADER);
	backtrace_full(backTracestate, BACKTRACE_SKIP, BacktraceFullCallback,
				   BacktraceErrorCallback, msgWithBacktrace);

	return msgWithBacktrace->data;
}


static int
BacktraceFullCallback(void *data, uintptr_t pc, const char *filename, int lineno,
					  const char *function)
{
	StringInfo str = (StringInfo) data;
	if (function && filename)
	{
		appendStringInfo(str, "%s:%s:%d\n", filename, function, lineno);
	}

	/* returning 0 means we will continue the backtrace */
	return 0;
}


static void
BacktraceErrorCallback(void *data, const char *msg, int errnum)
{
	/* currently NO-OP */
}
