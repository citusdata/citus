/*-------------------------------------------------------------------------
 * log_utils.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOG_UTILS_H
#define LOG_UTILS_H


#include "utils/guc.h"
#include "distributed/backtrace.h"

/* do not log */
#define CITUS_LOG_LEVEL_OFF 0


extern bool IsLoggableLevel(int logLevel);
extern char * HashLogMessage(const char *text);

#define ApplyLogRedaction(text) \
	(log_min_messages <= ereport_loglevel ? HashLogMessage(text) : text)
#undef ereport_domain
#ifdef HAVE__BUILTIN_CONSTANT_P
#define ereport_domain(elevel, domain, ...)	\
	do { \
		pg_prevent_errno_in_scope(); \
		if (errstart(elevel, __FILE__, __LINE__, PG_FUNCNAME_MACRO, domain)) { \
			__VA_ARGS__, Backtrace(elevel);errfinish(0); \
		}\
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			pg_unreachable(); \
	} while(0)
#else							/* !HAVE__BUILTIN_CONSTANT_P */
#define ereport_domain(elevel, domain, ...)	\
	do { \
		const int elevel_ = (elevel); \
		pg_prevent_errno_in_scope(); \
		if (errstart(elevel_, __FILE__, __LINE__, PG_FUNCNAME_MACRO, domain)) {\
			__VA_ARGS__, Backtrace(elevel);errfinish(0); \
		}\
		if (elevel_ >= ERROR) \
			pg_unreachable(); \
	} while(0)
#endif							/* HAVE__BUILTIN_CONSTANT_P */



#undef ereport
#define ereport(elevel, rest) \
	do { \
		int ereport_loglevel = elevel; \
		(void) (ereport_loglevel); \
		ereport_domain(elevel, TEXTDOMAIN, rest); \
	} while (0)

#endif /* LOG_UTILS_H */
