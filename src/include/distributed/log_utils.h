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

/* do not log */
#define CITUS_LOG_LEVEL_OFF 0


extern bool EnableUnsupportedFeatureMessages;

extern bool IsLoggableLevel(int logLevel);

#undef ereport

#define ereport(elevel, rest) \
	do { \
		int ereport_loglevel = elevel; \
		(void) (ereport_loglevel); \
		ereport_domain(elevel, TEXTDOMAIN, rest); \
	} while (0)

#endif /* LOG_UTILS_H */
