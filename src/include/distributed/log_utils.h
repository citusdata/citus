/*-------------------------------------------------------------------------
 * log_utils.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOG_UTILS_H
#define LOG_UTILS_H

extern bool IsLoggableLevel(int logLevel);
extern char * ApplyLogRedaction(const char *text);

#endif /* LOG_UTILS_H */
