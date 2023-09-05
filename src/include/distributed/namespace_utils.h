/*-------------------------------------------------------------------------
 *
 * namespace_utils.h
 *	  Utility function declarations related to namespace changes.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef NAMESPACE_UTILS_H
#define NAMESPACE_UTILS_H

extern int PushEmptySearchPath(void);
extern void PopEmptySearchPath(int saveNestLevel);

#endif /* NAMESPACE_UTILS_H */
