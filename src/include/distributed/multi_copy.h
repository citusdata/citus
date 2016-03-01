/*-------------------------------------------------------------------------
 *
 * multi_copy.h
 *    Declarations for public functions and variables used in COPY for
 *    distributed tables.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_COPY_H
#define MULTI_COPY_H


#include "nodes/parsenodes.h"


/* config variable managed via guc.c */
extern int CopyTransactionManager;


/* function declarations for copying into a distributed table */
extern void CitusCopyFrom(CopyStmt *copyStatement, char *completionTag);


#endif /* MULTI_COPY_H */
