/*-------------------------------------------------------------------------
 *
 * priority.h
 *	  Shared declarations for managing CPU priority.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_PRIORITY_H
#define CITUS_PRIORITY_H

#include "c.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/fd.h"

extern int CpuPriority;
extern int CpuPriorityLogicalRepSender;
extern int MaxHighPriorityBackgroundProcesess;

#define CPU_PRIORITY_INHERIT 1234

/* Function declarations for transmitting files between two nodes */
extern void SetOwnPriority(int priority);
extern int GetOwnPriority(void);


#endif   /* CITUS_PRIORITY_H */
