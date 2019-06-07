/*-------------------------------------------------------------------------
 *
 * citus_acquire_lock.h
 *	  Background worker to help with acquiering locks by canceling competing backends.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CITUS_ACQUIRE_LOCK_H
#define CITUS_CITUS_ACQUIRE_LOCK_H


#include "postmaster/bgworker.h"

BackgroundWorkerHandle * StartLockAcquireHelperBackgroundWorker(int backendToHelp,
																int32 lock_cooldown);

#endif /*CITUS_CITUS_ACQUIRE_LOCK_H */
