/*-------------------------------------------------------------------------
 *
 * citus_acquire_lock.h
 *	  Background worker to help with acquiering locks by canceling competing backends.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_ACQUIRE_LOCK_H
#define CITUS_ACQUIRE_LOCK_H


#include "postmaster/bgworker.h"

BackgroundWorkerHandle * StartLockAcquireHelperBackgroundWorker(int backendToHelp,
																int32 lock_cooldown);

#endif /* CITUS_ACQUIRE_LOCK_H */
