/*
 * memutils.h
 *   utility functions to help with postgres' memory management primitives
 */

#ifndef CITUS_MEMUTILS_H
#define CITUS_MEMUTILS_H

#include "utils/palloc.h"


/*
 * EnsureReleaseResource is an abstraction on MemoryContextRegisterResetCallback that
 * allocates the space for the MemoryContextCallback and registers it to the current
 * MemoryContext, ensuring the call of callback with arg as its argument during either the
 * Reset of Delete of a MemoryContext.
 */
static inline void
EnsureReleaseResource(MemoryContextCallbackFunction callback, void *arg)
{
	MemoryContextCallback *cb = MemoryContextAllocZero(CurrentMemoryContext,
													   sizeof(MemoryContextCallback));
	cb->func = callback;
	cb->arg = arg;
	MemoryContextRegisterResetCallback(CurrentMemoryContext, cb);
}


#endif /*CITUS_MEMUTILS_H */
