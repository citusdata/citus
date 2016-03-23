/*-------------------------------------------------------------------------
 *
 * multi_resowner.h
 *	  Citus resource owner integration.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#ifndef MULTI_RESOWNER_H
#define MULTI_RESOWNER_H

#include "utils/resowner.h"

/* resowner functions for temporary job directory management */
extern void ResourceOwnerEnlargeJobDirectories(ResourceOwner owner);
extern void ResourceOwnerRememberJobDirectory(ResourceOwner owner,
											  uint64 jobId);
extern void ResourceOwnerForgetJobDirectory(ResourceOwner owner,
											uint64 jobId);

#endif /* MULTI_RESOWNER_H */
