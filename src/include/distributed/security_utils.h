/*-------------------------------------------------------------------------
 *
 * security_utils.h
 *   security related utility functions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SECURITY_UTILS_H_
#define SECURITY_UTILS_H_

#include "postgres.h"
#include "miscadmin.h"

#define PushCitusSecurityContext() \
    Oid savedUserId_DONTUSE = InvalidOid; \
	int savedSecurityContext_DONTUSE = 0; \
    GetUserIdAndSecContext(&savedUserId_DONTUSE, &savedSecurityContext_DONTUSE); \
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

#define PopCitusSecurityContext() \
    SetUserIdAndSecContext(savedUserId_DONTUSE, savedSecurityContext_DONTUSE);

#endif
