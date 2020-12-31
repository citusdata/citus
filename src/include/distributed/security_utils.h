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

extern void PushCitusSecurityContext(void);
extern void PopCitusSecurityContext(void);

#endif
