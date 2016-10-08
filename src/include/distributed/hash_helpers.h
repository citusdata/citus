/*-------------------------------------------------------------------------
 * hash_helpers.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef HASH_HELPERS_H
#define HASH_HELPERS_H

#include "utils/hsearch.h"

/*
 * Combine two hash values, resulting in another hash value, with decent bit
 * mixing.
 *
 * Similar to boost's hash_combine().
 */
static inline uint32
hash_combine(uint32 a, uint32 b)
{
	a ^= b + 0x9e3779b9 + (a << 6) + (a >> 2);
	return a;
}


extern void hash_delete_all(HTAB *htab);

#endif
