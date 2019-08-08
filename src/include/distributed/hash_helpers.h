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

/* pg12 includes this exact implementation of hash_combine */
#if PG_VERSION_NUM < 120000

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


#endif


extern void hash_delete_all(HTAB *htab);

#endif
