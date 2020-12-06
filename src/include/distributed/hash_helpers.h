/*-------------------------------------------------------------------------
 * hash_helpers.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef HASH_HELPERS_H
#define HASH_HELPERS_H

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "utils/hsearch.h"

/* pg12 includes this exact implementation of hash_combine */
#if PG_VERSION_NUM < PG_VERSION_12

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

/*
 * foreach_htab -
 *	  a convenience macro which loops through a HTAB
 */

#define foreach_htab(var, status, htab) \
	hash_seq_init((status), (htab)); \
	for ((var) = hash_seq_search(status); \
		 (var) != NULL; \
		 (var) = hash_seq_search(status))

extern void foreach_htab_cleanup(void *var, HASH_SEQ_STATUS *status);

#endif
