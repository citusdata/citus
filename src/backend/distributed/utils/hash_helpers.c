/*-------------------------------------------------------------------------
 *
 * hash_helpers.c
 *   Helpers for dynahash.c style hash tables.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/hash_helpers.h"
#include "utils/hsearch.h"


/*
 * Empty a hash, without destroying the hash table itself.
 */
void
hash_delete_all(HTAB *htab)
{
	HASH_SEQ_STATUS status;
	void *entry = NULL;

	hash_seq_init(&status, htab);
	while ((entry = hash_seq_search(&status)) != 0)
	{
		bool found = false;

		hash_search(htab, entry, HASH_REMOVE, &found);
		Assert(found);
	}
}
