/*-------------------------------------------------------------------------
 *
 * hash_helpers.c
 *   Helpers for dynahash.c style hash tables.
 *
 * Copyright (c) Citus Data, Inc.
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


/*
 * foreach_htab_cleanup cleans up the hash iteration state after the iteration
 * is done. This is only needed when break statements are present in the
 * foreach block.
 */
void
foreach_htab_cleanup(void *var, HASH_SEQ_STATUS *status)
{
	if ((var) != NULL)
	{
		hash_seq_term(status);
	}
}
