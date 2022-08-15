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

#include "common/hashfn.h"
#include "distributed/citus_safe_lib.h"
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
 * CreateSimpleHashWithName creates a hashmap that hashes its key using
 * tag_hash function and stores the entries in the current memory context.
 */
HTAB
*
CreateSimpleHashWithName(Size keySize, Size entrySize, char *name)
{
	HASHCTL info;
	memset_struct_0(info);
	info.keysize = keySize;
	info.entrysize = entrySize;
	info.hcxt = CurrentMemoryContext;

	/*
	 * uint32_hash does the same as tag_hash for keys of 4 bytes, but it's
	 * faster.
	 */
	if (keySize == sizeof(uint32))
	{
		info.hash = uint32_hash;
	}
	else
	{
		info.hash = tag_hash;
	}

	int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	/*
	 * We use 32 as the initial number of elements that fit into this hash
	 * table. This value seems a reasonable tradeof between two issues:
	 * 1. An empty hashmap shouldn't take up a lot of space
	 * 2. Doing a few inserts shouldn't require growing the hashmap
	 *
	 * NOTE: No performance testing has been performed when choosing this
	 * value. If this ever turns out to be a problem, feel free to do some
	 * performance tests.
	 */
	HTAB *publicationInfoHash = hash_create(name, 32, &info, hashFlags);
	return publicationInfoHash;
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
