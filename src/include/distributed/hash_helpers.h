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

#include "utils/hsearch.h"

#include "pg_version_constants.h"

/*
 * assert_valid_hash_key2 checks if a type that contains 2 fields contains no
 * padding bytes. This is necessary to use a type as a hash key with tag_hash.
 */
#define assert_valid_hash_key2(type, field1, field2) \
	StaticAssertDecl( \
		sizeof(type) == sizeof(((type) { 0 }).field1) \
		+ sizeof(((type) { 0 }).field2), \
		# type " has padding bytes, but is used as a hash key in a simple hash");

/*
 * assert_valid_hash_key3 checks if a type that contains 3 fields contains no
 * padding bytes. This is necessary to use a type as a hash key with tag_hash.
 */
#define assert_valid_hash_key3(type, field1, field2, field3) \
	StaticAssertDecl( \
		sizeof(type) == sizeof(((type) { 0 }).field1) \
		+ sizeof(((type) { 0 }).field2) \
		+ sizeof(((type) { 0 }).field3), \
		# type " has padding bytes, but is used as a hash key in a simple hash");

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

extern HTAB * CreateSimpleHashWithNameAndSizeInternal(Size keysize, Size entrysize,
													  char *name, long nelem);

/*
 * CreatesSimpleHash creates a hash table that hash its key using the tag_hash
 * and stores then entries in the CurrentMemoryContext.
 *
 * We use 32 as the initial number of elements that fit into this hash
 * table. This value seems a reasonable tradeof between two issues:
 * 1. An empty hashmap shouldn't take up a lot of space
 * 2. Doing a few inserts shouldn't require growing the hashmap
 *
 * NOTE: No performance testing has been performed when choosing this
 * value. If this ever turns out to be a problem, feel free to do some
 * performance tests.
 *
 * IMPORTANT: Because this uses tag_hash it's required that the keyType
 * contains no automatic padding bytes, because that will result in tag_hash
 * returning undefined values. You can check this using assert_valid_hash_keyX.
 */
#define CreateSimpleHash(keyType, entryType) \
	CreateSimpleHashWithNameAndSize(keyType, entryType, \
									# entryType "Hash", 32)

/*
 * Same as CreateSimpleHash but allows specifying the name
 */
#define CreateSimpleHashWithName(keyType, entryType, name) \
	CreateSimpleHashWithNameAndSize(keyType, entryType, \
									name, 32)

/*
 * CreateSimpleHashWithSize is the same as CreateSimpleHash, but allows
 * configuring of the amount of elements that initially fit in the hash table.
 */
#define CreateSimpleHashWithSize(keyType, entryType, size) \
	CreateSimpleHashWithNameAndSize(keyType, entryType, \
									# entryType "Hash", size)

#define CreateSimpleHashWithNameAndSize(keyType, entryType, name, size) \
	CreateSimpleHashWithNameAndSizeInternal(sizeof(keyType), \
											sizeof(entryType), \
											name, size)


/*
 * CreatesSimpleHashSet creates a hash set that hashes its values using the
 * tag_hash and stores the values in the CurrentMemoryContext.
 */
#define CreateSimpleHashSet(keyType) \
	CreateSimpleHashWithName(keyType, keyType, \
							 # keyType "HashSet")

/*
 * CreatesSimpleHashSetWithSize creates a hash set that hashes its values using
 * the tag_hash and stores the values in the CurrentMemoryContext. It allows
 * specifying its number of elements.
 */
#define CreateSimpleHashSetWithSize(keyType, size) \
	CreateSimpleHashWithNameAndSize(keyType, keyType, # keyType "HashSet", size)

/*
 * CreatesSimpleHashSetWithName creates a hash set that hashes its values using the
 * tag_hash and stores the values in the CurrentMemoryContext. It allows
 * specifying its name.
 */
#define CreateSimpleHashSetWithName(keyType, name) \
	CreateSimpleHashWithName(keyType, keyType, name)

/*
 * CreatesSimpleHashSetWithName creates a hash set that hashes its values using the
 * tag_hash and stores the values in the CurrentMemoryContext. It allows
 * specifying its name and number of elements.
 */
#define CreateSimpleHashSetWithNameAndSize(keyType, name, size) \
	CreateSimpleHashWithNameAndSize(keyType, keyType, name, size)


#endif
