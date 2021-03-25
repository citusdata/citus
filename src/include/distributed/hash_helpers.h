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
