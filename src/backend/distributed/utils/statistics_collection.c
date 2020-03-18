/*-------------------------------------------------------------------------
 *
 * statistics_collection.c
 *	  Anonymous reports and statistics collection.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "citus_version.h"
#include "fmgr.h"
#include "utils/uuid.h"


PG_FUNCTION_INFO_V1(citus_server_id);


/*
 * citus_server_id returns a random UUID value as server identifier. This is
 * modeled after PostgreSQL's pg_random_uuid().
 */
Datum
citus_server_id(PG_FUNCTION_ARGS)
{
	uint8 *buf = (uint8 *) palloc(UUID_LEN);

	/*
	 * If pg_strong_random() fails, fall-back to using random(). In previous
	 * versions of postgres we don't have pg_strong_random(), so use it by
	 * default in that case.
	 */
	if (!pg_strong_random((char *) buf, UUID_LEN))
	{
		for (int bufIdx = 0; bufIdx < UUID_LEN; bufIdx++)
		{
			buf[bufIdx] = (uint8) (random() & 0xFF);
		}
	}

	/*
	 * Set magic numbers for a "version 4" (pseudorandom) UUID, see
	 * http://tools.ietf.org/html/rfc4122#section-4.4
	 */
	buf[6] = (buf[6] & 0x0f) | 0x40;    /* "version" field */
	buf[8] = (buf[8] & 0x3f) | 0x80;    /* "variant" field */

	PG_RETURN_UUID_P((pg_uuid_t *) buf);
}
