/*-------------------------------------------------------------------------
 *
 * master_expire_table_cache.c
 *	  This UDF is removed.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"

PG_FUNCTION_INFO_V1(master_expire_table_cache);

Datum
master_expire_table_cache(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("master_expire_table_cache UDF is dropped")));
	PG_RETURN_VOID();
}
