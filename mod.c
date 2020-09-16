/*-------------------------------------------------------------------------
 *
 * mod.c
 *
 * This file contains module-level definitions.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "mod.h"
#if PG_VERSION_NUM >= 120000
#include "cstore_tableam.h"
#endif
#include "cstore_fdw.h"

PG_MODULE_MAGIC;

void
_PG_init(void)
{
#if PG_VERSION_NUM >= 120000
	cstore_tableam_init();
#endif
	cstore_fdw_init();
}


void
_PG_fini(void)
{
#if PG_VERSION_NUM >= 120000
	cstore_tableam_finish();
#endif
	cstore_fdw_finish();
}
