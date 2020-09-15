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
#include "cstore_tableam.h"
#include "cstore_fdw.h"

PG_MODULE_MAGIC;

void
_PG_init(void)
{
	cstore_tableam_init();
	cstore_fdw_init();
}


void
_PG_fini(void)
{
	cstore_tableam_finish();
	cstore_fdw_finish();
}
