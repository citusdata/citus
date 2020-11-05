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

#include "columnar/cstore.h"
#include "columnar/mod.h"

#ifdef USE_TABLEAM
#include "columnar/cstore_tableam.h"
#endif

#ifdef USE_FDW
#include "columnar/cstore_fdw.h"
#endif

PG_MODULE_MAGIC;

void
_PG_init(void)
{
	cstore_init();

#ifdef USE_TABLEAM
	cstore_tableam_init();
#endif

#ifdef USE_FDW
	cstore_fdw_init();
#endif
}


void
_PG_fini(void)
{
#if USE_TABLEAM
	cstore_tableam_finish();
#endif

#ifdef USE_FDW
	cstore_fdw_finish();
#endif
}
