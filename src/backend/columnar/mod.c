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

#include "citus_version.h"

#include "columnar/cstore.h"
#include "columnar/cstore_fdw.h"
#include "columnar/mod.h"

#ifdef HAS_TABLEAM
#include "columnar/cstore_tableam.h"
#endif


void
columnar_init(void)
{
	cstore_init();
	cstore_fdw_init();

#ifdef HAS_TABLEAM
	cstore_tableam_init();
#endif
}


void
columnar_fini(void)
{
	cstore_fdw_finish();

#if HAS_TABLEAM
	cstore_tableam_finish();
#endif
}
