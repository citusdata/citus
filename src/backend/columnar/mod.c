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

#include "columnar/columnar.h"
#include "columnar/mod.h"

#ifdef HAS_TABLEAM
#include "columnar/columnar_tableam.h"
#endif

void
columnar_init(void)
{
	columnar_init_gucs();
#ifdef HAS_TABLEAM
	columnar_tableam_init();
#endif
}


void
columnar_fini(void)
{
#if HAS_TABLEAM
	columnar_tableam_finish();
#endif
}
