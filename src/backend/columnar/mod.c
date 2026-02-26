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
#include "columnar/columnar_tableam.h"


#if PG_VERSION_NUM >= PG_VERSION_18
PG_MODULE_MAGIC_EXT(.name = "citus_columnar", .version = "15.0devel");
#else
PG_MODULE_MAGIC;
#endif

void _PG_init(void);

void
_PG_init(void)
{
	columnar_init();
}
