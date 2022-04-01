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


PG_MODULE_MAGIC;

void _PG_init(void);

void
_PG_init(void)
{
	columnar_init();
}
