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

#include "columnar/columnar_tableam.h"
#include "columnar/columnar_wal.h"

void
columnar_init(void)
{
	columnar_wal_init();
	columnar_init_gucs();
	columnar_tableam_init();
}
