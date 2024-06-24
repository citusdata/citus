/*-------------------------------------------------------------------------
 *
 * namespace_utils.c
 *
 * Utility functions related to namespace changes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "utils/regproc.h"

#include "distributed/namespace_utils.h"

/*
 * We use the equivalent of a function SET option to allow the setting to
 * persist for the exact duration of the transaction, guc.c takes care of
 * undoing the setting on error.
 *
 * We set search_path to "pg_catalog" instead of "" to expose useful utilities.
 */
int
PushEmptySearchPath()
{
	int saveNestLevel = NewGUCNestLevel();
	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);
	return saveNestLevel;
}


/*
 * Restore the GUC variable search_path we set in PushEmptySearchPath
 */
void
PopEmptySearchPath(int saveNestLevel)
{
	AtEOXact_GUC(true, saveNestLevel);
}
