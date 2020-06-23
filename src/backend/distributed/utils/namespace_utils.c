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

#include "catalog/namespace.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/namespace_utils.h"
#include "utils/regproc.h"

/*
 * PushOverrideEmptySearchPath pushes search_path to be NIL and sets addCatalog to
 * true so that all objects outside of pg_catalog will be schema-prefixed.
 * Afterwards, PopOverrideSearchPath can be used to revert the search_path back.
 */
void
PushOverrideEmptySearchPath(MemoryContext memoryContext)
{
	OverrideSearchPath *overridePath = GetOverrideSearchPath(memoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;

	PushOverrideSearchPath(overridePath);
}
