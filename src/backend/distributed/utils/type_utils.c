/*-------------------------------------------------------------------------
 *
 * type_utils.c
 *
 * Utility functions related to types.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/type_utils.h"

#include "catalog/pg_type.h"
#include "utils/syscache.h"


/*
 * TypeOid looks for a type that has the given name and schema, and returns the
 * corresponding type's oid.
 */
Oid
TypeOid(Oid schemaId, const char *typeName)
{
	Oid typeOid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
								  PointerGetDatum(typeName),
								  ObjectIdGetDatum(schemaId));

	if (!OidIsValid(typeOid))
	{
		ereport(ERROR, (errmsg("Type (%s) not found", typeName)));
	}

	return typeOid;
}
