/*-------------------------------------------------------------------------
 *
 * format_collate.c
 *    Display collate names "nicely".
 *
 *    This file is modeled after postgres' utils/adt/format_*.c files
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/deparser.h"


/*
 * This version returns a name that is always qualified.
 */
char *
FormatCollateBEQualified(Oid collate_oid)
{
	return FormatCollateExtended(collate_oid, FORMAT_COLLATE_FORCE_QUALIFY);
}


/*
 * FormatCollateExtended - inspired by format_type_extended
 *		Generate a possibly-qualified collate name.
 *
 * The default behavior is to only qualify if the type is not in the search
 * path, and to raise an error if a non-existent collate_oid is given.
 *
 * The following bits in 'flags' modify the behavior:
 * - FORMAT_COLLATE_FORCE_QUALIFY
 *			always schema-qualify collate names, regardless of search_path
 *
 * Returns a palloc'd string.
 */
char *
FormatCollateExtended(Oid collid, bits16 flags)
{
	char *nspname = NULL;

	if (collid == InvalidOid && (flags & FORMAT_COLLATE_ALLOW_INVALID) != 0)
	{
		return pstrdup("-");
	}

	HeapTuple tuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
	if (!HeapTupleIsValid(tuple))
	{
		if ((flags & FORMAT_COLLATE_ALLOW_INVALID) != 0)
		{
			return pstrdup("???");
		}
		else
		{
			elog(ERROR, "cache lookup failed for collate %u", collid);
		}
	}
	Form_pg_collation collform = (Form_pg_collation) GETSTRUCT(tuple);

	if ((flags & FORMAT_COLLATE_FORCE_QUALIFY) == 0 && CollationIsVisible(collid))
	{
		nspname = NULL;
	}
	else
	{
		nspname = get_namespace_name_or_temp(collform->collnamespace);
	}

	char *typname = NameStr(collform->collname);

	char *buf = quote_qualified_identifier(nspname, typname);

	ReleaseSysCache(tuple);

	return buf;
}
