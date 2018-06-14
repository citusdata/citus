/*-------------------------------------------------------------------------
 *
 * test/src/relation_acess_tracking.c
 *
 * Some test UDF for tracking relation accesses within transaction blocks.
 *
 * Copyright (c) 2014-2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "distributed/relation_access_tracking.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(relation_select_access_mode);
PG_FUNCTION_INFO_V1(relation_dml_access_mode);
PG_FUNCTION_INFO_V1(relation_ddl_access_mode);

/*
 * relation_select_access_mode returns the SELECT access
 * type (e.g., single shard - multi shard) for the given relation.
 */
Datum
relation_select_access_mode(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	PG_RETURN_INT64(GetRelationSelectAccessMode(relationId));
}


/*
 * relation_dml_access_mode returns the DML access type (e.g.,
 * single shard - multi shard) for the given relation.
 */
Datum
relation_dml_access_mode(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	PG_RETURN_INT64(GetRelationDMLAccessMode(relationId));
}


/*
 * relation_ddl_access_mode returns the DDL access type (e.g.,
 * single shard - multi shard) for the given relation.
 */
Datum
relation_ddl_access_mode(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	PG_RETURN_INT64(GetRelationDDLAccessMode(relationId));
}
