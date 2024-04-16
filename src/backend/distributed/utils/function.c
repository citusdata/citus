/*-------------------------------------------------------------------------
 *
 * function.c
 *
 * Utility functions for dealing with functions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "commands/defrem.h"
#include "utils/lsyscache.h"

#include "distributed/utils/function.h"


/*
 * GetFunctionInfo first resolves the operator for the given data type, access
 * method, and support procedure. The function then uses the resolved operator's
 * identifier to fill in a function manager object, and returns this object.
 */
FmgrInfo *
GetFunctionInfo(Oid typeId, Oid accessMethodId, int16 procedureId)
{
	FmgrInfo *functionInfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo));

	/* get default operator class from pg_opclass for datum type */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamilyId = get_opclass_family(operatorClassId);
	Oid operatorClassInputType = get_opclass_input_type(operatorClassId);

	Oid operatorId = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
									   operatorClassInputType, procedureId);

	if (operatorId == InvalidOid)
	{
		ereport(ERROR, (errmsg("could not find function for data typeId %u", typeId)));
	}

	/* fill in the FmgrInfo struct using the operatorId */
	fmgr_info(operatorId, functionInfo);

	return functionInfo;
}
