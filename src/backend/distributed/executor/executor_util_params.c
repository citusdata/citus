/*-------------------------------------------------------------------------
 *
 * executor_util_tasks.c
 *
 * Utility functions for dealing with task lists in the executor.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "utils/lsyscache.h"

#include "distributed/executor_util.h"


/*
 * ExtractParametersForRemoteExecution extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 * It changes oid of custom types to InvalidOid so that they are the same in workers
 * and coordinators.
 */
void
ExtractParametersForRemoteExecution(ParamListInfo paramListInfo, Oid **parameterTypes,
									const char ***parameterValues)
{
	ExtractParametersFromParamList(paramListInfo, parameterTypes,
								   parameterValues, false);
}


/*
 * ExtractParametersFromParamList extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 * If useOriginalCustomTypeOids is true, it uses the original oids for custom types.
 */
void
ExtractParametersFromParamList(ParamListInfo paramListInfo,
							   Oid **parameterTypes,
							   const char ***parameterValues, bool
							   useOriginalCustomTypeOids)
{
	int parameterCount = paramListInfo->numParams;

	elog(DEBUG1, "Extracting %d parameters from ParamListInfo", parameterCount);

	*parameterTypes = (Oid *) palloc0(parameterCount * sizeof(Oid));
	*parameterValues = (const char **) palloc0(parameterCount * sizeof(char *));

	/* get parameter types and values */
	for (int parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
	{
		ParamExternData *parameterData = &paramListInfo->params[parameterIndex];
		Oid typeOutputFunctionId = InvalidOid;
		bool variableLengthType = false;

		/* Log parameter type */
		elog(DEBUG1, "Processing parameter %d, type: %d", parameterIndex + 1, parameterData->ptype);

		/*
		 * Use 0 for data types where the oid values can be different on
		 * the coordinator and worker nodes.
		 */
		if (parameterData->ptype >= FirstNormalObjectId && !useOriginalCustomTypeOids)
		{
			(*parameterTypes)[parameterIndex] = 0;
			elog(DEBUG1, "Using default OID (0) for parameter %d", parameterIndex + 1);
		}
		else
		{
			(*parameterTypes)[parameterIndex] = parameterData->ptype;
		}

		/* Handle unreferenced parameter */
		if (parameterData->ptype == 0)
		{
			(*parameterValues)[parameterIndex] = NULL;
			(*parameterTypes)[parameterIndex] = TEXTOID;

			elog(DEBUG1, "Parameter %d has ptype 0, setting TEXTOID", parameterIndex + 1);
			continue;
		}

		/* Handle NULL parameter */
		if (parameterData->isnull)
		{
			(*parameterValues)[parameterIndex] = NULL;
			elog(DEBUG1, "Parameter %d is NULL", parameterIndex + 1);
			continue;
		}

		/* Log the type output function */
		getTypeOutputInfo(parameterData->ptype, &typeOutputFunctionId, &variableLengthType);
		elog(DEBUG1, "Type output function ID for parameter %d: %u", parameterIndex + 1, typeOutputFunctionId);

		/* Log the parameter value */
		(*parameterValues)[parameterIndex] = OidOutputFunctionCall(typeOutputFunctionId, parameterData->value);
		elog(DEBUG1, "Parameter %d value after output function call: %s", parameterIndex + 1, (*parameterValues)[parameterIndex]);
	}
}

